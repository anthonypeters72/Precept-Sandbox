# api.py
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from typing import Any, Dict, List, Optional, Set, Tuple
import re

from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pathlib import Path

from engine import (
    build_engine,
    lookup_text_any,
    ensure_min_precepts,
    normalize_ref,
    fallback_metrics,
    meaningful_tokens,
    PARALLEL_MATCH_RATIO,
    PARALLEL_ANCHOR_HITS,
    PARALLEL_MIN_SCORE,
    score_similarity,
    weighted_overlap_score,
)
# If you have normalize_ref in engine, import it too:
# from engine import normalize_ref

DEBUG = False  # temporary while you verify IDF build
app = FastAPI(title="Precept Sandbox API")

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"

app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
eng = build_engine(use_strongs=True)  # builds once on server start

print("[BOOT] api_text_search_enabled")
print("[BOOT] USING api.py TEXT SEARCH PATCH")
# ----------------------------
# Existing POST model (keep)
# ----------------------------
class QueryIn(BaseModel):
    ref: str
    min_needed: int = 3
    min_books: int = 2
    strict_no_pad: bool = False
    near_miss_k: int = 5


@app.get("/corpora")
def corpora():
    return [{"name": n, "verses": len(d) if isinstance(d, dict) else 0} for n, d in eng.corpora.items()]


ALLOWED_CORPORA = {
    "kjv",
    "apoc",
    "quran",
    "jasher",
    "jubilees",
    "patriarchs",
    "hermas"
}

CORPUS_LABELS = {
    "kjv": "Bible",
    "apoc": "Apocrypha",
    "quran": "Quran",
    "jasher": "Jasher",
    "jubilees": "Jubilees",
    "patriarchs": "Patriarchs",
    "hermas": "Hermas",
}


COMMON_PHRASES = {
    "god won't give you more than you can handle": {
        "ref": "1 Corinthians 10:13",
        "note": "Common phrase; closest related verse."
    },
    "money is the root of all evil": {
        "ref": "1 Timothy 6:10",
        "note": "Common wording; scripture says the love of money is the root of all kinds of evil."
    },
    "clean hands pure heart": {
        "ref": "Psalms 24:4",
        "note": "Closest related verse."
    },
}


# adding dropdown selects, data pulls from "@" route endpoints
def _list_books_for_corpus(eng, corpus_name: str) -> List[str]:
    d = eng.corpora.get(corpus_name, {})
    books = []
    seen = set()

    if isinstance(d, dict):
        for ref in d.keys():
            if isinstance(ref, tuple) and len(ref) >= 1:
                book = str(ref[0])
                if book not in seen:
                    seen.add(book)
                    books.append(book)

    return books

def _list_chapters_for_book(eng, corpus_name: str, book: str) -> List[int]:
    d = eng.corpora.get(corpus_name, {})
    chapters = set()

    if isinstance(d, dict):
        for ref in d.keys():
            if isinstance(ref, tuple) and len(ref) >= 2:
                b, c, _ = ref
                if str(b).lower() == book.lower():
                    chapters.add(int(c))

    return sorted(chapters)




# @app.get("/meta/corpora")
# def meta_corpora():
#     available = {}
#     for ref, verse_text, corpus_name in getattr(eng, "all_verses", []) or []:
#         if not corpus_name:
#             continue
#         if not verse_text:
#             continue
#         available[corpus_name] = available.get(corpus_name, 0) + 1

#     items = []
#     for name, verse_count in available.items():
#         items.append({
#             "key": name,
#             "label": name.replace("_", " ").title(),
#             "verses": verse_count,
#         })

#     items.sort(key=lambda x: x["label"])
#     return {"corpora": items}


# @app.get("/meta/corpora")
# def meta_corpora():
#     available = {}

#     for ref, verse_text, corpus_name in getattr(eng, "all_verses", []) or []:
#         if not corpus_name:
#             continue
#         if not verse_text or not str(verse_text).strip():
#             continue

#         available[corpus_name] = available.get(corpus_name, 0) + 1

#     items = [
#         {
#             "key": name,
#             "label": name.replace("_", " ").title(),
#             "verses": count,
#         }
#         for name, count in available.items()
#         if count > 0
#     ]

#     # keep Bible first if you want, then preserve natural label sort for the rest
#     items.sort(key=lambda x: (x["key"] != "bible", x["label"]))

#     return {"corpora": items}



@app.get("/meta/corpora")
def meta_corpora():
    available = {}

    for ref, verse_text, corpus_name in getattr(eng, "all_verses", []) or []:
        if not corpus_name:
            continue
        if not verse_text or not str(verse_text).strip():
            continue
        if corpus_name not in ALLOWED_CORPORA:
            continue

        available[corpus_name] = available.get(corpus_name, 0) + 1

    items = [
        {
            "key": name,
            "label": CORPUS_LABELS.get(name, name.replace("_", " ").title()),
            "verses": count,
        }
        for name, count in available.items()
        if count > 0
    ]

    items.sort(key=lambda x: (x["key"] != "kjv", x["label"]))
    return {"corpora": items}



@app.get("/meta/books")
def meta_books(corpus: str):
    corpus = (corpus or "").strip().lower()
    if not corpus:
        return {"corpus": corpus, "books": []}

    books = _list_books_for_corpus(eng, corpus)
    return {"corpus": corpus, "books": books}


@app.get("/meta/chapters")
def meta_chapters(corpus: str, book: str):
    corpus = (corpus or "").strip().lower()
    book = (book or "").strip()

    if not corpus or not book:
        return {"corpus": corpus, "book": book, "chapters": []}

    chapters = _list_chapters_for_book(eng, corpus, book)
    return {"corpus": corpus, "book": book, "chapters": chapters}



# ----------------------------
# Helpers: parse & expand q=
# ----------------------------

_FLAG_STRONG = re.compile(r"(?:\s+|^)--strong(?:=(true|false))?\s*$", re.IGNORECASE)
_FLAG_CORPUS = re.compile(r"(?:\s+|^)--corpus=([a-zA-Z0-9_]+)\s*$", re.IGNORECASE)




_CHAPTER_RANGE = re.compile(r"^\s*(?P<book>.+?)\s+(?P<c1>\d+)\s*-\s*(?P<c2>\d+)\s*$")
_VERSE_REF = re.compile(r"^\s*(?P<book>.+?)\s+(?P<c>\d+)\s*:\s*(?P<v>\d+)\s*$")


_SAME_CHAPTER_VERSE_RANGE = re.compile(
    r"^\s*(?P<book>.+?)\s+(?P<c>\d+)\s*:\s*(?P<v1>\d+)\s*-\s*(?P<v2>\d+)\s*$")

_CROSS_CHAPTER_VERSE_RANGE = re.compile(
    r"^\s*(?P<book>.+?)\s+(?P<c1>\d+)\s*:\s*(?P<v1>\d+)\s*-\s*(?P<c2>\d+)\s*:\s*(?P<v2>\d+)\s*$")

_SINGLE_CHAPTER = re.compile(r"^\s*(?P<book>.+?)\s+(?P<c>\d+)\s*$")

_TUPLE_REF_RE = re.compile(r"^\s*(.+?)\s+(\d+)\s*:\s*(\d+)\s*$")



def _strip_flags(q: str) -> Tuple[str, Dict[str, Any]]:
    """
    Pulls supported flags from the end of the query string.
    Returns (clean_query, flags_dict)
    """
    flags: Dict[str, Any] = {"strong": False, "corpus": None}

    m = _FLAG_CORPUS.search(q)
    if m:
        flags["corpus"] = m.group(1).strip().lower()
        q = q[: m.start()].strip()

    m = _FLAG_STRONG.search(q)
    if m:
        val = m.group(1)
        flags["strong"] = True if val is None else (val.lower() == "true")
        q = q[: m.start()].strip()

    return q.strip(), flags



def _detect_query_kind(q: str) -> Dict[str, Any]:
    """
    Returns:
      {"kind":"chapter_range", "book":..., "c1":int, "c2":int}
      {"kind":"verse_range", "book":..., "c1":int, "v1":int, "c2":int, "v2":int}
      {"kind":"single_verse", "ref":...}
      {"kind":"text_search", "text":...}
    """
    m = _CHAPTER_RANGE.match(q)
    if m:
        return {
            "kind": "chapter_range",
            "book": m.group("book").strip(),
            "c1": int(m.group("c1")),
            "c2": int(m.group("c2")),
        }

    m = _CROSS_CHAPTER_VERSE_RANGE.match(q)
    if m:
        return {
            "kind": "verse_range",
            "book": m.group("book").strip(),
            "c1": int(m.group("c1")),
            "v1": int(m.group("v1")),
            "c2": int(m.group("c2")),
            "v2": int(m.group("v2")),
        }

    m = _SAME_CHAPTER_VERSE_RANGE.match(q)
    if m:
        c = int(m.group("c"))
        return {
            "kind": "verse_range",
            "book": m.group("book").strip(),
            "c1": c,
            "v1": int(m.group("v1")),
            "c2": c,
            "v2": int(m.group("v2")),
        }

    m = _VERSE_REF.match(q)
    if m:
        book = m.group("book").strip()
        c = int(m.group("c"))
        v = int(m.group("v"))
        return {"kind": "single_verse", "ref": f"{book} {c}:{v}"
        }

    m = _SINGLE_CHAPTER.match(q)
    if m:
        return {
            "kind": "single_chapter",
            "book": m.group("book").strip(),
            "c": int(m.group("c")),
        }

    return {"kind": "text_search", "text": q}
    
    
def _chapter_max_for_book(eng, corpus_name: str, book: str) -> int:
    """
    Fast path if you precompute: eng.chapter_max[(corpus_name, book)].
    Fallback: scan keys in that corpus.
    """
    if hasattr(eng, "chapter_max") and (corpus_name, book) in eng.chapter_max:
        return int(eng.chapter_max[(corpus_name, book)])

    d = eng.corpora.get(corpus_name, {})
    max_c = 0
    for (b, c, v) in d.keys():
        if b == book and c > max_c:
            max_c = c
    return max_c


def _verse_max_for_chapter(eng, corpus_name: str, book: str, chapter: int) -> int:
    """
    Fast path if you precompute: eng.verse_max[(corpus_name, book, chapter)].
    Fallback: scan keys in that corpus.
    """
    if hasattr(eng, "verse_max") and (corpus_name, book, chapter) in eng.verse_max:
        return int(eng.verse_max[(corpus_name, book, chapter)])

    d = eng.corpora.get(corpus_name, {})
    max_v = 0
    for (b, c, v) in d.keys():
        if b == book and c == chapter and v > max_v:
            max_v = v
    return max_v


def _pick_corpus_for_book(eng, book: str, preferred_corpus: Optional[str] = None) -> Optional[str]:
    """
    Prefer a requested corpus if it contains the book.
    Otherwise fall back to auto-detection.
    """
    if preferred_corpus:
        d = eng.corpora.get(preferred_corpus)
        if isinstance(d, dict):
            for (b, c, v) in d.keys():
                if b == book:
                    return preferred_corpus

    if hasattr(eng, "book_to_corpus") and book in eng.book_to_corpus:
        mapped = eng.book_to_corpus[book]
        if not preferred_corpus or mapped == preferred_corpus:
            return mapped

    for corpus_name, d in eng.corpora.items():
        if not isinstance(d, dict):
            continue
        if preferred_corpus and corpus_name != preferred_corpus:
            continue
        for (b, c, v) in d.keys():
            if b == book:
                return corpus_name

    return None


def _run_text_search(
    query: str,
    *,
    top_k: int,
    min_needed: int,
    min_books: int,
    strict_no_pad: bool,
    near_miss_k: int,
    strong: bool,
    preferred_corpus: Optional[str] = None,
):
    qtxt = (query or "").strip()
    if not qtxt:
        return {"count": 0, "matches": []}

    # Use Engine global IDF (Sprint goal #1)
    idf = getattr(eng, "idf", None) or {}

    # Quick reject: if query has no meaningful tokens, stop
    q_mean = meaningful_tokens(qtxt)
    if not q_mean:
        return {"count": 0, "matches": []}

    single_term = len(q_mean) == 1
    q_term = q_mean[0].lower() if single_term else None


    scored: List[Tuple[float, str, str, str, int, int, List[Tuple[str, float]]]] = []

    for ref, verse_text, corpus_name in getattr(eng, "all_verses", []) or []:
        if not corpus_name:
            continue
        if corpus_name not in ALLOWED_CORPORA:
            continue
        if preferred_corpus and corpus_name != preferred_corpus:
            continue
        if not verse_text or not str(verse_text).strip():
            continue


        s, top_terms, mcount, qcount = weighted_overlap_score(qtxt, verse_text, idf)


        verse_low = verse_text.lower()

        # gentle fallback for one-word searches
        if single_term and q_term:
            if q_term in verse_low:
                s, top_terms, mcount, qcount = weighted_overlap_score(qtxt, verse_text, idf)
                if s <= 0:
                    s = 1.0
                scored.append((float(s), ref, corpus_name, verse_text, max(1, int(mcount)), int(qcount or 1), top_terms))
                continue


        # Tighten quality so “random junk” doesn’t appear:
        # - Prefer 2+ meaningful hits
        # - Allow 1-hit only if it's a strong/rare hit (high IDF contribution)
        # Hard gate: longer queries must match at least 2 meaningful tokens
        if qcount >= 3 and mcount < 2:
            continue

        # For very short queries (1–2 tokens), we can allow a strong single hit
        top_hit = top_terms[0][1] if top_terms else 0.0
        if qcount <= 2:
            if mcount >= 1 and top_hit >= 3.0:
                pass
            elif mcount >= 2:
                pass
            else:
                continue
        scored.append((float(s), ref, corpus_name, verse_text, int(mcount), int(qcount), top_terms))

    scored.sort(key=lambda x: x[0], reverse=True)
    scored = scored[: max(1, int(top_k))]

    matches = []
    for s, ref, corpus_name, verse_text, mcount, qcount, top_terms in scored:
        matches.append({
            "ref": ref,
            "corpus": corpus_name,
            "text": verse_text,
            "score": s,
            "match": f"{mcount}/{qcount}",
            "top_terms": [(t, float(w)) for (t, w) in (top_terms or [])[:5]],
            "result": _run_single_ref(
                ref,
                min_needed=min_needed,
                min_books=min_books,
                strict_no_pad=True,
                #strict_no_pad=strict_no_pad,
                near_miss_k=near_miss_k,
                strong=strong,
            ),
        })

    top_matches = matches[:5]

    next_refs = []
    if matches:
        nm = matches[0].get("result", {}).get("near_misses", []) or []
        next_refs = [{"ref": ref} for ref, _why in nm[:5]]

    return {
        "query_text": qtxt,
        "count": len(matches),
        "matches": top_matches,
        "next_refs": next_refs,
    }


def _run_single_ref(
    ref: str,
    *,
    min_needed: int,
    min_books: int,
    strict_no_pad: bool,
    near_miss_k: int,
    strong: bool,
    preferred_corpus: Optional[str] = None,
):
    # Normalize inside engine if you have it; otherwise keep as-is and rely on lookup_text_any/engine xrefs.
    # ref_norm = normalize_ref(ref)
    
    ref = (ref or "").strip()
    ref_norm = normalize_ref(ref)

    print("RAW:", ref, "NORM:", ref_norm, "PREFERRED_CORPUS:", preferred_corpus)

    text = None
    corpus = None

    # Try preferred corpus first, raw then normalized
        # Try preferred corpus first, using lookup_text_any against only that corpus
        # Try preferred corpus first, raw then normalized
        # Try preferred corpus first, using lookup_text_any against only that corpus
    if preferred_corpus:
        d = eng.corpora.get(preferred_corpus, {})
        if isinstance(d, dict):
            text, corpus = lookup_text_any({preferred_corpus: d}, ref)

            if not text:
                text, corpus = lookup_text_any({preferred_corpus: d}, ref_norm)
    # Fallback to global lookup
    if not text:
        text, corpus = lookup_text_any(eng.corpora, ref)

    if not text:
        text, corpus = lookup_text_any(eng.corpora, ref_norm)


    # Final fallback: direct tuple lookup inside the preferred corpus
    if not text and preferred_corpus:
        d = eng.corpora.get(preferred_corpus, {})

        # --- Debug
        # print("PREFERRED CORPUS:", preferred_corpus)
        # print("TRYING REF:", ref)
        # print("TRYING REF_NORM:", ref_norm)
        # print("SAMPLE KEYS:", list(d.keys())[:10] if isinstance(d, dict) else "not dict")

        m = _TUPLE_REF_RE.match(ref)
        #m = re.match(r"^\s*(.+?)\s+(\d+)\s*:\s*(\d+)\s*$", ref)

        if isinstance(d, dict) and m:
            book = m.group(1).strip()
            chapter = int(m.group(2))
            verse = int(m.group(3))

            # --- Debug 
            # print("TRYING TUPLE FALLBACK:", book, chapter, verse)
            # print("SAMPLE KEYS:", list(d.keys())[:10])

            # exact book casing first
            text = d.get((book, chapter, verse))
            if text:
                corpus = preferred_corpus
                ref_norm = f"{book} {chapter}:{verse}"

            # case-insensitive book fallback
            if not text:
                for (b, c, v), verse_text in d.items():
                    if (
                        isinstance(b, str)
                        and b.strip().lower() == book.lower()
                        and int(c) == chapter
                        and int(v) == verse
                    ):
                        text = verse_text
                        corpus = preferred_corpus
                        ref_norm = f"{b} {chapter}:{verse}"
                        break


    print("FOUND?", bool(text), "CORPUS:", corpus)

    mapped = eng.xrefs.get(ref_norm, [])
    # Prevent self-link from ever entering the pipeline
    mapped = [m for m in mapped if m and m != ref_norm]
    final_precepts, added, near_misses = ensure_min_precepts(
        query_ref=ref_norm,
        precepts=mapped,
        min_needed=min_needed,
        min_books=min_books,
        xrefs=eng.xrefs,
        corpora=eng.corpora,
        all_verses=eng.all_verses,
        strict_no_pad=strict_no_pad,
        near_miss_k=near_miss_k,
        idf_global=getattr(eng, "idf", None), 
    )
    
    final_precepts = [p for p in final_precepts if p and p != ref_norm] 
    
    # --- precepts: tag source tier + confidence ---
    hydrated_precepts = []

    # direct (raw) xrefs for this verse, if available
    xrefs_raw = getattr(eng, "xrefs_raw", None) or {}
    direct_set = set(xrefs_raw.get(ref_norm, []) or [])

    # full xrefs (after symmetry) for this verse
    full_set = set((getattr(eng, "xrefs", {}) or {}).get(ref_norm, []) or [])

    # preserve output order, but we’ll tier-rank using a stable sort later
    for pref in final_precepts:
        pref = normalize_ref(pref)  # safe even if already normalized

        # determine source
        if pref in direct_set:
            source = "xref_direct"
            confidence = 0.95
        elif pref in full_set:
            source = "xref_symmetric"
            confidence = 0.80
        else:
            source = "fallback"

            m = fallback_metrics(
                corpora=eng.corpora,
                query_ref=ref_norm,
                cand_ref=pref,
                idf_global=getattr(eng, "idf", None),  # ✅ ADD THIS
            )

            # classify strong fallback as "parallel"
            if (
                m["match_ratio"] >= PARALLEL_MATCH_RATIO
                and m["anchor_hits"] >= PARALLEL_ANCHOR_HITS
                and m["score"] >= PARALLEL_MIN_SCORE
            ):
                source = "parallel"

            anchor_ratio = (
                m["anchor_hits"] / max(1, len(m["anchors"]))
                if m["anchors"] else 0.0
            )
        
            conf = 0.35 + 0.45 * m["match_ratio"] + 0.20 * anchor_ratio
            cap = 0.72 if source == "fallback" else 0.90
            confidence = max(0.0, min(cap, conf))
        
        ptext, pcorpus = lookup_text_any(eng.corpora, pref)

        hydrated_precepts.append({
            "ref": pref,
            "corpus": pcorpus,
            "text": ptext,
            "source": source,
            "confidence": confidence,
            "metrics": m if source == "fallback" else None,
        })

    # tiered ranking: direct > symmetric > fallback (stable within tier)
    tier_rank = {"xref_direct": 0, "xref_symmetric": 1, "fallback": 2}
    hydrated_precepts.sort(key=lambda d: tier_rank.get(d.get("source", ""), 99))
            
    payload = {
        "ref": ref_norm,
        "corpus": corpus,
        "text": text,
        "precepts": hydrated_precepts,
        "added": added,
        "near_misses": near_misses,
    }
        
    # Strong support (wire to your engine shape)
    if strong:
        payload["strong"] = []

        # Strong's is only wired for KJV OT in your engine right now
        # (based on load_kjv_ot_strongs_from_tsv + strongs_hebrew.csv)
        if getattr(eng, "strongs_ot_idx", None) is None:
            payload["strong"] = []
        else:
            # Parse "Book C:V"
            import re
            m = re.match(r"^\s*(.+?)\s+(\d+)\s*:\s*(\d+)\s*$", ref_norm)
            if not m:
                payload["strong"] = []
            else:
                book = m.group(1).strip()
                c = int(m.group(2))
                v = int(m.group(3))

                codes = eng.strongs_ot_idx.get((book, c, v), [])

                # Optional: enrich with lexicon data if present
                lex = getattr(eng, "strongs_lex", None)
                if isinstance(lex, dict) and codes:
                    enriched = []
                    for code in codes:
                        entry = lex.get(code, {})
                        enriched.append({
                            "code": code,
                            **entry,   # whatever fields your lex loader provides
                        })
                    payload["strong"] = enriched
                else:
                    # Return codes only
                    payload["strong"] = [{"code": code} for code in codes]
                    
                    
    return payload




def _normalize_book_name(book: str) -> str:
    """
    Normalize a book name using the engine's ref normalizer.
    Example:
      'genesis' -> 'Genesis'
      'gen' -> 'Genesis'
      'jubilees' -> 'Jubilees'
    """
    probe = normalize_ref(f"{book} 1:1")
    m = re.match(r"^\s*(?P<book>.+?)\s+\d+\s*:\s*\d+\s*$", probe)
    return m.group("book").strip() if m else book.strip()


#def _run_text_search(text: str, *, top_k: int, min_needed: int, min_books: int, strict_no_pad: bool, near_miss_k: int, strong: bool):
#    """
#    Free-text search over eng.all_verses using the existing deterministic tokenizer/scorer.
#    Returns top_k best-matching verse refs, and (optionally) their precepts via _run_single_ref.

#    This is intentionally lightweight and uses the existing engine score_similarity().
#    """
#    qtxt = (text or "").strip()
#    if not qtxt:
#        return {"query_text": text, "matches": []}

#    scored: list[tuple[float, str, str]] = []
#    for ref, verse_text, corpus_name in getattr(eng, "all_verses", []) or []:
#        s = score_similarity(qtxt, verse_text)
#        if s > 0:
#            scored.append((float(s), ref, corpus_name))

#    scored.sort(key=lambda x: x[0], reverse=True)
#    scored = scored[: max(1, int(top_k))]

#    matches = []
#    for s, ref, corpus_name in scored:
        # reuse the existing single-ref pipeline so fallback + diversity + metrics stay consistent
#        payload = _run_single_ref(
#            ref,
#            min_needed=min_needed,
#            min_books=min_books,
#            strict_no_pad=strict_no_pad,
#            near_miss_k=near_miss_k,
#            strong=strong,
#        )
#        matches.append({
#            "ref": ref,
#            "corpus": corpus_name,
#            "score": s,
#            "result": payload,
#        })

#   return {"query_text": qtxt, "count": len(matches), "matches": matches}




# ----------------------------
# New: GET /query?q=...
# ----------------------------
@app.get("/query")
def query_get(
    q: str = Query(..., description="e.g. 'Gen 1:1', 'Genesis 1-3 --strong'"),
    corpus: Optional[str] = Query(None),
    min_needed: int = 3,
    min_books: int = 2,
    strict_no_pad: bool = False,
    near_miss_k: int = 5,
):
    q_clean, flags = _strip_flags(q)


    if corpus and not flags.get("corpus"):
        flags["corpus"] = corpus.strip().lower()



    # --- Added to protect from certain corpua not being searchable 
    # --- i.e. books of Adam & Eve, Enoch, etc.
    requested_corpus = flags.get("corpus")
    if requested_corpus and requested_corpus not in ALLOWED_CORPORA:
        return {"error": "This corpus is not available in the public version."}
    # --- When viable soulution is found can remove this block between these




    # Make API input as forgiving as CLI normalization
    q_clean = q_clean.replace(";", ":")
    q_clean = q_clean.replace(",", " ")
    q_clean = re.sub(r"\s+", " ", q_clean).strip()
    parsed = _detect_query_kind(q_clean)

    #print("PARSED:", parsed)

    if parsed["kind"] == "single_verse":
        result = _run_single_ref(
            parsed["ref"],
            min_needed=min_needed,
            min_books=min_books,
            strict_no_pad=strict_no_pad,
            near_miss_k=near_miss_k,
            strong=bool(flags["strong"]),
            preferred_corpus=flags.get("corpus"),
        )
        
        if result is None:
            return {"query": q, "error": "Internal: _run_single_ref returned None"}
            
        return {"query": q, "kind": "single_verse", "result": result}



    if parsed["kind"] == "single_chapter":
        book = parsed["book"].strip()
        if not flags.get("corpus"):
            book = _normalize_book_name(book)
        chapter = parsed["c"]

        corpus_name = _pick_corpus_for_book(eng, book, flags.get("corpus"))
        if not corpus_name:
            return {"query": q, "kind": "single_chapter", "error": f"Book not found in corpora: {book}"}

        max_c = _chapter_max_for_book(eng, corpus_name, book)
        chapter = max(1, min(chapter, max_c))

        vmax = _verse_max_for_chapter(eng, corpus_name, book, chapter)
        if vmax <= 0:
            return {
                "query": q,
                "kind": "single_chapter",
                "book": book,
                "corpus": corpus_name,
                "chapter": chapter,
                "count": 0,
                "results": [],
            }

        results = []
        for verse in range(1, vmax + 1):
            ref = f"{book} {chapter}:{verse}"
            results.append(
                _run_single_ref(
                    ref,
                    min_needed=min_needed,
                    min_books=min_books,
                    strict_no_pad=strict_no_pad,
                    near_miss_k=near_miss_k,
                    strong=bool(flags["strong"]),
                )
            )

        return {
            "query": q,
            "kind": "single_chapter",
            "book": book,
            "corpus": corpus_name,
            "chapter": chapter,
            "count": len(results),
            "results": results,
        }



    if parsed["kind"] == "verse_range":
        book = parsed["book"].strip()
        if not flags.get("corpus"):
            book = _normalize_book_name(book)
        c1, v1 = parsed["c1"], parsed["v1"]
        c2, v2 = parsed["c2"], parsed["v2"]

        corpus_name = _pick_corpus_for_book(eng, book, flags.get("corpus"))
        if not corpus_name:
            return {"query": q, "kind": "verse_range", "error": f"Book not found in corpora: {book}"}

        # normalize reversed ranges
        if (c2, v2) < (c1, v1):
            c1, v1, c2, v2 = c2, v2, c1, v1

        max_c = _chapter_max_for_book(eng, corpus_name, book)
        c1 = max(1, min(c1, max_c))
        c2 = max(1, min(c2, max_c))

        results = []
        for chapter in range(c1, c2 + 1):
            vmax = _verse_max_for_chapter(eng, corpus_name, book, chapter)
            if vmax <= 0:
                continue

            start_v = v1 if chapter == c1 else 1
            end_v = v2 if chapter == c2 else vmax

            start_v = max(1, min(start_v, vmax))
            end_v = max(1, min(end_v, vmax))

            if end_v < start_v:
                continue

            for verse in range(start_v, end_v + 1):
                ref = f"{book} {chapter}:{verse}"
                results.append(
                    _run_single_ref(
                        ref,
                        min_needed=min_needed,
                        min_books=min_books,
                        strict_no_pad=strict_no_pad,
                        near_miss_k=near_miss_k,
                        strong=bool(flags["strong"]),
                    )
                )

        return {
            "query": q,
            "kind": "verse_range",
            "book": book,
            "corpus": corpus_name,
            "range": {
                "start": {"chapter": c1, "verse": v1},
                "end": {"chapter": c2, "verse": v2},
            },
            "count": len(results),
            "results": results,
        }



    if parsed["kind"] == "chapter_range":
        book = parsed["book"].strip()
        if not flags.get("corpus"):
            book = _normalize_book_name(book)
        c1, c2 = parsed["c1"], parsed["c2"]
        if c2 < c1:
            c1, c2 = c2, c1

        # Choose corpus that contains that book (bible/apoc/etc.)
        corpus_name = _pick_corpus_for_book(eng, book, flags.get("corpus"))
        if not corpus_name:
            return {"query": q, "kind": "chapter_range", "error": f"Book not found in corpora: {book}"}

        # Clamp to actual max chapter available in that corpus for that book
        max_c = _chapter_max_for_book(eng, corpus_name, book)
        c1 = max(1, min(c1, max_c))
        c2 = max(1, min(c2, max_c))

        results = []
        for chapter in range(c1, c2 + 1):
            vmax = _verse_max_for_chapter(eng, corpus_name, book, chapter)
            for verse in range(1, vmax + 1):
                ref = f"{book} {chapter}:{verse}"
                results.append(
                    _run_single_ref(
                        ref,
                        min_needed=min_needed,
                        min_books=min_books,
                        strict_no_pad=strict_no_pad,
                        near_miss_k=near_miss_k,
                        strong=bool(flags["strong"]),
                    )
                )

        return {
            "query": q,
            "kind": "chapter_range",
            "book": book,
            "corpus": corpus_name,
            "chapters": {"start": c1, "end": c2},
            "count": len(results),
            "results": results,
        }

        #if parsed["kind"] == "text_search":
            # Free text search (e.g., "day of the Lord fire judgment")
            # Use near_miss_k as the default top_k for text matches.
            #top_k = max(1, int(near_miss_k))
            #out = _run_text_search(
                #parsed.get("text", q_clean),
                #top_k=top_k,
                #min_needed=min_needed,
                #min_books=min_books,
                #strict_no_pad=strict_no_pad,
                #near_miss_k=near_miss_k,
                #strong=bool(flags["strong"]),
            #)
            #return {"query": q, "kind": "text_search", **out}
            
        
    if parsed["kind"] == "text_search":
        phrase_query = (parsed.get("text", q_clean) or "").strip().lower()

        if phrase_query in COMMON_PHRASES:
            hit = COMMON_PHRASES[phrase_query]
            result = _run_single_ref(
                hit["ref"],
                min_needed=min_needed,
                min_books=min_books,
                strict_no_pad=strict_no_pad,
                near_miss_k=near_miss_k,
                strong=bool(flags["strong"]),
                preferred_corpus=flags.get("corpus"),
            )

            result["note"] = hit.get("note", "")

            return {
                "query": q,
                "kind": "phrase_match",
                "result": result,
            }

        top_k = max(1, int(near_miss_k or 10))
        out = _run_text_search(
            parsed.get("text", q_clean),
            top_k=top_k,
            min_needed=min_needed,
            min_books=min_books,
            strict_no_pad=strict_no_pad,
            near_miss_k=near_miss_k,
            strong=bool(flags["strong"]),
            preferred_corpus=flags.get("corpus"),
        )
        return {"query": q, "kind": "text_search", **out}
    return {"query": q, "kind": "unknown", "error": "Unrecognized query format. Try: 'Gen 1:1' or 'Genesis 1-3'."}


# ----------------------------
# Keep existing POST /query
# ----------------------------
@app.post("/query")
def query_post(q: QueryIn):
    # Preserve current behavior, but you can normalize ref here too.
    return _run_single_ref(
        q.ref,
        min_needed=q.min_needed,
        min_books=q.min_books,
        strict_no_pad=q.strict_no_pad,
        near_miss_k=q.near_miss_k,
        strong=False,  # POST can be extended later if desired
    )
    
    
    


@app.get("/", response_class=HTMLResponse)
def home():
    return FileResponse(STATIC_DIR / "index.html")
"""
<!doctype html>
<html>
<head>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Precept Sandbox</title>
  <style>
    body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial; margin:0; padding:16px; background:#0b0b0f; color:#eee;}
    .card{max-width:720px; margin:0 auto; background:#151522; border:1px solid #2a2a3a; border-radius:14px; padding:14px;}
    input{width:100%; font-size:16px; padding:12px; border-radius:12px; border:1px solid #333; background:#0f0f18; color:#eee;}
    button{margin-top:10px; width:100%; font-size:16px; padding:12px; border-radius:12px; border:0; background:#4b6bff; color:white; font-weight:600;}
    pre{white-space:pre-wrap; word-wrap:break-word; background:#0f0f18; padding:12px; border-radius:12px; border:1px solid #333;}
    .hint{opacity:.8; font-size:13px; margin-top:8px;}
  </style>
</head>
<body>
  <div class="card">
    <h2 style="margin:4px 0 10px 0;">Precept Sandbox</h2>
    <input id="q" placeholder="Try: Gen 1:1   or   Genesis 1-3 --strong" />
    <button onclick="run()">Search</button>
    <div class="hint">Tip: add <b>--strong</b> for Strong’s output.</div>
    <div id="out" style="margin-top:12px;"></div>
  </div>

<script>

function verseCard(v){
  return `
    <div style="margin-bottom:10px;padding:10px;border:1px solid #333;border-radius:10px;background:#0f0f18;">
      <div style="font-weight:600">${v.ref}</div>
      <div style="font-size:14px;opacity:.9">${v.text || ""}</div>
      ${v.corpus ? `<div style="font-size:12px;opacity:.6">source: ${v.corpus}</div>` : ""}
    </div>
  `;
}

function preceptCard(p){
  return `
    <div style="margin-bottom:8px;padding:8px;border:1px solid #333;border-radius:10px;background:#0f0f18;">
      <div style="font-weight:600">${p.ref}</div>
      <div style="font-size:14px">${p.text || ""}</div>
      <div style="font-size:12px;opacity:.6">
        ${p.corpus || ""} • ${p.source || ""} • confidence ${Math.round((p.confidence||0)*100)}%
      </div>
    </div>
  `;
}

async function run(){

  const q = document.getElementById('q').value.trim();
  const out = document.getElementById('out');

  if(!q){
    out.innerHTML = '<div class="hint">Enter a query.</div>';
    return;
  }

  out.innerHTML = '<div class="hint">Searching…</div>';

  try{

    const res = await fetch('/query?q=' + encodeURIComponent(q));
    const data = await res.json();

    if(data.error){
      out.innerHTML = `<div style="color:#ff8080">${data.error}</div>`;
      return;
    }

    /* -------------------
       SINGLE VERSE
    ------------------- */

    if(data.kind === "single_verse"){

      const r = data.result;

      let html = `
        <h3 style="margin-top:0">${r.ref}</h3>
        <div style="margin-bottom:10px">${r.text}</div>
        <div style="font-size:12px;opacity:.6;margin-bottom:12px">source: ${r.corpus}</div>
      `;

      if(r.precepts && r.precepts.length){
        html += `<h4>Precepts</h4>`;
        html += r.precepts.map(preceptCard).join("");
      }

      if(r.near_misses && r.near_misses.length){
        html += `<h4>Next Closest</h4>`;
        html += r.near_misses.map(p => `<div style="font-size:13px">${p}</div>`).join("");
      }

      if(r.strong && r.strong.length){
        html += `<h4>Strong's</h4>`;
        html += r.strong.map(s => `<div>${s.code}</div>`).join("");
      }

      out.innerHTML = html;
      return;
    }

    /* -------------------
       CHAPTER RANGE
    ------------------- */

    if(data.kind === "chapter_range"){

      let html = `
        <h3>${data.book} ${data.chapters.start}-${data.chapters.end}</h3>
        <div class="hint">corpus: ${data.corpus} • verses: ${data.count}</div>
        <div style="margin-top:12px">
      `;

      html += data.results.map(v => verseCard(v)).join("");

      html += `</div>`;

      out.innerHTML = html;
      return;
    }

    /* -------------------
       TEXT SEARCH
    ------------------- */

    if(data.kind === "text_search"){

      let html = `
        <h3>Search Results</h3>
        <div class="hint">${data.count} matches</div>
      `;

      html += data.matches.map(m => `
        <div style="margin-top:12px;padding:10px;border:1px solid #333;border-radius:10px;background:#0f0f18;">
          <div style="font-weight:600">${m.ref}</div>
          <div style="font-size:14px">${m.text}</div>
          <div style="font-size:12px;opacity:.6">score ${m.score.toFixed(2)}</div>
        </div>
      `).join("");

      out.innerHTML = html;
      return;
    }

    out.innerHTML = '<div class="hint">No renderer for result type.</div>';

  }catch(e){

    out.innerHTML = `<div style="color:#ff8080">${e.message}</div>`;

  }
}

</script>
</body>
</html>
"""