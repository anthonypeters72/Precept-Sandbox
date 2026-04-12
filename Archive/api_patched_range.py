# api.py
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from typing import Any
import re

from engine import (
    build_engine,
    lookup_text_any,
    ensure_min_precepts,
    normalize_ref,
    fallback_metrics,
    PARALLEL_MATCH_RATIO,
    PARALLEL_ANCHOR_HITS,
    PARALLEL_MIN_SCORE,
)
# If you have normalize_ref in engine, import it too:
# from engine import normalize_ref

DEBUG = True  # temporary while you verify IDF build
app = FastAPI(title="Precept Sandbox API")
eng = build_engine(use_strongs=True)  # builds once on server start


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


# ----------------------------
# Helpers: parse & expand q=
# ----------------------------

_FLAG_STRONG = re.compile(r"(?:\s+|^)--strong(?:=(true|false))?\s*$", re.IGNORECASE)

def _strip_flags(q: str) -> tuple[str, dict[str, Any]]:
    """
    Pulls supported flags from the end of the query string.
    Returns (clean_query, flags_dict)
    """
    flags: dict[str, Any] = {"strong": False}

    m = _FLAG_STRONG.search(q)
    if m:
        val = m.group(1)
        flags["strong"] = True if val is None else (val.lower() == "true")
        q = q[: m.start()].strip()

    return q.strip(), flags


_CHAPTER_RANGE = re.compile(r"^\s*(?P<book>.+?)\s+(?P<c1>\d+)\s*-\s*(?P<c2>\d+)\s*$")
_VERSE_REF = re.compile(r"^\s*(?P<book>.+?)\s+(?P<c>\d+)\s*:\s*(?P<v>\d+)\s*$")
_VERSE_RANGE = re.compile(r"^\s*(?P<book>.+?)\s+(?P<c>\d+)\s*:\s*(?P<v1>\d+)\s*-\s*(?P<v2>\d+)\s*$")

def _detect_query_kind(q: str) -> dict[str, Any]:
    """
    Returns:
      {"kind":"chapter_range", "book":..., "c1":int, "c2":int}
      {"kind":"chapter_range", "book":..., "c1":int, "c2":int}
      {"kind":"verse_range", "book":..., "c":int, "v1":int, "v2":int}
      {"kind":"single_verse", "ref":...}
      {"kind":"unknown", "raw":...}
    """
    m = _CHAPTER_RANGE.match(q)
    if m:
        return {
            "kind": "chapter_range",
            "book": m.group("book").strip(),
            "c1": int(m.group("c1")),
            "c2": int(m.group("c2")),
        }

    m = _VERSE_RANGE.match(q)
    if m:
        book = m.group("book").strip()
        c = int(m.group("c"))
        v1 = int(m.group("v1"))
        v2 = int(m.group("v2"))
        return {"kind": "verse_range", "book": book, "c": c, "v1": v1, "v2": v2}

    m = _VERSE_REF.match(q)
    if m:
        # Keep as a ref string, let normalize_ref handle book aliases.
        book = m.group("book").strip()
        c = int(m.group("c"))
        v = int(m.group("v"))
        return {"kind": "single_verse", "ref": f"{book} {c}:{v}"}

    return {"kind": "unknown", "raw": q}


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


def _pick_corpus_for_book(eng, book: str) -> str | None:
    """
    Prefer precomputed index: eng.book_to_corpus[book] = "bible"/"apocrypha"/...
    Fallback: first corpus that contains the book.
    """
    if hasattr(eng, "book_to_corpus") and book in eng.book_to_corpus:
        return eng.book_to_corpus[book]

    for corpus_name, d in eng.corpora.items():
        if not isinstance(d, dict):
            continue
        for (b, c, v) in d.keys():
            if b == book:
                return corpus_name
    return None


def _run_single_ref(ref: str, *, min_needed: int, min_books: int, strict_no_pad: bool, near_miss_k: int, strong: bool):
    # Normalize inside engine if you have it; otherwise keep as-is and rely on lookup_text_any/engine xrefs.
    # ref_norm = normalize_ref(ref)
    
    ref = (ref or "").strip()
    ref_norm = normalize_ref(ref)

    print("RAW:", ref, "NORM:", ref_norm)

    text, corpus = lookup_text_any(eng.corpora, ref_norm)

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


# ----------------------------
# New: GET /query?q=...
# ----------------------------
@app.get("/query")
def query_get(
    q: str = Query(..., description="e.g. 'Gen 1:1', 'Genesis 1-3 --strong'"),
    min_needed: int = 3,
    min_books: int = 2,
    strict_no_pad: bool = False,
    near_miss_k: int = 5,
):
    q_clean, flags = _strip_flags(q)
    # Make API input as forgiving as CLI normalization
    q_clean = q_clean.replace(";", ":")
    q_clean = q_clean.replace(",", " ")
    q_clean = re.sub(r"\s+", " ", q_clean).strip()
    parsed = _detect_query_kind(q_clean)

    if parsed["kind"] == "single_verse":
        result = _run_single_ref(
            parsed["ref"],
            min_needed=min_needed,
            min_books=min_books,
            strict_no_pad=strict_no_pad,
            near_miss_k=near_miss_k,
            strong=bool(flags["strong"]),
        )
        
        if result is None:
            return {"query": q, "error": "Internal: _run_single_ref returned None"}
            
        return {"query": q, "kind": "single_verse", "result": result}

    if parsed["kind"] == "chapter_range":
        book = parsed["book"]
        c1, c2 = parsed["c1"], parsed["c2"]
        if c2 < c1:
            c1, c2 = c2, c1

        # Choose corpus that contains that book (bible/apoc/etc.)
        corpus_name = _pick_corpus_for_book(eng, book)
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


    if parsed["kind"] == "verse_range":
        raw_book = parsed["book"]
        c = int(parsed["c"])
        v1, v2 = int(parsed["v1"]), int(parsed["v2"])
        if v2 < v1:
            v1, v2 = v2, v1

        # Normalize using a concrete ref so aliases like "1 cor" resolve before corpus pick.
        start_ref_norm = normalize_ref(f"{raw_book} {c}:{v1}")
        m = re.match(r"^\s*(.+?)\s+(\d+)\s*:\s*(\d+)\s*$", start_ref_norm)
        if not m:
            return {"query": q, "kind": "verse_range", "error": f"Could not normalize verse range: {raw_book} {c}:{v1}-{v2}"}

        book = m.group(1).strip()
        c = int(m.group(2))  # keep normalized chapter
        # Choose corpus that contains that book (bible/apoc/etc.)
        corpus_name = _pick_corpus_for_book(eng, book)
        if not corpus_name:
            return {"query": q, "kind": "verse_range", "error": f"Book not found in corpora: {book}"}

        vmax = _verse_max_for_chapter(eng, corpus_name, book, c)
        if vmax <= 0:
            return {"query": q, "kind": "verse_range", "error": f"No verses found for {book} {c} in corpus {corpus_name}"}

        v1 = max(1, min(v1, vmax))
        v2 = max(1, min(v2, vmax))

        results = []
        for verse in range(v1, v2 + 1):
            ref = f"{book} {c}:{verse}"
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
            "chapter": c,
            "verses": {"start": v1, "end": v2},
            "count": len(results),
            "results": results,
        }

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
    return """
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
    <input id="q" placeholder="Try: Gen 1:1   or   1 Cor 13:5-9   or   Genesis 1-3 --strong" />
    <button onclick="run()">Search</button>
    <div class="hint">Tip: add <b>--strong</b> for Strong’s output.</div>
    <div id="out" style="margin-top:12px;"></div>
  </div>

<script>
async function run(){
  const q = document.getElementById('q').value.trim();
  const out = document.getElementById('out');
  if(!q){ out.innerHTML = '<div class="hint">Enter a reference.</div>'; return; }

  out.innerHTML = '<div class="hint">Searching…</div>';

  try{
    const res = await fetch('/query?q=' + encodeURIComponent(q));
    const data = await res.json();

    // Minimal render
    if(data.error){
      out.innerHTML = '<pre>' + JSON.stringify(data, null, 2) + '</pre>';
      return;
    }

    // If single verse
    if(data.kind === 'single_verse'){
      out.innerHTML = '<pre>' + JSON.stringify(data.result, null, 2) + '</pre>';
      return;
    }

    // If range
    if(data.kind === 'chapter_range'){
      out.innerHTML = '<pre>' + JSON.stringify({
        book: data.book,
        corpus: data.corpus,
        chapters: data.chapters,
        count: data.count,
        sample: data.results.slice(0, 3) // keep UI light; expand later
      }, null, 2) + '</pre>';
      return;
    }

    out.innerHTML = '<pre>' + JSON.stringify(data, null, 2) + '</pre>';
  }catch(e){
    out.innerHTML = '<pre>' + (e && e.message ? e.message : String(e)) + '</pre>';
  }
}
</script>
</body>
</html>
"""