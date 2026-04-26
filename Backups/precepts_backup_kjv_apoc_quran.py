import csv
import json
import sys
import re
import argparse
import math
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from collections import Counter
from engine import (build_engine, parse_bible_ref_to_tuple, lookup_text_any, ensure_min_precepts, expand_simple_range, quran_ref_variants, dedupe_preserve_order, expand_same_chapter_range, NEAR_MISS_K_DEFAULT,)
from engine import (book_of_ref, normalize_ref, infer_corpus, tokenize, top_overlap_keywords, bigrams, build_idf, weighted_overlap_score, parse_ref,)
from engine import (load_strongs_lexicon,)


# ----PRECEPTS.PY APP ----#


DEBUG = False
USE_STRONGS = False

AUTO_LOG = Path("PROJECT_LOG_AUTO.txt")

# TODO: Consolidate corpora into a single dict to avoid updating ~7 call sites per new corpus.
# Target: lookup_text_any(corpora, ref) + infer_corpus(ref) central rules.

####-------Constants-------####
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "Data"
BIBLE_CSV = DATA_DIR / "bible" / "bible.csv"
KJV_CSV   = DATA_DIR / "kjv" / "kjv.csv"
QURAN_CSV = DATA_DIR / "quran" / "quran.csv"

XREFS_JSON = BASE_DIR / "cross_refs.json"
TOPICS_JSON = BASE_DIR / "topics.json"
APOCRYPHA_CSV = BASE_DIR / "apocrypha.csv"
APOCRYPHA_DIR = BASE_DIR  # keep apocrypha_*.csv files in the same folder for now



CORPUS_REGISTRY = [
    ("enoch",  DATA_DIR / "enoch",  "enoch",  "dir_csv"),
    ("hermas", DATA_DIR / "hermas", "hermas", "dir_csv"),
]
      

def log_event(message: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with AUTO_LOG.open("a", encoding="utf-8") as f:
        f.write(f"[{ts}] {message}\n")



        
#####
def bible_rows_to_refdict(rows, canonical_book):
    """
    Convert load_bible_csv rows into {"Sirach 1:1": "text"}.
    Expects rows as list of dicts with chapter/verse/text.
    """
    out = {}
    for r in rows:
        ch = r.get("chapter") or r.get("ch")
        v = r.get("verse") or r.get("v")
        text = r.get("text") or r.get("verse_text")
        if not (ch and v and text):
            continue
        ref = normalize_ref(f"{canonical_book} {int(ch)}:{int(v)}")
        out[ref] = text.strip()
    return out


###################
def print_csv_stub_for_ref(ref: str) -> None:
    """
    Prints a CSV stub for either Bible or Apocrypha references.

    - Bible:  Book,Chapter,Verse,"Text"
      (intended for Data/bible/bible.csv)

    - Apoc:   Apocrypha,Book,Chapter,Verse,"Text"
      (intended for Data/apocrypha/apocrypha_<book>.csv)
    """
    APOC_BOOKS = {
        "Tobit", "Judith", "Wisdom", "Sirach", "Baruch", "Letter of Jeremiah",
        "1 Maccabees", "2 Maccabees", "1 Esdras", "2 Esdras",
        "Prayer of Manasseh", "Bel and the Dragon", "Susanna",
        "Additions to Esther"
    }

    # If user explicitly uses Apocrypha:Book Chapter:Verse format
    if ref.startswith("Apocrypha:"):
        inner = ref.split(":", 1)[1].strip()
        try:
            book, chapter, verse = parse_ref(inner)
        except ValueError as e:
            print(f"Invalid Apocrypha reference for --need: {ref}")
            print(e)
            return

        print("\n--------------------")
        print("CSV stub (add this to apocrypha_<book>.csv):")
        print("--------------------")
        fname = f"apocrypha_{book.lower().replace(' ', '_')}.csv"
        print(f"# File: Data/apocrypha/{fname}")
        print(f'Apocrypha,{book},{chapter},{verse},"PASTE VERSE TEXT HERE"')
        print("--------------------\n")
        return

    # Default parse
    try:
        book, chapter, verse = parse_ref(ref)
    except ValueError as e:
        print(f"Invalid reference for --need: {ref}")
        print(e)
        return

    # Route apoc books to apocrypha stub
    if book in APOC_BOOKS:
        print("\n--------------------")
        print("CSV stub (add this to apocrypha_<book>.csv):")
        print("--------------------")
        fname = f"apocrypha_{book.lower().replace(' ', '_')}.csv"
        print(f"# File: Data/apocrypha/{fname}")
        print(f'Apocrypha,{book},{chapter},{verse},"PASTE VERSE TEXT HERE"')
        print("--------------------\n")
        return

    # Otherwise Bible
    print("\n--------------------")
    print("CSV stub (add this to bible.csv):")
    print("--------------------")
    print(f'{book},{chapter},{verse},"PASTE VERSE TEXT HERE"')
    print("--------------------\n")

    

#######################
def list_available(bible: Dict[Tuple[str, int, int], str], xrefs: Dict[str, List[str]]) -> None:
    print("\nAvailable query refs (from cross_refs.json):")
    for k in sorted(xrefs.keys()):
        print(f"  - {k}")
    print("\nAvailable verse texts (from bible.csv):")
    # Only list a few to avoid noise; show count
    print(f"  Total verses loaded: {len(bible)}")
    print("  Tip: add more verses to bible.csv as you expand.\n")
    

######################
def parse_args():
    parser = argparse.ArgumentParser(
        description="Precept Sandbox – multi-corpus scripture lookup"
    )
    parser.add_argument(
        "ref",
        nargs="?",
        help="Scripture reference (e.g. 'Sirach 1:1', 'Quran 11:15')"
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug output"
    )
    return parser.parse_args()
    

def get_refs_for_topic(topic_name: str, topics: dict[str, list[str]]) -> tuple[str | None, list[str]]:
    if not topic_name:
        return None, []

    want = topic_name.strip().lower()
    if not want or not topics:
        return None, []

    # exact match first
    for tname, refs in topics.items():
        if isinstance(tname, str) and tname.strip().lower() == want:
            return tname, sorted({r for r in refs if r})

    # substring match fallback (first matching topic name wins)
    for tname, refs in topics.items():
        if isinstance(tname, str) and want in tname.strip().lower():
            out = sorted(set([r for r in refs if r]))
            return tname, out

    return None, []


##############################################
def main() -> int:
    global DEBUG, USE_STRONGS

    argv = sys.argv[1:]

    # flags anywhere
    if "--debug" in argv:
        DEBUG = True
        argv = [a for a in argv if a != "--debug"]

    if "--strong" in argv:
        USE_STRONGS = True
        argv = [a for a in argv if a != "--strong"]

    # optional: file checks (if you keep them)
    if not BIBLE_CSV.exists():
        print(f"Missing file: {BIBLE_CSV}")
        return 2
    if not XREFS_JSON.exists():
        print(f"Missing file: {XREFS_JSON}")
        return 2
    
    
    ### --- Test Call --- ###
    #engine = build_engine(use_strongs=USE_STRONGS, debug=DEBUG)
    
    # ✅ BUILD ENGINE HERE (before any mode logic)
    engine = build_engine(
        BASE_DIR=BASE_DIR,
        DATA_DIR=DATA_DIR,
        BIBLE_CSV=BIBLE_CSV,
        KJV_CSV=KJV_CSV,
        QURAN_CSV=QURAN_CSV,
        XREFS_JSON=XREFS_JSON,
        TOPICS_JSON=TOPICS_JSON,
        CORPUS_REGISTRY=CORPUS_REGISTRY,
        use_strongs=USE_STRONGS,
        debug=DEBUG,
    )

    corpora = engine.corpora
    all_verses = engine.all_verses
    xrefs = engine.xrefs
    topics = engine.topics
    topic_index = engine.topic_index
    strongs_ot_idx = engine.strongs_ot_idx
    strongs_lex = engine.strongs_lex
    bible = engine.bible_overrides

    # ... THEN your --list / --corpora / --need / --topic / default logic ...
    
    
    #engine = build_engine()
    #corpora = engine["corpora"]
    #strongs_ot_idx = engine.get("strongs_ot_idx")
    #strongs_lex    = engine.get("strongs_lex")
    #bible = engine["bible_overrides"]
    
    #all_verses = iter_all_verses_multi(corpora)
    #xrefs = load_xrefs(XREFS_JSON)
    #topics = load_topics(TOPICS_JSON)
    #topic_index = build_topic_index(topics)
    
    #engine = build_engine()

    #corpora = engine["corpora"]
    #all_verses = engine["all_verses"]
    #xrefs = engine["xrefs"]
    #topics = engine["topics"]
    #topic_index = engine["topic_index"]
    #strongs_ot_idx = engine["strongs_ot_idx"]
    #strongs_lex = engine["strongs_lex"]

    # Only include if used:
    #bible = engine["bible_overrides"]

    
    
    
    #engine = build_engine(
        #BASE_DIR=BASE_DIR,
        #DATA_DIR=BASE_DIR / "Data",
        #BIBLE_CSV=BIBLE_CSV,
        #KJV_CSV=KJV_CSV,
        #QURAN_CSV=QURAN_CSV,
        #XREFS_JSON=XREFS_JSON,
        #TOPICS_JSON=TOPICS_JSON,
        #CORPUS_REGISTRY=CORPUS_REGISTRY,
        #use_strongs=USE_STRONGS,
    #)
    #corpora = eng.corpora
    #all_verses = eng.all_verses
    #xrefs = eng.xrefs
    #topics = eng.topics
    #topic_index = eng.topic_index
    #bible = eng.bible_overrides
    #strongs_ot_idx = eng.strongs_ot_idx
    #strongs_lex = eng.strongs_lex




    ########################
    #corpora = {}

    #bible = load_bible_csv(BIBLE_CSV)      # curated/overrides
    #kjv   = load_kjv_numeric_csv(KJV_CSV)  # full KJV fallback
    
    
#    pat_raw = load_corpus_books(
#        BASE_DIR / "Data" / "patriarchs",
#        prefix="patriarchs",
#        corpus_label="Patriarchs",
#    )

#    patriarchs = {
#        normalize_ref(f"{book} {ch}:{vs}"): text
#        for (book, ch, vs), text in pat_raw.items()
#    }
    
#    corpora["patriarchs"] = patriarchs
    
#    if DEBUG:
#        books = sorted({k.rsplit(" ", 1)[0] for k in patriarchs.keys()})
#        print("[DEBUG] Patriarchs books:", books)

        
#    if DEBUG:
#        print("[DEBUG] Patriarchs size:", len(patriarchs))
#        sample = list(patriarchs.keys())[:3]
#        print("[DEBUG] Patriarchs sample keys:", sample)
#        print("[DEBUG] Has Reuben 1:2:",
#              normalize_ref("Testament of Reuben 1:2") in patriarchs)

#    if DEBUG:
#        print("[DEBUG] Has Simeon 1:2:",
#              normalize_ref("Testament of Simeon 1:2") in patriarchs)
#        print("[DEBUG] Simeon key count:",
#              sum(1 for k in patriarchs.keys() if "Simeon" in k))

    # --- Enoch Loader --- #
    #enoch_raw = load_corpus_books(BASE_DIR / "Data" / "enoch", prefix="enoch", corpus_label="Enoch")
    #enoch = {
        #normalize_ref(f"{book} {ch}:{vs}"): text
        #for (book, ch, vs), text in enoch_raw.items()
    #}

    # --- Hermas Loader --- #
    #hermas_raw = load_corpus_books(BASE_DIR / "Data" / "hermas", prefix="apocrypha", corpus_label="Apocrypha",)
    #hermas = {
        #normalize_ref(f"{book} {ch}:{vs}"): text
        #for (book, ch, vs), text in hermas_raw.items()
    #}
    
    
    #if DEBUG:
        #print("[DEBUG] Hermas size:", len(hermas_raw))
        #print("[DEBUG] Hermas sample keys:", list(hermas_raw.keys())[:3])

        
    
    
    #apoc_raw = load_apocrypha_books(Path("Data/apocrypha"))
    #apoc = {
        #normalize_ref(f"{book} {ch}:{vs}"): text
        #for (book, ch, vs), text in apoc_raw.items()
    #}
    #apoc = load_apocrypha_books(Path("Data/apocrypha"))  # keep tuple keys

    
    

    # flags already parsed into USE_STRONGS / DEBUG
    #strongs_ot_idx = None
    #strongs_lex = None

    #if USE_STRONGS:
        #strongs_ot_idx = load_kjv_ot_strongs_from_tsv(
            #BASE_DIR / "Data" / "strongs" / "kjv_ot_bhs.tsv"
        #)
        #strongs_lex = load_strongs_lexicon(
            #BASE_DIR / "Data" / "strongs" / "strongs_hebrew.csv"
        #)

        #if DEBUG:
            #print("[DEBUG] OT Strong’s verses:", len(strongs_ot_idx))
            #print("[DEBUG] Gen 1:1 codes:", strongs_ot_idx.get(("Genesis", 1, 1)))

    

    #apoc = load_apocrypha_books(APOCRYPHA_DIR)
    #apoc = load_apoc_csv("Data/apocrypha/sirach.csv")
    
    #wisdom = load_wisdom_csv("Data/apocrypha/wisdom.csv")
    #apoc.update(wisdom)
    
    #if DEBUG:
        #wisdom_keys = [k for k in apoc.keys()
                       #if isinstance(k, tuple) and "wisdom" in k[0].lower()][:10]
        #print("[DEBUG] Wisdom sample keys:", wisdom_keys)

    
    #quran = load_quran_csv(QURAN_CSV)
    
    
    #jasher = load_jasher_csv("Data/jasher/jasher.csv")
    
    # --- Lost Books ---
    #lost_raw = load_corpus_books(
        #BASE_DIR / "Data" / "lost_books",
        #prefix="lost",
        #corpus_label="Lost",
    #)

    #lost = {
        #normalize_ref(f"{book} {ch}:{vs}"): text
        #for (book, ch, vs), text in lost_raw.items()
    #}
    
    #corpora["lost"] = lost

    #if DEBUG:
        #print("[DEBUG] Lost size:", len(lost_raw))
        #print("[DEBUG] Lost sample keys:", list(lost_raw.keys())[:3])
        #print("[DEBUG] Has Lost Books 4:4:", "Lost Books 4:4" in lost)

        
    if DEBUG:
        lex = load_strongs_lexicon(BASE_DIR / "Data" / "strongs" / "strongs_hebrew.csv")
        print("[DEBUG] Strong’s H7225:", lex.get("H7225"))


    # --- Sirach (tuple-keyed dict) -> normalize into apoc string refs ---
    #sirach_rows = load_bible_csv(BASE_DIR / "Data" / "apocrypha" / "sirach.csv")
    
    
    #sirach_refdict = {
        #("Sirach", ch, vs): text
        #for (book, ch, vs), text in sirach_rows.items()
        #if text
    #}
    #apoc.update(sirach_refdict)

    
    #if DEBUG:
        #print("[DEBUG] sirach count:", len(sirach_refdict))
        #print("[DEBUG] apoc has Sirach 1:1:", "Sirach 1:1" in apoc)
        
    
    #if DEBUG:
        #print("[DEBUG] Apoc size (final):", len(apoc))
        #print("[DEBUG] Has Tobit 1:1:", "Tobit 1:1" in apoc)
        #print("[DEBUG] Has Wisdom 1:1:", "Wisdom 1:1" in apoc)
        #print("[DEBUG] Has Sirach 1:1:", "Sirach 1:1" in apoc)

    #corpora = {
        #"kjv": kjv,
        #"apoc": apoc,
        #"quran": quran,
        #"jasher": jasher,
        #"enoch": enoch,
        #"hermas": hermas,
        #"lost": lost,
    #}
    
    for key, folder, prefix, kind in CORPUS_REGISTRY:
        if kind != "dir_csv":
            continue

        # Don't overwrite corpora already loaded via special loaders (KJV/Quran/etc.)
        if key in corpora:
            if DEBUG:
                print(f"[DEBUG] Registry skip (already loaded): {key}")
            continue

        corpora[key] = load_dir_csvs(folder, prefix)
    
    
    if DEBUG:
        for k in ("kjv", "quran", "enoch", "hermas", "jasher", "apoc"):
            v = corpora.get(k)
            print(f"[DEBUG] corpora[{k}] size:", (len(v) if isinstance(v, dict) else None))

    
    
    # ---Keep for future inspect of Enoch/Hermas --- #
    #inspect_corpus(corpora["enoch"], "enoch")
    #inspect_corpus(corpora["hermas"], "hermas")

    
    
    

    # ---- LIST MODE ----
    if len(argv) >= 1 and argv[0] == "--list":
        list_available(bible, xrefs)
        return 0
        
       
    # ---- CORPORA MODE ----
    if len(argv) >= 1 and argv[0] == "--corpora":
        print("\nLoaded Corpora:")
        for name, corpus in corpora.items():
            size = len(corpus) if isinstance(corpus, dict) else 0
            print(f"  {name:<10} → {size:,} verses")
        return 0

    # ---- NEED MODE ----
    if len(argv) >= 2 and argv[0] == "--need":
        ref = normalize_ref(" ".join(argv[1:]))
        print_csv_stub_for_ref(ref)
        return 0
    
    # ---- TOPICS LIST MODE ----
    if len(argv) >= 1 and argv[0] == "--topics":
        print("\nAvailable topics:")
        for t in sorted(topics.keys(), key=lambda s: s.lower()):
            print(f"  - {t}")
        return 0
    if len(argv) > 1 and argv[0] == "--topics":
        print('Tip: use `--topic "Love"` to run a topic query.')

    if DEBUG:
        print("[DEBUG] topics keys sample:", list(topics.keys())[:25])
        
        
    # ---- TOPIC MODE ----
    if len(argv) >= 2 and argv[0] == "--topic":
        topic_name = " ".join(argv[1:]).strip()
        matched, query_refs = get_refs_for_topic(topic_name, topics)

        if not query_refs:
            print(f'No refs found for topic: "{topic_name}"')
            return 0

        print(f'\nTopic match: "{matched}" → {len(query_refs)} refs')

    else:
        # ---- DEFAULT MODE ----
        if len(argv) < 1:
            print('Usage: python precepts.py "Isaiah 28:10"')
            print('   or: python precepts.py --list')
            print('   or: python precepts.py --debug "Sirach 1:1"')
            return 2

        raw_query = " ".join(argv).strip()

        maybe_refs = expand_same_chapter_range(raw_query)  # returns list[str] OR []/None
        query_refs = [normalize_ref(r) for r in maybe_refs] if maybe_refs else [normalize_ref(raw_query)]
        
    # Loop each ref (range or single) — unified for both modes
    for query_ref in query_refs:
        text, corpus_name = lookup_text_any(corpora, query_ref)
        query_text = text

        print("\n====================")
        print(f"INPUT: {query_ref}")
        print("====================")
        if query_text is None:
            print("[Input text not found in loaded corpora — showing precepts anyway]")
        else:
            print(query_text)

        # Strong’s (OT-only is handled inside parse/lookup; you can keep as-is for now)
        if USE_STRONGS and (corpus_name == "kjv") and strongs_ot_idx and strongs_lex and query_text:
            t = parse_bible_ref_to_tuple(query_ref)
            if t:
                book, ch, vs = t
                codes = strongs_ot_idx.get((book, ch, vs), [])
                if codes:
                    shown = []
                    for c in codes[:20]:
                        entry = strongs_lex.get(c, {})
                        gloss = (entry.get("definition") or entry.get("lemma") or "").strip()
                        shown.append(
                            f"{c} ({gloss[:60]}{'…' if len(gloss)>60 else ''})" if gloss else c
                        )
                    print("\nStrong’s:")
                    print("  " + ", ".join(shown))
                else:
                    print("\nStrong’s: (no tags found for this OT verse)")

        mapped_precepts = xrefs.get(query_ref, [])

        _, src = lookup_text_any(corpora, query_ref)
        strict_no_pad = (src in {"hermas", "enoch", "quran", "jasher", "patriarchs"})

        final_precepts, added_by_fallback, near_misses = ensure_min_precepts(
            query_ref=query_ref,
            precepts=mapped_precepts,
            min_needed=3,
            min_books=2,
            xrefs=xrefs,
            corpora=corpora,
            all_verses=all_verses,
            strict_no_pad=strict_no_pad,
            near_miss_k=NEAR_MISS_K_DEFAULT,
        )

        if strict_no_pad and not final_precepts:
            log_event(f"Strict-no-pad: no credible precepts for {query_ref} (source={src})")


        # ...keep your existing printing logic...
        # but add near-miss printing if final_precepts empty (below)


        


        # Rule check: 2 different books (best effort)
        books = [book_of_ref(r) for r in final_precepts if book_of_ref(r)]
        unique_books: List[str] = []
        for b in books:
            if b and b not in unique_books:
                unique_books.append(b)

        if not final_precepts:
            print("\nNo credible precepts found for this verse yet")

        


        print("\n--------------------")
        print("PRECEPTS (cross-refs)")
        print("--------------------")
        
        
        
        # For reasons, try to use the input verse text if we have it.
        q_text_for_reasons = query_text  # may be None


        ###---HAVE TO ADD NEW BOOKS HERE
        for r in final_precepts:
            t, _ = lookup_text_any(corpora, r) ###---- add new book to lookup_text_any everywhere
            reasons: list[str] = []

            corpus = infer_corpus(r)
            if corpus != "Bible":
                reasons.append(f"Corpus: {corpus}")

            q_topics = topic_index.get(normalize_ref(query_ref), set())
            r_topics = topic_index.get(normalize_ref(r), set())
            shared_topics = q_topics & r_topics
            if shared_topics:
                reasons.append("Topic: " + ", ".join(sorted(list(shared_topics))[:2]))

            if q_text_for_reasons and t:
                # existing keyword overlap (keep)
                qtoks = tokenize(q_text_for_reasons)
                rtoks = tokenize(t)
                kws = top_overlap_keywords(qtoks, rtoks, k=4)
                if kws:
                    reasons.append("Keywords: " + ", ".join(kws))

                shared_phr = sorted(list(bigrams(qtoks) & bigrams(rtoks)))
                if shared_phr:
                    reasons.append(f'Phrase echo: "{shared_phr[0]}"')

                # NEW: rarity-weighted reasons
                pool = [q_text_for_reasons, t]
                idf = build_idf(pool)
                s, top_terms, mcount, qcount = weighted_overlap_score(q_text_for_reasons, t, idf)

                if top_terms:
                    top_str = ", ".join([f"{tok}(+{w:.2f})" for tok, w in top_terms[:3]])
                    reasons.append(f"Weighted: {top_str} | match={mcount}/{qcount} | score={s:.2f}")

            print(f"\n• {r}")
            if t:
                print(f"  {t}")
            else:
                print("  [Text not found yet — add it for full display]")

            if reasons:
                print("  Reasons: " + " | ".join(reasons[:3]))

    # Near misses (added below in section C)
    if near_misses:
        print("\n--------------------")
        print("Next closest (near-misses)")
        print("--------------------")
        for ref, why in near_misses[:NEAR_MISS_K_DEFAULT]:
            print(f"  ~ {ref} ({why})")

            continue

    if added_by_fallback:
        log_event(
            f"Fallback used for {query_ref} | added={len(added_by_fallback)} | source={src}"
        )

        print("\n--------------------")
        print("Fallback used")
        print("--------------------")
        print("Added to meet minimum precepts:")
        for a in added_by_fallback:
            print(f"  + {a}")


    print("\n--------------------")
    if len(unique_books) >= 2:
        print(f"Rule check: ✅ at least 2 different books ({unique_books[0]}, {unique_books[1]})")
    else:
        print("Rule check: ⚠️ less than 2 different books found in current mapping")
    print("--------------------\n")

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:
        log_event(f"ERROR: {e}")
        raise

