# engine.py
from __future__ import annotations

import csv
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable
import math
from collections import Counter
from typing import Any, Iterable, Optional, Tuple, Dict, List 
from collections import defaultdict

from datetime import datetime



try:
    csv.field_size_limit(10_000_000)
except OverflowError:
    csv.field_size_limit(1_000_000)

DEBUG = False



Key = Tuple[str, int, int]


@dataclass
class Engine:
    corpora: dict[str, dict]
    all_verses: Any
    xrefs: dict
    topics: dict
    topic_index: dict
    strongs_ot_idx: Any | None
    strongs_lex: dict | None
    bible_overrides: dict
    xrefs_raw: dict[str, list[str]] | None = None
    idf: dict[str, float] | None = None

# =========================
# Section 1: Regex/constants
# =========================
# Move constants like WISDOM_VERSE_RE here if used by loaders/scoring.



KNOWN_CORPORA = {"Apocrypha", "Enoch", "Jubilees", "Jasher", "Quran", "BookOfMormon", "SealedPortion", "ThirdTestament"}

_RE_JASHER_CH = re.compile(r'^\s*Jasher Chapter\s+(\d+)\s*$', re.I)
_RE_VERSE = re.compile(r'^\s*(\d+)\.\s+(.*)\s*$')
WISDOM_VERSE_RE = re.compile(r'\{(\d+):(\d+)\}')

_STRONGS_RE = re.compile(r"H\d{1,5}")
RANGE_RE = re.compile(r"^(?P<book>.+?)\s+(?P<ch>\d+):(?P<v1>\d+)-(?P<v2>\d+)\s*$", re.IGNORECASE)
_REF_RE = re.compile(r"^\s*(?P<book>.+?)\s+(?P<ch>\d+)\s*:\s*(?P<v>\d+)\s*$")

NEAR_MISS_K_DEFAULT = 5
MIN_FALLBACK_SCORE = 1.6  # tune later if needed
LOW_VALUE_MULT = 0.25
COMMON_THEOLOGY_MULT = 0.55
PARALLEL_MATCH_RATIO = 0.70
PARALLEL_ANCHOR_HITS = 3
PARALLEL_MIN_SCORE = 2.25





# =========================
# Synonym / equivalence bridge (Phase 1 relevance boost)
# =========================

SYNONYM_GROUPS = [
    # commandments / legal language
    {"murder", "kill", "slay", "slain"},
    {"adultery", "fornication", "whoredom"},
    {"steal", "stole", "theft", "rob", "robbed", "robbery"},
    {"lie", "lying", "false", "witness", "testimony", "accuse", "accusation"},
    {"honour", "honor"},  # KJV/US spelling
]

# token -> set(all equivalents)
SYN_MAP: dict[str, set[str]] = {}
for group in SYNONYM_GROUPS:
    g = set(group)
    for t in g:
        SYN_MAP[t] = g




def expand_tokens_with_synonyms(tokens: Iterable[str]) -> set[str]:
    out: set[str] = set()
    for t in tokens:
        out.add(t)
        out.update(SYN_MAP.get(t, set()))
    return out



# Small, manual fallback pool (Phase 0 scaffolding)
# These should be broadly useful “anchor” verses you don't mind reusing early on.
FALLBACK_POOL = [
    "2 Timothy 3:16",
    "1 Corinthians 14:40",
    "Psalm 119:105",
]

HARD_ANCHORS = {
    "father", "fathers",
    "son", "sons",
    "children", "child",
    "sin", "iniquity",
    "bear", "transgression",
    "covenant", "statutes",
    "law", "commanded",
    "righteous", "judgment",
}

BROAD_ACTION_TOKENS = {
    "suffer", "long", "live", "die", "make", "made", "do", "done",
    "say", "said", "come", "came", "go", "went", "take", "took",
    "give", "gave", "let", "put", "set", "turn", "bring", "get",
}

ANCHOR_SYNONYMS = {
    "murder": {"kill", "slay"},
    "kill": {"murder", "slay"},
    "slay": {"murder", "kill"},

    "adultery": {"fornication"},
    "fornication": {"adultery"},

    "steal": {"theft", "rob"},
    "rob": {"steal"},

    "witness": {"testimony", "testify", "lie", "lying"},
    "testimony": {"witness"},
    "lie": {"false"},
    "false": {"lie"},
}


STOPWORDS = {
    "the","and","of","to","in","a","an","is","it","that","for","with","as","be","by",
    "on","or","not","are","was","were","this","these","those","at","from","but","have",
    "hath","shall","will","do","doth","did","ye","you","your","my","mine","our","ours",
    "his","her","their","them","they","he","she","we","i","me","us"
}

BOOKS_66 = [
    "Genesis","Exodus","Leviticus","Numbers","Deuteronomy",
    "Joshua","Judges","Ruth","1 Samuel","2 Samuel","1 Kings","2 Kings",
    "1 Chronicles","2 Chronicles","Ezra","Nehemiah","Esther","Job","Psalms",
    "Proverbs","Ecclesiastes","Song of Solomon","Isaiah","Jeremiah",
    "Lamentations","Ezekiel","Daniel","Hosea","Joel","Amos","Obadiah",
    "Jonah","Micah","Nahum","Habakkuk","Zephaniah","Haggai","Zechariah",
    "Malachi","Matthew","Mark","Luke","John","Acts","Romans","1 Corinthians",
    "2 Corinthians","Galatians","Ephesians","Philippians","Colossians",
    "1 Thessalonians","2 Thessalonians","1 Timothy","2 Timothy","Titus",
    "Philemon","Hebrews","James","1 Peter","2 Peter","1 John","2 John",
    "3 John","Jude","Revelation"
]


LOW_VALUE_TOKENS = {
    "also","same","then","now","again","there","here","therefore",
    "behold","came","come","going","went","return","returned",
    "upon","unto","without","within","among","about",
    "night","day"
}


COMMON_THEOLOGY_TOKENS = {
    "god","lord","man","men","sin","sins","death","die","put","shall","unto"
}


STOPWORDS = {
    "and","the","of","to","in","a","an","that","this","these","those","for","with","as","is","are",
    "be","been","being","was","were","am","do","did","done","shall","will","may","might","can",
    "unto","upon","within","without","from","by","at","it","its","his","her","their","them","they",
    "he","she","ye","thou","thee","thy","your","you","i","we","our","us",
    # bible glue words that cause false positives:
    "said","say","saying","let","man","men","god","lord"
}


JUNK_ANCHORS = {
    "all", "him", "her", "his", "their", "them",
    "com", "come", "came",
    "ever", "also", "shall", "unto"
}


_ROMAN = {"i": "1", "ii": "2", "iii": "3"}


BOOK_ALIASES = {
    "Psalm": "Psalms",
    "Song of Songs": "Song of Solomon",
    "Canticles": "Song of Solomon",
    "Revelations": "Revelation",
    
    # --- Apocrypha: ---
    "sirach": "Sirach",
    "ecclesiasticus": "Sirach",
    
    "wisdom of solomon": "Wisdom",
    "the wisdom of solomon": "Wisdom",
    "wisdom": "Wisdom",
    
    "tobit": "Tobit",
    "the book of tobit": "Tobit",
    "tobias": "Tobit",
    
    "1 maccabees": "1 Maccabees",
    "1_maccabees": "1 Maccabees",
    "1st maccabees": "1 Maccabees",
    "first maccabees": "1 Maccabees",
    
    "1 esdras": "1 Esdras",
    "1_esdras": "1 Esdras",
    "first esdras": "1 Esdras",
    "i esdras": "1 Esdras",
    "2 esdras": "2 Esdras",
    "2_esdras": "2 Esdras",
    "second esdras": "2 Esdras",
    "ii esdras": "2 Esdras",
    
    "baruch": "Baruch",
    
    "bel and the dragon": "Bel and the Dragon",
    "bel & the dragon": "Bel and the Dragon",
    "bel_&_dragon": "Bel and the Dragon",
    "bel": "Bel and the Dragon",
    
    "additions to esther": "Additions to Esther",
    "esther (additions)": "Additions to Esther",
    "esther additions": "Additions to Esther",
    "add esther": "Additions to Esther",
    
    "judith": "Judith",
    
    "letter of jeremiah": "Letter of Jeremiah",
    "epistle of jeremiah": "Letter of Jeremiah",
    "jeremiah letter": "Letter of Jeremiah",
    
    "prayer of manasseh": "Prayer of Manasseh",
    "prayer_of_manasseh": "Prayer of Manasseh",
    "prayer_of_manassah": "Prayer of Manasseh",
    
    "susanna": "Susanna",
    "susanna (daniel)": "Susanna",
    
    #--- Other Books ---
    "enoch": "1 Enoch",
    "1 enoch": "1 Enoch",
    "1_enoch": "1 Enoch",
    "first enoch": "1 Enoch",
    
    "hermas": "Shepherd of Hermas",
    "shepherd of hermas": "Shepherd of Hermas",
    
    "reuben": "Testament of Reuben",
    "simeon": "Testament of Simeon",
    "levi": "Testament of Levi",
    "judah": "Testament of Judah",
    "issachar": "Testament of Issachar",
    "zebulun": "Testament of Zebulun",
    "dan": "Testament of Dan",
    "naphtali": "Testament of Naphtali",
    "gad": "Testament of Gad",
    "asher": "Testament of Asher",
    "joseph": "Testament of Joseph",
    "benjamin": "Testament of Benjamin",
    "siemon": "Testament of Simeon",   # common misspelling
    "testament of siemon": "Testament of Simeon",
    
    "lost": "Lost Books",
    "lost books": "Lost Books",

    "testament of reuben": "Testament of Reuben",
    "testament of simeon": "Testament of Simeon",
    "testament of levi": "Testament of Levi",
    "testament of judah": "Testament of Judah",
    "testament of issachar": "Testament of Issachar",
    "testament of zebulun": "Testament of Zebulun",
    "testament of dan": "Testament of Dan",
    "testament of naphtali": "Testament of Naphtali",
    "testament of gad": "Testament of Gad",
    "testament of asher": "Testament of Asher",
    "testament of joseph": "Testament of Joseph",
    "testament of benjamin": "Testament of Benjamin",
    }


BOOK_ABBREVS = {
    # Pentateuch
    "gen": "Genesis", "ge": "Genesis", "gn": "Genesis",
    "ex": "Exodus", "exo": "Exodus", "exod": "Exodus",
    "lev": "Leviticus", "lv": "Leviticus",
    "num": "Numbers", "nm": "Numbers",
    "deut": "Deuteronomy", "deu": "Deuteronomy", "dt": "Deuteronomy",

    # History / common
    "josh": "Joshua", "jos": "Joshua",
    "judg": "Judges", "jdg": "Judges",
    "1 sam": "1 Samuel", "2 sam": "2 Samuel",
    "1 kings": "1 Kings", "2 kings": "2 Kings",
    "1 chr": "1 Chronicles", "2 chr": "2 Chronicles",

    # Poetry/Wisdom
    "ps": "Psalms", "psa": "Psalms", "psalm": "Psalms",
    "prov": "Proverbs", "pr": "Proverbs",
    "eccl": "Ecclesiastes", "ecc": "Ecclesiastes",
    "song": "Song of Solomon", "sos": "Song of Solomon",

    # Prophets
    "isa": "Isaiah",
    "jer": "Jeremiah",
    "lam": "Lamentations",
    "ezek": "Ezekiel", "eze": "Ezekiel", "ezk": "Ezekiel",
    "dan": "Daniel",

    # New Testament (common abbreviations)
    "rom": "Romans",
    "1 cor": "1 Corinthians", "2 cor": "2 Corinthians",
    "1cor": "1 Corinthians", "2cor": "2 Corinthians",
    "1 co": "1 Corinthians", "2 co": "2 Corinthians",
    "cor": "Corinthians",  # handled with leading number when present
    "gal": "Galatians",
    "eph": "Ephesians",
    "phil": "Philippians",
    "col": "Colossians",
    "1 thess": "1 Thessalonians", "2 thess": "2 Thessalonians",
    "1thess": "1 Thessalonians", "2thess": "2 Thessalonians",
    "1 tim": "1 Timothy", "2 tim": "2 Timothy",
    "1tim": "1 Timothy", "2tim": "2 Timothy",
    "rev": "Revelation",
    "1 pet": "1 Peter", "2 pet": "2 Peter",
    "1 john": "1 John", "2 john": "2 John", "3 john": "3 John",
    "1jn": "1 John", "2jn": "2 John", "3jn": "3 John",
    "1 jn": "1 John", "2 jn": "2 John", "3 jn": "3 John",
    "heb": "Hebrews",
    "jam": "James", "jas": "James",
    "jude": "Jude",
}


OT_BOOKS = [
    "Genesis","Exodus","Leviticus","Numbers","Deuteronomy","Joshua","Judges","Ruth",
    "1 Samuel","2 Samuel","1 Kings","2 Kings","1 Chronicles","2 Chronicles","Ezra","Nehemiah",
    "Esther","Job","Psalms","Proverbs","Ecclesiastes","Song of Solomon","Isaiah","Jeremiah",
    "Lamentations","Ezekiel","Daniel","Hosea","Joel","Amos","Obadiah","Jonah","Micah","Nahum",
    "Habakkuk","Zephaniah","Haggai","Zechariah","Malachi"
]


#CORPUS_REGISTRY = [
    # key, folder, prefix, loader_kind
    #("enoch",  DATA_DIR / "enoch",  "enoch",  "dir_csv"),
    #("hermas", DATA_DIR / "hermas", "hermas", "dir_csv"),
    # apoc stays special for now
#]






# =========================
# Section 2: Parsing/normalizing
# =========================
# Move normalize_ref, expand_same_chapter_range, parse_* helpers, etc.
def normalize_xrefs(xrefs: dict[str, list[str]]) -> dict[str, list[str]]:
    out: dict[str, set[str]] = {}
    for k, vals in (xrefs or {}).items():
        nk = normalize_ref(k)
        if nk not in out:
            out[nk] = set()
        for v in (vals or []):
            nv = normalize_ref(v)
            if nv and nv != nk:
                out[nk].add(nv)
    return {k: sorted(list(v)) for k, v in out.items()}


def normalize_ref(ref: str) -> str:
    ref = (ref or "").strip()
    ref = ref.replace(";", ":")
    ref = re.sub(r"\s+", " ", ref)
    ref = re.sub(r"\s*:\s*", ":", ref)

    m = re.match(r"^(.+?)\s+(\d+):(\d+)$", ref)
    if not m:
        return ref  # leave as-is

    book_raw, ch, vs = m.group(1), m.group(2), m.group(3)
    book = normalize_book(book_raw)
    return f"{book} {int(ch)}:{int(vs)}"

def normalize_book(book_raw: str) -> str:
    b = (book_raw or "").strip()
    b = re.sub(r"\s+", " ", b)
    b = b.replace(".", "")  # "1 Sam." -> "1 Sam"
    b_low = b.lower()

    # Roman numeral leading token: "I Sam" -> "1 Sam"
    parts = b_low.split(" ")
    if parts and parts[0] in _ROMAN:
        parts[0] = _ROMAN[parts[0]]
        b_low = " ".join(parts)

    # Compact leading number: "1sam" -> "1 sam"
    b_low = re.sub(r"^([123])([a-z])", r"\1 \2", b_low)

    # 1) Exact abbrev match first (gen, ps, eze, 1 sam, etc.)
    if b_low in BOOK_ABBREVS:
        return BOOK_ABBREVS[b_low]

    # 2) Try no-space abbrev match ("1sam")
    b_compact = b_low.replace(" ", "")
    if b_compact in BOOK_ABBREVS:
        return BOOK_ABBREVS[b_compact]

    # 3) Your existing alias map (keep as-is)
    # NOTE: your BOOK_ALIASES keys are mixed case;
    # safest is to check lowercase keys too.
    if b_low in BOOK_ALIASES:
        return BOOK_ALIASES[b_low]
    if b in BOOK_ALIASES:
        return BOOK_ALIASES[b]

    # Also try lowercase lookup for aliases that are stored in lowercase
    # (most of your apocrypha keys are lowercase)
    if b_low in BOOK_ALIASES:
        return BOOK_ALIASES[b_low]

    # 4) Default: Title Case the book name
    return b.title()

def normalize_text(s: str) -> str:
    # --- SAFETY: handle tuple or None inputs ---
    if isinstance(s, tuple):
        s = s[0]
    if s is None:
        return ""
    if not isinstance(s, str):
        s = str(s)

    s = s.lower()
    s = re.sub(r"[^a-z0-9\s']", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

    


def book_of_ref(ref: str) -> Optional[str]:
    try:
        book, _, _ = parse_ref(ref)
        return book
    except ValueError:
        return None
        
        
def find_similar_refs(query_ref, query_text, all_verses, exclude, top_n=50):
    scored = []
    for ref, text, corpus in all_verses:
        if ref == query_ref or ref in exclude:
            continue
        s = score_similarity(query_text, text)
        if s > 0:
            scored.append((s, ref, corpus))

    # sort by score (descending)
    scored.sort(key=lambda x: x[0], reverse=True)

    # return refs only (keep corpus for later if needed)
    return [ref for _, ref, _ in scored[:top_n]]


def expand_simple_range(ref: str) -> list[str]:
    """
    Supports:
      - 'Genesis 1:1-5'
      - 'Gen 1:1-5' (after normalize_ref)
    Does NOT (yet) support cross-chapter: 'Gen 1:31-2:3'
    """
    r = normalize_ref(ref)
    m = re.match(r"^(.+?)\s+(\d+):(\d+)\s*-\s*(\d+)$", r)
    if not m:
        return [r]

    book = m.group(1).strip()
    ch = int(m.group(2))
    v1 = int(m.group(3))
    v2 = int(m.group(4))

    if v2 < v1:
        v1, v2 = v2, v1

    return [normalize_ref(f"{book} {ch}:{v}") for v in range(v1, v2 + 1)]





def expand_same_chapter_range(raw: str) -> list[str] | None:
    """
    Expands: 'Genesis 1:1-5' -> ['Genesis 1:1', ..., 'Genesis 1:5']
    Only supports same-chapter ranges for v1.
    Returns None if not a range.
    """
    m = RANGE_RE.match(raw.strip())
    if not m:
        return None

    book = m.group("book").strip()
    ch = int(m.group("ch"))
    v1 = int(m.group("v1"))
    v2 = int(m.group("v2"))

    if v2 < v1:
        v1, v2 = v2, v1

    return [f"{book} {ch}:{v}" for v in range(v1, v2 + 1)]



def parse_ref(ref: str) -> Tuple[str, int, int]:
    ref = normalize_ref(ref)
    if ":" not in ref:
        raise ValueError(f"Reference must contain ':', got: {ref}")

    left, verse_str = ref.rsplit(":", 1)
    left = left.strip()
    verse_str = verse_str.strip()

    parts = left.split()
    if len(parts) < 2:
        raise ValueError(f"Reference must include book and chapter, got: {ref}")

    chapter_str = parts[-1]
    book = " ".join(parts[:-1])
    book = BOOK_ALIASES.get(book, book)
    
    if not chapter_str.isdigit() or not verse_str.isdigit():
        raise ValueError(f"Chapter and verse must be numbers, got: {ref}")

    return book, int(chapter_str), int(verse_str)

def parse_jasher_ref_to_tuple(ref: str):
    ref = normalize_ref(ref)
    if not ref.lower().startswith("jasher"):
        return None

    parts = ref.split()
    if len(parts) != 2:
        return None

    cv = parts[1]
    if ":" not in cv:
        return None

    ch, v = cv.split(":", 1)
    if not ch.isdigit() or not v.isdigit():
        return None

    return ("Jasher", int(ch), int(v))

def parse_bible_ref_to_tuple(ref: str):
    """
    Converts 'leviticus 23:32' -> ('Leviticus', 23, 32)
    Returns None if it can't parse.
    """
    ref_n = " ".join(ref.strip().split())
    parts = ref_n.rsplit(" ", 1)
    if len(parts) != 2:
        return None

    book_raw, cv = parts[0], parts[1]
    if ":" not in cv:
        return None

    ch_s, v_s = cv.split(":", 1)
    if not (ch_s.isdigit() and v_s.isdigit()):
        return None

    book = book_raw.strip().title()   # "leviticus" -> "Leviticus"
    return (book, int(ch_s), int(v_s))

 
def quran_ref_variants(ref_n: str) -> list[str]:
    """
    Accept both formats:
      'Quran 11:15' and 'Quran:11:15'
    Return variants in most-likely order.
    """
    ref_n = (ref_n or "").strip()
    variants = [ref_n]

    if ref_n.lower().startswith("quran "):
        variants.append("Quran:" + ref_n.split(" ", 1)[1])  # Quran 11:15 -> Quran:11:15
    elif ref_n.lower().startswith("quran:"):
        variants.append("Quran " + ref_n.split(":", 1)[1])  # Quran:11:15 -> Quran 11:15

    # de-dup while preserving order
    out = []
    seen = set()
    for v in variants:
        if v not in seen:
            out.append(v)
            seen.add(v)
    return out


def _decode_vid(vid: int) -> tuple[int, int, int] | None:
    """
    Decode verse id like 1001002 => book=1, chapter=1, verse=2
    Assumes format: [book][ccc][vvv] where ccc and vvv are 3 digits each.
    """
    s = str(vid).strip()
    if not s.isdigit() or len(s) < 7:
        return None
    # last 6 digits are chapter+verse
    book = int(s[:-6])
    ch = int(s[-6:-3])
    vs = int(s[-3:])
    return book, ch, vs

def dedupe_preserve_order(items: List[str]) -> List[str]:
    seen = set()
    out = []
    for x in items:
        if x not in seen:
            out.append(x)
            seen.add(x)
    return out
    
    
def _ref_bucket(ref: str) -> str:
    if not ref:
        return "UNKNOWN"

    # Quran style: Quran:1:1
    if ref.lower().startswith("quran:"):
        parts = ref.split(":")
        return f"Quran:{parts[1]}" if len(parts) > 1 else "Quran"

    # Colon-based Bible / Apoc (future-safe)
    if ":" in ref:
        parts = ref.split(":")
        if parts[0].upper() in ("KJV", "APOC", "BIBLE"):
            return parts[1] if len(parts) > 1 else parts[0]
        return parts[0]

    # Space-based Bible refs: "2 Timothy 3:16"
    tokens = ref.split()
    if len(tokens) >= 2:
        return " ".join(tokens[:-1])  # book name only

    return ref

#def get_refs_for_topic(topic_name: str, topic_index: dict[str, set[str]]) -> list[str]:
    #"""
    #Returns refs that match a topic name.
    #Accepts exact match (case-insensitive). Also supports partial contains.
    #"""
    #if not topic_name:
        #return []

    #want = topic_name.strip().lower()

    # Exact topic match (case-insensitive)
    #hits = [ref for ref, topics in topic_index.items()
            #if any(t.lower() == want for t in topics)]
    #if hits:
        #return sorted(hits)

    # Partial contains fallback
    #hits = [ref for ref, topics in topic_index.items()
            #if any(want in t.lower() for t in topics)]
    #return sorted(hits)






def make_xrefs_symmetric(
    xrefs: dict[str, list[str]],
    *,
    max_per_verse: int | None = 200,
) -> dict[str, list[str]]:
    """
    Symmetrize xrefs: if A->B then also B->A.
    Dedupe and (optionally) cap list size per verse to avoid runaway growth.
    """
    out: dict[str, set[str]] = defaultdict(set)

    for src, dsts in (xrefs or {}).items():
        if not src or not dsts:
            continue
        for dst in dsts:
            if not dst or dst == src:
                continue
            out[src].add(dst)
            out[dst].add(src)
    
    for k in list(out.keys()):
        out[k].discard(k)
    # Cap if requested
    if max_per_verse is not None and max_per_verse > 0:
        capped: dict[str, list[str]] = {}
        for k, s in out.items():
            lst = sorted(s)
            capped[k] = lst[:max_per_verse]
        return capped

    return {k: sorted(v) for k, v in out.items()}


# =========================
# Section 3: Tokenize/scoring
# =========================
# Move tokenize, bigrams, top_overlap_keywords, build_idf, weighted_overlap_score, etc.
def fallback_metrics(
    *, 
    corpora: dict, 
    query_ref: str, 
    cand_ref: str, 
    idf_global: dict[str, float] | None = None
    ) -> dict:
    """
    Compute structured similarity metrics between query_ref and cand_ref.
    Designed for confidence scoring + explainability.
    """

    q_text, _ = lookup_text_any(corpora, query_ref)
    c_text, _ = lookup_text_any(corpora, cand_ref)

    if not q_text or not c_text:
        return {
            "score": 0.0,
            "match_ratio": 0.0,
            "anchor_hits": 0,
            "anchors": [],
            "top_terms": [],
            "mcount": 0,
            "qcount": 0,
        }

    # Build anchors from query
    anchors = []
    for t in meaningful_tokens(q_text):
        if t in JUNK_ANCHORS:
            continue
        anchors.append(t)
        if len(anchors) >= 6:
            break

    anchors_set = set(anchors)

    # Weighted scoring
    idf = idf_global or build_idf([q_text, c_text])
    s, top_terms, mcount, qcount = weighted_overlap_score(q_text, c_text, idf)

    c_mean = set(meaningful_tokens(c_text))
    anchor_hits = len(anchors_set & c_mean) if anchors_set else 0
    match_ratio = (mcount / qcount) if qcount else 0.0

    return {
        "score": float(s),
        "match_ratio": float(match_ratio),
        "anchor_hits": int(anchor_hits),
        "anchors": anchors,
        "top_terms": [(t, float(w)) for t, w in top_terms],
        "mcount": int(mcount),
        "qcount": int(qcount),
    }

def score_similarity(query_text: str, cand_text: str) -> float:
    qt = tokenize(query_text)
    ct = tokenize(cand_text)
    if not qt or not ct:
        return 0.0
    qset = set(qt)
    cset = set(ct)
    kw_overlap = len(qset & cset)
    bi_overlap = len(bigrams(qt) & bigrams(ct))
    
    return float(kw_overlap) + (2.0 * float(bi_overlap))



def tokenize(s: str) -> list[str]:
    s = normalize_text(s)
    toks = [t.strip("'") for t in s.split() if t and t not in STOPWORDS]
    # light normalization for KJV-ish endings (very basic)
    toks2 = []
    for t in toks:
        if t.endswith("eth") and len(t) > 4:
            toks2.append(t[:-3])  # loveth -> lov
        elif t.endswith("est") and len(t) > 4:
            toks2.append(t[:-3])
        else:
            toks2.append(t)
    return toks2

def bigrams(tokens: list[str]) -> set[str]:
    return set(f"{tokens[i]} {tokens[i+1]}" for i in range(len(tokens)-1))

def top_overlap_keywords(a_tokens: list[str], b_tokens: list[str], k: int = 4) -> list[str]:
    a = Counter(a_tokens)
    b = Counter(b_tokens)
    overlap = {w: min(a[w], b[w]) for w in a.keys() & b.keys()}
    # prioritize “rarer” feel by using overlap count then length
    ranked = sorted(overlap.keys(), key=lambda w: (overlap[w], len(w)), reverse=True)
    return ranked[:k]



def build_idf(texts: list[str]) -> dict[str, float]:
    """
    Build a lightweight IDF over a local pool of texts.
    IDF = log((N+1)/(df+1)) + 1
    """
    df: dict[str, int] = {}
    N = 0
    for t in texts:
        if not t:
            continue
        toks = set(meaningful_tokens(t))
        if not toks:
            continue
        N += 1
        for w in toks:
            df[w] = df.get(w, 0) + 1

    if N == 0:
        return {}

    idf = {}
    for w, d in df.items():
        idf[w] = math.log((N + 1.0) / (d + 1.0)) + 1.0
    return idf


def weighted_overlap_score(query_text: str, cand_text: str, idf: dict[str, float]) -> tuple[float, list[tuple[str, float]], int, int]:
    """
    Returns:
      score,
      top_terms [(token, contrib), ...],
      matched_count,
      query_meaningful_count
    """
    q = meaningful_tokens(query_text)
    c = set(meaningful_tokens(cand_text))
    if not q or not c:
        return 0.0, [], 0, len(q)

    # count matches (unique on candidate side)
    matched = [t for t in q if t in c]
    matched_uniq = list(dict.fromkeys(matched))  # preserve order, unique

    # additive weighted score
    contribs: list[tuple[str, float]] = []
    score = 0.0

    for t in matched_uniq:
        w = float(idf.get(t, 1.0))

        t0 = t  # already normalized by tokenize(), safe hook for future stemming

        if t0 in LOW_VALUE_TOKENS:
            w *= LOW_VALUE_MULT
        elif t0 in COMMON_THEOLOGY_TOKENS:
            w *= COMMON_THEOLOGY_MULT

        score += w
        contribs.append((t0, w))
    
    # small phrase echo bonus (bigrams)
    qtoks = tokenize(query_text)
    ctoks = tokenize(cand_text)
    shared_phr = bigrams(qtoks) & bigrams(ctoks)
    if shared_phr:
        score += 1.5  # modest bump; keeps old behavior mostly intact

    
    # Length normalization so very long verses don't dominate
    cand_len = max(1, len(set(ctoks)))
    score = score / (cand_len ** 0.35)

    contribs.sort(key=lambda x: x[1], reverse=True)
    return score, contribs[:5], len(set(matched_uniq)), len(set(q))



def meaningful_tokens(text: str) -> list[str]:
    toks = tokenize(text)  # uses your existing tokenize()
    return [t for t in toks if t not in STOPWORDS and len(t) >= 3]


# =========================
# Section 4: Loaders
# =========================
# Move ALL load_* functions + load_dir_csvs here

def load_dir_csvs(dir_path: Path, prefix: str) -> dict[tuple[str, int, int], str]:
    """
    Loads Data/<corpus>/<prefix>_*.csv where each row has:
      book, chapter, verse, text
    Returns dict keyed by (book, chapter, verse) -> text
    """
    data: dict[tuple[str, int, int], str] = {}
    if not dir_path.exists():
        return data

    for csv_path in sorted(dir_path.glob(f"{prefix}_*.csv")):
        with csv_path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                book = (row.get("book") or "").strip()
                if not book:
                    continue
                try:
                    ch = int(row.get("chapter"))
                    vs = int(row.get("verse"))
                except Exception:
                    continue
                text = (row.get("text") or "").strip()
                if not text:
                    continue
                data[(book, ch, vs)] = text

    return data




def load_corpus_books(dir_path: Path, prefix: str, corpus_label: str) -> dict[tuple[str, int, int], str]:
    data: dict[tuple[str, int, int], str] = {}
    if not dir_path.exists():
        return data

    for csv_path in dir_path.glob(f"{prefix}_*.csv"):
        with csv_path.open("r", encoding="utf-8", newline="") as f:
            # --- allow very large fields (Lost Books, etc.) ---
            import csv as _csv
            _csv.field_size_limit(50_000_000)

            reader = _csv.DictReader(f)
            for row in reader:
                if row.get("corpus", "").strip() != corpus_label:
                    continue
                book = row["book"].strip()
                ch = int(row["chapter"])
                vs = int(row["verse"])
                text = (row.get("text") or "").strip()
                if text:
                    data[(book, ch, vs)] = text
    return data



def load_kjv_ot_strongs_from_tsv(path: Path) -> dict[tuple[str,int,int], list[str]]:
    """
    Reads the KJV-OT-mapped-to-BHS tab-delimited file and returns:
      {(BookName, ch, v): ["H####", ...]}
    Expected columns:
      0=verse_id, 1=book_num, 2=chapter, 3=verse, 4=tagged KJV text
    """
    idx: dict[tuple[str,int,int], list[str]] = {}
    if not path.exists():
        return idx

    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f, delimiter="\t")

        for i, row in enumerate(reader):
            if len(row) < 5:
                continue

            # row[0] is verse_id (ignore)
            try:
                book_num = int(row[1])
                ch = int(row[2])
                vs = int(row[3])
            except ValueError:
                continue

            if not (1 <= book_num <= 39):
                continue

            book = OT_BOOKS[book_num - 1]
            tagged = row[4]

            codes = _STRONGS_RE.findall(tagged)
            if codes:
                # dedupe while preserving order
                seen = set()
                codes = [c for c in codes if not (c in seen or seen.add(c))]
                idx[(book, ch, vs)] = codes

            if DEBUG and i == 0:
                print("[DEBUG] Strong’s OT first row:", row[:5])
                print("[DEBUG] Strong’s OT first codes:", codes[:10] if codes else None)

    return idx



def load_strongs_lexicon(csv_path: Path) -> dict[str, dict]:
    """
    Returns:
      {
        "H7225": {
            "lemma": "רֵאשִׁית",
            "xlit": "rê'shı̂yth",
            "pronounce": "ray-sheeth'",
            "definition": "beginning"
        },
        ...
      }
    """
    lex = {}
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            code = row["number"].strip()
            lex[code] = {
                "lemma": row.get("lemma", "").strip(),
                "xlit": row.get("xlit", "").strip(),
                "pronounce": row.get("pronounce", "").strip(),
                "definition": row.get("description", "").strip(),
            }
    return lex




def load_bible_csv(filepath: Path) -> Dict[Tuple[str, int, int], str]:
    filepath = Path(filepath)
    data: Dict[Tuple[str, int, int], str] = {}
    with filepath.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            book = row["book"].strip().lower()
            chapter = int(row["chapter"])
            verse = int(row["verse"])
            text = row["text"].strip()
            data[(book, chapter, verse)] = text
    return data
    
    
def load_kjv_numeric_csv(filepath: Path) -> dict[tuple[str, int, int], str]:
    """
    Loads a KJV CSV with columns like: id,b,c,v,t
    Where:
      b = book number (1..66)
      c = chapter
      v = verse
      t = text
    Returns a dict keyed by (book_name, chapter, verse) -> text
    """

    data: dict[tuple[str, int, int], str] = {}
    if not filepath.exists():
        return data

    with filepath.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # tolerate a couple header variations (just in case)
            b_raw = row.get("b") or row.get("book") or row.get("Book")
            c_raw = row.get("c") or row.get("chapter") or row.get("Chapter")
            v_raw = row.get("v") or row.get("verse") or row.get("Verse")
            t_raw = row.get("t") or row.get("text") or row.get("Text")

            if not (b_raw and c_raw and v_raw and t_raw):
                continue

            try:
                b = int(str(b_raw).strip())
                c = int(str(c_raw).strip())
                v = int(str(v_raw).strip())
            except ValueError:
                continue

            if b < 1 or b > 66:
                continue

            book = BOOKS_66[b - 1]
            text = str(t_raw).strip()
            data[(book, c, v)] = text

    return data
    

def load_apocrypha_books(apoc_dir: Path) -> dict[tuple[str, int, int], str]:
    """
    Loads all apocrypha_*.csv files from a directory into a single lookup table.
    Key: (book, chapter, verse)
    Value: verse text
    """

    data: dict[tuple[str, int, int], str] = {}

    if not apoc_dir.exists():
        return data

    for csv_path in apoc_dir.glob("apocrypha_*.csv"):
        with csv_path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("corpus", "").strip() != "Apocrypha":
                    continue

                book = row["book"].strip()
                chapter = int(row["chapter"])
                verse = int(row["verse"])
                text = row["text"].strip()

                data[(book, chapter, verse)] = text

    return data


def load_quran_csv(filepath: Path) -> dict[tuple[str, int, int], str]:
    """
    Loads Quran CSV with columns:
      id,surahs,ayahs,ayahs-translation
    Stores as ("Quran", surah, ayah) -> text
    """
    data: dict[tuple[str, int, int], str] = {}
    if not filepath.exists():
        return data

    with filepath.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                s = int(str(row["surahs"]).strip())
                a = int(str(row["ayahs"]).strip())
                text = str(row["ayahs-translation"]).strip()
            except Exception:
                continue

            if text:
                data[("Quran", s, a)] = text

    return data

    

def load_jasher_csv(path: str) -> dict[tuple, str]:
    data: dict[tuple, str] = {}

    chapter = None
    verse = None
    buf: list[str] = []

    def flush():
        nonlocal chapter, verse, buf
        if chapter is None or verse is None or not buf:
            return
        key = ("Jasher", int(chapter), int(verse))
        text = " ".join(s.strip() for s in buf if s.strip())
        data[key] = text
        buf = []

    with open(path, "r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row:
                continue
            line = (row[0] or "").strip()
            if not line or line.startswith("#"):
                continue

            m_ch = _RE_JASHER_CH.match(line)
            if m_ch:
                flush()
                chapter = int(m_ch.group(1))
                verse = None
                buf = []
                continue

            m_v = _RE_VERSE.match(line)
            if m_v and chapter is not None:
                flush()
                verse = int(m_v.group(1))
                buf = [m_v.group(2).strip()]
                continue

            # Continuation line
            if chapter is not None and verse is not None:
                buf.append(line)

    flush()
    return data




def load_wisdom_csv(path: str) -> dict[tuple[str, int, int], str]:
    """
    Loads Wisdom of Solomon from the Scriptural-Truth style 'CSV' (actually text)
    with verse markers like {1:1}. Produces tuple keys: ('Wisdom', chapter, verse).
    """
    wisdom: dict[tuple[str, int, int], str] = {}

    cur_key: tuple[str, int, int] | None = None
    cur_parts: list[str] = []

    def flush():
        nonlocal cur_key, cur_parts
        if cur_key and cur_parts:
            text = " ".join(cur_parts)
            text = text.replace('"', "").strip()
            text = re.sub(r"\s+", " ", text)
            wisdom[cur_key] = text
        cur_key = None
        cur_parts = []

    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for raw_line in f:
            line = raw_line.strip()

            if not line:
                continue
            if line.lower().startswith("page"):
                continue
            if "www.scriptural-truth.com" in line.lower():
                continue
            if line.lower().startswith("apocrypha"):
                continue

            m = WISDOM_VERSE_RE.search(line)
            if m:
                flush()
                ch = int(m.group(1))
                vs = int(m.group(2))

                # Tuple key (normalized internal format)
                cur_key = ("Wisdom", ch, vs)

                after = line[m.end():].strip()
                after = after.lstrip("}").strip()
                if after:
                    cur_parts.append(after)
                continue

            if cur_key:
                cur_parts.append(line)

    flush()
    return wisdom


def load_structured_csv(path: Path) -> dict[tuple[str, int, int], str]:
    """
    Loads a single CSV with columns:
      book,chapter,verse,text
    Returns:
      {(book, chapter, verse): text}
    """
    
    data: dict[tuple[str, int, int], str] = {}
    if not path.exists():
        return data

    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            book = (row.get("book") or "").strip()
            text = (row.get("text") or "").strip()
            if not book or not text:
                continue

            try:
                ch = int((row.get("chapter") or "").strip())
                vs = int((row.get("verse") or "").strip())
            except Exception:
                continue

            data[(book, ch, vs)] = text

    return data



def _set_csv_field_limit():
    """
    Raise CSV field size limit so large corpus text cells do not crash loading.
    """
    try:
        csv.field_size_limit(10_000_000)
    except OverflowError:
        csv.field_size_limit(1_000_000)


def _slugify_corpus_name(name: str) -> str:
    """
    Folder name -> engine corpus key
    Example:
      '1_enoch' -> '1_enoch'
      'Letter Of Aristeas' -> 'letter_of_aristeas'
      'patriarchs' -> 'patriarchs'
    """
    s = (name or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


def _find_structured_csvs(dir_path: Path) -> list[Path]:
    """
    Return candidate structured CSV files in a corpus folder.
    Ignores _raw and obvious helper files.
    """
    out: list[Path] = []
    if not dir_path.exists() or not dir_path.is_dir():
        return out

    for p in sorted(dir_path.glob("*.csv")):
        name = p.name.lower()

        # skip helper / non-engine files
        if name.startswith("_"):
            continue
        if name in {
            "raw.csv",
            "source.csv",
        }:
            continue

        out.append(p)

    return out


def _looks_like_structured_corpus_csv(path: Path) -> bool:
    """
    True if CSV header contains the normalized corpus schema:
      book, chapter, verse, text
    """
    try:
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            header = next(reader, [])
    except Exception:
        return False

    header_norm = {str(h).strip().lower() for h in header}
    required = {"book", "chapter", "verse", "text"}
    return required.issubset(header_norm)


def load_auto_corpus_dir(dir_path: Path) -> dict[tuple[str, int, int], str]:
    """
    Load every structured CSV in a corpus folder and merge them.
    Safe for corpora already normalized by validate_corpus.py.
    """
    data: dict[tuple[str, int, int], str] = {}

    for csv_path in _find_structured_csvs(dir_path):
        if not _looks_like_structured_corpus_csv(csv_path):
            continue

        try:
            rows = load_structured_csv(csv_path)
        except Exception as e:
            print(f"[DISCOVERY-ERROR] failed loading {csv_path}: {e}")
            continue

        if rows:
            data.update(rows)

    return data


def discover_auto_corpora(
    data_dir: Path,
    *,
    exclude: set[str] | None = None,
    debug: bool = False,
) -> dict[str, dict[tuple[str, int, int], str]]:
    """
    Auto-discovers Data/<corpus>/ folders and loads normalized structured CSVs.
    Only intended for corpora that do NOT need a custom loader.
    """
    exclude = {x.lower() for x in (exclude or set())}
    discovered: dict[str, dict[tuple[str, int, int], str]] = {}

    if not data_dir.exists():
        return discovered

    for child in sorted(data_dir.iterdir()):
        if not child.is_dir():
            continue

        folder_name = child.name
        folder_key = _slugify_corpus_name(folder_name)

        if folder_name.lower().startswith("_"):
            continue
        if folder_key in exclude:
            continue

        corpus_rows = load_auto_corpus_dir(child)
        if corpus_rows:
            discovered[folder_key] = corpus_rows
            if debug:
                print(f"[DISCOVERY] loaded {folder_key}: {len(corpus_rows)} verses from {child}")

    return discovered



# =========================
# Section 5: Xrefs/topics/index
# =========================
# Move load_xrefs, load_topics, build_topic_index, get_refs_for_topic, etc.
def load_xrefs(filepath: Path) -> Dict[str, List[str]]:
    with filepath.open("r", encoding="utf-8") as f:
        raw = json.load(f)
    return {normalize_ref(k): [normalize_ref(r) for r in v] for k, v in raw.items()}
    
    
def load_topics(filepath: Path) -> Dict[str, List[str]]:
    if not filepath.exists():
        return {}
    with filepath.open("r", encoding="utf-8") as f:
        raw = json.load(f)
    return {k.strip(): [normalize_ref(r) for r in v] for k, v in raw.items()}
    

def build_topic_index(topics: dict[str, list[str]]) -> dict[str, set[str]]:
    idx: dict[str, set[str]] = {}
    for topic, refs in topics.items():
        for r in refs:
            nr = normalize_ref(r)
            idx.setdefault(nr, set()).add(topic)
    return idx

# =========================
# Section 6: Lookup + corpus utils
# =========================
# Move lookup_text_any, lookup_text_flexible (if still used), infer_corpus, book_of_ref, iter_all_verses_multi, etc.


def infer_corpus(ref: str) -> str:
    r = normalize_ref(ref).lower()
    if r.startswith("quran:"):
        return "Quran"
    if r.startswith("jasher"):
        return "Jasher"
    first = r.split()[0] if r.split() else ""
    if first in {"wisdom", "sirach"}:
        return "Apocrypha"
    return "Bible"



def lookup_text_flexible(table: dict, ref: str) -> str | None:
    """
    Lookup verse text from either:
      A) dict keyed by "Book Chapter:Verse" strings, or
      B) dict keyed by (book, chapter, verse) tuples like ('Leviticus', 23, 32),
      C) dict keyed by ('Quran', surah, ayah) tuples.
    """
    ref = normalize_ref(ref)

    # --- If tuple-keyed corpus (KJV numeric, Apoc books, Quran CSV) ---
    if isinstance(table, dict) and table:
        # Peek one key to detect schema
        k0 = next(iter(table.keys()))

        # Quran tuple keys: ("Quran", s, a)
        if isinstance(k0, tuple) and len(k0) == 3 and str(k0[0]).lower() == "quran":
            # Accept "Quran:1:1"
            if ref.lower().startswith("quran:"):
                parts = ref.split(":")
                if len(parts) == 3 and parts[1].isdigit() and parts[2].isdigit():
                    return table.get(("Quran", int(parts[1]), int(parts[2])))
            return None

        # Bible/Apoc tuple keys: (Book, ch, vs)
        if isinstance(k0, tuple) and len(k0) == 3:
            tkey = parse_bible_ref_to_tuple(ref)
            if tkey is None:
                return None
            return table.get(tkey)

    # --- String-keyed corpus path ---
    t = table.get(ref)
    if t:
        return t

    nref = normalize_ref(ref)
    if nref != ref:
        t = table.get(nref)
        if t:
            return t

    return None



def lookup_text(bible: Dict[Tuple[str, int, int], str], ref: str) -> Optional[str]:
    try:
        book, chapter, verse = parse_ref(ref)
    except ValueError:
        return None
    return bible.get((book.lower(), chapter, verse))
    
    
def lookup_bible_text(primary: dict, fallback: dict, ref: str) -> str | None:
    t = lookup_text_flexible(primary, ref)
    if t:
        return t
    return lookup_text_flexible(fallback, ref)






def lookup_text_any(corpora: dict, ref: str) -> tuple[str | None, str | None]:
    ref_n = normalize_ref(ref)

    # Letter of Jeremiah numbering normalization
    if ref_n.lower().startswith("letter of jeremiah 1:"):
        ref_n = re.sub(r"(?i)^letter of jeremiah 1:", "Letter of Jeremiah 6:", ref_n)

    low = ref_n.lower()

    # --- Quran special handling ---
    if low.startswith("quran"):
        quran = corpora.get("quran")
        if not quran:
            return None, None

        for qref in quran_ref_variants(ref_n):
            hit = lookup_text_flexible(quran, qref)
            if hit:
                return hit, "quran"

            m = re.match(r"(?i)^quran[:\s]+(\d+)\s*:\s*(\d+)\s*$", qref.strip())
            if m and isinstance(quran, dict):
                tkey = ("Quran", int(m.group(1)), int(m.group(2)))
                hit2 = quran.get(tkey)
                if hit2:
                    return hit2, "quran"

        return None, None

    # --- Jasher special handling ---
    if low.startswith("jasher"):
        jasher = corpora.get("jasher")
        if not jasher:
            return None, None
        tkey = parse_jasher_ref_to_tuple(ref_n)
        hit = jasher.get(tkey) if tkey else None
        return (hit, "jasher") if hit else (None, None)

    # --- Generic tuple lookup for all other corpora ---
    m = re.match(r"^\s*(?P<book>.+?)\s+(?P<ch>\d+)\s*:\s*(?P<v>\d+)\s*$", ref_n)
    if not m:
        return None, None

    tkey = (m.group("book").strip(), int(m.group("ch")), int(m.group("v")))

    # Prefer apoc before kjv if overlapping alias space ever occurs
    preferred_order = ["apoc", "kjv"]
    seen = set()

    for name in preferred_order:
        corpus = corpora.get(name)
        if isinstance(corpus, dict):
            hit = corpus.get(tkey)
            if hit:
                return hit, name
            seen.add(name)

    for name, corpus in corpora.items():
        if name in seen:
            continue
        if isinstance(corpus, dict):
            hit = corpus.get(tkey)
            if hit:
                return hit, name

    return None, None




def iter_all_verses_from_kjv(kjv: dict) -> list[tuple[str, str]]:
    return [(f"{book} {ch}:{vs}", text) for (book, ch, vs), text in kjv.items()]



def iter_all_verses(bible_primary: dict, bible_fallback: dict) -> list[tuple[str, str]]:
    """
    Returns list of (ref_string, text) for every verse we can read.
    Prefers primary text if present; otherwise fallback.
    """
    # This assumes fallback is tuple-keyed (book, ch, vs) -> text like your numeric KJV loader.
    # We'll build ref strings from tuple keys.
    out: list[tuple[str, str]] = []

    # Add all fallback verses first (full Bible)
    for (book, ch, vs), text in bible_fallback.items():
        ref = f"{book} {ch}:{vs}"
        out.append((ref, text))

    # Overlay primary (curated) if you want it to override text for same refs
    # (Optional: usually not needed for similarity)
    # for ref, text in bible_primary.items(): ...

    return out






def iter_all_verses_multi(corpora: dict) -> list[tuple[str, str, str]]:
    """
    Returns a flat list of (ref, text, corpus_name) for similarity search.
    Ensures ref and text are strings (no tuple keys/values).
    """
    out: list[tuple[str, str, str]] = []

    def key_to_ref(k) -> str:
        if isinstance(k, tuple) and len(k) == 3:
            b, ch, vs = k
            return f"{b} {ch}:{vs}"
        return str(k)

    def val_to_text(v) -> str:
        if v is None:
            return ""
        if isinstance(v, tuple):
            v = v[0]
        return str(v)

    #for corpus_name in ("kjv", "apoc", "quran", "jasher"):
    for corpus_name, corpus in corpora.items():
        d = corpora.get(corpus_name) or {}
        for k, v in d.items():
            ref = normalize_ref(key_to_ref(k))
            txt = val_to_text(v).strip()
            if txt:
                out.append((ref, txt, corpus_name))

    return out


def inspect_corpus(corpus_dict, corpus_name):
    print(f"\n--- Inspecting corpus: {corpus_name} ---")

    try:
        sample_key = next(iter(corpus_dict.keys()))
        print("[DEBUG] Sample key:", sample_key, "| type =", type(sample_key).__name__)
    except StopIteration:
        print("[DEBUG] Corpus is empty.")
        return

    chapters = {}
    books = set()
    skipped = 0

    for key in corpus_dict.keys():
        book = ch = vs = None

        # Case 1: tuple keys
        if isinstance(key, tuple):
            if len(key) == 3:
                book, ch, vs = key
            elif len(key) == 4:
                _corp, book, ch, vs = key
            else:
                skipped += 1
                continue

        # Case 2: string keys like "1 Enoch 81:5"
        elif isinstance(key, str):
            m = _REF_RE.match(key)
            if not m:
                skipped += 1
                continue
            book = m.group("book").strip()
            ch = int(m.group("ch"))
            vs = int(m.group("v"))

        else:
            skipped += 1
            continue

        book = str(book).strip()
        books.add(book)
        chapters.setdefault(book, {}).setdefault(int(ch), []).append(int(vs))

    print("Books found:", sorted(books))

    for book in sorted(chapters):
        ch_nums = sorted(chapters[book])
        missing = [c for c in range(min(ch_nums), max(ch_nums) + 1) if c not in set(ch_nums)]
        print(f"  Missing chapters ({len(missing)}): {missing[:20]}{' ...' if len(missing) > 20 else ''}")

        print(f"\n{book}")
        print(f"  Chapters: {min(ch_nums)} → {max(ch_nums)} (total {len(ch_nums)})")
        last_ch = max(ch_nums)
        print(f"  Last chapter ({last_ch}) max verse: {max(chapters[book][last_ch])}")

    if skipped:
        print(f"\n[DEBUG] Skipped {skipped} entries that didn't match expected ref formats.")


def ensure_min_precepts(
    *,
    query_ref: str,
    precepts: list[str],
    min_needed: int,
    min_books: int,
    xrefs: dict[str, list[str]],
    corpora: dict,
    all_verses,
    strict_no_pad: bool = False,
    near_miss_k: int = 0,          # NEW
    idf_global: dict[str, float] | None = None,
):
    # --- NEVER allow the verse to reference itself ---
    precepts = [p for p in (precepts or []) if p and p != query_ref]
    
    
    def _nref(r: str) -> str:
        return normalize_ref(r or "")

    def _bucket(r: str) -> str:
        r = _nref(r)
        if r.lower().startswith("quran:"):
            parts = r.split(":")
            return f"Quran:{parts[1]}" if len(parts) > 1 else "Quran"
        toks = r.split()
        return " ".join(toks[:-1]) if len(toks) >= 2 else r

    near_misses: list[tuple[str, str]] = []   # ✅ must exist before any return

    # Start from existing mapped precepts (normalized + deduped)
    final_precepts = [_nref(r) for r in precepts if _nref(r)]
    seen = set()
    final_precepts = [r for r in final_precepts if not (r in seen or seen.add(r))]

    orig_set = set(final_precepts)
    exclude = set(final_precepts)

    def _try_add(r: str) -> bool:
        r = _nref(r)
        if not r or r in exclude:
            return False
        final_precepts.append(r)
        exclude.add(r)
        return True

    # If already enough, done
    if len(final_precepts) >= min_needed:
        return final_precepts[:min_needed], [], near_misses
    
    credible_found = False

    # --- Debug remove later
    print("FALLBACK TRIGGERED?", credible_found)
    print("POOL SIZE:", len(all_verses))


    # Similarity fallback
    #near_misses: list[tuple[str, str]] = []

    q_text, _ = lookup_text_any(corpora, query_ref)
    if q_text:
        sim_refs = find_similar_refs(query_ref, q_text, all_verses, exclude, top_n=200)
        
        # --- Debug remove later
        print("SIM_REFS returned:", len(sim_refs))
        print("SIM_REFS sample:", sim_refs[:5])
        
        
        # --- build anchors first ---
        anchors: set[str] = set()
        q_meaning = meaningful_tokens(q_text)
        for t in q_meaning:
            if t in JUNK_ANCHORS:
                continue
            anchors.add(t)
            if len(anchors) >= 6:
                break
        
        
        # ✅ expand anchors with light synonyms (keeps engine “smart” without AI)
        expanded = set(anchors)
        for a in list(anchors):
            expanded |= ANCHOR_SYNONYMS.get(a, set())
        anchors = expanded
        
        
                
        # --- derive strong anchors (concept-heavy tokens) ---
        strong_anchors = {
            t for t in anchors
            if t not in LOW_VALUE_TOKENS
            and t not in COMMON_THEOLOGY_TOKENS
        }
                
        # --- ALWAYS collect near misses (before filtering) ---
        #k = near_miss_k or NEAR_MISS_K_DEFAULT
        #for cand in sim_refs:
            #c_text, _ = lookup_text_any(corpora, cand)
            #if not c_text:
                #continue

            #c_meaning = set(meaningful_tokens(c_text))
            #overlap = anchors & c_meaning if anchors else set()

            # store ref + quick reason
            #near_misses.append((_nref(cand), f"overlap={len(overlap)}"))

            #if len(near_misses) >= k:
                #break

        creation_anchors = {"create", "created", "likeness"}
        query_meaning = set(q_meaning) if q_text else set()
        is_creation_query = bool(query_meaning & creation_anchors)

        # --- Relevance gate filtering ---
        if anchors:
            original = list(sim_refs)

            # If anchors are small, don’t demand 2 overlaps (NT commandment verses get hurt here)
            min_overlap = 2 if len(anchors) >= 6 else 1

            # synonym-extended anchors (compute once)
            anchors_x = expand_tokens_with_synonyms(anchors)

            filtered = []
            for cand in sim_refs:
                c_text, _ = lookup_text_any(corpora, cand)
                if not c_text:
                    continue

                c_meaning = set(meaningful_tokens(c_text))
                c_meaning_x = expand_tokens_with_synonyms(c_meaning)

                overlap = anchors_x & c_meaning_x
                if len(overlap) < min_overlap:
                    continue

                if is_creation_query and not (c_meaning_x & creation_anchors):
                    continue

                filtered.append(cand)

            # ✅ Critical: never allow gating to zero-out everything
            sim_refs = filtered if filtered else original

        # --- Debug remove later ---
        print("SIM_REFS after gating:", len(sim_refs))
                
        # --- Rerank by rarity-weighted overlap (minimal relevance upgrade) ---
        # Build IDF over local pool: query + candidate texts we can resolve
        pool_texts = []
        if q_text:
            pool_texts.append(q_text)

        cand_text_map: dict[str, str] = {}
        for r in sim_refs:
            txt, _ = lookup_text_any(corpora, r)
            if txt:
                cand_text_map[r] = txt
                pool_texts.append(txt)

        #idf = build_idf(pool_texts)
        idf = idf_global or build_idf(pool_texts)
        
        # --- Core anchors: content-heavy tokens (prevents "suffer"/"not" junk matches) ---
        oov = float(idf.get("__OOV__", 1.0))

        # Prefer strong_anchors if available; else fall back to anchors_x/anchors
        base = strong_anchors if "strong_anchors" in locals() and strong_anchors else (
            set(anchors_x) if "anchors_x" in locals() else set(anchors)
        )

        # Remove broad/noisy tokens from core consideration
        base = {t for t in base if t not in BROAD_ACTION_TOKENS and t not in {"not", "nor", "neither"}}

        # If we filtered too hard, fall back to strong_anchors without BROAD_ACTION_TOKENS
        if not base and "strong_anchors" in locals():
            base = {t for t in strong_anchors if t not in {"not","nor","neither"}}

        # Pick top 3 by IDF
        anchors_u = list(dict.fromkeys(list(base)))
        anchors_u.sort(key=lambda t: float(idf.get(t, oov)), reverse=True)
        core_anchors = set(anchors_u[:3])

        scored = []
        
        for r in sim_refs:
            txt = cand_text_map.get(r)
            if not txt or not q_text:
                continue
            s, top_terms, mcount, qcount = weighted_overlap_score(q_text, txt, idf)
            scored.append((s, r, top_terms, mcount, qcount))
            
            
            
        if scored:
            scored.sort(key=lambda x: x[0], reverse=True)

            # --- Credibility floor ---
            scored = [x for x in scored if x[0] >= MIN_FALLBACK_SCORE]

            sim_refs = [r for _, r, *_ in scored]

        
        
        # --- Near misses: from filtered sim_refs, excluding chosen refs ---
        k = near_miss_k or NEAR_MISS_K_DEFAULT
        if k:
            near_misses = []
            chosen_now = set(final_precepts)

            # Build IDF using query + first chunk of sim_refs for stable weights
            pool_texts = [q_text] if q_text else []
            cand_text_map: dict[str, str] = {}
            for rr in sim_refs[:200]:
                tt, _ = lookup_text_any(corpora, rr)
                if tt:
                    cand_text_map[rr] = tt
                    pool_texts.append(tt)
            idf = idf_global or build_idf(pool_texts)

            for cand in sim_refs:
                cand_n = _nref(cand)
                if not cand_n or cand_n in chosen_now:
                    continue

                c_text = cand_text_map.get(cand) or (lookup_text_any(corpora, cand_n)[0])
                if not c_text or not q_text:
                    continue

                # keep your anchor overlap count
                c_meaning = set(meaningful_tokens(c_text))
                overlap = anchors & c_meaning if anchors else set()

                # new weighted score + top terms
                s, top_terms, mcount, qcount = weighted_overlap_score(q_text, c_text, idf)
                top_str = ", ".join([f"{t}(+{w:.2f})" for t, w in top_terms[:3]]) if top_terms else "—"
                why = f"score={s:.2f} | overlap={len(overlap)} | top={top_str} | match={mcount}/{qcount}"

                near_misses.append((cand_n, why))
                if len(near_misses) >= k:
                    break





        credible_found = bool(sim_refs)

        if not credible_found:
            return final_precepts, [], near_misses

        # STRICT MODE: do not pad, do not force-fill
        if strict_no_pad:
            return final_precepts, [], near_misses

        used = {_bucket(r) for r in final_precepts}
        
        
        def _corpus_of_ref(ref: str) -> str:
            ref = _nref(ref)
            # cheap inference by book name
            parts = ref.split()
            book = " ".join(parts[:-1]).lower() if len(parts) >= 2 else ref.lower()
            if book == "shepherd of hermas":
                return "hermas"
            if book == "jasher":
                return "jasher"
            if book == "quran":
                return "quran"
            # treat apoc books as apoc bucket (you can expand this list later)
            if book in {
                "tobit","judith","baruch","sirach","wisdom",
                "1 maccabees","2 maccabees","1 esdras","2 esdras",
                "additions to esther","letter of jeremiah","prayer of manasseh",
                "bel and the dragon","susanna"
            }:
                return "apoc"
            return "bible"  # default

        # Cap ONLY for fallback padding (not for direct xrefs)
        FALLBACK_CORPUS_CAP = {"hermas": 1}  # tune later if desired
        picked_corpus_counts = {}

        def _can_take_fallback(ref: str) -> bool:
            c = _corpus_of_ref(ref)
            picked_corpus_counts[c] = picked_corpus_counts.get(c, 0)
            cap = FALLBACK_CORPUS_CAP.get(c)
            if cap is None:
                return True
            return picked_corpus_counts[c] < cap

        def _mark_taken(ref: str) -> None:
            c = _corpus_of_ref(ref)
            picked_corpus_counts[c] = picked_corpus_counts.get(c, 0) + 1
                
        
        
        # 1) Add from NEW buckets until we hit min_books (best effort)
        if len(used) < min_books:
            for cand in sim_refs:
                if len(final_precepts) >= min_needed:
                    break
                cand_n = _nref(cand)
                if not cand_n or cand_n in exclude:
                    continue
                if _bucket(cand_n) in used:
                    continue
                    
                c_text, _ = lookup_text_any(corpora, cand_n)
                if c_text and core_anchors:
                    c_tokens = set(meaningful_tokens(c_text))
                    if not (core_anchors & c_tokens):
                        continue
                    
                if _try_add(cand_n):
                    used.add(_bucket(cand_n))
                    if len(used) >= min_books:
                        break

        
        # 2) Fill remaining slots (with core-anchor gate)
        for cand in sim_refs:
            if len(final_precepts) >= min_needed:
                break

            cand_n = _nref(cand)
            if not cand_n or cand_n in exclude:
                continue

            # core-anchor gate: require at least 1 core anchor hit for longer/meaningful queries
            c_text, _ = lookup_text_any(corpora, cand_n)
            if c_text and core_anchors:
                c_tokens = set(meaningful_tokens(c_text))
                core_hits = len(core_anchors & c_tokens)

                # if query has a decent anchor set, don't allow 0-core-hit padding
                if core_hits == 0:
                    continue
                # --- implement and take out line above if fallback dries up to much --- #    
                #if core_hits == 0:
                    # allow only if very strong overall
                    #s, top_terms, mcount, qcount = weighted_overlap_score(q_text, c_text, idf)
                    #if not (mcount >= 3 and s >= (MIN_FALLBACK_SCORE + 2.0)):
                        #continue

            if not _can_take_fallback(cand_n):
                continue

            if _try_add(cand_n):
                _mark_taken(cand_n)

    # If no credible matches, return what we have (no padding)
    if not credible_found:
        return final_precepts, [], near_misses

    # Legacy filler if still short (disabled in strict mode)
    if (not strict_no_pad) and (len(final_precepts) < min_needed):
        for r in ["2 Timothy 3:16", "1 Corinthians 14:40", "Psalm 119:105"]:
            if len(final_precepts) >= min_needed:
                break
            _try_add(r)

    # Trim to exactly min_needed
    if not strict_no_pad:
        final_precepts = final_precepts[:min_needed]

    if near_misses:
        chosen_final = set(final_precepts)
        near_misses = [(r, why) for (r, why) in near_misses if r not in chosen_final]


    added = [r for r in final_precepts if r not in orig_set]
    
    final_precepts = [p for p in final_precepts if p and p != query_ref]
    
    return final_precepts, added, near_misses




# =========================
# Section 7: Core engine entrypoints
# =========================
def build_engine(
    *,
    BASE_DIR: Path | None = None,
    DATA_DIR: Path | None = None,
    BIBLE_CSV: Path | None = None,
    KJV_CSV: Path | None = None,
    QURAN_CSV: Path | None = None,
    XREFS_JSON: Path | None = None,
    xrefs_raw: dict[str, list[str]] | None = None,
    TOPICS_JSON: Path | None = None,
    CORPUS_REGISTRY: list | None = None,
    use_strongs: bool = False,
    debug: bool = False,
) -> Engine:
    """
    Loads everything needed to answer queries. Returns an Engine.
    No printing. No sys.argv. Safe for CLI + web/PWA.
    """

    # --- safe defaults (no NameError at import time) ---
    if BASE_DIR is None:
        BASE_DIR = Path(__file__).resolve().parent
    if DATA_DIR is None:
        DATA_DIR = BASE_DIR / "Data"
    if BIBLE_CSV is None:
        BIBLE_CSV = DATA_DIR / "bible" / "bible.csv"
    if KJV_CSV is None:
        KJV_CSV = DATA_DIR / "kjv" / "kjv.csv"
    if QURAN_CSV is None:
        QURAN_CSV = DATA_DIR / "quran" / "quran.csv"
    if XREFS_JSON is None:
        XREFS_JSON = BASE_DIR / "cross_refs.json"
    if TOPICS_JSON is None:
        TOPICS_JSON = BASE_DIR / "topics.json"
    if CORPUS_REGISTRY is None:
        # expects you already have CORPUS_REGISTRY defined at module scope
        CORPUS_REGISTRY = globals().get("CORPUS_REGISTRY", [])

    # --- wire debug + strongs flags into module globals (so helper funcs behave) ---
    global DEBUG
    DEBUG = bool(debug)

    corpora: dict[str, dict] = {}

    # --- core corpora ---
    bible = load_bible_csv(BIBLE_CSV)
    kjv   = load_kjv_numeric_csv(KJV_CSV)
    quran = load_quran_csv(QURAN_CSV)
    jasher = load_jasher_csv(str(DATA_DIR / "jasher" / "jasher.csv"))
    #patriarchs = load_structured_csv(DATA_DIR / "patriarchs" / "patriarchs.csv")
    
    # --- apocrypha (tuple-keyed) ---
    apoc = load_apocrypha_books(DATA_DIR / "apocrypha")

    # wisdom parser output must match apoc key-shape (tuple), so ensure tuple keys
    wisdom = load_wisdom_csv(str(DATA_DIR / "apocrypha" / "wisdom.csv"))
    # if wisdom returns "Wisdom 1:1" style strings, convert them to ("Wisdom",1,1)
    # if it already returns tuples, this will no-op.
    wisdom_tuple: dict[tuple[str, int, int], str] = {}
    for k, v in wisdom.items():
        if isinstance(k, tuple):
            wisdom_tuple[k] = v
        else:
            t = parse_bible_ref_to_tuple(str(k))
            if t:
                wisdom_tuple[t] = v
    apoc.update(wisdom_tuple)

    # sirach -> tuple keys
    sirach_rows = load_bible_csv(DATA_DIR / "apocrypha" / "sirach.csv")
    sirach_refdict = {
        ("Sirach", ch, vs): text
        for (book, ch, vs), text in sirach_rows.items()
        if text
    }
    apoc.update(sirach_refdict)

    corpora.update({
        "kjv": kjv,
        "apoc": apoc,
        "quran": quran,
        "jasher": jasher,
    })

    # Auto-discover normalized corpora in Data/<corpus>/
    # Keep special-loader folders excluded.
    auto_corpora = discover_auto_corpora(
        DATA_DIR,
        exclude={
            "bible",
            "kjv",
            "apocrypha",
            "quran",
            "jasher",
            "strongs",
        },
        debug=DEBUG,
    )

    for key, rows in auto_corpora.items():
        if key not in corpora:
            corpora[key] = rows

    # --- strongs (optional) ---
    strongs_ot_idx = None
    strongs_lex = None
    if use_strongs:
        strongs_ot_idx = load_kjv_ot_strongs_from_tsv(
            DATA_DIR / "strongs" / "kjv_ot_bhs.tsv"
        )
        strongs_lex = load_strongs_lexicon(
            DATA_DIR / "strongs" / "strongs_hebrew.csv"
        )

    all_verses = list(iter_all_verses_multi(corpora))
    
    
    # --- build global IDF ---
    print("[DEBUG] Building global IDF...") if DEBUG else None

    all_texts = []
    for corpus_dict in corpora.values():
        for _, txt in corpus_dict.items():
            if isinstance(txt, str) and txt.strip():
                all_texts.append(txt)

    global_idf = build_idf(all_texts)

    print(f"[DEBUG] Global IDF size: {len(global_idf)}") if DEBUG else None
        
        
    xrefs_raw = load_xrefs(XREFS_JSON)
    xrefs = xrefs_raw  # if you already symmetrize elsewhere, keep that as your xrefs
    xrefs = normalize_xrefs(xrefs)  

    PATCH_JSON = BASE_DIR / "xrefs_patch.json"
    if PATCH_JSON.exists():
        patch = load_xrefs(PATCH_JSON)  # reuse loader
        patch = normalize_xrefs(patch)
        # merge
        for k, vals in patch.items():
            xrefs.setdefault(k, [])
            xrefs[k].extend(vals)
        
    xrefs = make_xrefs_symmetric(xrefs, max_per_verse=200)
    
    if DEBUG:
        k = "Deuteronomy 24:16"
        print("DEBUG: xrefs has Deut 24:16 key?", k in xrefs)
        print("DEBUG: Deut 24:16 xrefs:", xrefs.get(k))
        
        print("XREFS for Deut 24:16:", xrefs.get("Deuteronomy 24:16"))
        print("XREFS for Ezek 18:19:", xrefs.get("Ezekiel 18:19"))
        
    topics = load_topics(TOPICS_JSON)
    topic_index = build_topic_index(topics)

    return Engine(
        corpora=corpora,
        all_verses=all_verses,
        xrefs=xrefs,
        xrefs_raw=xrefs_raw,
        topics=topics,
        topic_index=topic_index,
        strongs_ot_idx=strongs_ot_idx,
        strongs_lex=strongs_lex,
        bible_overrides=bible,
        idf=global_idf,
    )


def corpora_summary(eng: Engine) -> list[dict]:
    return [
        {"name": name, "verses": (len(corpus) if isinstance(corpus, dict) else 0)}
        for name, corpus in eng.corpora.items()
    ]
