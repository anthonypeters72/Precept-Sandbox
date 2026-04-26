# tools/normalize_inline_verses.py
from __future__ import annotations

import csv
import re
import sys
from pathlib import Path

START_RE = re.compile(r"^\s*(\d+)\s+(\d+)\s+(.*\S)\s*$")  # "ch vs text..."
INLINE_VS_RE = re.compile(r"^(.*?)(?:\s+)(\d{1,3})(?:\s+)(.*)$")  # "... 2 died ..."

# Strong: true header lines (usually "THE TESTAMENT OF LEVI")
HEADER_STRONG_RE = re.compile(r"\bTHE\s+TESTAMENT\s+OF\s+([A-Z][A-Z]+)\b", re.IGNORECASE)

# Weak: looser reference (keep for standalone lines that are just "TESTAMENT OF LEVI")
HEADER_WEAK_RE = re.compile(r"\bTESTAMENT\s+OF\s+([A-Z][A-Z]+)\b", re.IGNORECASE)



CANON_NAMES = {
    "symeon": "Simeon",
    "nephthali": "Naphtali",
    "naphtali": "Naphtali",
    "juda": "Judah",
    "issachar": "Issachar",
    "zabulon": "Zebulun",
    "zebulon": "Zebulun",
}


def clean_line(raw: str) -> str:
    s = raw.strip()

    # Remove wrapping quotes that appear in this file
    if s.startswith('"') and s.endswith('"'):
        s = s[1:-1].strip()

    # Remove trailing CSV artifact commas like: '... ",'
    s = s.rstrip().rstrip(",")

    # Collapse whitespace
    s = re.sub(r"\s+", " ", s).strip()
    return s

def split_inline_verses(text: str, current_vs: int) -> list[tuple[int, str]]:
    """
    Split a continuation line that may contain inline verse numbers:
      "... he 2 died ..." -> [(1, "... he"), (2, "died ...")]
    Heuristic: only treat a number as a verse marker if it is next or later verse.
    """
    parts: list[tuple[int, str]] = []
    buf = text

    while True:
        m = INLINE_VS_RE.match(buf)
        if not m:
            break

        left, n_str, right = m.group(1).strip(), m.group(2), m.group(3).strip()

        try:
            n = int(n_str)
        except ValueError:
            break

        # Heuristic: verse markers should move forward (usually +1)
        if n <= current_vs:
            break

        # Emit left as current verse continuation
        if left:
            parts.append((current_vs, left))

        # Start new verse at n with right as its initial content
        current_vs = n
        buf = right

    # Remainder belongs to current_vs
    if buf:
        parts.append((current_vs, buf))

    return parts

def main() -> int:
    if len(sys.argv) != 5:
        print("Usage: python tools/normalize_inline_verses.py <input> <output.csv> <corpus_label> <book_prefix>")
        print('Example: python tools/normalize_inline_verses.py Data/patriarchs/_raw/twelve_patriarchs.csv Data/patriarchs/patriarchs_twelve_patriarchs.csv Patriarchs "Testament of"')
        return 2

    inp = Path(sys.argv[1])
    outp = Path(sys.argv[2])
    corpus_label = sys.argv[3]
    book_prefix = sys.argv[4].strip()

    raw_lines = inp.read_text(encoding="utf-8", errors="replace").splitlines()

    rows: list[dict[str, str]] = []
    current_book = None
    current_ch = None
    current_vs = None
    current_text = ""

    def flush():
        nonlocal current_text
        if current_book and current_ch is not None and current_vs is not None:
            t = current_text.strip()
            if t:
                rows.append({
                    "corpus": corpus_label,
                    "book": current_book,
                    "chapter": str(current_ch),
                    "verse": str(current_vs),
                    "text": t
                })
        current_text = ""

    for raw in raw_lines:
        line = clean_line(raw)
        if not line:
            continue

        # Standalone header line like "THE TESTAMENT OF REUBEN"
        m_header = HEADER_STRONG_RE.search(line) or HEADER_WEAK_RE.search(line)
        if m_header:
            name = m_header.group(1).strip()
            canon = CANON_NAMES.get(name.lower(), name.title())
            current_book = f"{book_prefix} {canon}".strip()
            continue


        # New explicit chapter/verse start line: "1 1 ..."
        m = START_RE.match(line)
        if m:
            ch = int(m.group(1))
            vs = int(m.group(2))
            body = m.group(3).strip()
            
            # Header embedded inside verse body (this is what was missing)
            m2 = HEADER_STRONG_RE.search(body)
            if m2:
                name = m2.group(1).strip()
                canon = CANON_NAMES.get(name.lower(), name.title())
                current_book = f"{book_prefix} {canon}".strip()
                continue


        m_son = re.search(r"\b([A-Z][a-z]+)\b,\s+the\s+\w+\s+son\s+of\s+Jacob\b", body)
        if m_son:
            name = m_son.group(1).strip()
            canon = CANON_NAMES.get(name.lower(), name.title())
            current_book = f"{book_prefix} {canon}".strip()


            if current_book and body:
                rows.append({
                    "corpus": corpus_label,
                    "book": current_book,
                    "chapter": ch,
                    "verse": vs,
                    "text": body,
                })

            continue

        # (optional) If you support continuation lines, keep that logic below unchanged


            # Start new verse
            flush()
            current_ch = ch
            current_vs = vs
            current_text = body
            continue

        # Continuation line: may contain inline verse numbers
        if current_book and current_ch is not None and current_vs is not None:
            splits = split_inline_verses(line, current_vs)
            for vs_num, chunk in splits:
                if vs_num != current_vs:
                    # Verse changed mid-line
                    flush()
                    current_vs = vs_num
                    current_text = chunk
                else:
                    # Same verse continuation
                    if current_text:
                        current_text += " " + chunk
                    else:
                        current_text = chunk
            continue

        # If we still don't have a book yet, ignore preface noise
        # (This keeps strict relevance — no intro commentary in the verse stream.)

    flush()

    outp.parent.mkdir(parents=True, exist_ok=True)
    with outp.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["corpus", "book", "chapter", "verse", "text"])
        w.writeheader()
        w.writerows(rows)

    print(f"Wrote normalized: {outp} ({len(rows)} verses)")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
