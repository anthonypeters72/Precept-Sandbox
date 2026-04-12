#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path


SKIP_PREFIXES = (
    "This document was supplied for free educational purposes.",
    "Unless it is in the public domain",
    "or hosted on a webserver",
    "If you find it of help to you",
    "https://",
    "PayPal ",
    "TRANSLATIONS OF EARLY DOCUMENTS",
    "SERIES II",
    "HELLENISTIC-JEWISH TEXTS",
    "THE LETTER OF ARISTEAS",
    "THE",
    "TRANSLATED",
    "WITH AN APPENDIX",
    "BY",
    "SOCIETY FOR PROMOTING",
    "LONDON:",
    "NEW YORK:",
    "First Edition",
    "EDITORS' PREFACE",
)

START_MARKERS = (
    "INTRODUCTORY ADDRESS TO PHILOCRATES",
    "1. When I had received",
    "1 When I had received",
)

def normalize_whitespace(s: str) -> str:
    s = s.replace("\u00ad", "")
    s = s.replace("\ufeff", "")
    s = s.replace("\u2010", "-").replace("\u2011", "-").replace("\u2013", "-").replace("\u2014", "-")
    s = re.sub(r"[ \t]+", " ", s)
    return s.strip()


def clean_lines(text: str) -> list[str]:
    out = []
    started = False

    for raw in text.splitlines():
        line = normalize_whitespace(raw)
        if not line:
            continue

        if any(line.startswith(prefix) for prefix in SKIP_PREFIXES):
            continue

        # Start only at the real letter body
        if not started:
            if "When I had received" in line:
                started = True
            else:
                continue

        out.append(line)

    return out
    
    
    
def parse_aristeas(txt_path: Path):
    text = txt_path.read_text(encoding="utf-8", errors="ignore")
    lines = clean_lines(text)

    rows = []
    book = "Letter of Aristeas"
    chapter = 1
    verse = None
    current = []

    # section starts can look like:
    # 1 When I had received ...
    # 12 The high priest ...
    verse_pat = re.compile(r"^\(?(\d{1,3})\)?[\.\)]?\s+(.*)$")

    def flush():
        nonlocal current
        if verse is None:
            current = []
            return
        joined = normalize_whitespace(" ".join(current))
        if joined:
            rows.append((book, chapter, verse, joined))
        current = []

    def absorb_inline_sections(textline: str):
        nonlocal verse, current

        matches = list(re.finditer(r"(?<!\d)(\d{1,3})(?=\s+[A-Z\(\['\"])", textline))
        if not matches:
            current.append(textline)
            return

        first = True
        last_idx = 0

        for m in matches:
            num = int(m.group(1))
            idx = m.start()

            if first:
                prefix = textline[:idx].strip()
                if prefix:
                    current.append(prefix)
                    flush()
                    verse = num
                    current = []
                    last_idx = m.end()
                    first = False
                else:
                    current.append(textline)
                    return
            else:
                body = textline[last_idx:m.start()].strip()
                if body:
                    current.append(body)
                flush()
                verse = num
                current = []
                last_idx = m.end()

        tail = textline[last_idx:].strip()
        if tail:
            current.append(tail)

    for line in lines:
        m_v = verse_pat.match(line)
        if m_v:
            flush()
            verse = int(m_v.group(1))
            current = []
            rest = m_v.group(2).strip()
            absorb_inline_sections(rest)
            continue

        if verse is not None:
            absorb_inline_sections(line)

    flush()
    return rows

def write_csv(rows, out_path: Path):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["book", "chapter", "verse", "text"])
        w.writerows(rows)

def main():
    ap = argparse.ArgumentParser(description="Parse Letter of Aristeas txt into engine-friendly CSV.")
    ap.add_argument("txt", type=Path, help="Path to Aristeas txt file")
    ap.add_argument("--csv-out", required=True, type=Path, help="Output CSV path")
    args = ap.parse_args()

    rows = parse_aristeas(args.txt)

    # Keep only the actual letter sections
    rows = [r for r in rows if 1 <= r[2] <= 322]

    rows.sort(key=lambda r: (r[1], r[2]))
    write_csv(rows, args.csv_out)

    print(f"Wrote CSV to: {args.csv_out}")
    print(f"Rows: {len(rows)}")
    if rows:
        print("First row:", rows[0])
        print("Last row:", rows[-1])

if __name__ == "__main__":
    main()