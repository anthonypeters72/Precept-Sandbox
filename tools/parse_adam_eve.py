#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path

ROMAN_MAP = {
    "M": 1000, "CM": 900, "D": 500, "CD": 400,
    "C": 100, "XC": 90, "L": 50, "XL": 40,
    "X": 10, "IX": 9, "V": 5, "IV": 4, "I": 1,
}

def roman_to_int(s: str) -> int:
    s = s.strip().upper()
    i = 0
    total = 0
    while i < len(s):
        if i + 1 < len(s) and s[i:i+2] in ROMAN_MAP:
            total += ROMAN_MAP[s[i:i+2]]
            i += 2
        else:
            total += ROMAN_MAP[s[i]]
            i += 1
    return total

def normalize_whitespace(s: str) -> str:
    s = s.replace("\u00ad", "")
    s = s.replace("\ufeff", "")
    s = re.sub(r"[ \t]+", " ", s)
    return s.strip()

def clean_lines(text: str) -> list[str]:
    out = []
    for raw in text.splitlines():
        line = normalize_whitespace(raw)
        if not line:
            continue
        out.append(line)
    return out

def parse_book(txt_path: Path, book_name: str):
    text = txt_path.read_text(encoding="utf-8", errors="ignore")
    lines = clean_lines(text)

    rows = []
    chapter = None
    verse = None
    current = []

    chap_pat = re.compile(r"^Chapter\s+([IVXLCDM]+)(?:\s*[\.\-–—:]\s*.*)?$", re.I)
    verse_pat = re.compile(r"^(\d{1,3})\s+(.*)$")

    def flush():
        nonlocal current
        if chapter is None or verse is None:
            current = []
            return
        text = normalize_whitespace(" ".join(current))
        if text:
            rows.append((book_name, chapter, verse, text))
        current = []

    for line in lines:
        m_ch = chap_pat.match(line)
        if m_ch:
            flush()
            chapter = roman_to_int(m_ch.group(1))
            verse = None
            current = []
            continue

        m_v = verse_pat.match(line)
        if m_v and chapter is not None:
            flush()
            verse = int(m_v.group(1))
            current = [m_v.group(2).strip()]
            continue

        if chapter is not None and verse is not None:
            current.append(line)

    flush()
    return rows

def write_csv(rows, out_path: Path):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["book", "chapter", "verse", "text"])
        w.writerows(rows)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("txt", type=Path, help="Path to Adam & Eve txt file")
    ap.add_argument("--book-name", required=True, help="Canonical book name")
    ap.add_argument("--csv-out", required=True, type=Path, help="Output CSV path")
    args = ap.parse_args()

    rows = parse_book(args.txt, args.book_name)
    write_csv(rows, args.csv_out)

    print(f"Wrote CSV to: {args.csv_out}")
    print(f"Rows: {len(rows)}")
    if rows:
        print("First row:", rows[0])

if __name__ == "__main__":
    main()