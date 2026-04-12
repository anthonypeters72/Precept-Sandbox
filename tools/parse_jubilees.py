#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path


SKIP_PREFIXES = (
    "Page |",
    "<PARSED TEXT FOR PAGE:",
    "For more history and information about this book",
    "http://",
    "https://",
)

def normalize_whitespace(s: str) -> str:
    s = s.replace("\u00ad", "")
    s = s.replace("\ufeff", "")
    s = s.replace("\u2010", "-").replace("\u2011", "-").replace("\u2013", "-").replace("\u2014", "-")
    s = re.sub(r"[ \t]+", " ", s)
    return s.strip()

def clean_lines(text: str) -> list[str]:
    out = []
    for raw in text.splitlines():
        line = normalize_whitespace(raw)
        if not line:
            continue
        if any(line.startswith(prefix) for prefix in SKIP_PREFIXES):
            continue
        out.append(line)
    return out

def parse_jubilees(txt_path: Path):
    text = txt_path.read_text(encoding="utf-8", errors="ignore")
    lines = clean_lines(text)

    rows = []
    book = "Jubilees"
    chapter = None
    verse = None
    current = []

    chap_pat = re.compile(r"^\[?Chapter\s+(\d+)\]?$", re.I)
    verse_pat = re.compile(r"^(\d{1,3})(?:[\.,])?\s+(.*)$")

    def flush():
        nonlocal current
        if chapter is None or verse is None:
            current = []
            return
        joined = normalize_whitespace(" ".join(current))
        if joined:
            rows.append((book, chapter, verse, joined))
        current = []

    def absorb_inline_verses(textline: str):
        nonlocal verse, current

        # Catch inline verse numbers like:
        # "... commandment, which 2 I have written ..."
        # "... the 3 glory ..."
        matches = list(re.finditer(r"(?<!\d)(\d{1,3})(?=\s+[A-Za-z\[\('\"-])", textline))

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
                    # number is at the very start; leave normal verse handling to caller
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
        m_ch = chap_pat.match(line)
        if m_ch:
            flush()
            chapter = int(m_ch.group(1))
            verse = None
            current = []
            continue

        m_v = verse_pat.match(line)
        if m_v and chapter is not None:
            flush()
            verse = int(m_v.group(1))
            current = []
            rest = m_v.group(2).strip()
            absorb_inline_verses(rest)
            continue

        if chapter is not None and verse is not None:
            absorb_inline_verses(line)

    flush()
    return rows

def write_csv(rows, out_path: Path):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["book", "chapter", "verse", "text"])
        w.writerows(rows)

def main():
    ap = argparse.ArgumentParser(description="Parse Jubilees txt into engine-friendly CSV.")
    ap.add_argument("txt", type=Path, help="Path to Jubilees txt file")
    ap.add_argument("--csv-out", required=True, type=Path, help="Output CSV path")
    args = ap.parse_args()

    rows = parse_jubilees(args.txt)
    write_csv(rows, args.csv_out)

    print(f"Wrote CSV to: {args.csv_out}")
    print(f"Rows: {len(rows)}")
    if rows:
        print("First row:", rows[0])

if __name__ == "__main__":
    main()