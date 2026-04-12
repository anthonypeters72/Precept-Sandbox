#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path
from typing import Iterable

try:
    from pdfminer.high_level import extract_text
except Exception as e:
    raise SystemExit(
        "pdfminer.six is required. Install with: pip install pdfminer.six\n"
        f"Original import error: {e}"
    )

BOOK_ORDER = [
    "Testament of Reuben",
    "Testament of Simeon",
    "Testament of Levi",
    "Testament of Judah",
    "Testament of Issachar",
    "Testament of Zebulun",
    "Testament of Dan",
    "Testament of Naphtali",
    "Testament of Gad",
    "Testament of Asher",
    "Testament of Joseph",
    "Testament of Benjamin",
]


BOOK_INDEX = {name: i for i, name in enumerate(BOOK_ORDER)}


SKIP_PREFIXES = (
    "Page |",
    "www.Scriptural-Truth.com",
    "From Wikipedia, the free encyclopedia",
    "The Testaments of the Twelve Patriarchs is",
    "The Testaments were written in Greek",
    "With the critical methods",
    "Presently, scholarly opinions",
    "****",
    "END.",
)

BOOK_START_PATTERNS = {
    "Testament of Reuben": re.compile(r"\bThe copy of the Testament of Reuben\b", re.I),
    "Testament of Simeon": re.compile(r"\bThe copy of the words of Simeon\b", re.I),
    "Testament of Levi": re.compile(r"\bThe copy of the words of Levi\b", re.I),
    "Testament of Judah": re.compile(r"\bThe copy of the words of Judah\b", re.I),
    "Testament of Issachar": re.compile(r"\bThe copy of the words of Issachar\b", re.I),
    "Testament of Zebulun": re.compile(r"\bThe copy of the words of Zebulun\b", re.I),
    "Testament of Dan": re.compile(r"\bThe copy of the words of Dan\b", re.I),
    "Testament of Naphtali": re.compile(r"\bThe copy of the testament of Naphtali\b", re.I),
    "Testament of Gad": re.compile(r"\bThe copy of the testament of Gad\b", re.I),
    "Testament of Asher": re.compile(r"\bThe copy of the Testament of Asher\b", re.I),
    "Testament of Joseph": re.compile(r"\bThe copy of the Testament of Joseph\b", re.I),
    "Testament of Benjamin": re.compile(r"\bThe copy of the words of Benjamin\b", re.I),
}

def normalize_whitespace(s: str) -> str:
    s = s.replace("\u00ad", "")
    s = s.replace("\u2010", "-").replace("\u2011", "-").replace("\u2013", "-").replace("\u2014", "-")
    s = s.replace("\ufeff", "")
    s = re.sub(r"[ \t]+", " ", s)
    return s.strip()

def clean_text(text: str) -> str:
    lines = []
    for raw in text.splitlines():
        line = normalize_whitespace(raw)
        if not line:
            continue
        if any(line.startswith(prefix) for prefix in SKIP_PREFIXES):
            continue
        if line.startswith("<PARSED TEXT FOR PAGE:"):
            continue
        lines.append(line)
    text = "\n".join(lines)
    text = re.sub(r"\n(?=[a-z,\]\)])", " ", text)
    text = re.sub(r"(?<=[a-z,\]\)])\n(?=[a-z])", " ", text)
    text = re.sub(r"\n{2,}", "\n", text)
    return text

def split_books(text: str):
    starts = []
    for book, pat in BOOK_START_PATTERNS.items():
        m = pat.search(text)
        if m:
            starts.append((m.start(), book))
    starts.sort()
    if len(starts) < 2:
        raise ValueError("Could not reliably detect book boundaries in the PDF text.")
    chunks = []
    for i, (pos, book) in enumerate(starts):
        end = starts[i + 1][0] if i + 1 < len(starts) else len(text)
        chunks.append((book, text[pos:end].strip()))
    return chunks

def parse_book_chunk(book: str, chunk: str):
    rows = []
    chapter = 1
    verse = 1
    current_text_parts = []

    def flush():
        nonlocal current_text_parts, verse
        if chapter is None or verse is None:
            return
        text = normalize_whitespace(" ".join(current_text_parts))
        if text:
            rows.append((book, int(chapter), int(verse), text))
        current_text_parts = []

    def absorb_inline_verses(textline: str):
        nonlocal verse, current_text_parts
        matches = list(re.finditer(r"(?<!\d)(\d{1,3})(?=\s+[A-Za-z\[\(])", textline))
        if not matches:
            current_text_parts.append(textline)
            return

        first = True
        last_idx = 0
        for m in matches:
            num = int(m.group(1))
            idx = m.start()

            if first:
                prefix = textline[:idx].strip()
                if prefix:
                    current_text_parts.append(prefix)
                else:
                    current_text_parts.append(textline)
                    return
                flush()
                verse = num
                current_text_parts = []
                last_idx = m.end()
                first = False
            else:
                body = textline[last_idx:m.start()].strip()
                if body:
                    current_text_parts.append(body)
                flush()
                verse = num
                current_text_parts = []
                last_idx = m.end()

        tail = textline[last_idx:].strip()
        if tail:
            current_text_parts.append(tail)

    lines = [normalize_whitespace(x) for x in chunk.splitlines() if normalize_whitespace(x)]

    # Remove duplicated opening title/incorporated heading line if present
    start_pat = BOOK_START_PATTERNS.get(book)
    if lines and start_pat:
        lines[0] = start_pat.sub("", lines[0]).strip(" .:-")
        if not lines[0]:
            lines = lines[1:]

    for line in lines:
        if line.startswith("From Wikipedia, the free encyclopedia"):
            break

        m = re.match(r"^(\d{1,2})\s+(\d{1,3})(?:\s*,\s*(\d{1,3}))?\s+(.*)$", line)
        if m:
            new_chapter = int(m.group(1))
            v1 = int(m.group(2))
            v2 = int(m.group(3)) if m.group(3) else None
            rest = m.group(4).strip()

            if new_chapter >= chapter:
                flush()
                chapter = new_chapter

                if v2 is not None and v2 == v1 + 1:
                    rows.append((book, int(chapter), v1, rest))
                    rows.append((book, int(chapter), v2, rest))
                    verse = v2
                    current_text_parts = []
                else:
                    verse = v1
                    current_text_parts = []
                    absorb_inline_verses(rest)
                continue

        m2 = re.match(r"^(\d{1,3})(?:\s*,\s*(\d{1,3}))?\s+(.*)$", line)
        if m2 and chapter is not None:
            v1 = int(m2.group(1))
            v2 = int(m2.group(2)) if m2.group(2) else None
            rest = m2.group(3).strip()

            if verse is None or (v1 >= verse and v1 - verse <= 5):
                flush()

                if v2 is not None and v2 == v1 + 1:
                    rows.append((book, int(chapter), v1, rest))
                    rows.append((book, int(chapter), v2, rest))
                    verse = v2
                    current_text_parts = []
                else:
                    verse = v1
                    current_text_parts = []
                    absorb_inline_verses(rest)
                continue

        absorb_inline_verses(line)

    flush()
    return rows


def main():
    ap = argparse.ArgumentParser(description="Parse The Testaments of the Twelve Patriarchs PDF into engine-friendly CSV.")
    ap.add_argument("pdf", type=Path, help="Path to source PDF")
    ap.add_argument("--raw-out", type=Path, default=Path("patriarchs_raw.txt"), help="Raw extracted text output")
    ap.add_argument("--csv-out", type=Path, default=Path("patriarchs.csv"), help="Structured CSV output")
    args = ap.parse_args()

    raw = extract_text(str(args.pdf))
    args.raw_out.write_text(raw, encoding="utf-8")

    cleaned = clean_text(raw)
    books = split_books(cleaned)

    all_rows = []
    for book, chunk in books:
        rows = parse_book_chunk(book, chunk)
        all_rows.extend(rows)

    all_rows = dedupe_rows(all_rows)

    all_rows.sort(key=lambda r: (BOOK_INDEX.get(r[0], 999), r[1], r[2]))

    with args.csv_out.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["book", "chapter", "verse", "text"])
        for row in all_rows:
            writer.writerow(row)

    print(f"Wrote raw text to: {args.raw_out}")
    print(f"Wrote CSV to: {args.csv_out}")
    print(f"Rows: {len(all_rows)}")
    by_book = {}
    for b, ch, vs, txt in all_rows:
        by_book[b] = by_book.get(b, 0) + 1
    for b in BOOK_ORDER:
        if b in by_book:
            print(f"  {b}: {by_book[b]} rows")

if __name__ == "__main__":
    main()
