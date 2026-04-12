#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from pathlib import Path

def main():
    ap = argparse.ArgumentParser(description="Validate structured corpus CSV with columns: book,chapter,verse,text")
    ap.add_argument("csv_path", type=Path)
    args = ap.parse_args()

    seen = set()
    duplicates = []
    empty_text = []
    bad_ints = []
    chapter_verse_map = defaultdict(list)

    with args.csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        required = {"book", "chapter", "verse", "text"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise SystemExit(f"Missing required columns: {sorted(missing)}")

        for i, row in enumerate(reader, start=2):
            book = (row.get("book") or "").strip()
            text = (row.get("text") or "").strip()
            try:
                ch = int((row.get("chapter") or "").strip())
                vs = int((row.get("verse") or "").strip())
            except Exception:
                bad_ints.append(i)
                continue

            key = (book, ch, vs)
            if key in seen:
                duplicates.append((i, key))
            seen.add(key)

            if not text:
                empty_text.append((i, key))

            chapter_verse_map[(book, ch)].append(vs)

    gaps = []
    regressions = []
    for (book, ch), verses in sorted(chapter_verse_map.items()):
        verses_sorted = sorted(verses)
        if verses_sorted != verses:
            regressions.append((book, ch, verses[:10]))
        for a, b in zip(verses_sorted, verses_sorted[1:]):
            if b <= a:
                regressions.append((book, ch, verses[:10]))
                break
            if b - a > 2:
                gaps.append((book, ch, a, b))

    print(f"Checked: {args.csv_path}")
    print(f"Unique verse keys: {len(seen)}")
    print(f"Duplicate rows: {len(duplicates)}")
    print(f"Empty text rows: {len(empty_text)}")
    print(f"Bad chapter/verse rows: {len(bad_ints)}")
    print(f"Verse gaps found: {len(gaps)}")
    print(f"Ordering regressions: {len(regressions)}")

    if duplicates[:10]:
        print("\nSample duplicates:")
        for item in duplicates[:10]:
            print(" ", item)

    if empty_text[:10]:
        print("\nSample empty text rows:")
        for item in empty_text[:10]:
            print(" ", item)

    if bad_ints[:10]:
        print("\nSample bad chapter/verse rows:")
        for item in bad_ints[:10]:
            print(" ", item)

    if gaps[:10]:
        print("\nSample verse gaps:")
        for item in gaps[:10]:
            print(" ", item)

    if regressions[:10]:
        print("\nSample ordering regressions:")
        for item in regressions[:10]:
            print(" ", item)

if __name__ == "__main__":
    main()
