# tools/flatten_multicol_csv.py
from __future__ import annotations

import csv
import re
import sys
from pathlib import Path

PUNCT_FIX = re.compile(r"\s+([,.;:!?])")

def join_cells(row: list[str]) -> str:
    parts = [c.strip() for c in row if c and c.strip()]
    if not parts:
        return ""
    s = " ".join(parts)
    s = PUNCT_FIX.sub(r"\1", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def main() -> int:
    if len(sys.argv) != 3:
        print("Usage: python tools/flatten_multicol_csv.py <input.csv> <output_lines.csv>")
        return 2

    inp = Path(sys.argv[1])
    out = Path(sys.argv[2])
    out.parent.mkdir(parents=True, exist_ok=True)

    with inp.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        rows = list(reader)

    with out.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["line"])
        for r in rows:
            line = join_cells(r)
            # Keep blank separators as blank rows (helps paragraph detection later)
            w.writerow([line])

    print(f"Wrote lines file: {out}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
