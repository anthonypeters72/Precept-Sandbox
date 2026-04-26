# tools/normalize_sectioned_lines.py
from __future__ import annotations

import csv
import re
import sys
from pathlib import Path


BOOK_START_RE = re.compile(r"^\s*THE\s+BOOK\s+OF\b", re.IGNORECASE)

# Matches: "Chapter 1"
CHAPTER_BARE_RE = re.compile(r"^\s*chapter\s+(\d+)\s*$", re.IGNORECASE)

# Matches: "Chapter 1 - CREATION" (TOC/listing style) -> ignore
CHAPTER_TOC_RE = re.compile(r"^\s*chapter\s+(\d+)\s*[-–—:]\s*.+$", re.IGNORECASE)

# Hermas-style section headers (optional; safe for Kolbrin too)
SECTION_RE = re.compile(
    r"^(?P<ord>First|Second|Third|Fourth|Fifth|Sixth|Seventh|Eighth|Ninth|Tenth|Eleventh|Twelfth)\s+"
    r"(?P<label>Vision|Commandment|Similitude)\s*:\s*(?P<title>.*)$",
    re.IGNORECASE
)

ALLCAPS_RE = re.compile(r"^[A-Z0-9 ,.'\";:!?()\-\u2014]+$")  # mostly caps + punctuation


def is_heading(line: str) -> bool:
    """Heuristic headings (used only BEFORE first real chapter starts)."""
    if not line:
        return True
    if line.lower().strip() in {"contents"}:
        return True
    # Big all-caps running headers
    if ALLCAPS_RE.match(line) and len(line) >= 20:
        return True
    # Section markers are headings
    if SECTION_RE.match(line):
        return True
    # Chapter markers are headings (but loop handles them explicitly)
    if CHAPTER_BARE_RE.match(line) or CHAPTER_TOC_RE.match(line):
        return True
    return False


def main() -> int:
    if len(sys.argv) != 5:
        print("Usage: python tools/normalize_sectioned_lines.py <lines.csv> <output.csv> <corpus> <book>")
        return 2

    lines_path = Path(sys.argv[1])
    out_path = Path(sys.argv[2])
    corpus = sys.argv[3].strip()
    book = sys.argv[4].strip()

    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Read line stream (expects a CSV with header 'line')
    with lines_path.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        if not r.fieldnames or "line" not in r.fieldnames:
            raise SystemExit("Expected a CSV with header 'line'.")
        lines = [(row.get("line") or "").strip() for row in r]

    # State
    current_chapter = 0
    verse_in_chapter = 0
    buf: list[str] = []
    rows_out: list[dict[str, str]] = []
    started = False  # becomes True after first real "Chapter N" (bare) is seen
    in_body = False

    def emit_paragraph() -> None:
        nonlocal buf, verse_in_chapter, current_chapter, rows_out, started

        text = " ".join(buf).strip()
        buf = []
        if not text:
            return

        # Only emit once we've entered a real chapter
        if not started or current_chapter <= 0:
            return

        verse_in_chapter += 1
        rows_out.append({
            "corpus": corpus,
            "book": book,
            "chapter": str(current_chapter),
            "verse": str(verse_in_chapter),
            "text": text,
        })

    for ln in lines:
        ln = ln.strip().strip('"').strip()
        
        
        if BOOK_START_RE.match(ln):
            in_body = True
            emit_paragraph()
            continue

        
        # Ignore TOC-style chapter listing
        m_toc = CHAPTER_TOC_RE.match(ln)
        if m_toc:
            emit_paragraph()
            # Before body starts, treat as TOC/listing
            if not in_body:
                continue
            # After body starts, it's a real chapter header
            current_chapter = int(m_toc.group(1))
            verse_in_chapter = 0
            started = True
            continue


        # Real chapter start
        mch = CHAPTER_BARE_RE.match(ln)
        if mch:
            emit_paragraph()
            current_chapter = int(mch.group(1))
            verse_in_chapter = 0
            started = True
            continue

        # Hermas-style section heading (treat like boundary, but keep chapter numbering as-is)
        if SECTION_RE.match(ln):
            emit_paragraph()
            continue

        # Blank line splits paragraphs
        if not ln:
            emit_paragraph()
            continue

        # Headings only matter BEFORE we start the real text
        if not started and is_heading(ln):
            emit_paragraph()
            continue

        # Accumulate prose lines into paragraphs
        buf.append(ln)

    emit_paragraph()

    # Write normalized output
    with out_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["corpus", "book", "chapter", "verse", "text"])
        w.writeheader()
        w.writerows(rows_out)

    print(f"Wrote normalized: {out_path} ({len(rows_out)} verses)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
