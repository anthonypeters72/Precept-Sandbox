import re
import csv
import sys
from pathlib import Path


VERSE_RE = re.compile(r"\{(\d+):(\d+)\}")
DOT_RE = re.compile(r"(?<!\d)(\d+)\.\s*(\d+)(?!\d)")



def slugify(s: str) -> str:
    s = s.strip().lower()
    # keep letters/numbers/_; convert spaces/dashes to underscore
    s = re.sub(r"[ \-]+", "_", s)
    s = re.sub(r"[^a-z0-9_]+", "", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


def normalize_marker_book(corpus: str, book: str, src: Path, out: Path) -> int:
    rows = []
    current_ch = None
    current_vs = None
    buffer = []

    with src.open(encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()

            # skip empty/comment
            if not line or line.startswith("#"):
                continue

            # skip common pdf artifacts
            if line.lower().startswith("page |"):
                continue

            # Clean leading CSV commas (your file has lots of them)
            clean = line.lstrip(",").strip()

            m = VERSE_RE.search(clean)          # {ch:vs}
            m2 = DOT_RE.search(clean) if not m else None   # ch.vs (Enoch style)

            if m or m2:
                # flush previous verse
                if current_ch is not None and buffer:
                    rows.append([
                        corpus,
                        book,
                        int(current_ch),
                        int(current_vs),
                        " ".join(buffer).strip()
                    ])
                    buffer = []

                if m:
                    current_ch, current_vs = m.groups()
                    clean = VERSE_RE.sub("", clean, count=1).strip()
                else:
                    current_ch, current_vs = m2.groups()
                    clean = DOT_RE.sub("", clean, count=1).strip()

            # only record text after we’ve seen the first verse marker
            if current_ch is not None:
                buffer.append(clean.strip('"'))


    # flush last verse
    if current_ch is not None and buffer:
        rows.append([
            corpus,
            book,
            int(current_ch),
            int(current_vs),
            " ".join(buffer).strip()
        ])

    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["corpus", "book", "chapter", "verse", "text"])
        w.writerows(rows)

    print(f"✔ Wrote {len(rows)} verses to {out}")
    return 0


def main():
    # Usage:
    # python tools/normalize_marker_book.py apocrypha "2 Maccabees" Data/apocrypha/2_maccabees.csv
    # Optional:
    # python tools/normalize_marker_book.py apocrypha "2 Maccabees" Data/apocrypha/2_maccabees.csv Data/apocrypha/apocrypha_2_maccabees.csv

    if len(sys.argv) < 4:
        print('Usage: python tools/normalize_marker_book.py <corpus> "<Book Name>" <src_csv> [out_csv]')
        return 2

    corpus_in = sys.argv[1].strip()
    # normalize corpus label used inside CSV (capitalize like your others)
    corpus_label = corpus_in.strip().lower()
    corpus_label = "Apocrypha" if corpus_label == "apocrypha" else corpus_in.strip()

    book = sys.argv[2].strip()
    src = Path(sys.argv[3])

    base_dir = Path(__file__).resolve().parent.parent

    # If src is relative, make it relative to project root
    if not src.is_absolute():
        src = base_dir / src

    if len(sys.argv) >= 5:
        out = Path(sys.argv[4])
        if not out.is_absolute():
            out = base_dir / out
    else:
        corpus_slug = slugify(corpus_in)
        book_slug = slugify(book)
        # Default output: Data/<corpus>/<corpus>_<book>.csv
        out = base_dir / "Data" / corpus_slug / f"{corpus_slug}_{book_slug}.csv"

    if not src.exists():
        print(f"Missing src file: {src}")
        return 2

    return normalize_marker_book(corpus_label, book, src, out)


if __name__ == "__main__":
    raise SystemExit(main())
