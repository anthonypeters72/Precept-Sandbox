import re
import csv
from pathlib import Path


# === CONFIG ===
BASE_DIR = Path(__file__).resolve().parent.parent

SRC = BASE_DIR / "Data" / "apocrypha" / "tobit.csv"
OUT = BASE_DIR / "Data" / "apocrypha" / "apocrypha_tobit.csv"

BOOK_NAME = "Tobit"
CORPUS_NAME = "Apocrypha"


# Matches {chapter:verse}
VERSE_RE = re.compile(r"\{(\d+):(\d+)\}")

rows = []

current_ch = None
current_vs = None
buffer = []

with SRC.open(encoding="utf-8") as f:
    for raw_line in f:
        line = raw_line.strip()

        # skip comments / empty lines
        if not line or line.startswith("#"):
            continue

        match = VERSE_RE.search(line)
        if match:
            # flush previous verse
            if current_ch is not None and buffer:
                rows.append([
                    CORPUS_NAME,
                    BOOK_NAME,
                    int(current_ch),
                    int(current_vs),
                    " ".join(buffer).strip()
                ])
                buffer = []

            current_ch, current_vs = match.groups()
            line = VERSE_RE.sub("", line).strip()

        if current_ch is not None:
            buffer.append(line.strip('"'))

# flush last verse
if current_ch is not None and buffer:
    rows.append([
        CORPUS_NAME,
        BOOK_NAME,
        int(current_ch),
        int(current_vs),
        " ".join(buffer).strip()
    ])

# write CSV
with OUT.open("w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["corpus", "book", "chapter", "verse", "text"])
    writer.writerows(rows)

print(f"✔ Wrote {len(rows)} verses to {OUT}")
