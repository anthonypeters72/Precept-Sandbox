import sys
from pathlib import Path
import pdfplumber


def extract_text(pdf_path):
    text = []

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            t = page.extract_text()
            if t:
                text.append(t)

    return "\n".join(text)


def main():
    if len(sys.argv) < 2:
        print("Usage: python pdf_to_txt.py <file.pdf>")
        return

    pdf_file = Path(sys.argv[1])

    if not pdf_file.exists():
        print("File not found:", pdf_file)
        return

    txt_file = pdf_file.with_suffix(".txt")

    print(f"Extracting text from: {pdf_file}")

    text = extract_text(pdf_file)

    with open(txt_file, "w", encoding="utf-8") as f:
        f.write(text)

    print(f"Saved text to: {txt_file}")


if __name__ == "__main__":
    main()