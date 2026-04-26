import sys
from pathlib import Path
from pdfminer.high_level import extract_text

def main():
    if len(sys.argv) < 2:
        print("Usage: python tools/pdf_to_txt_fast.py <file.pdf>")
        return

    pdf_file = Path(sys.argv[1])
    if not pdf_file.exists():
        print(f"File not found: {pdf_file}")
        return

    txt_file = pdf_file.with_suffix(".txt")
    print(f"Extracting text from: {pdf_file}")

    text = extract_text(str(pdf_file))

    txt_file.write_text(text, encoding="utf-8")
    print(f"Saved text to: {txt_file}")

if __name__ == "__main__":
    main()