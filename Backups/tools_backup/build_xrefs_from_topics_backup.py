import json
from pathlib import Path
from typing import Dict, List

BASE_DIR = Path(__file__).resolve().parent
TOPICS_JSON = BASE_DIR / "topics.json"
XREFS_JSON = BASE_DIR / "cross_refs.json"

def normalize_ref(ref: str) -> str:
    return " ".join(ref.strip().split())

def load_json(path: Path) -> dict:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

def save_json(path: Path, data: dict) -> None:
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def dedupe_preserve_order(items: List[str]) -> List[str]:
    seen = set()
    out = []
    for x in items:
        if x not in seen:
            out.append(x)
            seen.add(x)
    return out

def main() -> int:
    topics_raw: Dict[str, List[str]] = load_json(TOPICS_JSON)
    if not topics_raw:
        print(f"No topics found. Create {TOPICS_JSON.name} first.")
        return 1

    existing_xrefs: Dict[str, List[str]] = load_json(XREFS_JSON)

    added_links = 0
    affected_sources = set()

    # Build cross refs: within each topic, every ref links to every other ref
    for topic, refs in topics_raw.items():
        refs_norm = [normalize_ref(r) for r in refs]
        refs_norm = dedupe_preserve_order(refs_norm)

        for source in refs_norm:
            targets = [r for r in refs_norm if r != source]
            if not targets:
                continue

            before = existing_xrefs.get(source, [])
            before_norm = [normalize_ref(r) for r in before]
            combined = dedupe_preserve_order(before_norm + targets)

            # Count new additions
            new_count = len(set(combined)) - len(set(before_norm))
            if new_count > 0:
                added_links += new_count
                affected_sources.add(source)

            existing_xrefs[source] = combined

    save_json(XREFS_JSON, existing_xrefs)

    print("\n==============================")
    print("build_xrefs_from_topics DONE ✅")
    print("==============================")
    print(f"Sources updated: {len(affected_sources)}")
    print(f"New links added: {added_links}")
    print(f"Saved: {XREFS_JSON}")
    print("==============================\n")

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
