# Precept Sandbox – Project State

## Purpose
A scripture study engine built on Isaiah 28:10 principles:
“precept upon precept, line upon line”
Enforces corroboration across books and corpora without commentary.

This is a **non-commercial, mission-focused** project.

---

## What Works (Confirmed)

### Core Engine
- Accepts scripture references via CLI
- Displays precepts (cross-references)
- Enforces rule: minimum 2 different books
- Shows precepts even if input verse text is missing (Mode A)
- Fallback logic only triggers when no mapping exists

### Data Architecture
- `topics.json` → conceptual groupings
- `cross_refs.json` → runtime lookup graph (merge-safe)
- Manual + generated links coexist cleanly

### Corpora Support
- Bible (KJV-compatible CSV format)
- Apocrypha supported via prefix:
  - Format: `Apocrypha:Book Chapter:Verse`
- Separate files:
  - `bible.csv`
  - `apocrypha.csv`

### CLI Helpers
- `--need "Book Chapter:Verse"`
  - Outputs CSV stub for `bible.csv`
- Mode A behavior confirmed (no early exit)

---

## Current Files
- `precepts.py`
- `topics.json`
- `cross_refs.json`
- `bible.csv` (partial / curated)
- `apocrypha.csv` (Sirach 2:15 implemented)
- `build_xrefs_from_topics.py`
- `kjv.csv` (full canon – not yet integrated)
- `PROJECT_STATE.md`

---

## Implemented Apocrypha Test
- Sirach 2:15
- Displays correctly from `apocrypha.csv`
- Mixed-corpus cross-ref works
- Rules still enforced correctly

---

## Intentional Exclusions (for now)
- No AI / embeddings
- No commentary layer
- No UI
- No inference across corpora
- No monetization

---

## Next Planned Steps
1. Add Sirach 2:16–17
2. Extend `--need` to support Apocrypha
3. Decide how to integrate `kjv.csv`:
   - Replace `bible.csv`
   - OR fallback lookup
4. Phase 1 planning (only after stabilization)
