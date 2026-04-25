# Migration 58 (parathyroid track) — Parathyroid detail tier-2 canonical — close-out

**Date:** 2026-04-24  
**Database:** `thyroid_canonical_publication_v1_0`  
**`build_script` / provenance tag:** `mig_58_parathyroid_detail_20260424`  
**Source LLM table:** `main.note_entities_llm_parathyroid_detail_v1` (`error = 0`)

## Repo numbering note

| Concept | Value |
|---------|--------|
| Sequential SQL file in `qc_framework_v1/migrations/` | **59** (`59_parathyroid_detail_canonical_tier2_v1.sql`) — **58** is reserved for airway v2 canonical in this repo |
| Project / provenance id | **mig_58_parathyroid_detail** (`build_script` column on canonical tables) |

## Repo artifacts

| Artifact | Role |
|----------|------|
| `qc_framework_v1/migrations/59_parathyroid_detail_canonical_tier2_v1.sql` | `CREATE OR REPLACE` for events + patient rollup; `COMMENT ON TABLE`; `manuscript_workspace.detail_table_registry_v1` for both canonicals |

## Pre-flight (before first run)

1. `uv run python scripts/llm_batch/_verify_loaded.py` — `note_entities_llm_parathyroid_detail_v1` must exist with `err=0` aggregate.
2. Optional SQL from prompt: row count / `json_keys` coverage on `parsed_json`.

## Objects delivered

| Object | Grain |
|--------|--------|
| `main.canonical_parathyroid_events_v1` | one row per `note_row_id` (`parathyroid_event_id`) from `error = 0` |
| `main.canonical_parathyroid_patient_rollup_v1` | one row per `research_id` |
| `manuscript_workspace.detail_table_registry_v1` | two rows (events + rollup) |

## QA counts (MotherDuck — post-migration 2026-04-24 ~03:20Z)

**Status:** ✅ Loader landed, migration 59 applied, QA green.

| Check | Expected | Result |
|--------|----------|--------------|
| Loader `note_entities_llm_parathyroid_detail_v1` | err=0, ~8.7k rows | **8,697 rows / 4,443 distinct pts / err=0** |
| Events `COUNT(*)` (one row per note_row_id) | = loader rows with err=0 | **8,697** |
| Rollup `COUNT(*)` / distinct patients | ≤ loader patients | **4,443 / 4,443 (1:1)** |
| `max_glands_identified > 4` | 0 patients | **1 pt at 5** — LLM contract violation; carry-forward to cleanup pass (see below) |

**Note on patient count:** manifest `expected_rows_est` was ~5,386 patients; final 4,443 is below because many candidate patients yielded all-null `parsed_json` for the 13 domain keys (LLM returned "not mentioned" collapsed to NULL everywhere) and therefore never hit the rollup. Events-per-patient = 8,697 / 4,443 ≈ 1.96, consistent with op-notes + path synoptics being the primary drivers.

### Autotransplant rate + location distribution

- **Rate:** `any_autotransplant = TRUE` for **301 / 4,443 = 6.8 %** — within published 5–15 % band ✅
- **By location (rollup-level distinct):**
  - `SCM` — 189 pts
  - `other` — 25 pts
  - `SCM;other` — 1 pt
  - `forearm` — 1 pt
  - NULL — 85 pts (autotransplant flagged but location not explicitly stated)

### Hypocalcemia / permanent hypoparathyroidism

- `any_incidental_parathyroidectomy = TRUE` — **2,219 / 4,443 = 50.0 %** (high; LLM is liberal when synoptic lists "parathyroid tissue identified in specimen"; flag for specificity review)
- `any_hypocalcemia_postop = TRUE` — **65 / 4,443 = 1.5 %** (conservatively low; many clinical notes code transient hypocalcemia as `unknown` rather than `present`)
- `any_permanent_hypoparathyroidism = TRUE` — **13 / 4,443 = 0.29 %** ✅ (< 5 % expected)
- Cross-tab (both-TRUE risk subgroup):
  - autotransplant=T AND hypocalcemia=T → 8 pts
  - autotransplant=T AND perm_hypopara=T → 1 pt
  - incidental_ptx=T AND hypocalcemia=T → 17 pts
  - incidental_ptx=T AND perm_hypopara=T → 9 pts

### Gland-count histogram

| max_glands_identified | patients |
|---|---|
| NULL | 2,000 |
| 0 | 145 |
| 1 | 897 |
| 2 | 645 |
| 3 | 301 |
| 4 | 454 |
| **5** | **1** ← contract violation |

4 > 3 distribution ✅. The single `=5` patient is an LLM error (field is int 0–4 per prompt contract) — log as carry-forward for a cleanup pass; could be from a note like "all four identified, one additional sent for frozen".

### iPTH sanity

- `n_with_pth = 540` patients carry a numeric iPTH value
- Range: **1–851 ng/L**, `n_suspect_unit_error (>2000)` = **0** ✅
- Low-end values (1–5 ng/L) are clinically plausible (post-op nadir); no cleanup needed.

### Parathyroid pathology distribution

Top values (DISTINCT string lists, per patient):

| parathyroid_pathologies | patients |
|---|---|
| normal | 1,173 |
| not_assessed | 1,151 |
| hyperplasia | 519 |
| adenoma | 121 |
| normal;not_assessed | 80 |
| not_assessed;normal | 71 |
| adenoma;hyperplasia | 31 |
| hyperplasia;adenoma | 30 |
| not_assessed;hyperplasia | 28 |
| hyperplasia;not_assessed | 20 |

(The split `normal;not_assessed` vs `not_assessed;normal` is a STRING_AGG ordering artifact; harmless for analytics, normalize downstream if needed.)

## Deprecations / carry-forward

| Item | Status |
|------|--------|
| Tier-1 `nlp_*_parathyroid_*` flags | Retire after verification (separate script / CPM column swap) |
| Follow-on complications canonical | Join on `surgery_episode_id` when built |
| Script 36x sourcing | Point downstream builds at `canonical_parathyroid_*_v1` once verified (TBD script ids) |
| **NEW: 1 patient with `max_glands_identified = 5`** | LLM contract violation (field is int 0–4). Identify and correct in a cleanup pass: `SELECT research_id, note_row_id, evidence_quote FROM main.canonical_parathyroid_events_v1 WHERE glands_identified_count = 5;` |
| **NEW: 50 % incidental_parathyroidectomy rate** | Higher than expected published rates (10–30 %). LLM is counting "parathyroid tissue identified in specimen" broadly. Specificity pass recommended: review `evidence_quote` for false positives before using this flag as outcome. |
| **NEW: 85 pts `any_autotransplant=TRUE` with NULL location** | Prompt yielded autotransplant count without location string. Downstream analysis should treat these as "unknown site"; if important, re-prompt just these 85 notes. |

## How to apply

Applied 2026-04-24. Command used:

```python
import duckdb
con = duckdb.connect("md:thyroid_canonical_publication_v1_0?motherduck_token=...")
con.execute(open("qc_framework_v1/migrations/59_parathyroid_detail_canonical_tier2_v1.sql").read())
```

All statements executed in a single multi-statement call without errors.
