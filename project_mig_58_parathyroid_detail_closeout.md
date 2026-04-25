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

## QA counts (MotherDuck — fill in post-migration)

**Status:** Loader not present on MotherDuck at closeout draft time; run migration after `note_entities_llm_parathyroid_detail_v1` lands.

| Check | Expected | Result (TBD) |
|--------|----------|--------------|
| Events `COUNT(*)` | ≈ rows in loader with `error = 0` (~8,697 manifest) | |
| Rollup `COUNT(*)` / distinct patients | ~5,386 patients | |
| `max_glands_identified > 4` | 0 patients (contract violation if non-zero) | |

### Autotransplant rate + location distribution

- **Rate:** `any_autotransplant = true` / total rollup patients — expect ~5–15% (~300–800 pts if total ~5.4k).
- **By location:** group `autotransplant_locations` on events or rollup (TBD).

### Hypocalcemia / permanent hypoparathyroidism

- **Rates:** `any_hypocalcemia_postop`, `any_permanent_hypoparathyroidism` (permanent hypoparathyroidism expected below ~5% of thyroidectomy cohort at patient level — use as sanity, not hard gate).
- **Cross-tab:** `any_autotransplant` × `any_hypocalcemia_postop` (both TRUE = risk subgroup).

### Gland-count histogram

```sql
SELECT max_glands_identified, COUNT(*)
FROM main.canonical_parathyroid_patient_rollup_v1
GROUP BY 1
ORDER BY 1 NULLS LAST;
```

**Result (TBD):** expect more patients at 4 than 3 than null; document any patient with max gland count above 4.

### iPTH sanity

```sql
SELECT COUNT(*), MIN(min_intact_pth_value_ngL), MAX(max_intact_pth_value_ngL)
FROM main.canonical_parathyroid_patient_rollup_v1
WHERE max_intact_pth_value_ngL IS NOT NULL;
```

**Suspected LLM unit errors (TBD):** flag values outside ~0–500 ng/L (e.g. above 2000) for a cleanup pass.

### Path / synoptic reconciliation

Reconcile with path-synoptic parathyroid columns for the ~5.4k cohort: structured parathyroid tissue hits should tend to align with `any_incidental_parathyroidectomy = TRUE` or non-null gland counts (TBD).

## Deprecations / carry-forward

| Item | Status |
|------|--------|
| Tier-1 `nlp_*_parathyroid_*` flags | Retire after verification (separate script / CPM column swap) |
| Follow-on complications canonical | Join on `surgery_episode_id` when built |
| Script 36x sourcing | Point downstream builds at `canonical_parathyroid_*_v1` once verified (TBD script ids) |

## How to apply

```bash
# After loader verification:
# Execute 59_parathyroid_detail_canonical_tier2_v1.sql against thyroid_canonical_publication_v1_0
# (MotherDuck console, duckdb CLI with md: connection, or project motherduck client)
```

Then re-run QA queries in this doc and replace TBD sections with final numbers.
