# Cursor / Composio 2.0 Index — Migrations 54–58 (LLM Tier-2 canonicals)

**Date:** 2026-04-24
**DB:** `thyroid_canonical_publication_v1_0`
**Upstream commit:** `9b82651` (`scripts/llm_batch: 5-domain scaffolding + 4/5 loaded to PUB v1_0`)
**Run order:** 54 → 55 → 56 → 57 → 58. Each is independent except #58 depends on the parathyroid runner finishing on the pod.

## Loader tables (built; these are the inputs)

| table | rows | patients | err | status |
|---|---|---|---|---|
| `main.note_entities_llm_ete_subgrade_v1`         |   287 |   151 | 0 | ✅ loaded |
| `main.note_entities_llm_t4b_invasion_v1`         |   944 |   434 | 0 | ✅ loaded |
| `main.note_entities_llm_vascular_invasion_v2`    | 3,861 | 3,745 | 0 | ✅ loaded |
| `main.note_entities_llm_airway_invasion_v2`      | 6,054 | 2,820 | 0 | ✅ loaded |
| `main.note_entities_llm_parathyroid_detail_v1`   | ~8,697 | ~5,386 | — | 🟡 runner in-flight |

All loader tables share the same shape (note_row_id, research_id, note_type, note_index, source_*, parsed_json, raw_llm_response, error, extracted_at, llm_model, elapsed_s, build_ts).

## Migration prompts (one file per migration)

| # | File | Canonical deliverables |
|---|---|---|
| 54 | `CURSOR_PROMPT_MIG_54_ETE_SUBGRADE_20260424.md`       | `canonical_ete_subgrade_events_v1` / `_patient_rollup_v1` |
| 55 | `CURSOR_PROMPT_MIG_55_T4B_INVASION_20260424.md`       | `canonical_t4b_invasion_events_v1` / `_patient_rollup_v1` |
| 56 | `CURSOR_PROMPT_MIG_56_VASCULAR_INVASION_V2_20260424.md` | `canonical_vascular_invasion_events_v1` / `_patient_rollup_v1` |
| 57 | `CURSOR_PROMPT_MIG_57_AIRWAY_INVASION_V2_20260424.md` | `canonical_airway_invasion_events_v1` / `_patient_rollup_v1` |
| 58 | `CURSOR_PROMPT_MIG_58_PARATHYROID_DETAIL_20260424.md` | `canonical_parathyroid_events_v1` / `_patient_rollup_v1` |

## Common conventions across all 5

Per memory (`reference_canonical_naming_convention`, `reference_view_naming_convention`):

- Tier-2 master tables → `canonical_<domain>_events_v1` + `canonical_<domain>_patient_rollup_v1`.
- Any VIEW must carry `_VIEW` in name (e.g., `canonical_<domain>_<grain>_VIEW_v1`). No plain view names.
- `build_script` + `build_ts` are mandatory audit columns; `build_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP)` (avoid silent TIMESTAMPTZ via `reference_duckdb_timestamp_tz`).
- All PATCH to `detail_table_registry_v1` uses the `detail_table_name` column; probe columns with `information_schema.columns` first (per `reference_detail_table_registry_schema`).
- No cross-DB sourcing (`feedback_no_cross_db_canonical_sourcing`) — everything reads from `main.*` in this DB.
- No cross-domain linkage IDs baked into these tables (`feedback_no_crossdomain_linkage_ids`); join at query time.

## Cross-prompt consistency notes

- **ETE ↔ T4b**: both tables can assert pT4b. `canonical_ete_subgrade_patient_rollup_v1.any_pT4b` is the ETE-path call; `canonical_t4b_invasion_patient_rollup_v1.any_pT4b_final` is the anatomic-component call. Disagreements are an expected and useful audit.
- **ETE ↔ Airway (pT4a)**: ETE `pT4a` + airway `pT4a_final` should largely overlap; disagreements surface free-text vs structured evidence mismatches.
- **VI v2 ↔ existing `canonical_invasion_patient_rollup_v1`**: the old rollup's `any_vascular_microscopic_*` / `any_lymphatic_microscopic_*` / `any_perineural_*` columns are coarser; don't rewrite the existing rollup in these migrations — that's a Script 363+ job.
- **Airway v2**: `tracheal_invasion_depth` is a ranked ladder (full_thickness > cartilage > adventitia > mucosal); MAX of rank = worst depth.

## Execution checklist

For each migration:

1. **Pre-flight probe** — row counts + key coverage (see individual prompts).
2. **Events CTAS** — atomic `CREATE OR REPLACE TABLE ... AS ...` with deterministic column order.
3. **Rollup CTAS** — `GROUP BY research_id`.
4. **QA queries** — distribution checks against expected counts from the prompt.
5. **Registry patch** — `INSERT ... WHERE NOT EXISTS` into `detail_table_registry_v1`.
6. **Commit** — one commit per migration, paths explicit (never `git add -A`, per `feedback_surgical_git_add`).
7. **Close-out memory** — write `project_mig_<N>_<domain>_closeout.md` with final SHA, counts, carry-forwards.

## Parathyroid timing

Migration 58 blocks on the RunPod runner finishing:

```bash
# Verify before starting mig 58:
python3 scripts/llm_batch/_verify_loaded.py
# Line for note_entities_llm_parathyroid_detail_v1 must show non-zero rows + err=0.
```

If the pod crashed mid-run, the runner resumes on `(research_id, note_type, note_index)` keys — just restart the same command on the pod and let it finish before scp'ing the results down.

## After all 5 land

The next Tier-2 follow-up (Script 363+ territory) should:

1. Rebuild `canonical_invasion_patient_rollup_v1` to source from `canonical_vascular_invasion_patient_rollup_v1` + `canonical_airway_invasion_patient_rollup_v1` + `canonical_ete_subgrade_patient_rollup_v1` + `canonical_t4b_invasion_patient_rollup_v1` (plus the existing path-synoptic feeders).
2. Demote / archive the deprecated `note_entities_llm_vascular_invasion` (non-v2) and `note_entities_llm_airway_invasion` (non-v2) loader tables.
3. Fold the parathyroid rollup into whatever "complications" canonical lives downstream.
