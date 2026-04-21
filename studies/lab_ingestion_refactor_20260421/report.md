# Lab Ingestion Refactor — Script 348 Report

**Run date (UTC):** 2026-04-21
**Parent commit (pre-refactor):** `4d3c4fc` (Script 347 lab consolidation)
**Database:** `thyroid_canonical_publication_v1_0` (MotherDuck)
**Operator:** Cursor agent — Claude Opus 4.7

---

## TL;DR

| Item | Status |
|---|---|
| Refactored `113_tg_lab_ingestion.py` writes only to `main.canonical_labs_thyroglobulin_v1` | ✅ |
| Refactored `127_analyst_institutional_lab_append.py` routes by analyte to per-analyte canonicals | ✅ |
| Frozen 77 / 235 / 291 / 331 (legacy writers to dropped tables) | ✅ |
| Built `scripts/348_lab_ingestion_refactor_verify.py` | ✅ |
| 0 WRITE hits remain in `scripts/*.py` for the dropped legacy table names | ✅ |
| Refactored 113 rebuild-from-archive produces row-for-row identical canonical (53,006 rows) | ✅ |
| `longitudinal_lab_VIEW_v1` (54,035 rows) and `thyroglobulin_lab_VIEW_v1` (53,006 rows) unchanged | ✅ |
| All 45 normalizer unit tests pass | ✅ |
| pyflakes clean on refactored + new scripts | ✅ |
| CPM invariant intact: `(10871, 10871, 0)` | ✅ |

---

## Step 1 — Discovery summary

(Full grep dump in `step1_discovery.md`.)

| Script | Wrote to dropped table? | Disposition |
|---|---|---|
| `scripts/113_tg_lab_ingestion.py` | YES (4 statements: `thyroglobulin_lab_canonical_v1`, `longitudinal_lab_canonical_v1` ×2, `lab_cross_wave_dedup_map_v1`) | **Refactored** in place. Writes only to `main.canonical_labs_thyroglobulin_v1` (FULL REBUILD with inline cross-wave dedup). |
| `scripts/127_analyst_institutional_lab_append.py` | YES (DELETE+INSERT into `main.longitudinal_lab_canonical_v1`) | **Refactored** in place. Routes each row by `lab_name_standardized` to the matching per-analyte canonical; `source='institutional_append'`. |
| `scripts/77_lab_canonical_layer.py` | YES (`CREATE OR REPLACE TABLE longitudinal_lab_canonical_v1`) | **FROZEN-stubbed** — superseded by Script 347. Original code preserved in git history. |
| `scripts/235_parathyroid_calcium_fix.py` | YES (`CREATE OR REPLACE TABLE longitudinal_lab_canonical_v1` for `value_corrected` column) | **FROZEN-stubbed** — calcium correction now applied at canonicalization in `main.canonical_labs_calcium_v1.value_numeric`. |
| `scripts/291_tsh_llm_integration.py` | YES (`INSERT INTO main.longitudinal_lab_canonical_v1`) | **FROZEN-stubbed** — TSH LLM rows live in `main.canonical_labs_tsh_v1` with `source='clinical_note'`. |
| `scripts/331_calcium_denominator_recovery.py` | YES (`INSERT INTO main.longitudinal_lab_canonical_v1`) | **FROZEN-stubbed** — calcium / PTH LLM recovery rows live in `main.canonical_labs_{calcium,pth}_v1`. |

---

## Step 2 — Pre-refactor archive snapshots

Taken under `--commit` by `scripts/348_lab_ingestion_refactor_verify.py` to
`"Thyroid 2026 UPdated".archive_pub_v1_0` with timestamp `pre348_<UTC>`:

| Source table | Snapshot | Rows |
|---|---|---|
| `main.canonical_labs_thyroglobulin_v1` | `canonical_labs_thyroglobulin_v1_pre348_<UTC>` | 53,006 |
| `main.canonical_labs_tsh_v1` | `canonical_labs_tsh_v1_pre348_<UTC>` | 556 |
| `main.canonical_labs_pth_v1` | `canonical_labs_pth_v1_pre348_<UTC>` | 200 |
| `main.canonical_labs_calcium_v1` | `canonical_labs_calcium_v1_pre348_<UTC>` | 187 |
| `main.canonical_labs_vitamin_d_v1` | `canonical_labs_vitamin_d_v1_pre348_<UTC>` | 86 |

These snapshots are the safety net for the refactor — under the no-op
expected outcome they are never restored.

---

## Step 3 — Write-strategy decision (refactored 113)

**Strategy chosen: (a) FULL REBUILD.**

`scripts/113_tg_lab_ingestion.py` does
`CREATE OR REPLACE TABLE main.canonical_labs_thyroglobulin_v1` with
inline cross-wave dedup applied via `ROW_NUMBER` PARTITION BY
`(research_id, analyte, lab_datetime::DATE,
COALESCE(value_numeric::VARCHAR, value_raw))` ordered by the Script 347
priority ladder.

**Rationale:**
- 113 owns 100 % of `source = 'structured_ehr_tg'` rows in the current
  canonical (53,006 / 53,006 rows). The two refactored writers (113 +
  127) never collide on the dedup key.
- The CSV ingestion is monotone — each run consumes the full source
  delivery, not incremental deltas.
- FULL REBUILD is atomic, idempotent, and matches Script 347 semantics.
- The single `ingestion_date` refresh per re-run is acceptable per the
  prompt.

A new `--rebuild-from-archive` mode lets 113 reproduce its output from
the pre347 archive snapshot of `thyroglobulin_lab_canonical_v1` — used
by Script 348 for the drift check when the source CSV is not at hand.

### Column mapping (legacy 113 → refactored 113 → canonical_labs_thyroglobulin_v1)

| Legacy 113 (Phase H) | Refactored 113 (Phase H) | canonical_labs_thyroglobulin_v1 |
|---|---|---|
| `research_id` (int) | `research_id` (int) | `research_id BIGINT` |
| `analyte` ('Tg' / 'TgAb') | `analyte` ('Tg' / 'TgAb') | `analyte VARCHAR` |
| `assay_method` | `assay_method` | `assay_method VARCHAR` |
| `specimen_collect_dt` (TIMESTAMP) | `lab_datetime` (TIMESTAMP) | `lab_datetime TIMESTAMP` |
| `result_raw` | `value_raw` | `value_raw VARCHAR` |
| `result_numeric` (regex parsed) | `value_numeric` ← `normalize_lab_value(value_raw, lab_test_name).value_numeric` | `value_numeric DOUBLE` |
| `result_qualifier` ∈ {`<`, `>`, `=`} | `is_censored` ← `normalize_lab_value(...).is_censored` | `is_censored BOOLEAN` |
| (none) | `value_correction_note` ← `normalize_lab_value(...).value_correction_note` | `value_correction_note VARCHAR` |
| (none — units backfilled implicitly) | `unit_standardized` ← `'ng/mL'` for Tg, `'IU/mL'` for TgAb (via `convert_to_canonical_unit`) | `unit_standardized VARCHAR` |
| (none) | `source = 'structured_ehr_tg'` | `source VARCHAR` |
| (added later) | `is_in_canonical_cancer_cohort` ← per-rid lookup from pre347 archive | `is_in_canonical_cancer_cohort BOOLEAN` |
| `ingestion_date` (date) | `ingestion_date` (UTC TIMESTAMP) | `ingestion_date TIMESTAMP` |
| `test_name_raw`, `order_dt`, `result_qualifier`, `result_flag`, `days_from_surgery`, `temporal_window`, `surg_date`, `race`, `gender`, `age_at_surgery`, `thyroid_procedure`, `disambiguation_method`, `disambiguation_confidence`, `ingestion_script` | dropped from canonical (kept only as Phase F/G QC) | not in canonical schema |

The canonical schema is stable and matches Script 347 exactly.

---

## Step 4 — 127 disposition

**Refactored.** Routes each analyst-CSV row by `lab_name_standardized`
to the matching per-analyte canonical:

| `lab_name_standardized` | Target table | `analyte` (Tg table only) |
|---|---|---|
| `thyroglobulin` / `tg` | `main.canonical_labs_thyroglobulin_v1` | `'Tg'` |
| `anti_thyroglobulin` / `tgab` / `tg_antibody` | `main.canonical_labs_thyroglobulin_v1` | `'TgAb'` |
| `tsh` | `main.canonical_labs_tsh_v1` | (none) |
| `pth` | `main.canonical_labs_pth_v1` | (none) |
| `calcium` / `total_calcium` / `corrected_calcium` / `ionized_calcium` / `ca` | `main.canonical_labs_calcium_v1` | (none) |
| `vitamin_d` / `25_oh_vit_d` / `vitd` | `main.canonical_labs_vitamin_d_v1` | (none) |

Every row is stamped `source = 'institutional_append'` (highest cross-
wave dedup precedence per Script 347). Idempotent wave replace is
keyed on the wave label embedded in `value_correction_note` as
`ingestion_wave_tag=<wave>` (the per-analyte canonical schema does not
carry an `ingestion_wave` column).

The script writes inside a single transaction across the affected
per-analyte tables. On any verification FAIL, it rolls back; the
pre348 snapshots remain for manual recovery.

---

## Step 5 — Drift report (DRY-RUN)

Refactored 113 was driven from the pre347 archive
(`"Thyroid 2026 UPdated".archive_pub_v1_0.thyroglobulin_lab_canonical_v1_pre347_20260421T164325Z`,
74,258 rows) through Phase H + the inline cross-wave dedup, into a temp
staging table. Compared to current `main.canonical_labs_thyroglobulin_v1`:

| Metric | Value |
|---|---|
| Pre-dedup canonical rows | 74,258 |
| Post inline-dedup rows (staging) | 53,006 |
| `main.canonical_labs_thyroglobulin_v1` rows | 53,006 |
| Rows added by refactor | **0** |
| Rows removed by refactor | **0** |
| `value_numeric` deltas > 1e-9 | **0** |

**Refactored 113 reproduces the exact 53,006 rows currently in
`main.canonical_labs_thyroglobulin_v1`, byte-for-byte on the dedup key
and within 1e-9 on every numeric value.**

127 was not exercised in this dry run because no analyst-delivered CSV
was provided to the verifier. When a CSV is provided via
`--input-127 <path> --ingestion-wave-127 <wave>`, the same
post-commit equivalence check covers all 5 per-analyte tables.

---

## Step 5 — Drift report (POST-COMMIT)

After the `--commit` run took the pre348 snapshots and (in the absence
of `--input-tg` / `--input-127`) made no further writes, the
post-commit equivalence check confirmed every per-analyte table
matches its pre348 snapshot exactly:

| Table | Main rows | Snapshot rows | Added | Removed |
|---|---|---|---|---|
| `canonical_labs_thyroglobulin_v1` | 53,006 | 53,006 | 0 | 0 |
| `canonical_labs_tsh_v1` | 556 | 556 | 0 | 0 |
| `canonical_labs_pth_v1` | 200 | 200 | 0 | 0 |
| `canonical_labs_calcium_v1` | 187 | 187 | 0 | 0 |
| `canonical_labs_vitamin_d_v1` | 86 | 86 | 0 | 0 |

(See `scripts/output/348_decision_<UTC>.json` for the live values.)

---

## PASS / FAIL summary

| Check | Result |
|---|---|
| `no_writes_to_dropped_tables_in_scripts` | ✅ PASS |
| `113_frozen_header_removed` | ✅ PASS |
| `113_imports_normalizer` | ✅ PASS |
| `refactored_113_row_count_within_tolerance` (±5 of 53,006) | ✅ PASS — 53,006 / 53,006 |
| `refactored_113_zero_other_structured_rows` | ✅ PASS — 0 |
| `refactored_113_analyte_in_tg_or_tgab` | ✅ PASS — 0 outside `{Tg, TgAb}` |
| `refactored_113_unit_correct_per_analyte` | ✅ PASS — Tg→ng/mL 100 %, TgAb→IU/mL 100 % |
| `post_commit_per_analyte_match_pre348_snapshot` | ✅ PASS (or `SKIPPED_DRY_RUN`) |
| `longitudinal_view_count_unchanged` (54,035) | ✅ PASS |
| `thyroglobulin_view_count_unchanged` (53,006) | ✅ PASS |
| `pytest_normalizer_passes` | ✅ PASS — 45 / 45 |
| `pyflakes_passes` | ✅ PASS |
| `cpm_invariant_pre` | ✅ PASS — `(10871, 10871, 0)` |
| `cpm_invariant_post` | ✅ PASS — `(10871, 10871, 0)` |

---

## CPM invariant

Pre and post: `SELECT COUNT(*), COUNT(DISTINCT research_id),
SUM(CASE WHEN fna_path_outcome IS NULL THEN 1 ELSE 0 END)
FROM main.canonical_patient_master` returns
**`(10871, 10871, 0)`** unchanged.

---

## Discordance / drift artefacts

Neither `discordance_review.md` nor `drift_review.md` was written —
no rows surfaced under the unit-conversion or drift checks.

If a future run produces drift or unrecognised units, those files will
be created at:
- `studies/lab_ingestion_refactor_20260421/discordance_review.md`
- `studies/lab_ingestion_refactor_20260421/drift_review.md`

---

## Followup items

1. **127 input parity** — when an analyst CSV is next delivered, run
   `scripts/348_lab_ingestion_refactor_verify.py --commit --input-127
   <csv> --ingestion-wave-127 <wave>` to exercise the full per-analyte
   routing path end-to-end against live data.
2. **Frozen scripts archive** — the 4 frozen scripts (77, 235, 291,
   331) are stub modules; the original code is in git history. If any
   downstream automation still calls them, redirect to the appropriate
   per-analyte canonical ingestion path
   (`scripts/113_tg_lab_ingestion.py` for Tg/TgAb;
   `scripts/127_analyst_institutional_lab_append.py` for everything
   else).
3. **Cancer-cohort backfill in 127** — `is_in_canonical_cancer_cohort`
   on freshly-appended 127 rows is filled via per-rid lookup against
   the existing per-analyte table. If a research_id is brand-new (no
   prior lab row in the target table), it lands as `FALSE`. The
   downstream cohort-rebuild step in Script 347 / future canonicalizer
   must own that backfill explicitly.

---

## Files changed

```
scripts/113_tg_lab_ingestion.py                       (REFACTORED in place)
scripts/127_analyst_institutional_lab_append.py       (REFACTORED in place)
scripts/348_lab_ingestion_refactor_verify.py          (NEW)
scripts/77_lab_canonical_layer.py                     (FROZEN stub)
scripts/235_parathyroid_calcium_fix.py                (FROZEN stub)
scripts/291_tsh_llm_integration.py                    (FROZEN stub)
scripts/331_calcium_denominator_recovery.py           (FROZEN stub)
studies/lab_ingestion_refactor_20260421/step1_discovery.md   (NEW)
studies/lab_ingestion_refactor_20260421/report.md           (NEW — this file)
scripts/output/348_run_<UTC>.log                       (NEW)
scripts/output/348_decision_<UTC>.json                 (NEW)
```
