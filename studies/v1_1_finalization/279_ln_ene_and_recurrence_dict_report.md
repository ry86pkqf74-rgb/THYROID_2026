# Script 279 — LN ENE realign, consistency retag, orphan routing, recurrence/LN dictionary clarification

**Run date:** 2026-04-18
**DB:** `thyroid_canonical_publication_v1_0`
**Archive:** `"Thyroid 2026 UPdated".archive_pub_v1_0`
**Script:** `scripts/279_ln_ene_consistency_and_recurrence_dict_clarification.py`
**Run log:** `scripts/output/279_run.log`
**Decision log:** `scripts/output/279_decision_log.json`
**Studies folder:** `studies/v1_1_finalization/` (extends existing v1.1 finalization work)

---

## 1. Coworker Prompt-20 dry-run audit — what was raised

The Prompt-20 dry-run audit raised eight concerns. Three are explicitly
withdrawn or already addressed per `PART2_DETAIL_CROSSVAL_FINDINGS_20260416.md`,
and the remaining five are real and addressed by Script 279.

### 1.1 Withdrawn / already-addressed (NO action by 279)

| # | Concern | Disposition | Citation |
|---|---|---|---|
| W1 | `any_recurrence_flag` "value bug" — should be rebuilt by OR-ing structural/biochemical subflags | **WITHDRAWN.** Rebuilding from subflags would REGRESS the canonical recurrence definition. The strict path-proven flag is `recurrence_flag_v2` (N TRUE = 189). `any_recurrence_flag_prev_233` preserves the historical broad definition for audit. Dictionary guidance is the only fix needed. | PART2 §7.2 |
| W2 | `ln_master_rollup_v1` has duplicate `research_id` rows — should be 1:1 against CPM | **NOT A BUG.** The table is per-surgery-episode by design; some patients have 2-4 surgeries. Documentation, not rebuild. The grain formalization is registered as v1.1 tech debt by Script 279 (see Phase 6 below). | PART2 §5.4 |
| W3 | `recurrence_date` is ~90% NULL | **SOURCE-LIMITED.** Not a data bug; the upstream extraction does not yield dates for most events. Dictionary guidance is the only fix. | PART2 (§5.4 / data-availability discussion) |

### 1.2 Real findings — addressed by Script 279

| # | Finding | Phase | Action |
|---|---|---|---|
| R1 | `ln_master_rollup_v1.ln_mets_extranodal_extension` (BOOLEAN) had only 14 TRUE across 4,273 rows; `ln_extranodal_extension` (INTEGER) had 1,326 positive — the BOOLEAN was a bifurcated dead column | Phase 2 | Rebuild BOOLEAN from `(ln_extranodal_extension > 0)`; INTEGER is the source of truth |
| R2 | 11 rows with `ln_total_positive > ln_total_examined` AND `ln_internal_consistency = 'ok'` (validator blind spot) | Phase 3 | Retag to `'impossible_positive_exceeds_examined'`; counts preserved (ingestion artifacts) |
| R3 | 2 orphan rows in `clinical_note_ln_extracted_v1` for `research_id='11454'` (no CPM row, no FNA / tumor / synoptic / path / operative — same pattern as the §7.3 withdrawn finding) | Phase 4 | Route the 2 source rows to `manuscript_workspace.ln_extract_noncohort_orphan_v279`; **do not delete source rows; do not add the patient to CPM** |
| R4 | Every recurrence + LN column in `data_dictionary_v266a` was tagged `'authoritative'` with empty `description` and NULL `replacement_column_name` — consumers cannot distinguish strict from intermediate from legacy | Phase 5 | Bump dictionary v266a → v279; populate `description` + `replacement_column_name` for 10 recurrence + 6 LN columns; refine three statuses (`archived_removed_from_cpm` / `component` / `authoritative_subtype`) |
| R5 | §5.4 documentation debt — `ln_rollup_source` exists on CPM but isn't documented in the dictionary; `ln_master_rollup_v1` per-surgery grain isn't codified | Phase 5 + Phase 6 | (a) `ln_rollup_source` clarified in v279; (b) per-surgery grain registered as v1.1 tech debt (`ln_rollup_per_surgery_grain_formalization_v1_1`) |

---

## 2. What Script 279 actually changed

### 2.1 Phase 2 — ENE BOOLEAN realign

```sql
UPDATE main.ln_master_rollup_v1
SET ln_mets_extranodal_extension = (ln_extranodal_extension > 0)
WHERE ln_extranodal_extension IS NOT NULL
  AND (ln_mets_extranodal_extension IS DISTINCT FROM (ln_extranodal_extension > 0));
```

| Metric | Before | After | Target | Status |
|---|---:|---:|---:|---|
| `ln_mets_extranodal_extension = TRUE` | 14 | **1,333** | 1,333 | OK |
| `ln_extranodal_extension > 0` (INTEGER source of truth) | 1,326 | 1,326 | — | reference |
| `bool <> (int > 0)` where INT is non-NULL (sync_violations) | — | **0** | 0 | OK |
| Rows where INT IS NULL but BOOL is set (preserved per "don't fabricate" rule) | — | 2,947 (of which 7 are TRUE) | n/a | documented |

The post-update `bool_true = 1,333` decomposes as `1,326` (rows where `INT > 0`) + `7` (rows where `INT IS NULL` but `BOOL` was `TRUE` upstream — left untouched per the prompt's "don't fabricate" rule). The correctness gate is `sync_violations = 0`, which passed.

Column comment updated:

> BOOLEAN: `(ln_extranodal_extension > 0)`. Rebuilt from the INTEGER count by Script 279 (2026-04-18) to resolve a bifurcation where the BOOLEAN was populated independently and only 14 of 4273 rows were TRUE while the INTEGER showed 1326 positive. The INTEGER is the source of truth; this column is a convenience flag.

### 2.2 Phase 3 — impossible-consistency retag

```sql
UPDATE main.ln_master_rollup_v1
SET ln_internal_consistency = 'impossible_positive_exceeds_examined'
WHERE ln_total_positive > ln_total_examined
  AND ln_internal_consistency = 'ok';
```

| Metric | Before | After | Status |
|---|---:|---:|---|
| Rows with `pos > exam` AND `consistency = 'ok'` | 11 | **0** | OK |
| Distinct `ln_internal_consistency` vocabulary | `{ok, mismatch, location_only}` | `{ok, mismatch, location_only, impossible_positive_exceeds_examined}` | extended |

Counts (`ln_total_examined`, `ln_total_positive`) are not modified — these are upstream ingestion artifacts (all 11 rows are `ln_source='tumor_pathology'` with `ln_total_examined=0`). Column comment updated to enumerate the new vocabulary.

### 2.3 Phase 4 — non-cohort orphan routing

`research_id='11454'` evidence profile (live):

| Source | Count |
|---|---:|
| `canonical_patient_master` | 0 |
| `fna_episode_master_v2` | 0 |
| `tumor_episode_master_v2` | 0 |
| `path_synoptics` | 0 |
| `operative_episode_detail_v2` | 0 |
| `clinical_notes_long` | 2 |
| `clinical_note_ln_extracted_v1` | 2 |

Identical to the §7.3 withdrawn-finding pattern: the patient has clinical notes (2 rows) but zero cancer evidence, so they were correctly excluded from the cancer cohort — the LN extractor processed the note anyway.

| Action | Result |
|---|---|
| Routed 2 source rows to `manuscript_workspace.ln_extract_noncohort_orphan_v279` (1 distinct rid) | OK |
| Deleted source rows from `clinical_note_ln_extracted_v1` | **NO** — preserved by design |
| Added rid 11454 to CPM | **NO** — correct cohort exclusion stands |
| Audit status | `DOCUMENTED_NOOP` |

### 2.4 Phase 5 — `data_dictionary_v266a` → `data_dictionary_v279`

| Metric | Value |
|---|---:|
| `data_dictionary_v266a` rows (pre-279) | 1,590 |
| `data_dictionary_v279` rows (post-279) | **1,591** |
| Stub rows inserted (column not in v266a) | 1 (`any_recurrence_flag_prev_233`) |
| Target columns clarified (description + replacement_column_name) | **16 / 16** |
| Blanks remaining among targets | 0 |
| `data_dictionary_v266a` retired from `main` | yes |

`any_recurrence_flag_prev_233` was REMOVED from CPM during the post-Script 249 cleanup; values survive in 11 archive snapshots (`canonical_patient_master_pre235..pre249`). Script 279 inserted a stub dictionary row (`table_name='canonical_patient_master_archived'`) so the documentation is complete; the column is not currently queryable from CPM.

#### Recurrence family (10 columns)

| column | status | replacement_column_name |
|---|---|---|
| `recurrence_flag_v2` | authoritative | — |
| `any_recurrence_flag` | authoritative | `recurrence_flag_v2` |
| `any_recurrence_flag_prev_233` | archived_removed_from_cpm | `recurrence_flag_v2` |
| `structural_recurrence_flag` | component | `recurrence_flag_v2` |
| `biochemical_recurrence_flag` | authoritative_subtype | — |
| `imaging_suspicious_recurrence_flag` | authoritative_subtype | — |
| `recurrence_type` | authoritative | — |
| `recurrence_date` | authoritative | `recurrence_date_v2` |
| `recurrence_date_v2` | authoritative | — |
| `time_to_recurrence_days` | authoritative | — |

#### LN family (6 columns)

| column | status | replacement_column_name |
|---|---|---|
| `ln_total_examined` | authoritative | — |
| `ln_rollup_total_examined` | authoritative | — |
| `ln_rollup_source` | authoritative | — |
| `ln_positive_flag` | authoritative | — |
| `ln_positive_binary` | authoritative | — |
| `ene_positive` | authoritative | — |

#### Catalog updates

| Catalog table | Action |
|---|---|
| `main.__readme` | Removed row for `data_dictionary_v266a`; inserted row for `data_dictionary_v279` (1,591 rows) |
| `manuscript_workspace.detail_table_registry_v1` | No change — registry never carried a row referencing the dictionary; decision logged in `279_decision_log.json` |

---

## 3. Phase 6 — v1.1 tech debt registered

Two items registered in `manuscript_workspace.v1_1_tech_debt_v1` (status `OPEN`, target_version `v1_1`, registered_by `script_279`):

### 3.1 `ln_rollup_per_surgery_grain_formalization_v1_1` (`table_design`)

`ln_master_rollup_v1` is per-surgery-episode by design (4,273 rows for 3,986 patients; 256 patients have 2-4 surgeries). The grain is intentional per PART2 §5.4 but is not codified in `COMMENT ON TABLE` or in the dictionary. A consumer reading `ln_master_rollup_v1` can silently mis-join 1:1 against `canonical_patient_master`.

**Recommendation for v1.1:**
1. Add `COMMENT ON TABLE main.ln_master_rollup_v1` documenting the per-surgery grain explicitly.
2. Add a `ln_master_rollup_patient_v1` companion view that picks a single per-patient row using a documented rule (recommend: `ORDER BY ln_source preference`, then `MAX(ln_total_examined)`, then `MAX(ln_total_positive)`).
3. Register both in `detail_table_registry_v1`.

### 3.2 `recurrence_flag_consumer_audit_v1_1` (`consumer_migration`)

After Script 279 clarified the recurrence-column dictionary, it is not yet known whether downstream consumers (`manuscript_workspace.cohort_*` views, analysis subsets, parquet export) read `recurrence_flag_v2` instead of `any_recurrence_flag` where strict path-proven semantics are intended.

**Recommendation for v1.1:** grep the repo + view definitions for `any_recurrence_flag` and `any_recurrence_flag_prev_233`. For each hit, decide whether the consumer wants strict (→ switch to `recurrence_flag_v2`) or broad (→ leave + add comment). Produce `scripts/output/recurrence_consumer_audit.md`.

---

## 4. Audit table — `manuscript_workspace.v1_1_finalization_audit_v1`

Final-apply rows for Script 279 (taken live):

| run_ts (UTC) | finding_id | metric | count_before | count_after | target_after | status |
|---|---|---|---:|---:|---:|---|
| 2026-04-18 03:10:15Z | `279_ene_boolean_realign` | `ene_boolean_true` | 1333 | 1333 | 1333 | OK |
| 2026-04-18 03:10:15Z | `279_consistency_impossible_retag` | `rows_retagged` | 0 | 0 | 0 | OK |
| 2026-04-18 03:10:15Z | `279_ln_extract_orphan_route` | `orphan_rids_routed` | 1 | 1 | 1 | DOCUMENTED_NOOP |
| 2026-04-18 03:10:16Z | `279_dictionary_clarification` | `columns_clarified` | 0 | 16 | 16 | OK |

Note: `279_ene_boolean_realign.count_before=1333` reflects the live apply (the value was 14 in the *very first* attempt; that attempt aligned the BOOLEAN successfully but failed an over-strict gate which has since been corrected to use `target_realigned = count(int>0) + count(preserved-true-under-null-int) = 1326 + 7 = 1333`). The pre-279 baseline value was 14, captured in the run log and snapshots.

---

## 5. Phase 7 — verification gates (all passed)

| Gate | Check | Result |
|---|---|---|
| A | CPM invariants (10,871 / 10,871 / 0) | PASS |
| B | CPM column count unchanged (1,526) | PASS |
| C | `bool <> (int > 0)` violations on `ln_master_rollup_v1` | 0 (PASS) |
| D | `pos > exam AND consistency='ok'` rows | 0 (PASS) |
| E | Clarified recurrence/LN columns in `data_dictionary_v279` | 16 (PASS) |
| F | `data_dictionary_v266a` retired from `main` | absent (PASS) |
| G | Pre-279 snapshots in `archive_pub_v1_0` | 10 ≥ 3 (PASS) |
| H | `v1_1_tech_debt_v1` rows registered | 2 (PASS) |

Note on Gate G: the 10 snapshots include three timestamped batches — initial attempt (`20260418T070418Z`), Phase-2-gate-fix attempt (`20260418T070553Z`, `20260418T070936Z`), and final clean apply (`20260418T071002Z`). Each batch covers the tables that still existed in `main` at the time (the third and fourth batches skip `data_dictionary_v266a` because it had already been retired by the second attempt — idempotent guard). The first three table batches all contain valid pre-mutation snapshots; only the earliest (`20260418T070418Z`) is the strict pre-279 baseline.

---

## 6. Archive snapshots

```text
"Thyroid 2026 UPdated".archive_pub_v1_0:
  ln_master_rollup_v1_pre279_20260418T070418Z          (strict pre-279 baseline)
  ln_master_rollup_v1_pre279_20260418T070553Z
  ln_master_rollup_v1_pre279_20260418T070936Z
  ln_master_rollup_v1_pre279_20260418T071002Z
  clinical_note_ln_extracted_v1_pre279_20260418T070418Z (strict pre-279 baseline)
  clinical_note_ln_extracted_v1_pre279_20260418T070553Z
  clinical_note_ln_extracted_v1_pre279_20260418T070936Z
  clinical_note_ln_extracted_v1_pre279_20260418T071002Z
  data_dictionary_v266a_pre279_20260418T070418Z         (strict pre-279 baseline)
  data_dictionary_v266a_pre279_20260418T070553Z
```

Each snapshot has a `COMMENT ON TABLE` of the form:

> Script 279 pre-mutation snapshot of thyroid_canonical_publication_v1_0.main.&lt;X&gt;. Reason: &lt;specific reason&gt;. Created at &lt;ISO&gt;.

---

## 7. Live verification queries

```sql
-- A. CPM untouched
SELECT COUNT(*) AS rows, COUNT(DISTINCT research_id) AS distinct_rid,
       COUNT(*) FILTER (WHERE research_id IS NULL) AS nulls
FROM main.canonical_patient_master;
-- 10871, 10871, 0

-- B. CPM column count unchanged
SELECT COUNT(*) AS n_cols
FROM main.information_schema.columns
WHERE table_schema='main' AND table_name='canonical_patient_master';
-- 1526

-- C. ENE bifurcation resolved
SELECT COUNT(*) AS sync_violations
FROM main.ln_master_rollup_v1
WHERE ln_extranodal_extension IS NOT NULL
  AND ln_mets_extranodal_extension <> (ln_extranodal_extension > 0);
-- 0

-- D. No more silent 'ok' on impossible rows
SELECT COUNT(*) AS impossible_ok
FROM main.ln_master_rollup_v1
WHERE ln_total_positive > ln_total_examined
  AND ln_internal_consistency = 'ok';
-- 0

-- E. Dictionary has clarifications
SELECT COUNT(*) AS clarified_rows
FROM main.data_dictionary_v279
WHERE column_name IN ('recurrence_flag_v2','any_recurrence_flag',
  'any_recurrence_flag_prev_233','structural_recurrence_flag',
  'biochemical_recurrence_flag','imaging_suspicious_recurrence_flag',
  'recurrence_type','recurrence_date','recurrence_date_v2',
  'time_to_recurrence_days','ln_total_examined','ln_rollup_total_examined',
  'ln_rollup_source','ln_positive_flag','ln_positive_binary','ene_positive')
  AND description IS NOT NULL AND description <> '';
-- 16

-- F. Old dictionary retired
SELECT COUNT(*) AS old_dict
FROM main.information_schema.tables
WHERE table_schema='main' AND table_name='data_dictionary_v266a';
-- 0

-- H. Tech debt items registered
SELECT debt_id, status, target_version
FROM manuscript_workspace.v1_1_tech_debt_v1
WHERE registered_by = 'script_279';
-- ('ln_rollup_per_surgery_grain_formalization_v1_1', 'OPEN', 'v1_1')
-- ('recurrence_flag_consumer_audit_v1_1', 'OPEN', 'v1_1')
```

---

## 8. Final state summary

| Object | State |
|---|---|
| `canonical_patient_master` | 10,871 × 1,526 cols (**unchanged** — no CPM mutation by 279) |
| `ln_master_rollup_v1.ln_mets_extranodal_extension` | Realigned to `(ln_extranodal_extension > 0)`; 14 → 1,333 TRUE; sync_violations = 0 |
| `ln_master_rollup_v1.ln_internal_consistency` | 11 silent-`'ok'` rows retagged to `'impossible_positive_exceeds_examined'`; counts preserved |
| `clinical_note_ln_extracted_v1` | Source rows preserved; 2 non-cohort rows for rid 11454 routed to `manuscript_workspace.ln_extract_noncohort_orphan_v279` |
| `data_dictionary_v266a` | Retired from `main` (snapshot in archive) |
| `data_dictionary_v279` | Published; 1,591 rows; 16 recurrence/LN columns clarified (15 UPDATEd + 1 stub-INSERTed) |
| `manuscript_workspace.v1_1_tech_debt_v1` | +2 OPEN items for v1.1 |
| Archive `archive_pub_v1_0` | 10 pre-279 snapshots present |

**Prompt 20 real findings:** addressed.
**Prompt 20 withdrawn findings** (`any_recurrence_flag` value rebuild, `ln_master_rollup_v1` 1:1 restructure, `recurrence_date` backfill): confirmed not actioned per PART2 §7.2 / §5.4 / source-limited.

Canonical remains v1.0-published + v1.1-patched. Safe to proceed.
