# MotherDuck lymph node completeness audit (THYROID_2026)

**Generated (UTC):** 2026-03-27T05:59Z  
**Database:** `thyroid_research_2026` (MotherDuck prod, authenticated)  
**Runner:** `studies/proposal2_ete_staging/run_motherduck_ln_completeness_audit.py` (`--sa` for GitHub / `MD_SA_TOKEN`; `--deep` for extra `COUNT(*)` proof; `--quiet` to silence timing logs)  
**SQL reference:** `studies/proposal2_ete_staging/sql/motherduck_lymph_node_completeness_audit.sql` (specimen spine SQL is embedded in the runner)

## 0. Execution profile & why wall time is often only a few seconds

This audit is **not** a full-text scan of pathology narratives or clinical notes. It only:

- Builds one temp table over **`path_synoptics`** (≈ tens of thousands of synoptic rows) with a join to **`tumor_pathology`**.
- Runs grouped aggregates and exports **CSV** extracts.

DuckDB is a **columnar** engine; MotherDuck runs those operators on **remote** storage. For this data volume, **sub‑second to a few seconds** of compute is expected. Short runtime **does not** imply the script skipped MotherDuck: see **`motherduck_connection_proof`** in `ln_audit_summary.json` (`pragma_database_list` paths include the `md:` MotherDuck attachment) and the timed steps below.

**DuckDB version (server-reported):** `v1.4.4`

| Step | Seconds |
|------|--------:|
| `build_ln_specimen_temp` | 0.2129 |
| `connect_md` | 0.4983 |
| `deep_full_table_counts` | 0.072 |
| `query_inconsistencies` | 0.0245 |
| `query_missing_unresolved` | 0.0247 |
| `query_subgroup_summary` | 0.0133 |
| `recurrence_risk_mv_summary` | 0.0418 |
| `specimen_summary_aggregates` | 0.0044 |
| `table_presence_check` | 0.4201 |
| `wall_clock_total_s` | 3.0445 |
| `write_csv_exports` | 0.0802 |

### Deep full-table counts (this run, `--deep`)

| Table / metric | Rows |
|----------------|-----:|
| path_synoptics_all_rows | 11688 |
| path_synoptics_rid_not_null | 11688 |
| path_synoptics_distinct_patients | 10871 |
| tumor_pathology_rows | 4290 |
| recurrence_risk_features_rows | 4976 |


## 1. Canonical lineage (proposal2_ete_staging)

| Layer | MotherDuck object | LN-related fields |
|-------|-------------------|-------------------|
| Specimen / synoptic (structured pathology rows) | `path_synoptics` | `tumor_1_ln_examined`, `tumor_1_ln_involved`; neck surgery descriptors (`central_compartment_dissection`, `tumor_1_level_examined`, `other_ln_dissection`, `tumor_1_ln_location`) |
| Patient-level tumor table | `tumor_pathology` | `histology_1_ln_examined`, `histology_1_ln_positive`, `histology_1_ln_ratio`, `histology_1_n_stage_ajcc8` |
| Risk / analytic MV (documented study source) | `recurrence_risk_features_mv` | `ln_examined`, `ln_positive`, `ln_ratio`, `pn_stage` (from `tumor_pathology` in repo definition) |
| Risk + survival (dashboard / sap) | `risk_enriched_mv` | Same as `recurrence_risk_features_mv` for pathology/LN columns (plus survival fields) |
| Canonical episode table | `tumor_episode_master_v2` | Nodal counts copied from path_synoptics in script 22 (`nodal_disease_*`) when materialized |

**Note:** `studies/proposal2_ete_staging/proposal2_ete_analysis.py` loads frozen CSVs (`exports/ptc_full.csv`), not DuckDB directly. The **MotherDuck-analytic** cohort described in `README.md` for this study is `risk_enriched_mv` / `recurrence_risk_features_mv`; LN variables in models (`ln_ratio`) trace to `tumor_pathology` in those views.

## 2. Null / placeholder semantics (explicit)

- **NULL (cleaned numeric):** No parseable integer/double in structured LN fields after stripping `;` and `x` and trimming. Does *not* prove absence of nodal sampling; means structured synoptic/pathology did not yield a usable count in this ETL pass.
- **0:** Explicit numeric zero in structured field after cleaning — treated as **explicit zero-node / no positive** representation for that field.
- **Raw `x`:** Stripped before cast; if the cell is only `x`, cleaned value becomes NULL (unresolved count, not interpreted as positive).

## 3. Pathology-bearing cohort and completeness (path_synoptics grain)

Denominator: all rows in `path_synoptics` with non-null `research_id` (synoptic / specimen spine).

| Metric | Count | % of specimens |
|--------|------:|---------------:|
| Total specimens | 12,396 | 100.00 |
| `ln_examined` populated (cleaned non-null) | 9,586 | 77.33 |
| `ln_positive` populated (cleaned non-null) | 4,467 | 36.04 |
| Both populated | 4,453 | 35.92 |
| Explicit zero on examined or positive (cleaned) | 7,920 | 63.89 |
| Both NULL (unresolved) | 2,796 | 22.56 |

**Stratification note:** `path_synoptics` on this database does not expose a `specimen_type` column; `specimen_type` in exports is NULL (placeholder). Subgroup CSV still stratifies by surgery year, extent, histology bucket, and central LND composite flag.

## 4. recurrence_risk_features_mv (patient row grain)

| Metric | Value |
|--------|------:|
| Rows | 4,976 |
| Distinct patients | 3,986 |
| ln_examined non-null | 4,884 |
| ln_positive non-null | 4,611 |
| Both non-null | 4,610 |
| ln_ratio non-null | 4,608 |
| ln_ratio present but missing examined or positive | 0 |

`recurrence_risk_features_mv` can list **multiple rows per patient** (see workspace notes on this view); denominators above are **rows**, not deduplicated patients.

## 5. Logical checks (automated)

Exported rows: **`audit_motherduck_ln/ln_audit_logical_inconsistencies.csv`** (4,786 rows).

| Issue | Rows |
|-------|-----:|
| `specimen_vs_tumor_path_examined_mismatch` | 2,327 |
| `n1_family_zero_or_missing_positive_nodespec` | 2,033 |
| `specimen_vs_tumor_path_positive_mismatch` | 407 |
| `positive_without_examined` | 14 |
| `duplicate_surgery_conflicting_ln_counts` | 4 |
| `positive_gt_examined` | 1 |

**Interpretation:** `specimen_vs_tumor_path_*_mismatch` compares each **synoptic row** to **`tumor_pathology` joined only on `research_id`** (patient-level pathology table, not surgery-episode–matched). Large counts are therefore expected when multi-specimen patients differ from the single aggregated pathology row, or when sources capture different levels of detail — this is **discordance for review**, not automatically a row-level data entry error.

Other flags: positive > examined; positive without examined; N1-stage (tumor_pathology) with zero/missing **specimen-level** positive count; `tp_ln_ratio` without backing counts; duplicate surgery date with conflicting LN pairs.

Rows flagged `duplicate_surgery_conflicting_ln_counts`: 4.

## 6. Stratified unresolved missingness

See **`audit_motherduck_ln/ln_audit_subgroup_summary.csv`** (by year, surgery extent bucket, histology bucket, central LND composite flag).

## 7. Deliverables

| File | Description |
|------|-------------|
| `sql/motherduck_lymph_node_completeness_audit.sql` | Documented SQL fragments / temp table definition |
| `audit_motherduck_ln/ln_audit_missing_unresolved.csv` | Specimens without both LN counts populated |
| `audit_motherduck_ln/ln_audit_logical_inconsistencies.csv` | Automated inconsistency flags |
| `audit_motherduck_ln/ln_audit_subgroup_summary.csv` | Subgroup completeness |
| `audit_motherduck_ln/ln_audit_summary.json` | Machine-readable summary + verdict |

## 8. Final verdict

**NOT ADEQUATE / REQUIRES REMEDIATION**

Rationale: on the **`path_synoptics` specimen spine**, only **35.92%** of rows have **both** examined and positive numeric LN fields populated; **22.56%** have **both** NULL after cleaning — **not** “complete for analytic use” without explicit missing-data handling. By contrast, among rows in **`recurrence_risk_features_mv`** (the MotherDuck object documented for proposal2 analytic features), **~93%** of rows have both `ln_examined` and `ln_positive` non-null — but that view is a **narrower, tumor-pathology–filtered cohort** with multiple rows per patient possible, **not** proof that every synoptic specimen row is enumerated. Any analysis must align the completeness statement with the **exact table grain** used. Structured synoptic LN coverage remains a **remediation target** if specimen-level completeness is required.


**patient_refined_master_clinical_v12:** 10,350 patient rows (joined to specimen cohort) have `ln_total_examined` OR `ln_positive_v6` non-null (ad hoc count; columns may differ from path_synoptics grain).

## Related — Excel source-of-truth cross-check

To verify that `path_synoptics` lymph-node fields match the canonical synoptic workbook row-for-key (after join on `research_id` + surgery date), run:

`studies/proposal2_ete_staging/run_excel_vs_motherduck_ln_reconcile.py`

Outputs: `EXCEL_VS_MOTHERDUCK_LN_RECONCILE.md` and `audit_excel_vs_md_ln/` under the same study folder.

---
*This report is generated from live MotherDuck queries; re-run the runner to refresh.*
