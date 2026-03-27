# Provenance — `studies/molecular_utilization_2026`

| Item | Value |
|------|--------|
| Repository release tag | `v2026.03.13` |
| Zenodo DOI | [10.5281/zenodo.18945510](https://doi.org/10.5281/zenodo.18945510) |
| Database | DuckDB / local DuckDB `thyroid_master.duckdb` |
| Study SQL (V1 operated-histology shell) | [`sql/01_views_and_cohort.sql`](sql/01_views_and_cohort.sql) |
| Study SQL (V2 manuscript refresh: III/IV denom + episode QA) | [`sql/01_views_and_cohort_v2.sql`](sql/01_views_and_cohort_v2.sql) |
| Verification SQL (Console-ready) | [`sql/02_local DuckDB_verification.sql`](sql/02_local DuckDB_verification.sql) |
| Verification SQL V2 | [`sql/02_local DuckDB_verification_v2.sql`](sql/02_local DuckDB_verification_v2.sql) |
| V2 exports | `outputs/v2/` (CSV, `table_v2_*.md`, `fig_v2_*.csv`, `freeze_manifest_v2.json`, `qa_report_v2.md`) |
| Schema / linkage audit | [`SCHEMA_LINKAGE_AUDIT.md`](SCHEMA_LINKAGE_AUDIT.md) |
| Executable analysis | [`python/run_analysis.py`](python/run_analysis.py) |

## Source tables & views (primary)

- `manuscript_cohort_v1` — patient spine, Bethesda, surgery, histology, sizes
- `molecular_test_episode_v2` — ThyroSeq / Afirma episodes, dates, classes
- `operative_episode_detail_v2` — procedure_normalized, dates
- `tumor_episode_master_v2` — completion thyroidectomy inference
- `fna_molecular_linkage_v2` / `fna_molecular_linkage_v3` — **audit only** (v2 empty; v3 sparse on operated Bethesda chain — see audit)
- `preop_surgery_linkage_v3`, `surgery_pathology_linkage_v3` — episode bridge diagnostics (`audit_fna_episode_operated_b35_v1`)
- `val_episode_linkage_completeness_v1` — linkage KPI snapshot

## Pipeline scripts cited in audit

- `scripts/78_final_hardening.py`
- `scripts/95_episode_linkage_repair.py`
- `scripts/96_episode_downstream_repair.py`
- `scripts/97_episode_linkage_audit.py`
- `scripts/98_final_verification_pass.py`

## Transparency recommendations

1. **Archive** the `outputs/` CSV bundle plus `01_views_and_cohort.sql` alongside the Zenodo dataset on the next archive bump.
2. **local DuckDB Pro:** materialize `indeterminate_molecular_cohort_v1` as a TABLE (CTAS) for scheduled refresh dashboards; use read-only sharing for collaborators.
3. **Supplement:** include `outputs/cohort.csv` dictionary (column definitions mirror SQL view) and the `mol_result_class_map_v1` mapping.
4. **Anonymized Parquet:** `COPY (SELECT … FROM indeterminate_molecular_cohort_v1) TO 'cohort.parquet' (FORMAT PARQUET);` after stripping direct identifiers if any are ever added to the view.
