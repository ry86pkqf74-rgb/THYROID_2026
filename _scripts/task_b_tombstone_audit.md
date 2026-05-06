# Task B — Tombstoned `views_readable` audit (BigQuery migration)

## Pathology_Tumor_Characteristics — **REBUILD (THY-18)**

| Item | Value |
|------|-------|
| Linear | [THY-18](https://linear.app/rostemp/issue/THY-18/) |
| Root cause | `pub_canonical.canonical_tumor_characteristics_v1` never migrated from MotherDuck; view DDL `SELECT * FROM` base table failed (404). |
| Original tombstone | `mig_040_view_Pathology_Tumor_Characteristics` |
| Rebuild migration ids | `mig_323_export_ctc_md_to_parquet` (`.py` + `.sql` runbook), `mig_324_load_ctc_bq.py`, `mig_325_register_ctc_signoff.sql`, `mig_326_view_Pathology_Tumor_Characteristics_rebuild.sql` |
| Execution order | (1) `323_export_ctc_md_to_parquet.py` → (2) `324_load_ctc_bq.py` → (3) `bq query` 325 → (4) `bq query` 326 |
| Parity | `COUNT(*)` MD vs BQ within ±0.1% |
| PHI | Exporter drops MRN/name/full-DOB–pattern columns; BQ keyed by `research_id` + clinical tumor fields only. |

Optional smoke:

```sql
SELECT COUNT(*) FROM `thyroid-canonical-pub-2026.pub_views_readable.Pathology_Tumor_Characteristics`;
```

---

## ETE Manuscript Analytic Family — **REBUILT (THY-19)**

| Item | Value |
|------|-------|
| Linear | [THY-19](https://linear.app/rostemp/issue/THY-19/) |
| Root cause | 10 upstream helpers (`path_malignant_event_fingerprint_v1` + 9 `path_malignant_overlay_*_w_fp_v1`) did not exist in BQ; DuckDB `rowid`-based fingerprint pattern replaced with MD5+QUALIFY dedup pattern |
| Original tombstones | `mig_037, mig_039, mig_059, mig_067, mig_068, mig_069` |
| Rebuild migration IDs | `mig_089` (10 helpers + deduped base), `mig_090` (v1), `mig_091` (v2), `mig_092` (v3), `mig_093` (v4), `mig_094` (v6, ~ fixed), `mig_095` (v7) |
| Migration files | `bq_migrations/mig_089_ete_fp_helpers_bq_20260506.sql` through `mig_095_ete_analytic_v7_bq_20260506.sql` |
| DDL source | `views_sql/ete_family/*.duckdb.sql` (18 files captured from MotherDuck 2026-05-06) |
| BQ row counts (all views) | 6,466 each (MD baseline: 6,469; parity: 0.046% ✓) |
| Guard table | `cohort_m044_ajcc_ete_v1`: 3,868 rows ✓ (not regressed) |
| Key translation notes | DuckDB `rowid` → `MD5(composite_key)` + `QUALIFY ROW_NUMBER()=1`; `~~` → `LIKE`; `CAST(x AS VARCHAR)` → `CAST(x AS STRING)`; `date_diff('day',a,b)` → `DATE_DIFF(CAST(b AS DATE),CAST(a AS DATE),DAY)`; `main.*` → `pub_canonical.*`; `manuscript_workspace.*` → `pub_workspace.*`; `canonical_patient_master.research_id` INT64 → CAST |
| BQ fan-out fix | 3 truly identical duplicate rows in `canonical_path_malignant_events_v1` (research_id=593) caused 2^9=512× fan-out through 9 fingerprint JOINs; fixed with `QUALIFY ROW_NUMBER() OVER (PARTITION BY natural_key) = 1`; `canonical_path_malignant_events_v1_deduped` helper VIEW added |
| DFL | `DFL-20260506-ETEFAMILY` (action_type=md_to_bq_migrate) |
| Completion date | 2026-05-06 |

Optional smoke:

```sql
SELECT view_name, COUNT(*) AS n FROM (
  SELECT 'v1' AS view_name FROM `thyroid-canonical-pub-2026.pub_workspace.ete_manuscript_analytic_v1`
  UNION ALL SELECT 'v7' FROM `thyroid-canonical-pub-2026.pub_workspace.ete_manuscript_analytic_v7`
  UNION ALL SELECT 'guard_m044' FROM `thyroid-canonical-pub-2026.pub_workspace.cohort_m044_ajcc_ete_v1`
) GROUP BY view_name;
-- Expected: v1=6466, v7=6466, guard_m044=3868
```

---

## Other tombstoned views

Record per-view decisions in follow-on PRs as they are addressed. This file was seeded during THY-18; extend the table above for remaining views.
