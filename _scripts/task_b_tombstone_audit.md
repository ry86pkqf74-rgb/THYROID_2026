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

## Other tombstoned views

Record per-view decisions in follow-on PRs as they are addressed. This file was seeded during THY-18; extend the table above for remaining views.
