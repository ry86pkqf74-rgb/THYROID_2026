# VIEW Labeling Pass — Closeout (2026-04-21)

**Status:** Phases 1–7 complete (per Logan adjudication: Q1–Q3). **Lab `*_VIEW_v1` canonical prefix:** deferred (carry-forward, not this PR). **Platform VIEWs:** excluded in QA (hardcoded `NOT IN` list).

## Renames (MotherDuck `thyroid_canonical_publication_v1_0.main`)

| Old | New | Rows (smoke) |
|-----|-----|----------------|
| `canonical_us_exam_master_v2` | `canonical_us_exam_master_VIEW_v2` | 18,551 |
| `canonical_us_patient_master_v2` | `canonical_us_patient_master_VIEW_v2` | 10,859 |
| `molecular_fusions_unnested_v2` | `molecular_fusions_unnested_VIEW_v2` | 60 |
| `molecular_variants_unnested_v2` | `molecular_variants_unnested_VIEW_v2` | 936 |

**Dependency note:** `ALTER VIEW … RENAME` left `canonical_us_patient_master_VIEW_v2` with stored SQL still referencing the old exam object name. The patient view was immediately rebuilt with `CREATE OR REPLACE` using the same logic as the archived pre-rename definition, with `main.canonical_us_exam_master_v2` → `main.canonical_us_exam_master_VIEW_v2` in `FROM` clauses. Exam and molecular views were also refreshed with `CREATE OR REPLACE` so definitions match rename semantics without compat shims.

## Registry

`manuscript_workspace.detail_table_registry_v1`: `detail_table_name` updated for the two US rows (exam + patient). Molecular unnested views were not listed in the registry.

## Provenance

Pre-rename `CREATE VIEW` text (four files): `scripts/archive/view_definitions_20260421/<old_stem>.sql` (stems are the **old** object names, per Phase 0 handoff).

## Live code + artifacts touched (non-frozen)

- **~44** first-pass `*.py` / `*.sql` / `*.md` + subsequent **11** `scripts/output` `*.json` / `*.csv` (frozen and `scripts/archive/` **not** updated; archive intentionally retains old `CREATE VIEW` names in body).
- Representative scripts: `scripts/preB_cupm_v2_canonical_backfill.py`, `scripts/366_*.py`, `scripts/367_*.py`, `scripts/cpm_tirads_partB_*.py`, `scripts/output/_cpm_tirads_partB_phase1_coverage.py`, cohort SQL under `scripts/output/_partB_phase2_view_defs/`.

**Frozen:** `scripts/frozen/**` left unchanged; grep for old US v2 object names is expected there.

## QA

- **6.1** `information_schema` check with platform `NOT IN` + `_VIEW_` `LIKE` — **0 rows** (no violations) after renames.
- **6.2** Old names: expect hits only under `scripts/frozen/**` and `scripts/archive/view_definitions_20260421/**` (and historical `*.log` if present).
- **Smokes:** `scripts/qa_view_labeling_main_20260421.py` (row counts + 6.1 query).

## Compat shims

None. No `CREATE OR REPLACE VIEW <old> AS SELECT * FROM <new>` temporary aliases.

## Commits (see `git log`)

| Order | Message | SHA (short) |
|-------|---------|-------------|
| 1 | `archive: capture view DDLs before _VIEW rename pass (view_labeling_20260421)` | `45993e3` |
| 2 | `migrate: main VIEWs → _VIEW suffix (US + molecular readers, registry, outputs, QA)` | `87ccd4c` |

---

*Logan: green-light adjudication 2026-04-21 on four renames, platform exception list, lab deferral, frozen exclusion.*
