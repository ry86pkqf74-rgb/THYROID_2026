# Cursor Prompt — mig_180 PM `nlp_*` cluster verify + apply (~116 cols)

**Date:** 2026-04-29 (late evening)
**Lane:** mig_180 / pm_nlp_cluster_path_c_verify_apply
**Batch (proposed):** `mig_180_patient_master_nlp_cluster_path_c_20260429`
**Predecessor:** mig_152 (placeholder; never actually authored apply SQL); mig_159 closed clinical residual (27 cols verified)
**Posture:** SQL-only authoring + commit + push. **DO NOT execute against MotherDuck.** Cowork applies via Path C with full pre-snapshot.
**Target DB:** `thyroid_canonical_publication_v1_0`
**Primary table touched:** `main.canonical_column_verification_registry_v1` (registry-only — NO data writes to canonical_patient_master)

---

## Mission

Close out the largest remaining PM not_started backlog. Live MD shows **116 `nlp_*` prefix cols not_started** out of 132 total PM not_started. These are NLP-derived "has_data / n_entities / n_notes / positive_mentioned / key_finding / confidence_tier" cluster columns built upstream from `note_entities_llm_*` and related Tier 1 tables. After this lane lands, PM not_started drops 132 → 16 (the 15 new `syn_*` cols from mig_173b + 1 other) and PM verified climbs from 1,458 → 1,574 (~98%).

---

## Concrete scope (live MD probed by Cowork 2026-04-29)

**116 `nlp_*` cols** in `canonical_column_verification_registry_v1` where:
- `schema_name='main'`
- `table_name='canonical_patient_master'`
- `column_name LIKE 'nlp\_%'` ESCAPE '\\'
- `verification_status='not_started'`
- `batch_id IS NULL` (currently un-batched; will be set to mig_180 batch_id during apply)

Sample column families observed in probe:
- `nlp_airway_*` (has_data, key_finding, n_entities, n_notes)
- `nlp_cervln_*` (confidence_tier, has_data, n_entities, positive_mentioned)
- `nlp_dynrisk_*` (has_data, key_finding, n_entities, n_notes)
- `nlp_esoph_*` (confidence_tier, has_data, n_entities, positive_mentioned)
- `nlp_frozensec_*`, `nlp_funcoutcome_*`, `nlp_imaging_*`, `nlp_labs_*`
- `nlp_ln_*` (has_data, levels_mentioned, n_entities, n_notes, positive_mentioned)
- `nlp_ne_complications_*`, `nlp_ne_genetics_*`, `nlp_ne_medications_*`, `nlp_ne_operative_*`, `nlp_ne_problemlist_*`, `nlp_ne_staging_*`
- `nlp_parathyroid_*` (and ~10 more; full list to be enumerated by SELECT)

---

## Required scope

### §1 Catalog the 116 cols
Enumerate every `nlp_*` not_started col on `main.canonical_patient_master` with full metadata (data_type from `information_schema.columns`, current registry row state). Group by family prefix (`nlp_<domain>_<metric>`). Output a CSV `qc_framework_v1/reports/mig_180_nlp_cluster_inventory_20260429.csv` with columns: `col_name, family, metric_kind, data_type, description_inferred`.

### §2 Identify upstream lineage per family
For each `nlp_<domain>` family, locate the upstream NLP source table — one of:
- `main.note_entities_llm_<domain>` (or domain alias)
- `main.note_entities_<domain>` (LLM v0)
- A canonical_*_events_v1 derived from these
- Or a feeder table in `manuscript_workspace.*`

For each col, write a derivation expression that re-derives the value from the upstream table. E.g., `nlp_airway_has_data = EXISTS (SELECT 1 FROM main.note_entities_llm_airway_invasion WHERE research_id=pm.research_id AND error=0)`. Standard metric-kind mapping:
- `_has_data` (BOOLEAN) → EXISTS join on (research_id, error=0)
- `_n_entities` (INTEGER) → COUNT(*) over upstream rows
- `_n_notes` (INTEGER) → COUNT(DISTINCT note_id-equivalent)
- `_positive_mentioned` (BOOLEAN) → BOOL_OR(<positivity flag>)
- `_levels_mentioned` (VARCHAR) → STRING_AGG(<level>) ordered
- `_confidence_tier` (VARCHAR) → MAX(confidence_band) or rule
- `_key_finding` (VARCHAR) → first/highest-confidence finding
- `_n_rows` (INTEGER) → COUNT(*)

### §3 Author the verify SQL
Per mig_152 placeholder pattern (see Cowork close-outs `project_meds_parathyroid_families_complete_2026-04-29.md` for tier-2 extraction-faithfulness pattern, adapted to PM):

For each col: full re-derivation against upstream. Mass-equivalence probe:
```sql
SELECT
  '<col_name>' AS col,
  COUNT(*) FILTER (WHERE pm.<col> IS DISTINCT FROM rederived.<col>) AS n_mismatches,
  COUNT(*) AS n_rows
FROM main.canonical_patient_master pm
LEFT JOIN (<rederivation CTE>) rederived USING (research_id);
```

Aggregate result table per col into `qc_framework_v1/reports/mig_180_nlp_rederivation_audit_20260429.md` with each col's mismatch count.

### §4 Cohort-uniformity sweep on every BOOLEAN
For every `_has_data` and `_positive_mentioned` col (likely 30-40 cols), run BOTH-direction sweep:
```sql
SELECT
  '<col>' AS col,
  COUNT(*) FILTER (WHERE <col> = TRUE) AS n_true,
  COUNT(*) FILTER (WHERE <col> = FALSE) AS n_false,
  COUNT(*) FILTER (WHERE <col> IS NULL) AS n_null
FROM main.canonical_patient_master;
```

Rule per `feedback_use_desktop_commander_first.md` working memory:
- 0 TRUE → Type-B placeholder → propose verified→**na** with `helper_<col>_pending_real_extraction` methodology
- 0 FALSE / TRUE-only → Type-A presence flag → verified + add `CF-COHORT-NEAR-UNIFORM-TRUE-<col>` informational note
- All NULL → na
- Otherwise → verified

### §5 Author apply SQL artifact
`qc_framework_v1/migrations/180_patient_master_nlp_cluster_path_c_20260429.sql` with:

§A — pre-snapshot of affected registry rows
```sql
CREATE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_column_verification_registry_pre_mig180_20260429 AS
SELECT *, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig180_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_column_verification_registry_v1
WHERE schema_name='main' AND table_name='canonical_patient_master'
  AND column_name LIKE 'nlp\_%' ESCAPE '\\';
```

§B — global Path-C stamp on all 116 cols (one UPDATE; sets verified_by/batch_id/verification_method/verified_ts/methodology per family)

§C — per-col status flips: bulk verified set + Type-B na set + degenerate-NULL na set (one UPDATE per group with CASE)

§D — per-family CF appendices in `notes` for any cohort-uniformity finding (Type-A near-uniform / Type-B placeholder)

§E — resync `canonical_table_signoff_registry_v1` for `canonical_patient_master`:
```sql
UPDATE main.canonical_table_signoff_registry_v1
SET n_verified = (SELECT COUNT(*) FROM main.canonical_column_verification_registry_v1 WHERE schema_name='main' AND table_name='canonical_patient_master' AND verification_status='verified'),
    n_na = (SELECT COUNT(*) FROM main.canonical_column_verification_registry_v1 WHERE schema_name='main' AND table_name='canonical_patient_master' AND verification_status='na'),
    n_not_started = (SELECT COUNT(*) FROM main.canonical_column_verification_registry_v1 WHERE schema_name='main' AND table_name='canonical_patient_master' AND verification_status='not_started'),
    signoff_migration = 'qc_framework_v1/migrations/180_patient_master_nlp_cluster_path_c_20260429.sql',
    updated_at = CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
WHERE table_name='canonical_patient_master';
```

§F — post-state verification probe (read-only): expect `n_verified` to grow from 1,458 → 1,574 if all 116 verified, OR proportionally less if some go na.

### §6 Audit/report
`qc_framework_v1/reports/mig_180_nlp_cluster_audit_20260429.md` with:
- Summary table: family × n_cols × n_verified × n_na proposed
- Per-family upstream lineage rationale
- Mismatch counts from §3 mass-equivalence
- All Type-A / Type-B findings from §4 sweeps
- Open carry-forwards if any cluster has structural issues

### §7 Open carry-forwards (anticipate)
- `CF-mig180-NLP-PLACEHOLDER-FAMILIES` — any family with 0 TRUE patterns (if upstream extraction not yet run)
- `CF-mig180-NLP-NEAR-UNIFORM-TRUE-<col>` — any near-uniform-TRUE col
- `CF-mig180-NLP-UPSTREAM-MISSING-<domain>` — any family whose upstream table doesn't exist yet

---

## Governance reminders

- **Read-only audit + SQL authoring only.** Cowork executes Path C apply with full pre-snapshot.
- Author = `Logan Glosser <logan.glosser@gmail.com>` for all commits
- Surgical git add only (explicit paths; never `-A`)
- DuckDB MCP wrapper: one statement per call; no `BEGIN TRANSACTION;` / `COMMIT;`
- Pre-flight cohort parity 10,871 invariant (registry-only writes — no PM data touched)

---

## Deliverables

1. `qc_framework_v1/migrations/180_patient_master_nlp_cluster_path_c_20260429.sql` — apply SQL (§A-§F)
2. `qc_framework_v1/reports/mig_180_nlp_cluster_inventory_20260429.csv` — full col list
3. `qc_framework_v1/reports/mig_180_nlp_cluster_audit_20260429.md` — derivation rationale + audit findings
4. `qc_framework_v1/reports/mig_180_nlp_rederivation_audit_20260429.md` — mass-equivalence mismatch counts

Commit message: `qc: mig_180 PM nlp_* cluster path-c verify + apply authoring (116 cols)`

---

End of prompt.
