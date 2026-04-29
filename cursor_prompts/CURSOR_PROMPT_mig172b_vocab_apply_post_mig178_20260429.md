# Cursor Prompt — mig_172b vocab normalization apply (post-mig_178; CSV rewrite + remaining 4 cols)

**Date:** 2026-04-29 (late evening)
**Lane:** mig_172b / vocab_apply_post_mig178_recurrence_completion
**Batch (proposed):** `mig_172b_vocabulary_normalization_apply_recurrence_completion_20260429`
**Predecessor:** mig_172 (original 8-col plan; on hold pending SSOT CSV rewrite); mig_178 (CLOSED — covered `histologic_types_all`, `histologic_variants_all`, `path_histology_raw`, `path_histology_variant_raw` per `qc_framework_v1/reports/mig_178_histology_vocab_cleanup_20260429.md`)
**Posture:** SQL-only authoring + commit + push. **DO NOT execute against MotherDuck.** Cowork applies via Path C with full pre-snapshot.
**Target DB:** `thyroid_canonical_publication_v1_0`
**Primary table:** `main.canonical_patient_master`

---

## Mission

mig_172 was scoped to apply vocab normalization across 8 PM cols. **mig_178 already closed 4 of them** (`path_histology_raw`, `path_histology_variant_raw`, `histologic_types_all`, `histologic_variants_all`) AND implemented Logan's rejection of `mtc_ptc_mixed`. Remaining work is the **recurrence + completion histology family** (4 cols) that mig_178 did not touch:

| Col | n_nonnull (per mig_172 prompt) | Status |
|---|---:|---|
| `recurrence_histology` | 440 | verified VARCHAR — 42 raw values; needs normalization |
| `recurrence_histology_v2` | 118 | verified VARCHAR |
| `completion_prior_histology` | 385 | verified VARCHAR |
| `completion_histology_type` | 188 | verified VARCHAR |

This lane (1) rewrites the ratified SSOT enum CSV to **drop `mtc_ptc_mixed` rows entirely** (Logan-rejected per `RATIFICATION_NOTES_20260429.md` and CF-mig172-MTC-PTC-MIXED-REJECT) AND aligns labeling with mig_178's `MTC | PTC` convention; (2) authors apply SQL that updates the 4 remaining cols only.

---

## Required scope

### §1 Locate & verify ratified SSOT CSV
Live MD verified by Cowork 2026-04-29: `exports/mig168_pm_vocab_audit_20260429_175417/pm_ssot_enum_dictionary_draft.csv` was the working source for mig_172. The **ratified** version (Logan's edits) should be at the same path or under `exports/mig168_pm_vocab_audit_20260429_175417/RATIFICATION_NOTES_20260429.md`'s referenced filename. **First action:** confirm the latest ratified CSV exists and has been git-committed; STOP if not.

### §2 Rewrite CSV to remove `mtc_ptc_mixed`
Produce `exports/mig168_pm_vocab_audit_20260429_175417/pm_ssot_enum_dictionary_post_mig178_v1.csv` with:
- All rows where `canonical_code='mtc_ptc_mixed'` (any case/whitespace variant) **removed**
- Any raw value that previously mapped to `mtc_ptc_mixed` **remapped to `MTC | PTC`** (per mig_178 convention; see `manuscript_workspace.mig178_ptc_mtc_patient_audit_v1`)
- Filter to `source_col IN ('recurrence_histology','recurrence_histology_v2','completion_prior_histology','completion_histology_type')` (drop the 4 cols mig_178 already covered)
- Author script `scripts/_mig172b_csv_rewrite.py` (Python; reads original, applies rewrite rules, emits new CSV with checksum + row-delta log to stdout)

### §3 Pre-flight probes (paste in SQL header, run live)
```sql
-- §3a Ratified CSV exists in repo + is referenced by design doc
-- (manual confirm; STOP if absent)

-- §3b Pre-state distinct-value counts on the 4 cols
SELECT
  COUNT(DISTINCT recurrence_histology)        AS recurrence_histology_n,
  COUNT(DISTINCT recurrence_histology_v2)     AS recurrence_histology_v2_n,
  COUNT(DISTINCT completion_prior_histology)  AS completion_prior_histology_n,
  COUNT(DISTINCT completion_histology_type)   AS completion_histology_type_n
FROM main.canonical_patient_master;

-- §3c Confirm mig_178 already cleaned the 4 histologic_*_all + path_histology_*_raw cols
SELECT
  COUNT(*) FILTER (WHERE histologic_types_all ILIKE '%mtc_ptc_mixed%') AS hta_mtc_ptc_mixed,
  COUNT(*) FILTER (WHERE histologic_variants_all ILIKE '%mtc_ptc_mixed%') AS hva_mtc_ptc_mixed
FROM main.canonical_patient_master;
-- Expect: 0 / 0 (post-mig_178)

-- §3d Cohort parity invariant
SELECT COUNT(*) AS n_rows, COUNT(DISTINCT research_id) AS n_rids FROM main.canonical_patient_master;
-- Expect: 10871 / 10871
```

### §4 Apply SQL artifact
`qc_framework_v1/migrations/172b_vocabulary_normalization_apply_recurrence_completion_20260429.sql` with:

§A — pre-snapshots (4 snapshots, one per col)
```sql
CREATE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_patient_master_recurrence_histology_pre_mig172b_20260429 AS
SELECT research_id, recurrence_histology, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig172b_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_patient_master;
-- (repeat for the other 3 cols)
```

§B — load rewritten CSV into `main.histology_vocab_normalization_map_v1` (CREATE OR REPLACE; idempotent). Schema: `(raw_value VARCHAR, canonical_code VARCHAR, display_label VARCHAR, source_col VARCHAR)`.

§C — 4 UPDATE statements (one per col), each:
```sql
UPDATE main.canonical_patient_master pm
SET recurrence_histology = m.canonical_code
FROM main.histology_vocab_normalization_map_v1 m
WHERE m.source_col='recurrence_histology' AND pm.recurrence_histology = m.raw_value;
```

§D — registry note appendix on each of the 4 col rows:
```sql
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'') || ' | mig_172b: vocab_normalization_apply — recurrence_histology raw values normalized via histology_vocab_normalization_map_v1; mtc_ptc_mixed dropped (Logan-rejected per CF-mig172); mig_178 convention `MTC | PTC` applied'
WHERE schema_name='main' AND table_name='canonical_patient_master'
  AND column_name IN ('recurrence_histology','recurrence_histology_v2','completion_prior_histology','completion_histology_type');
```

§E — post-state distinct-value counts (read-only) — expect distinct-count to drop substantially (e.g., recurrence_histology 42 → ~12).

### §5 Audit/report
`qc_framework_v1/reports/mig_172b_vocab_apply_audit_20260429.md` with:
- CSV rewrite log (rows in / rows out / rows remapped / rows dropped)
- Per-col pre/post distinct-value tables
- Mapping spot-check: 5 sample rids per col with raw → canonical
- CF closure list (CF-mig168-VOCAB-DRIFT-RECURRENCE-HISTOLOGY-42-VALUES if applicable; check live registry for actual CF tag)

### §6 Open carry-forwards (anticipate)
- `CF-mig172b-RAW-UNMAPPED-<col>` — any raw value in MD without a CSV map row (should be 0 if CSV is complete)

---

## Governance reminders

- **Read-only audit + SQL authoring + Python CSV rewrite only.** Cowork executes Path C apply.
- mtc_ptc_mixed: **Logan-rejected per CF-mig172-MTC-PTC-MIXED-REJECT.** Drop these rows from CSV; do NOT remap them to `mtc_ptc_mixed`.
- mig_178 convention is `MTC | PTC` (sorted-distinct, pipe-delimited). Match exactly.
- Author = `Logan Glosser <logan.glosser@gmail.com>` for all commits
- Surgical git add only — explicit paths
- DuckDB MCP wrapper: one statement per call

---

## Deliverables

1. `scripts/_mig172b_csv_rewrite.py` — Python that produces the post-mig_178 CSV
2. `exports/mig168_pm_vocab_audit_20260429_175417/pm_ssot_enum_dictionary_post_mig178_v1.csv` — rewritten ratified CSV
3. `qc_framework_v1/migrations/172b_vocabulary_normalization_apply_recurrence_completion_20260429.sql` — apply SQL
4. `qc_framework_v1/reports/mig_172b_vocab_apply_audit_20260429.md` — rewrite log + audit

Commit message: `qc: mig_172b vocab normalization apply for recurrence + completion histology family (CF-mig168-VOCAB-DRIFT)`

---

End of prompt.
