# Cursor Prompt — mig_172 Vocabulary Normalization Apply (recurrence + completion + path histology family)

**Lane:** 61 / mig_172
**Batch_id:** `mig_172_vocabulary_normalization_apply_20260429`
**Generated:** 2026-04-29
**Type:** **Apply lane (data writes)**. **High clinical-review priority.** Logan must ratify the SSOT enum dictionary before any UPDATE.

---

## §0 Why this lane exists

mig_168 (Cowork-verified, commit `742bf69`) audited 461 verified PM VARCHAR cols and surfaced **702 vocabulary drift findings across 123 cols**. The `pm_ssot_enum_dictionary_draft.csv` (2,128 rows) at `exports/mig168_pm_vocab_audit_20260429_175417/` proposes a canonical enum + display label for each raw variant.

This lane is the **apply pass for the histology family** — the highest manuscript-impact subset. The 8 cols in scope (Cowork live 2026-04-29 verified):

| Col | n_nonnull | Status |
|---|---:|---|
| `path_histology_raw` | 4,137 | verified VARCHAR |
| `path_histology_variant_raw` | 3,317 | verified VARCHAR |
| `histologic_types_all` | 4,137 | verified VARCHAR |
| `histologic_variants_all` | 3,310 | verified VARCHAR |
| `recurrence_histology` | 440 | verified VARCHAR |
| `recurrence_histology_v2` | 118 | verified VARCHAR |
| `completion_prior_histology` | 385 | verified VARCHAR |
| `completion_histology_type` | 188 | verified VARCHAR |

The `recurrence_histology` column alone has 42 raw values for what should be ~10-12 enums (per mig_168 report); manuscript analyses grouping by histology will silently undercount without normalization.

**Logan's pre-flight checkpoint** (binding for this lane): Logan must clinically review and edit `pm_ssot_enum_dictionary_draft.csv` for these 8 cols before agent generates any UPDATE SQL. The agent's first action is to confirm the dictionary CSV is Logan-ratified (commit hash + filename in the design doc), and STOP if it is not.

## §1 Governance posture

- Apply lane: data writes via UPDATE on `canonical_patient_master`.
- Pre-snapshot every col before mutation. Snapshot to `archive_pub_v1_0` per `feedback_no_cross_db_canonical_sourcing.md`.
- AGENTS-governance binding: agent ships SQL only. Cowork applies. **DO NOT `query_rw` anywhere.**
- Mapping table built fresh in `main` (no cross-DB sourcing).
- Pattern follows mig_160 structural-apply governance: pre-snapshot → BEGIN TRANSACTION → UPDATE col → UPDATE registry note → COMMIT.

## §2 Pre-flight probes (paste in SQL header, run live)

```sql
-- §2a Confirm SSOT enum dictionary is Logan-ratified
-- Filename + git hash recorded in design doc; STOP if not present.

-- §2b Current value distribution (for each of the 8 cols, ~10-row sample)
SELECT recurrence_histology, COUNT(*) AS n
FROM main.canonical_patient_master
WHERE recurrence_histology IS NOT NULL
GROUP BY 1 ORDER BY 2 DESC LIMIT 50;
-- Repeat for each of the 8 cols.

-- §2c Mapping table cardinality probe (after ingest)
SELECT raw_value, canonical_code, display_label, source_col, COUNT(*) OVER () AS total_rows
FROM main.histology_vocab_normalization_map_v1 LIMIT 25;

-- §2d Cohort parity invariant
SELECT COUNT(*) AS pm_rows, COUNT(DISTINCT research_id) AS pm_distinct_rids FROM main.canonical_patient_master;
-- Expect: 10871 / 10871

-- §2e Pre-state distinct value counts per col (will compare post-state)
SELECT
  COUNT(DISTINCT recurrence_histology)        AS recurrence_histology_n,
  COUNT(DISTINCT recurrence_histology_v2)     AS recurrence_histology_v2_n,
  COUNT(DISTINCT completion_prior_histology)  AS completion_prior_histology_n,
  COUNT(DISTINCT completion_histology_type)   AS completion_histology_type_n,
  COUNT(DISTINCT histologic_types_all)        AS histologic_types_all_n,
  COUNT(DISTINCT histologic_variants_all)     AS histologic_variants_all_n,
  COUNT(DISTINCT path_histology_raw)          AS path_histology_raw_n,
  COUNT(DISTINCT path_histology_variant_raw)  AS path_histology_variant_raw_n
FROM main.canonical_patient_master;
```

## §3 Required SQL structure

### Section A — Pre-snapshots (8 snapshots, one per col)

```sql
CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_patient_master_recurrence_histology_pre_mig172_20260429 AS
SELECT research_id, recurrence_histology, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig172_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_patient_master;
-- Repeat for the other 7 cols.
```

### Section B — Mapping table from Logan-ratified CSV

```sql
-- B1: Create normalization mapping table in main (no cross-DB sourcing)
CREATE OR REPLACE TABLE main.histology_vocab_normalization_map_v1 (
  source_col      VARCHAR NOT NULL,    -- one of the 8 col names
  raw_value       VARCHAR NOT NULL,    -- exact raw value as observed
  canonical_code  VARCHAR NOT NULL,    -- snake_case enum (e.g., 'ptc_metastatic')
  display_label   VARCHAR NOT NULL,    -- human-readable (e.g., 'Metastatic PTC')
  notes           VARCHAR,             -- mapping rationale
  ratified_ts     TIMESTAMP DEFAULT CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
  ratified_by     VARCHAR DEFAULT 'logan_2026-04-29'
);

-- B2: INSERT from Logan-ratified CSV — agent loads CSV via DuckDB read_csv
-- Spec the read_csv call so Cowork can replay it from the local repo path.
INSERT INTO main.histology_vocab_normalization_map_v1
SELECT * FROM read_csv('exports/mig168_pm_vocab_audit_20260429_175417/pm_ssot_enum_dictionary_draft_ratified.csv',
  header=true, columns={'source_col':'VARCHAR','raw_value':'VARCHAR','canonical_code':'VARCHAR','display_label':'VARCHAR','notes':'VARCHAR'});
```

### Section C — UPDATE each col via mapping (8 UPDATEs in single transaction)

```sql
BEGIN TRANSACTION;

UPDATE main.canonical_patient_master AS pm
SET recurrence_histology = m.canonical_code
FROM main.histology_vocab_normalization_map_v1 m
WHERE m.source_col = 'recurrence_histology'
  AND m.raw_value = pm.recurrence_histology;

-- Repeat 7 more UPDATEs, one per source col.

-- Section C2: registry note appendix per col
UPDATE main.canonical_column_verification_registry_v1
SET notes = COALESCE(notes,'')
            || ' | mig_172: vocabulary normalized via histology_vocab_normalization_map_v1 '
            || '(Logan-ratified pm_ssot_enum_dictionary_draft 2026-04-29). '
            || 'Pre-state distinct=<N>, post-state distinct=<M>; canonical codes are snake_case manuscript enums. '
            || 'Pre-snapshot canonical_patient_master_<col>_pre_mig172_20260429.'
WHERE schema_name='main'
  AND table_name='canonical_patient_master'
  AND column_name IN ('recurrence_histology','recurrence_histology_v2','completion_prior_histology',
                      'completion_histology_type','histologic_types_all','histologic_variants_all',
                      'path_histology_raw','path_histology_variant_raw');

COMMIT;
```

### Section D — Post-state verification (commented, Cowork runs)

```sql
-- D1: Distinct-value counts post-flip should be much smaller than pre
-- (e.g., recurrence_histology pre=42 → post≈10-12)
-- SELECT COUNT(DISTINCT recurrence_histology) FROM main.canonical_patient_master;

-- D2: 0 rows should have a value not in the mapping table
-- SELECT COUNT(*) FROM main.canonical_patient_master pm
-- WHERE recurrence_histology IS NOT NULL
--   AND recurrence_histology NOT IN (SELECT canonical_code FROM main.histology_vocab_normalization_map_v1
--                                    WHERE source_col='recurrence_histology');
-- Expect: 0
```

## §4 Required CFs

- `CF-mig168-VOCAB-DRIFT-RECURRENCE-HISTOLOGY-42-VALUES` → CLOSED via mig_172
- `CF-mig168-VOCAB-DRIFT-COMPLETION-HISTOLOGY` → CLOSED via mig_172
- `CF-mig172-RAW-VALUE-PRESERVED-IN-SNAPSHOT` (informational) — confirms raw values archived in `archive_pub_v1_0` for any future re-mapping
- `CF-mig172-MAPPING-TABLE-LIVE` (informational) — `main.histology_vocab_normalization_map_v1` is the canonical map for downstream display labels

## §5 Git workflow

- File: `qc_framework_v1/migrations/172_vocabulary_normalization_apply_20260429.sql`
- Companion: `qc_framework_v1/reports/mig_172_vocabulary_normalization_apply_20260429.md` (records mapping decisions, before/after distinct counts, sample mappings)
- Commit message: `qc: mig_172 histology vocabulary normalization apply (8 cols, Logan-ratified)`
- Stage surgically; push.

## §6 Out of scope

- Do NOT touch any of the other 115 cols flagged in mig_168 audit. This lane is **histology family only** (8 cols).
- Do NOT touch `syn_*_size_cm` (mig_173 covers).
- Do NOT touch `cnln_img_laterality` (mig_174 covers).
- Do NOT modify `recurrence_status_final`, `recurrence_confirmed`, or any non-histology recurrence col.
- Do NOT apply on MD; ship SQL only — Cowork applies via Path C after pre-snapshot.
- Do NOT skip Logan's ratification step (§0 checkpoint). If `pm_ssot_enum_dictionary_draft_ratified.csv` is not present in `exports/mig168_pm_vocab_audit_20260429_175417/`, agent must STOP and ask Logan to ratify.

## §7 Apply governance

Cowork applies via Path C after:
1. Confirming Logan ratified the SSOT enum dictionary CSV (filename + commit hash recorded in §0).
2. Independently verifying the mapping table count + a 10-row sample.
3. Running pre-snapshots A1-A8.
4. Running Section C single transaction.
5. Running Section D post-state verify.

Per AGENTS governance: agent ships SQL only. **No `query_rw` from agent.**
