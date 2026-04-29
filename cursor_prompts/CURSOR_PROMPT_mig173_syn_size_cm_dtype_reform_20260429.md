# Cursor Prompt — mig_173 syn_*_size_cm 3-axis Dtype Reform

**Lane:** 62 / mig_173
**Batch_id:** `mig_173_syn_size_cm_dtype_reform_20260429`
**Generated:** 2026-04-29
**Type:** Design + apply schema reform. Pre-snapshot required. Multi-step build.

---

## §0 Why this lane exists

The 3 synoptic-pathology lobe-size cols on `canonical_patient_master` are typed VARCHAR but are populated with multi-axis dimension strings (`'4.0 x 3.0 x 2.0'`, `'3.5 cm superior to inferior by 3.0 cm transverse by 1.5 cm anterior to posterior'`). They cannot be analyzed numerically as they stand — any manuscript volume / size analytic silently coerces to NULL or fails.

Cowork live 2026-04-29 probe:

| Col | n_distinct | n_nonnull | n_sentinel ('n/s') |
|---|---:|---:|---:|
| `syn_right_lobe_size_cm` | 6,599 | 7,058 | 39 |
| `syn_left_lobe_size_cm` | 6,715 | 7,204 | 33 |
| `syn_isthmus_size_cm` | 3,500 | 3,981 | 2 |

mig_169 (Cursor `49f6b61`) flagged all 3 as `VARCHAR-with-units` (high priority): `n_with_units` 588-1,372 per col (i.e., units appear inline like `'cm'`); `n_with_alpha` 7,057-7,203 per col (i.e., almost every value carries alpha tokens).

This lane reforms the typing: decompose each `<lobe>_size_cm` VARCHAR into 3 new DOUBLE cols (`length_cm`, `width_cm`, `height_cm`) plus a computed `volume_cc` col, with sentinel handling.

## §1 Governance posture

- Schema reform: ADD COLUMN ×4 per col (12 new DOUBLE/numeric cols total), POPULATE via parser, deprecate or rename original VARCHAR.
- Pre-snapshot full PM slice for the 3 cols before any structural change.
- AGENTS-governance binding: agent authors SQL only; Cowork applies via Path C.
- Pattern: design → skeleton ALTERs (committed) → parser SQL (committed) → Cowork applies in sequence with verification between steps.

## §2 Required pre-flight probes

```sql
-- §2a Sample non-null values per col (for parser design)
SELECT syn_right_lobe_size_cm, COUNT(*) AS n
FROM main.canonical_patient_master
WHERE syn_right_lobe_size_cm IS NOT NULL
GROUP BY 1 ORDER BY 2 DESC LIMIT 30;
-- Repeat for left + isthmus.

-- §2b Whitespace drift quantification
SELECT
  SUM(CASE WHEN syn_right_lobe_size_cm <> TRIM(syn_right_lobe_size_cm) THEN 1 ELSE 0 END) AS n_whitespace_drift_right,
  SUM(CASE WHEN syn_left_lobe_size_cm <> TRIM(syn_left_lobe_size_cm) THEN 1 ELSE 0 END) AS n_whitespace_drift_left,
  SUM(CASE WHEN syn_isthmus_size_cm <> TRIM(syn_isthmus_size_cm) THEN 1 ELSE 0 END) AS n_whitespace_drift_isthmus
FROM main.canonical_patient_master;

-- §2c Sentinel-value catalog
SELECT raw_value, COUNT(*) AS n FROM (
  SELECT LOWER(TRIM(syn_right_lobe_size_cm)) AS raw_value FROM main.canonical_patient_master
  UNION ALL
  SELECT LOWER(TRIM(syn_left_lobe_size_cm)) FROM main.canonical_patient_master
  UNION ALL
  SELECT LOWER(TRIM(syn_isthmus_size_cm)) FROM main.canonical_patient_master
) t WHERE raw_value IN ('n/s','ns','none','null','','x','c/a','-')
GROUP BY 1 ORDER BY 2 DESC;

-- §2d Parse coverage estimate (using a candidate regex)
SELECT
  SUM(CASE WHEN regexp_matches(syn_right_lobe_size_cm, '^\s*(\d+(?:\.\d+)?)\s*[xX×]\s*(\d+(?:\.\d+)?)\s*[xX×]\s*(\d+(?:\.\d+)?)') THEN 1 ELSE 0 END) AS n_3axis_clean,
  SUM(CASE WHEN syn_right_lobe_size_cm IS NOT NULL THEN 1 ELSE 0 END) AS n_nonnull
FROM main.canonical_patient_master;
-- Expect: most rows match the 'A x B x C' pattern; the prose 'A cm by B cm by C cm' rows need a richer regex.
```

## §3 Parser design (DuckDB regex)

The parser must handle four patterns observed in the data (see mig_169 samples):
1. `'A x B x C'` — most common (e.g., `'4.0 x 3.0 x 2.0'`)
2. `'A x B x C cm)'` — trailing closing paren (e.g., `'4.5 x 3.5 x 2.4 cm)'`)
3. `'A cm <axis> by B cm <axis> by C cm <axis>'` — verbose prose
4. `'A.B cm <axis-pole> to <axis-pole>, ...'` — narrative prose

For (1)-(2): single regex with three capture groups + optional unit suffix.
For (3)-(4): cascade of more permissive regexes; if all fail, leave `length_cm/width_cm/height_cm` NULL and set a flag col.

Sentinel handling: `'n/s'`, `'ns'`, `'none'`, `'null'`, `''`, `'x'`, `'c/a'`, `'-'` (case-insensitive after TRIM) all map to NULL on the new cols.

```sql
-- Parser SQL skeleton (Cursor agent fills out cascade logic)
WITH parsed AS (
  SELECT
    research_id,
    syn_right_lobe_size_cm AS raw_value,
    TRY_CAST(regexp_extract(syn_right_lobe_size_cm, '^\s*(\d+(?:\.\d+)?)', 1) AS DOUBLE) AS length_cm_attempt1,
    -- ... etc.
  FROM main.canonical_patient_master
  WHERE syn_right_lobe_size_cm IS NOT NULL
    AND LOWER(TRIM(syn_right_lobe_size_cm)) NOT IN ('n/s','ns','none','null','','x','c/a','-')
)
SELECT * FROM parsed;
```

Coverage target: ≥85% of non-sentinel non-null rows should parse to 3 DOUBLE values via Pattern 1+2; the rest tagged with `parse_status='unparsed'` for follow-up (likely manual review or LLM rescue lane).

## §4 Schema reform plan

For **each** of the 3 cols (right / left / isthmus), the structural change is:

```sql
ALTER TABLE main.canonical_patient_master ADD COLUMN syn_right_lobe_length_cm DOUBLE;
ALTER TABLE main.canonical_patient_master ADD COLUMN syn_right_lobe_width_cm  DOUBLE;
ALTER TABLE main.canonical_patient_master ADD COLUMN syn_right_lobe_height_cm DOUBLE;
ALTER TABLE main.canonical_patient_master ADD COLUMN syn_right_lobe_volume_cc DOUBLE;
ALTER TABLE main.canonical_patient_master ADD COLUMN syn_right_lobe_size_parse_status VARCHAR;
```

Volume = length_cm × width_cm × height_cm (rough estimate; not corrected for ellipsoid factor — Logan can decide later if a 0.524 factor is wanted).

`syn_right_lobe_size_parse_status` ∈ `{'parsed_3axis','parsed_partial','sentinel','unparsed'}`.

The original VARCHAR cols `syn_right_lobe_size_cm` / `syn_left_lobe_size_cm` / `syn_isthmus_size_cm` are **kept** but renamed to `syn_right_lobe_size_cm_legacy_raw` etc. to preserve the raw text and signal to manuscript queries that the typed version is now `syn_right_lobe_*_cm` numeric cols.

## §5 SQL structure

### Section A — Pre-snapshot (3 snapshots, one per col)
### Section B — ALTER ADD COLUMN ×15 (3 cols × 5 new cols)
### Section C — UPDATE populate via parser (single transaction)
### Section D — RENAME original VARCHAR to `_legacy_raw`
### Section E — Registry resync (3 deprecation rows + 15 new col rows)
### Section F — Post-state verify (commented; coverage + value-equivalence)

Mass-equivalence post-verify probe (commented; Cowork runs):
```sql
-- F1: parse coverage by col
-- SELECT
--   COUNT(*) FILTER (WHERE syn_right_lobe_size_parse_status = 'parsed_3axis') AS parsed_n,
--   COUNT(*) FILTER (WHERE syn_right_lobe_size_parse_status = 'unparsed') AS unparsed_n,
--   COUNT(*) FILTER (WHERE syn_right_lobe_size_parse_status = 'sentinel') AS sentinel_n
-- FROM main.canonical_patient_master;

-- F2: volume sanity (no NaN, no negative, max < 1000 cc safety)
-- SELECT MIN(syn_right_lobe_volume_cc), MAX(syn_right_lobe_volume_cc),
--        SUM(CASE WHEN syn_right_lobe_volume_cc < 0 THEN 1 ELSE 0 END) AS n_neg
-- FROM main.canonical_patient_master;
```

## §6 Required CFs

- `CF-mig169-DTYPE-VARCHAR-WITH-UNITS-syn_right_lobe_size_cm` → CLOSED
- `CF-mig169-DTYPE-VARCHAR-WITH-UNITS-syn_left_lobe_size_cm` → CLOSED
- `CF-mig169-DTYPE-VARCHAR-WITH-UNITS-syn_isthmus_size_cm` → CLOSED
- `CF-mig168-VOCAB-DRIFT-SYN-SIZE-3AXIS-VARCHAR` → CLOSED
- `CF-mig173-PARSE-COVERAGE-LT-100PCT-PER-COL` (informational) — quantifies leftover unparsed rows
- `CF-mig173-VOLUME-CALC-NO-ELLIPSOID-FACTOR` (informational) — flags that volume is L×W×H rectangular, not ellipsoid 0.524 factor; Logan can decide later

## §7 Git workflow

- Files:
  - `qc_framework_v1/migrations/173_syn_size_cm_dtype_reform_20260429.sql`
  - `qc_framework_v1/reports/mig_173_syn_size_cm_design_20260429.md` (decisions doc)
- Commit: `qc: mig_173 syn_*_size_cm 3-axis dtype reform (12 new DOUBLE cols + 3 status cols + 3 legacy renames)`
- Stage surgically.

## §8 Out of scope

- Do NOT touch synoptic non-size cols (`syn_margin_distance_mm_raw_str`, etc.); separate lane.
- Do NOT touch `ops_max_diameter_cm`, `ops_preop_nodules_count_size`, `ops_dominant_nodule_size_us` (mig_169b future lane).
- Do NOT compute ellipsoid-corrected volume — Logan ratifies the formula choice separately.
- Do NOT apply on MD. Ship SQL only.
- Do NOT drop the legacy VARCHAR cols. Rename to `_legacy_raw` to preserve raw text for audit.

## §9 Apply governance

Cowork applies in this sequence with verification between each:
1. Pre-snapshots A1-A3
2. ALTER ADD COLUMN block (15 statements)
3. Parser UPDATE (single transaction; verify parse_status counts after)
4. RENAME original cols to `_legacy_raw`
5. Registry resync
6. Post-state verify (Section F)

Per AGENTS governance: agent ships SQL only. **No `query_rw` from agent.**
