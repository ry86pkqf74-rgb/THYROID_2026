# Cursor prompt — mig_315: M044 cohort flat rebuild + ete_grade_final cleanup

**Agent:** cursor_composer
**Estimated time:** 1.5–3 hours (rebuild + investigation + v6 manuscript regen)
**Priority:** P0 — blocks M044 v6 numerical patch and Cortex Analyst binding for M044
**Closes:** `CF-M044-DUP-COLS`, opens follow-on `CF-M044-V6-MANUSCRIPT-PATCH` (Cowork lane)

## Problem (two compounding defects)

### Defect 1 — Duplicate columns

`manuscript_workspace.cohort_m044_ajcc_ete_v1` has every column duplicated. `DESCRIBE` returns 64 rows for what should be ~32. Pattern: `research_id (VARCHAR)`, `research_id (VARCHAR)`, `age_at_surgery (BIGINT)`, `age_at_surgery (BIGINT)`, etc. One column pair has different types (`surg_first_date` shows TIMESTAMP and DATE side-by-side), confirming this is a `JOIN` without explicit projection — likely `SELECT a.*, b.*` from the cohort flat rebuild that landed alongside mig_312.

This breaks Cortex Analyst binding (semantic models choke on duplicate column names) and risks silent COALESCE-style errors in any consumer that picks "the" `ete_grade_final`.

### Defect 2 — `ete_grade_final` Boolean→VARCHAR cast artifacts

Distribution in current cohort flat:

| ete_grade_final | n |
|---|---:|
| `microscopic` | 2,413 |
| `gross` | 1,239 |
| `false` | 158 ← should be `no_negative` |
| `present_ungraded` | 28 |
| `absent` | 15 ← should be `no_negative` |
| (NULL) | 11 |
| `true` | 4 ← should be `gross` |

The strict-DTC analytic cohort in M044 v5 had **68 no/negative ETE patients**. Post-rebuild, that group has been **completely lost** — split across `false`, `absent`, and possibly `present_ungraded` with `Boolean→string` cast artifacts.

This is the single largest threat to M044 v5's findings: the no/negative-vs-microscopic comparison (aOR 2.72; 95% CI 0.80–9.30) was already imprecise; if the no/negative cohort is silently misclassified, the Discussion's no-negative audit paragraph is wrong.

## Recipe

### Step 1 — Investigate the source (where did the cast happen?)

```bash
cd /Users/loganglosser/THYROID_2026
grep -rn "ete_grade_final" scripts/ qc_framework_v1/ snowflake_trial/ | grep -v __pycache__ | head -40
```

Identify the canonical pipeline that derives `ete_grade_final`. Likely candidate: `scripts/m044_ete_fit_models.py` or `scripts/m044_master_analytic.sql`. Find the CASE statement that maps source flags to `{no_negative, microscopic, gross}` and trace back:
- Was there a Boolean column (e.g. `gross_ete_flag`) that got cast directly to string?
- Was the no_negative branch removed in a recent edit?

Document findings in a short investigation note (`studies/m044_cohort_audit/INVESTIGATION_20260505.md`).

### Step 2 — Rebuild canonical_path_malignant_events_v1.ete_grade_resolved (if defect is upstream)

If the cast happened in `canonical_path_malignant_events_v1`, fix at the canonical layer with a CASE statement that explicitly handles:

```sql
CASE
  WHEN ete_op_note_grade IS NOT NULL THEN ete_op_note_grade  -- 'gross'/'microscopic'/'no_negative' literal strings
  WHEN gross_ete_flag = TRUE  THEN 'gross'
  WHEN path_gross_ete_flag = 1 THEN 'gross'
  WHEN microscopic_ete_flag = TRUE THEN 'microscopic'
  WHEN any_ete_flag = FALSE THEN 'no_negative'
  WHEN any_ete_flag IS NULL THEN NULL  -- truly indeterminate
  ELSE 'present_ungraded'
END AS ete_grade_resolved
```

(Adapt column names to actual source.) The key is: **never let a Boolean fall through to its `'true'`/`'false'` string representation.**

### Step 3 — Rebuild cohort_m044_ajcc_ete_v1 with explicit projection

The recipe must use `SELECT col1 AS col1, col2 AS col2, …` with no `*` and no implicit JOIN aliasing. Reference v5 column set (32 unique fields, see investigation note). Suggested template:

```sql
CREATE OR REPLACE TABLE manuscript_workspace.cohort_m044_ajcc_ete_v1 AS
SELECT DISTINCT
  cpm.research_id,
  cpm.age_at_surgery,
  cpm.sex,
  cpm.histology_final,
  pme.tumor_size_cm,
  pme.ete_grade_resolved AS ete_grade_final,  -- using cleaned canonical
  pme.ete_grade_resolved AS ete_grade,
  pme.ete_grade_source,
  pme.gross_ete_flag,
  pme.path_gross_ete_flag,
  pme.ete_op_note_grade,
  pme.ete_original_grade,
  cpm.ajcc8_t_stage,
  cpm.ajcc8_n_stage,
  cpm.ajcc8_m_stage,
  cpm.ajcc8_stage_group,
  -- ... (full v5 column set)
FROM main.canonical_patient_master cpm
LEFT JOIN main.canonical_path_malignant_events_v1 pme USING (research_id)
WHERE cpm.is_malignant
  AND cpm.histology_final IN (
    'PTC','follicular carcinoma','poorly differentiated thyroid carcinoma',
    'differentiated high grade thyroid carcinoma','metastatic PTC')
  AND cpm.surg_first_date BETWEEN '1999-01-01' AND '2024-06-04';
```

(Logan's `scripts/m044_master_analytic.sql` is the source of truth — adapt that.)

### Step 4 — Validation gates (acceptance)

```sql
-- 4a. No duplicate columns
SELECT COUNT(*) AS n_cols, COUNT(DISTINCT column_name) AS n_unique
FROM information_schema.columns
WHERE table_schema='manuscript_workspace' AND table_name='cohort_m044_ajcc_ete_v1';
-- Acceptance: n_cols = n_unique
```

```sql
-- 4b. ete_grade_final restored
SELECT ete_grade_final, COUNT(*) AS n
FROM manuscript_workspace.cohort_m044_ajcc_ete_v1
GROUP BY 1 ORDER BY n DESC;
-- Acceptance: only {no_negative, microscopic, gross, NULL}; no 'false'/'true'/'absent' string values
-- Acceptance: no_negative count between 50 and 100 (v5 had 68; post-mig_313 may shift slightly)
```

```sql
-- 4c. Cohort N within 5% of v5
SELECT COUNT(*) AS n FROM manuscript_workspace.cohort_m044_ajcc_ete_v1;
-- Acceptance: 3,400 ≤ n ≤ 3,750 (v5 was 3,578)
```

```sql
-- 4d. Stage IVB plausible (post-mig_313 expected ~60-90)
SELECT ajcc8_stage_group, COUNT(*)
FROM manuscript_workspace.cohort_m044_ajcc_ete_v1
GROUP BY 1 ORDER BY 1;
-- Acceptance: IVB count between 50 and 120 (was 684 pre-fix, ~76 post-fix in dirty cohort)
```

### Step 5 — Re-run M044 regression and confirm aOR is unchanged

```bash
.venv/bin/python scripts/m044_ete_fit_models.py --cohort manuscript_workspace.cohort_m044_ajcc_ete_v1 \
  --out studies/m044_v6_audit/ 2>&1 | tee logs/m044_v6_models.log
```

Acceptance:
- Primary aOR gross-vs-micro: **1.77 [1.15–2.71], p=0.009** (cf. v5 locked numbers; ≤0.05 drift acceptable due to no_negative restoration)
- N=3,572 with 105 events (cf. v5 locked; ≤2% drift acceptable)
- Crude OR ~2.68
- Sensitivity model OR's directionally consistent

If aOR drifts by >0.10, **stop and investigate** — the no_negative restoration may have changed the reference structure. Document deltas in `studies/m044_v6_audit/REGRESSION_DELTA_v5_vs_v6.md`.

### Step 6 — Regenerate M044 v6 deliverables

```bash
.venv/bin/python scripts/m044_regenerate_outputs.py --version v6 \
  --in-cohort manuscript_workspace.cohort_m044_ajcc_ete_v1 \
  --out M044_FINAL_PACKAGE_v6/ 2>&1 | tee logs/m044_v6_regen.log
```

Or, if `m044_regenerate_outputs.py` doesn't exist or doesn't support `--version v6`, regenerate piece-by-piece using the v5 idiom:
- `scripts/build_m044_master_excel.py` → updated `M044_ETE_FINAL_all_stats.xlsx`
- `scripts/m044_make_figures.py` → Figures 1–3 PNG/SVG
- `scripts/build_m044_docx.js` → manuscript v6 docx
- `scripts/build_m044_supp_docx.js` → supplement v6 docx

### Step 7 — Manuscript v6 production rules

The v6 docx should:
- Replace v5 Table 1 stage rows entirely (Stage I/II/III/IVA/IVB/IVC counts and percentages, in all four columns: no_neg / micro / gross / overall)
- Replace v5 Table 1 ETE-group counts (the Method's "n=68 no/neg, n=2,359 micro, n=1,151 gross" line in the Abstract and Results §1)
- Update any prose mentioning Stage IV frequency
- **Preserve verbatim** the regression tables (Table 2, Table 3) UNLESS Step 5 reveals >0.05 drift
- Update CONSORT flow diagram if cohort N changed by ≥10 patients
- Add a brief eMethods note documenting the mig_315 cohort rebuild (no_negative reclassification + duplicate-column elimination)

### Step 8 — Signoff

```sql
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
SELECT 'mig_315', CURRENT_TIMESTAMP, 'cursor_composer_mig315',
  'mig_315: M044 cohort flat rebuild. Defect 1 (duplicate columns from JOIN-without-projection): fixed via explicit column projection (n_cols=n_unique). Defect 2 (ete_grade_final Boolean→VARCHAR cast — no_negative split across "false"/"absent"/"true"): fixed at canonical layer with explicit CASE on source flags. Cohort N=<...>, no_neg=<...>, micro=<...>, gross=<...>. Primary aOR gross-vs-micro: <...> [<CI>] p=<...> (v5 locked: 1.77 [1.15-2.71] p=0.009; drift <0.05). M044 v6 deliverables in M044_FINAL_PACKAGE_v6/. Closes CF-M044-DUP-COLS; opens CF-M044-V6-MANUSCRIPT-PATCH for Cowork prose review.'
WHERE NOT EXISTS (SELECT 1 FROM main.signoff_migration WHERE mig_id='mig_315');
```

## Carry-forwards

- Closes: `CF-M044-DUP-COLS`
- Opens (Cowork lane): `CF-M044-V6-MANUSCRIPT-PATCH` (writing-side review of v6 docx for prose consistency, references, figure legends)

## Out of scope

- Do NOT republish the M044 v5 submission package — keep the v5 docx untouched in `M044_FINAL_PACKAGE/`.
- Do NOT modify `canonical_patient_master` directly — the cleanup happens in `canonical_path_malignant_events_v1` (or the cohort flat layer if that's where the defect lives).
- Do NOT touch `cohort_m025_*` or `cohort_m032_*` cohorts even if they share the same JOIN-without-projection pattern — those are separate carry-forwards.
