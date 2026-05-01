# Cursor Composer Dispatch — mig_263: Re-derive `ajcc8_stage_group` overlay (mig_266b → CPM) — formerly authored as mig_259, renumbered

**Generated:** 2026-05-01 by Cowork (renumber from earlier mig_259 prompt due to file-slot collision with the LN-status mig).
**Lane:** mig_263 — investigate whether `mig_266b` patient-level AJCC overlay should be re-derived from `mig_184` source so M1 patients get consistent stage_group derivation, OR whether a thin `IVA→IVB`, `IVC→IVB` coalesce rule should be formalized at the rollup boundary. Source `mig_184` writes IVA/IVB/IVC for ATC/MTC, but the rollup at CPM only carries {I, II, III, IVB} — anything outside that set silently becomes NULL or II, requiring case-by-case bandaid migs (mig_254 + mig_254b were the latest examples).
**Recommended agent:** **Cursor Chat (Claude Sonnet 4 / GPT-5)** for architectural decision pass + per-manuscript impact analysis → **Cursor Composer** for the actual mig once a strategy is picked. Do NOT let Composer self-direct — touches the AJCC derivation backbone.
**Estimated runtime:** 4–6 hrs (decision pass + dry-run + apply + view cascades + manuscript regression)
**Triggered by:** mig_254 root cause analysis (40 II→IVB flips) + mig_254b residual rid 9600.
**Severity:** MED. No patients miscoded TODAY (mig_254/254b cleared the active bugs), but every future M1 + (MTC/ATC/PDTC) patient added to the canonical is at risk of repeating mig_254-style cleanup.
**Closes carry-forward:** CF-mig254-MIG266B-OVERLAY-RE-DERIVE.

---

## §0 — First message to paste into Cursor Chat

> mig_263 architectural decision pass. Read this prompt end-to-end. The question is structural, not a single SQL: should `mig_266b`'s patient-level AJCC overlay be re-derived from `mig_184` source labels (which include IVA/IVC), or should we add a thin `IVA→IVB`, `IVC→IVB` coalesce rule at the rollup boundary?
>
> Run §2 probes; surface the per-rule impact analysis to Logan in a single concise message. Specifically: (a) how many patients today have mig_184 source labels outside {I,II,III,IVB}? (b) which manuscripts (M032/M037/M038/M044) filter on stage_group and would change behavior under each option? (c) what's the dependent-VIEW cascade if we add IVA/IVC to the allowed set?
>
> Pick A/B/C with Logan, then move to Composer.

---

## §1 — Why this lane exists

`canonical_patient_master.ajcc8_stage_group` only carries **{I, II, III, IVB}** in the publication DB. The source-of-truth derivation in `qc_framework_v1/migrations/184_v2_r1_ajcc_derivation_ratified_20260430.sql` writes the full AJCC 8 set including:

```sql
WHEN stage_component = 'ATC' AND m8 = 'M1' THEN 'IVC'
WHEN stage_component = 'ATC' AND t8 = 'T4b' THEN 'IVB'
WHEN stage_component = 'ATC' THEN 'IVA'
WHEN stage_component = 'MTC' AND m8 = 'M1' THEN 'IVC'
```

The mig_266b overlay (`patient_level_ajcc_overlay_dominant_tumor_mig266b_family`) collapses to the smaller set when promoting to CPM. The collapse logic is implicit and lossy — anything outside {I, II, III, IVB} becomes NULL or stale II, depending on how it was reached.

Evidence the collapse is the root cause:
- mig_254 found 40 patients (29 MTC + 2 ATC + 9 PDTC) with M1 + Stage II, all of which should have been IVC per mig_184 but got II via the collapse
- mig_254b found rid 9600 with M1 + NULL stage_group — same pattern, NULL instead of II
- Every new M1 + (MTC/ATC/PDTC) patient added to the canonical going forward will hit the same trap unless the overlay knows about IVA/IVC

## §2 — Pre-task probes

```sql
-- Probe 1: enumerate every distinct (mig_184 derived stage, current CPM stage) pair
SELECT DISTINCT
  src.derived_label AS source_stage,
  cpm.ajcc8_stage_group AS cpm_stage,
  COUNT(*) AS n
FROM main.canonical_patient_master cpm
LEFT JOIN <mig_184_source_table> src USING (research_id)
WHERE cpm.is_malignant = TRUE
GROUP BY 1, 2 ORDER BY 1, 2;

-- Probe 2: which manuscripts filter on stage_group?
SELECT DISTINCT cohort_view_name, definition
FROM main.manuscript_dive_map_v1
WHERE LOWER(definition) LIKE '%stage%' OR LOWER(definition) LIKE '%ivb%'
   OR LOWER(definition) LIKE '%iva%' OR LOWER(definition) LIKE '%ivc%';

-- Probe 3: dependent VIEWs on canonical_patient_master.ajcc8_stage_group
SELECT view_name FROM information_schema.views
WHERE view_definition ILIKE '%ajcc8_stage_group%';

-- Probe 4: how many M1 patients exist by histology category, and what are their CURRENT vs WOULD-BE stage_groups?
SELECT
  CASE WHEN histology_final ILIKE 'MTC%' OR histology_final ILIKE '%medullary%' THEN 'MTC'
       WHEN histology_final ILIKE '%anaplastic%' THEN 'ATC'
       WHEN histology_final ILIKE '%poorly differentiated%' THEN 'PDTC'
       WHEN histology_final ILIKE 'PTC%' OR histology_final ILIKE '%follicular%' THEN 'DTC'
       ELSE 'OTHER' END AS hist,
  ajcc8_stage_group AS current_label,
  COUNT(*) AS n
FROM main.canonical_patient_master
WHERE is_malignant = TRUE AND ajcc8_m_stage = 'M1'
GROUP BY 1, 2 ORDER BY 1, 2;
```

## §3 — Three candidate strategies

### Option A: Add IVA + IVC to allowed CPM label set (AJCC-faithful)
- Re-derive `ajcc8_stage_group` from `mig_184` source for all malignant patients
- Update mig_266b overlay logic to preserve IVA/IVC instead of collapsing
- Cascade: every dependent VIEW filtering on stage_group needs to handle the extended set
- Manuscript impact: M032/M037/M038/M044 cohorts using `WHERE stage_group = 'IVB'` need update to `IN ('IVA','IVB','IVC')` or split A/B/C
- **Pro:** AJCC-published convention; reviewer-defensible
- **Con:** large blast radius (≥10 dependent views, all manuscripts)

### Option B: Coalesce IVA/IVC → IVB at rollup boundary (current de-facto, RECOMMENDED)
- Document `mig_266b` formally with explicit rule: `WHEN IN ('IVA','IVC') THEN 'IVB'`
- Re-run overlay so existing NULL/II miscodings get IVB
- Manuscript impact: zero — all dependent views and manuscripts continue to work
- **Pro:** minimal blast radius; preserves manuscript pipeline
- **Con:** IVA/IVB/IVC granularity permanently lost at CPM (still queryable via mig_184 source if needed)

### Option C: Dual-column — keep CPM `ajcc8_stage_group` collapsed, add `ajcc8_stage_group_full` with IVA/IVC
- Add new column carrying full AJCC 8 label
- Existing column stays {I,II,III,IVB}; new column carries {I,II,III,IVA,IVB,IVC}
- Manuscripts pick whichever they want
- **Pro:** preserves both views
- **Con:** schema bloat; downstream queries must declare which column to trust

## §4 — Apply (after Logan picks A/B/C)

### Pre-snapshot (all options)
```sql
CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.cpm_pre_mig263_20260501 AS
SELECT research_id, ajcc8_t_stage, ajcc8_n_stage, ajcc8_m_stage, ajcc8_stage_group,
       histology_final, age_at_surgery
FROM main.canonical_patient_master WHERE is_malignant = TRUE;
```

### Option A SQL (AJCC-faithful)
```sql
UPDATE main.canonical_patient_master cpm
SET ajcc8_stage_group = src.derived_label
FROM <mig_184_source_table> src
WHERE cpm.research_id = src.research_id AND src.derived_label IS NOT NULL;
```

### Option B SQL (recommended)
```sql
UPDATE main.canonical_patient_master cpm
SET ajcc8_stage_group = CASE
  WHEN src.derived_label IN ('IVA','IVC') THEN 'IVB'
  ELSE src.derived_label
END
FROM <mig_184_source_table> src
WHERE cpm.research_id = src.research_id;
```

### Option C SQL
```sql
ALTER TABLE main.canonical_patient_master
  ADD COLUMN IF NOT EXISTS ajcc8_stage_group_full VARCHAR;
UPDATE main.canonical_patient_master cpm
SET ajcc8_stage_group_full = src.derived_label
FROM <mig_184_source_table> src
WHERE cpm.research_id = src.research_id;
```

## §5 — Verify

```sql
-- All M1 + (MTC|ATC|PDTC) should have non-NULL stage_group
SELECT COUNT(*) FROM main.canonical_patient_master
WHERE is_malignant = TRUE AND ajcc8_m_stage = 'M1' AND ajcc8_stage_group IS NULL;
-- Expect: 0 under any option
```

## §6 — Manuscript regression check (REQUIRED before signoff under Option A)

```bash
# Snapshot M037 Table 1 pre-apply
SNOWFLAKE_PAT='...' python snowflake_trial/scripts/09_m037_table1.py
mv snowflake_trial/reports/m037_table1.md snowflake_trial/reports/m037_table1_pre_mig263.md

# Apply mig_263 in MD (Cursor)
# Re-export, reload Snowflake, regenerate Table 1
SNOWFLAKE_PAT='...' python snowflake_trial/scripts/01_export_md_to_parquet.py
SNOWFLAKE_PAT='...' python snowflake_trial/scripts/02_load_to_snowflake.py
SNOWFLAKE_PAT='...' python snowflake_trial/scripts/04_build_flat_views.py
SNOWFLAKE_PAT='...' python snowflake_trial/scripts/09_m037_table1.py
mv snowflake_trial/reports/m037_table1.md snowflake_trial/reports/m037_table1_post_mig263.md
diff snowflake_trial/reports/m037_table1_pre_mig263.md snowflake_trial/reports/m037_table1_post_mig263.md
```

## §7 — Carry-forwards
- CF-mig254-MIG266B-OVERLAY-RE-DERIVE → CLOSED on apply
- CF-mig263-MANUSCRIPT-IMPACT (open if Option A picked)

## §8 — Surgical git add
```
scripts/output/mig_263_*.md
scripts/output/mig_263_pre_snapshot_log.txt
scripts/output/mig_263_dependent_view_diff.csv
qc_framework_v1/migrations/263_*.sql
```

## §9 — Note: file-slot history
- mig_259 file slot used by `259_ln_status_source_*.sql` (CF-mig258 LN reconcile)
- This prompt's apply lands as `qc_framework_v1/migrations/263_*.sql`
- The previous Cursor prompt at `cursor_prompts/CURSOR_PROMPT_MIG_259_AJCC_OVERLAY_RE_DERIVE_20260501.md` is superseded by this one — Logan can delete the older file or leave it as historical
