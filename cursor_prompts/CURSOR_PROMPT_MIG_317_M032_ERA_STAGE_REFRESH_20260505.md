# Cursor prompt — mig_317: M032 era × stage refresh post-mig_313

**Agent:** cursor_composer
**Estimated time:** 1–2 hours (analysis re-run + delta audit + numerical patch decision)
**Priority:** P3 — M032 v1 submission package is shipped; this is a numerical-integrity audit, not a blocker
**Closes:** none new; cleans up M-stage cascade audit trail

## Problem

The cursor agent's mig_313 report flagged: *"M032 25yr ⚠️ Era-by-stage tables need re-run (IVB inflation was temporal)."*

M032 ("25-year descriptive") is a published-ready manuscript (`M032_submission_package_v1_0/`, mig_290/290b, frozen 2026-05-04). Its central tables include era-stratified counts of stage at presentation. The pre-mig_313 M-stage corruption produced **age-dependent, era-correlated false-M1 inflation** — so era × stage tables likely showed a spurious increase in Stage IV cases over time.

This prompt produces a v2 era × stage analysis using the corrected canonical layer and quantifies the manuscript-relevant deltas.

## Recipe

### Step 1 — Locate M032 era × stage script(s)

```bash
cd /Users/loganglosser/THYROID_2026
find scripts/ studies/ -name "*m032*" -type f | head -20
ls M032_submission_package_v1_0/
```

Identify the script that produces the era × stage table (likely `scripts/m032_era_stratified.py` or similar; if not present, the analysis was a notebook — locate it).

### Step 2 — Run the era × stage analysis on post-mig_313 cohort

```bash
.venv/bin/python scripts/m032_<era_stage_script>.py \
  --out studies/m032_era_stage_v2_post_mig313/ 2>&1 | tee logs/m032_era_v2.log
```

Or, if the script doesn't exist, write a one-off SQL:

```sql
-- Era × stage distribution, post-mig_313
WITH cohort AS (
  SELECT
    research_id,
    ajcc8_stage_group,
    EXTRACT(YEAR FROM surg_first_date) AS surg_year,
    CASE
      WHEN EXTRACT(YEAR FROM surg_first_date) BETWEEN 1999 AND 2007 THEN '1999-2007'
      WHEN EXTRACT(YEAR FROM surg_first_date) BETWEEN 2008 AND 2015 THEN '2008-2015'
      WHEN EXTRACT(YEAR FROM surg_first_date) BETWEEN 2016 AND 2024 THEN '2016-2024'
      ELSE 'other'
    END AS era
  FROM main.canonical_patient_master
  WHERE is_malignant
)
SELECT
  era,
  ajcc8_stage_group,
  COUNT(*) AS n,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY era), 1) AS pct_in_era
FROM cohort
GROUP BY 1, 2
ORDER BY era, ajcc8_stage_group;
```

(Adapt era boundaries to whatever M032 v1 used.)

### Step 3 — Diff post-mig_313 vs M032 v1 frozen tables

Open `M032_submission_package_v1_0/M032_*all_stats.xlsx` (or equivalent), find the era × stage table, and produce a side-by-side delta:

| Era | Stage | v1 frozen n | v2 (post-mig_313) n | Δ | Δ% |
|---|---|---|---|---|---|

Save as `studies/m032_era_stage_v2_post_mig313/delta_v1_vs_v2.xlsx`.

### Step 4 — Decision tree based on deltas

- **If max(|Δ%|) < 5%** for all era × stage cells: M032 v1 numbers are robust to the M-stage fix. Document deltas as a footnote-only update; **no v2 numerical patch needed**.
- **If max(|Δ%|) is 5–15%** in any cell: M032 needs a v2 numerical patch (Table updates only, regression unchanged). Flag for Cowork manuscript-edit pass.
- **If max(|Δ%|) > 15%** in any cell: M032 v1 needs a substantive correction notice. Flag for Logan to review before any republication.

### Step 5 — Write delta report

`studies/m032_era_stage_v2_post_mig313/M032_DELTA_REPORT_v1_vs_v2.md`

Should include:
- Top 5 cells with largest |Δ| (absolute count)
- Top 5 cells with largest |Δ%| (relative)
- Headline interpretation (was IVB temporally inflated as suspected? Where else?)
- Decision per Step 4
- Recommended next action (footnote / numerical patch / correction notice)

### Step 6 — Signoff

```sql
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
SELECT 'mig_317', CURRENT_TIMESTAMP, 'cursor_composer_mig317',
  'mig_317: M032 era × stage refresh post-mig_313. Cohort N=4,019 malignant. Max |Δ%|=<X%> in era=<Y>, stage=<Z>. Decision: <footnote-only / v2 patch needed / correction notice>. Delta report: studies/m032_era_stage_v2_post_mig313/M032_DELTA_REPORT_v1_vs_v2.md. M032 v1 submission package unchanged.'
WHERE NOT EXISTS (SELECT 1 FROM main.signoff_migration WHERE mig_id='mig_317');
```

## Carry-forwards

No new carry-forwards. Documents the cascade impact of `CF-MSTAGE-CORRUPTION` (which is closed) on a downstream shipped manuscript.

## Out of scope

- Do NOT modify `M032_submission_package_v1_0/` files — that's frozen.
- Do NOT touch the M032 era boundaries; use whatever the v1 analysis used.
- Do NOT regenerate the M032 v1 manuscript; this prompt is delta-audit only. If the decision in Step 4 is "v2 numerical patch needed", that's a separate Cowork prompt.
