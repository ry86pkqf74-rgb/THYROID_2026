# Cursor Composer Dispatch — mig_254: Reconcile M1 patients miscoded as Stage II

**Generated:** 2026-05-01 by Cowork (Snowflake Cortex trial round 2). HEAD ref: post-mig_253 (Snowflake trial round 2 findings).
**Lane:** mig_254 — investigate and resolve 1,058 malignant patients with `AJCC8_M_STAGE = 'M1'` whose `AJCC8_STAGE_GROUP` is recorded as `'II'`. Per AJCC 8 thyroid rules, M1 → Stage IVB regardless of T/N **for patients ≥55** with differentiated thyroid carcinoma; for age <55 with DTC, M1 → Stage II is **correct** (the AJCC 8 age-cutoff rule). MTC/ATC have separate staging tables (no age cutoff for MTC).
**Recommended agent:** **Cursor Chat (Claude Sonnet 4 or GPT-5)** for the rule-disambiguation pass first → then **Cursor Composer** for the apply. The decision tree (age × histology × M1) is non-trivial; do not let Composer guess it from a single instruction.
**Estimated runtime:** 90–120 min (decision review + dry-run + apply + verify)
**Triggered by:** Snowflake validation Prompt 5 (AJCC staging consistency) flagged 1,058 M1+Stage II combinations; AI_COMPLETE on a 100-pt sample agreed (36/100 INCONSISTENT).
**Severity:** HIGH. Affects every manuscript using `ajcc8_stage_group`. M044 (ETE) primary outcome stratifies by stage; M037 (LN predictors) Table 1 includes stage. Cannot be deferred to a future round.
**Opens carry-forward:** CF-mig254-M1-STAGEII-DECISION.

---

## §0 — First message to paste into Cursor Chat (NOT Composer yet)

> mig_254 decision pass. Read `cursor_prompts/CURSOR_PROMPT_MIG_254_M1_STAGEII_RECONCILE_20260501.md` end-to-end. You have MotherDuck MCP authed to `logan.glosser.eras@gmail.com`; database is `thyroid_canonical_publication_v1_0`. **Do not run any UPDATE yet.** Use the §2 probes to characterize the 1,058 patients along (age <55 vs ≥55) × (DTC vs MTC vs ATC) × (T-stage). Surface to Logan: which subgroups are correctly coded II per AJCC 8 age-cutoff rule, vs which are mislabeled and should be IVB. Output a disposition table before authoring any `UPDATE` SQL.

---

## §1 — Why this lane exists

Snowflake validation against `CANONICAL_PATIENT_MASTER_FLAT` flagged 1,058 patients with M1 + Stage II. Three competing explanations, all clinically plausible — Logan needs to pick:

1. **Correct per AJCC 8 age-cutoff:** DTC patients <55 with M1 are Stage II by definition. If most of the 1,058 are <55, there's no bug.
2. **Misapplication of cutoff:** if patients ≥55 with M1 are also coded II, the stage_group derivation in upstream is using a too-permissive rule.
3. **Pre-AJCC8 legacy:** some early-era patients (<2018) may have been staged with AJCC 7, where M1 → Stage IVC for DTC regardless of age.

### Snowflake breakdown — top T/N combinations among the 1,058

| AJCC8_T_STAGE | AJCC8_N_STAGE | AJCC8_STAGE_GROUP | N | MEAN_AGE | N_UNDER55 |
| --- | --- | --- | --- | --- | --- |
| T3b | N1a | II | 240 | 38.3 | 240 |
| T1b | N1a | II | 144 | 38.5 | 144 |
| T2 | N1a | II | 137 | 38.6 | 137 |
| T1a | N1a | II | 130 | 42.9 | 130 |
| T3b | N0 | II | 82 | 39.5 | 82 |
| T1a | N0 | II | 71 | 39.9 | 71 |
| T1b | N0 | II | 62 | 42.2 | 62 |
| T3a | N1a | II | 57 | 41.1 | 57 |
| T2 | N0 | II | 50 | 37.9 | 50 |
| T3a | N0 | II | 33 | 41.0 | 33 |
| T3b | N1b | II | 18 | 37.6 | 18 |
| T2 | N1b | II | 8 | 40.3 | 8 |
| T1a | Nx | II | 5 | 44.2 | 5 |
| T3a | N1b | II | 4 | 34.8 | 4 |
| T3a |  | II | 3 | 40.7 | 3 |


### By histology group

| hist_group | n | n_age_under_55 |
| --- | --- | --- |
| PTC | 837 | 837 |
| FTC | 144 | 144 |
| Other | 46 | 46 |
| MTC | 29 | 29 |
| ATC | 2 | 2 |


The mean age within this 1,058-patient cohort and the histology distribution will drive the disposition — see §2.

---

## §2 — Pre-task probes (run via MotherDuck MCP `query_rw` is NOT needed; use `query`)

```sql
-- Probe 1: age cutoff distribution among the 1,058
SELECT
  CASE WHEN age_at_surgery < 55 THEN 'under_55' WHEN age_at_surgery >= 55 THEN '55_plus' ELSE 'unknown' END AS age_bucket,
  CASE WHEN histology_final ILIKE 'PTC%' OR histology_final ILIKE '%follicular%' THEN 'DTC'
       WHEN histology_final ILIKE 'MTC%' THEN 'MTC'
       WHEN histology_final ILIKE '%anaplastic%' THEN 'ATC'
       ELSE 'OTHER' END AS hist_group,
  COUNT(*) AS n
FROM main.canonical_patient_master
WHERE is_malignant = TRUE AND ajcc8_m_stage = 'M1' AND ajcc8_stage_group = 'II'
GROUP BY 1, 2 ORDER BY 1, 2;

-- Probe 2: surgery year (legacy AJCC7 hypothesis)
SELECT
  CASE WHEN EXTRACT(YEAR FROM first_surgery_date) < 2018 THEN 'pre_2018'
       WHEN EXTRACT(YEAR FROM first_surgery_date) >= 2018 THEN '2018_plus'
       ELSE 'unknown' END AS era,
  COUNT(*) AS n
FROM main.canonical_patient_master
WHERE is_malignant = TRUE AND ajcc8_m_stage = 'M1' AND ajcc8_stage_group = 'II'
GROUP BY 1 ORDER BY 1;

-- Probe 3: trace stage_group provenance (which mig set it?)
-- Look at the registry for stage_group lineage:
SELECT *
FROM main.canonical_column_verification_registry_v1
WHERE table_name = 'canonical_patient_master' AND column_name = 'ajcc8_stage_group';
```

Surface results from probes 1–3 to Logan in a single concise message. **Wait for Logan to ratify the disposition rule** before §3.

---

## §3 — Plan (after disposition is ratified)

### 3a. Pre-snapshot
```sql
CREATE OR REPLACE TABLE archive_pub_v1_0.canonical_patient_master_pre_mig254_20260501 AS
SELECT research_id, ajcc8_t_stage, ajcc8_n_stage, ajcc8_m_stage, ajcc8_stage_group, age_at_surgery, histology_final, first_surgery_date
FROM main.canonical_patient_master
WHERE is_malignant = TRUE AND ajcc8_m_stage = 'M1';
```

### 3b. Apply rule (placeholder — fill in after §2 disposition)
```sql
-- Example: if rule = "age >=55 + DTC + M1 + II  ->  flip to IVB"
UPDATE main.canonical_patient_master
SET ajcc8_stage_group = 'IVB'
WHERE is_malignant = TRUE
  AND ajcc8_m_stage = 'M1'
  AND ajcc8_stage_group = 'II'
  AND age_at_surgery >= 55
  AND (histology_final ILIKE 'PTC%' OR histology_final ILIKE '%follicular%');
```

### 3c. Verify
```sql
-- Should be 0 (or whatever the disposition allows)
SELECT COUNT(*)
FROM main.canonical_patient_master
WHERE is_malignant = TRUE AND ajcc8_m_stage = 'M1' AND ajcc8_stage_group = 'II'
  AND age_at_surgery >= 55
  AND (histology_final ILIKE 'PTC%' OR histology_final ILIKE '%follicular%');
```

### 3d. Registry signoff
```sql
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
VALUES ('mig_254', CURRENT_TIMESTAMP::TIMESTAMP, 'logan_via_cursor',
        'M1+StageII reconciliation: <N flipped> records flipped to IVB per AJCC 8 age cutoff disposition');
```

### 3e. Re-verify on Snowflake
After mig lands in MD, Cowork re-runs Snowflake Prompt 5 (`snowflake_trial/scripts/07_prompt5_staging.py`) and confirms M1+II count goes to the disposition target.

---

## §4 — Out of scope
- M-stage assignment itself (M0 → M1 corrections) — separate audit
- AJCC 7 → 8 backfill — if probe 2 shows pre-2018 dominance, scope a separate mig
- Manuscript-specific recompute (M044, M037) — ETE/LN papers re-pull stage post-flip

## §5 — Carry-forwards
- CF-mig254-M1-STAGEII-DECISION (resolved by this lane)
- CF-mig254-LEGACY-AJCC7 (open if probe 2 shows >100 pre-2018 patients in the 1,058)

## §6 — Surgical git add paths
```
scripts/output/mig_254_*.md
scripts/output/mig_254_pre_snapshot_log.txt
scripts/output/mig_254_diff.csv
```
(Never `git add scripts/output/` — explicit paths only per `feedback_surgical_git_add.md`.)
