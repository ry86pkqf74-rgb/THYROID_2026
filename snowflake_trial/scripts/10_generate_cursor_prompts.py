"""
Generate Cursor Composer / VSC mig prompts from Snowflake validation findings.

For each finding the script:
  1. Queries Snowflake for cohort detail + impact stats
  2. Writes a Cursor-Composer-style prompt to cursor_prompts/
     following Logan's existing convention (see CURSOR_PROMPT_MIG_252_*.md)

Routes each finding to:
  - Composer  = mechanical mig, pattern-match existing migs
  - Chat-then-Composer = needs clinical reasoning before edit (use Claude Sonnet
    or GPT-5.x in Cursor Chat / VSC Copilot Chat first, then Composer applies)
"""
import sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from _sf_client import get_cursor

OUT_DIR = Path("/Users/ros/THyroid 2026/cursor_prompts")
OUT_DIR.mkdir(exist_ok=True)
TODAY = "20260501"
HEAD_NOTE = "post-mig_253 (Snowflake trial round 2 findings)"

ctx, cur = get_cursor()


def query(sql):
    cur.execute(sql)
    return cur.fetchall(), [c[0] for c in cur.description]


def md_table(rows, cols, max_rows=None):
    out = ["| " + " | ".join(str(c) for c in cols) + " |"]
    out.append("| " + " | ".join("---" for _ in cols) + " |")
    n = max_rows if max_rows else len(rows)
    for r in rows[:n]:
        out.append("| " + " | ".join("" if v is None else str(v) for v in r) + " |")
    return "\n".join(out) + "\n"


# ============================================================================
# mig_254 — M1 → Stage II misclassification (HIGHEST IMPACT)
# ============================================================================
print("=== mig_254: M1 -> Stage II ===")
m1_breakdown, cols = query("""
SELECT
  AJCC8_T_STAGE, AJCC8_N_STAGE, AJCC8_STAGE_GROUP,
  COUNT(*) AS n,
  ROUND(AVG(AGE_AT_SURGERY), 1) AS mean_age,
  COUNT_IF(AGE_AT_SURGERY < 55) AS n_under55
FROM CANONICAL_PATIENT_MASTER_FLAT
WHERE IS_MALIGNANT = TRUE
  AND AJCC8_M_STAGE = 'M1'
  AND AJCC8_STAGE_GROUP = 'II'
GROUP BY 1, 2, 3
ORDER BY n DESC LIMIT 15
""")
hist_breakdown, _ = query("""
SELECT
  CASE WHEN HISTOLOGY_FINAL ILIKE 'PTC%' THEN 'PTC'
       WHEN HISTOLOGY_FINAL ILIKE '%follicular%' THEN 'FTC'
       WHEN HISTOLOGY_FINAL ILIKE 'MTC%' THEN 'MTC'
       WHEN HISTOLOGY_FINAL ILIKE '%anaplastic%' THEN 'ATC'
       ELSE 'Other'
  END AS hist_group,
  COUNT(*) AS n,
  COUNT_IF(AGE_AT_SURGERY < 55) AS n_under55
FROM CANONICAL_PATIENT_MASTER_FLAT
WHERE IS_MALIGNANT = TRUE AND AJCC8_M_STAGE = 'M1' AND AJCC8_STAGE_GROUP = 'II'
GROUP BY 1 ORDER BY n DESC
""")

mig_254 = f"""# Cursor Composer Dispatch — mig_254: Reconcile M1 patients miscoded as Stage II

**Generated:** 2026-05-01 by Cowork (Snowflake Cortex trial round 2). HEAD ref: {HEAD_NOTE}.
**Lane:** mig_254 — investigate and resolve 1,058 malignant patients with `AJCC8_M_STAGE = 'M1'` whose `AJCC8_STAGE_GROUP` is recorded as `'II'`. Per AJCC 8 thyroid rules, M1 → Stage IVB regardless of T/N **for patients ≥55** with differentiated thyroid carcinoma; for age <55 with DTC, M1 → Stage II is **correct** (the AJCC 8 age-cutoff rule). MTC/ATC have separate staging tables (no age cutoff for MTC).
**Recommended agent:** **Cursor Chat (Claude Sonnet 4 or GPT-5)** for the rule-disambiguation pass first → then **Cursor Composer** for the apply. The decision tree (age × histology × M1) is non-trivial; do not let Composer guess it from a single instruction.
**Estimated runtime:** 90–120 min (decision review + dry-run + apply + verify)
**Triggered by:** Snowflake validation Prompt 5 (AJCC staging consistency) flagged 1,058 M1+Stage II combinations; AI_COMPLETE on a 100-pt sample agreed (36/100 INCONSISTENT).
**Severity:** HIGH. Affects every manuscript using `ajcc8_stage_group`. M044 (ETE) primary outcome stratifies by stage; M037 (LN predictors) Table 1 includes stage. Cannot be deferred to a future round.
**Opens carry-forward:** CF-mig254-M1-STAGEII-DECISION.

---

## §0 — First message to paste into Cursor Chat (NOT Composer yet)

> mig_254 decision pass. Read `cursor_prompts/CURSOR_PROMPT_MIG_254_M1_STAGEII_RECONCILE_{TODAY}.md` end-to-end. You have MotherDuck MCP authed to `logan.glosser.eras@gmail.com`; database is `thyroid_canonical_publication_v1_0`. **Do not run any UPDATE yet.** Use the §2 probes to characterize the 1,058 patients along (age <55 vs ≥55) × (DTC vs MTC vs ATC) × (T-stage). Surface to Logan: which subgroups are correctly coded II per AJCC 8 age-cutoff rule, vs which are mislabeled and should be IVB. Output a disposition table before authoring any `UPDATE` SQL.

---

## §1 — Why this lane exists

Snowflake validation against `CANONICAL_PATIENT_MASTER_FLAT` flagged 1,058 patients with M1 + Stage II. Three competing explanations, all clinically plausible — Logan needs to pick:

1. **Correct per AJCC 8 age-cutoff:** DTC patients <55 with M1 are Stage II by definition. If most of the 1,058 are <55, there's no bug.
2. **Misapplication of cutoff:** if patients ≥55 with M1 are also coded II, the stage_group derivation in upstream is using a too-permissive rule.
3. **Pre-AJCC8 legacy:** some early-era patients (<2018) may have been staged with AJCC 7, where M1 → Stage IVC for DTC regardless of age.

### Snowflake breakdown — top T/N combinations among the 1,058

{md_table(m1_breakdown, cols)}

### By histology group

{md_table(hist_breakdown, ['hist_group','n','n_age_under_55'])}

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
"""

(OUT_DIR / f"CURSOR_PROMPT_MIG_254_M1_STAGEII_RECONCILE_{TODAY}.md").write_text(mig_254)
print(f"  saved mig_254 ({len(mig_254):,} chars)")


# ============================================================================
# mig_255 — Recurrence flag/timing mismatch (740 patients)
# ============================================================================
print("\n=== mig_255: recurrence flag/timing mismatch ===")
recur_breakdown, cols = query("""
SELECT
  IS_MALIGNANT,
  CASE WHEN ANY_RECURRENCE_FLAG IS NULL THEN 'NULL' ELSE ANY_RECURRENCE_FLAG::VARCHAR END AS flag,
  CASE WHEN TIME_TO_RECURRENCE_DAYS IS NULL THEN 'days_NULL'
       WHEN TIME_TO_RECURRENCE_DAYS = 0 THEN 'days_0'
       WHEN TIME_TO_RECURRENCE_DAYS > 0 THEN 'days_pos'
       ELSE 'other' END AS days_bucket,
  COUNT(*) AS n
FROM CANONICAL_PATIENT_MASTER_FLAT
WHERE ANY_RECURRENCE_FLAG = FALSE AND TIME_TO_RECURRENCE_DAYS IS NOT NULL
GROUP BY 1, 2, 3
ORDER BY n DESC
""")

mig_255 = f"""# Cursor Composer Dispatch — mig_255: Reconcile recurrence flag vs time-to-recurrence

**Generated:** 2026-05-01 by Cowork (Snowflake Cortex trial round 2).
**Lane:** mig_255 — 740 patients have `any_recurrence_flag = FALSE` but `time_to_recurrence_days IS NOT NULL`. Either the flag is wrong (recurrence happened, days is correct) or `days` is a stale/spurious value that should be NULL.
**Recommended agent:** **Cursor Composer** — pattern matches existing recurrence-derivation migs. The rule choice is: trust the timing column (it has more nuance) or trust the flag (it's the rolled-up assertion).
**Estimated runtime:** 45–60 min
**Triggered by:** Snowflake validation Prompt 3 (survival/recurrence integrity).
**Severity:** MEDIUM-HIGH. M044, M032, and any survival/recurrence outcome paper.
**Opens carry-forward:** CF-mig255-RECUR-FLAG-TIMING.

---

## §0 — First message to paste into Cursor Composer

> mig_255 dispatch. Read `cursor_prompts/CURSOR_PROMPT_MIG_255_RECUR_FLAG_TIMING_{TODAY}.md` end-to-end. MotherDuck MCP authed. The 740 patients in question have `any_recurrence_flag=FALSE` but `time_to_recurrence_days IS NOT NULL`. Run §2 probes first; surface the upstream source of `time_to_recurrence_days` (canonical_recurrence_events_v1? a derivation in CPM?) before deciding flip-flag vs null-days.

---

## §1 — Why this lane exists

Snowflake breakdown of the 740 mismatches:

{md_table(recur_breakdown, cols)}

The rolled-up patient-level flag conflicts with the days-since-recurrence numeric. Two fixes are possible:
- **Option A:** flip `any_recurrence_flag` to TRUE when `time_to_recurrence_days IS NOT NULL`. Treats timing as truth.
- **Option B:** null out `time_to_recurrence_days` when `any_recurrence_flag = FALSE`. Treats flag as truth.
- **Option C:** trace each upstream signal — `canonical_recurrence_events_v1` event count, NLP recurrence mentions, structured radiology — and derive both fields together from a unified source.

Option C is correct per Logan's Protocol v2 (no fix in isolation; rebuild from canonical events). A and B are quick patches.

---

## §2 — Pre-task probes

```sql
-- Probe 1: do these 740 have any source events?
WITH bad AS (
  SELECT research_id FROM main.canonical_patient_master
  WHERE any_recurrence_flag = FALSE AND time_to_recurrence_days IS NOT NULL
)
SELECT
  COUNT(*) AS n_bad_pts,
  COUNT(DISTINCT r.research_id) AS n_with_recur_event_in_canonical
FROM bad b LEFT JOIN main.canonical_recurrence_events_v1 r USING (research_id);

-- Probe 2: time_to_recurrence_days distribution
SELECT
  CASE WHEN time_to_recurrence_days < 0 THEN 'neg'
       WHEN time_to_recurrence_days = 0 THEN 'zero'
       WHEN time_to_recurrence_days BETWEEN 1 AND 365 THEN '1-365'
       WHEN time_to_recurrence_days BETWEEN 366 AND 1825 THEN '366-1825'
       ELSE '>1825' END AS days_bucket,
  COUNT(*) AS n
FROM main.canonical_patient_master
WHERE any_recurrence_flag = FALSE AND time_to_recurrence_days IS NOT NULL
GROUP BY 1 ORDER BY 1;

-- Probe 3: registry lineage for both columns
SELECT column_name, last_signed_off_mig, verification_method
FROM main.canonical_column_verification_registry_v1
WHERE table_name = 'canonical_patient_master'
  AND column_name IN ('any_recurrence_flag', 'time_to_recurrence_days');
```

Surface to Logan; pick A/B/C.

---

## §3 — Apply (after disposition)

```sql
-- Pre-snapshot
CREATE OR REPLACE TABLE archive_pub_v1_0.canonical_patient_master_pre_mig255_20260501 AS
SELECT research_id, any_recurrence_flag, time_to_recurrence_days
FROM main.canonical_patient_master
WHERE any_recurrence_flag = FALSE AND time_to_recurrence_days IS NOT NULL;

-- Option A example
UPDATE main.canonical_patient_master
SET any_recurrence_flag = TRUE
WHERE any_recurrence_flag = FALSE AND time_to_recurrence_days IS NOT NULL;
```

Verify on Snowflake post-fix:
```sql
SELECT COUNT(*) FROM CANONICAL_PATIENT_MASTER_FLAT
WHERE ANY_RECURRENCE_FLAG = FALSE AND TIME_TO_RECURRENCE_DAYS IS NOT NULL;
```
(Should be 0 after Option A or B; Option C may leave a small residual.)

## §4 — Carry-forwards
- CF-mig255-RECUR-FLAG-TIMING (closed by this lane)
- CF-mig255-RECUR-RESOURCING-FROM-EVENTS (open if Option C deferred)

## §5 — Surgical git add paths
```
scripts/output/mig_255_*.md
scripts/output/mig_255_pre_snapshot_log.txt
```
"""
(OUT_DIR / f"CURSOR_PROMPT_MIG_255_RECUR_FLAG_TIMING_{TODAY}.md").write_text(mig_255)
print(f"  saved mig_255 ({len(mig_255):,} chars)")


# ============================================================================
# mig_256 — 6 benign patients with recurrence flagged
# ============================================================================
print("\n=== mig_256: 6 benign + recurrence ===")
benign_recur, cols = query("""
SELECT RESEARCH_ID, HISTOLOGY_FINAL, IS_MALIGNANT, ANY_RECURRENCE_FLAG,
       TIME_TO_RECURRENCE_DAYS, FIRST_SURGERY_DATE,
       AJCC8_STAGE_GROUP, AJCC8_T_STAGE
FROM CANONICAL_PATIENT_MASTER_FLAT
WHERE IS_MALIGNANT = FALSE AND ANY_RECURRENCE_FLAG = TRUE
ORDER BY RESEARCH_ID
""")

mig_256 = f"""# Cursor Composer Dispatch — mig_256: 6 benign patients flagged with recurrence

**Generated:** 2026-05-01 by Cowork (Snowflake Cortex trial round 2).
**Lane:** mig_256 — 6 patients have `is_malignant = FALSE` but `any_recurrence_flag = TRUE`. By construction, benign disease cannot recur (it was never cancer). Either malignancy was upstaged at recurrence, or the recurrence flag is a false positive.
**Recommended agent:** **Cursor Composer** with explicit case-by-case review (N=6 is small enough for direct adjudication).
**Estimated runtime:** 30–45 min
**Triggered by:** Snowflake validation Prompt 3.
**Severity:** LOW (small N) but high data-quality optics.
**Opens carry-forward:** CF-mig256-BENIGN-RECUR-RECONCILE.

---

## §0 — First message to paste into Cursor Composer

> mig_256 dispatch. The 6 patients in §1 each need clinical adjudication — pull each `research_id` against `canonical_recurrence_events_v1`, `canonical_path_*_events_v1`, `clinical_notes_long`, and the path_synoptics. Decide per case: (A) was a malignancy event missed at index, mark `is_malignant=TRUE`; or (B) is the recurrence flag spurious, set `any_recurrence_flag=FALSE`. Surface table to Logan for sign-off before any UPDATE.

---

## §1 — Patient list

{md_table(benign_recur, cols)}

## §2 — Per-patient probe template

For each `research_id` above:
```sql
-- Path events
SELECT * FROM main.canonical_path_malignant_events_v1 WHERE research_id = '<RID>';
SELECT * FROM main.canonical_path_gland_events_v1 WHERE research_id = '<RID>';

-- Recurrence events (the source of the flag)
SELECT * FROM main.canonical_recurrence_events_v1 WHERE research_id = '<RID>';

-- Note text — radiology / oncology follow-up
SELECT note_date, note_type, content
FROM main.clinical_notes_long
WHERE research_id = '<RID>'
  AND note_date >= (SELECT first_surgery_date FROM main.canonical_patient_master WHERE research_id = '<RID>')
ORDER BY note_date LIMIT 50;
```

## §3 — Apply

After per-case disposition, batch the UPDATEs:
```sql
-- Pre-snapshot
CREATE OR REPLACE TABLE archive_pub_v1_0.cpm_pre_mig256_20260501 AS
SELECT research_id, is_malignant, any_recurrence_flag
FROM main.canonical_patient_master
WHERE research_id IN (<the 6 RIDs>);

-- Apply per disposition (example: 4 are upstage to malignant, 2 are spurious-flag)
-- (concrete UPDATEs to be drafted by Cursor after adjudication)
```

## §4 — Re-verify on Snowflake

```sql
SELECT COUNT(*) FROM CANONICAL_PATIENT_MASTER_FLAT
WHERE IS_MALIGNANT = FALSE AND ANY_RECURRENCE_FLAG = TRUE;
```
(Should be 0.)

## §5 — Carry-forwards
- CF-mig256-BENIGN-RECUR-RECONCILE (closed)

## §6 — Surgical git add paths
```
scripts/output/mig_256_per_patient_disposition.csv
scripts/output/mig_256_apply_log.txt
```
"""
(OUT_DIR / f"CURSOR_PROMPT_MIG_256_BENIGN_RECUR_RECONCILE_{TODAY}.md").write_text(mig_256)
print(f"  saved mig_256 ({len(mig_256):,} chars)")


# ============================================================================
# mig_257 — followup_years > overall_survival_years for deceased
# ============================================================================
print("\n=== mig_257: followup > survival for deceased ===")
fu_breakdown, cols = query("""
SELECT
  CASE WHEN FOLLOWUP_YEARS - OVERALL_SURVIVAL_YEARS < 1 THEN '<1yr_diff'
       WHEN FOLLOWUP_YEARS - OVERALL_SURVIVAL_YEARS < 3 THEN '1-3yr_diff'
       ELSE '>3yr_diff' END AS gap_bucket,
  COUNT(*) AS n,
  ROUND(AVG(FOLLOWUP_YEARS - OVERALL_SURVIVAL_YEARS), 2) AS mean_gap_yrs
FROM CANONICAL_PATIENT_MASTER_FLAT
WHERE DEATH_OCCURRED = TRUE
  AND FOLLOWUP_YEARS > OVERALL_SURVIVAL_YEARS
GROUP BY 1 ORDER BY 1
""")

mig_257 = f"""# Cursor Composer Dispatch — mig_257: Repair followup_years for deceased patients

**Generated:** 2026-05-01 by Cowork (Snowflake Cortex trial round 2).
**Lane:** mig_257 — 100 deceased patients have `followup_years > overall_survival_years`. Followup cannot extend past death; either followup_years was sourced from a non-clinical contact (research-staff outreach post-mortem), or the survival/death-date pair is wrong.
**Recommended agent:** **Cursor Composer** — matches the mig_101 stale-rollup-after-events-repair pattern (memory: `project_path_gland_family_complete_2026-04-28.md`).
**Estimated runtime:** 45–60 min
**Triggered by:** Snowflake validation Prompt 3.
**Severity:** MEDIUM. Survival KM estimates and any time-to-event manuscript.
**Opens carry-forward:** CF-mig257-FU-POST-DEATH.

---

## §0 — First message to paste into Cursor Composer

> mig_257 dispatch. 100 deceased patients have followup_years > overall_survival_years. Per the mig_101 pattern, this is a stale-rollup; the rule is `followup_years = MIN(followup_years, overall_survival_years)` for deceased patients. Pre-snapshot to archive_pub_v1_0; apply UPDATE; verify Snowflake count drops to 0.

---

## §1 — Distribution of the gap

{md_table(fu_breakdown, cols)}

If the gap is mostly <1 year, this is likely a date-rounding artifact between death-date and last-followup-contact. If >3yr is common, an upstream date-encoding bug is likely.

## §2 — Apply (rule = clamp followup to survival for deceased)

```sql
-- Pre-snapshot
CREATE OR REPLACE TABLE archive_pub_v1_0.cpm_pre_mig257_20260501 AS
SELECT research_id, followup_years, overall_survival_years, death_occurred, vital_status
FROM main.canonical_patient_master
WHERE death_occurred = TRUE AND followup_years > overall_survival_years;

-- Apply
UPDATE main.canonical_patient_master
SET followup_years = overall_survival_years
WHERE death_occurred = TRUE AND followup_years > overall_survival_years;

-- Registry signoff
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
VALUES ('mig_257', CURRENT_TIMESTAMP::TIMESTAMP, 'logan_via_cursor',
        'Followup_years clamped to overall_survival_years for 100 deceased patients (followup-post-death repair).');
```

## §3 — Re-verify on Snowflake

```sql
SELECT COUNT(*) FROM CANONICAL_PATIENT_MASTER_FLAT
WHERE DEATH_OCCURRED = TRUE AND FOLLOWUP_YEARS > OVERALL_SURVIVAL_YEARS;
```
(Should be 0.)

## §4 — Carry-forwards
- CF-mig257-FU-POST-DEATH (closed)
- CF-mig257-DATE-ENCODING (open if §1 shows >3yr-gap dominance — root cause investigation)

## §5 — Surgical git add paths
```
scripts/output/mig_257_apply_log.txt
```
"""
(OUT_DIR / f"CURSOR_PROMPT_MIG_257_FU_POST_DEATH_{TODAY}.md").write_text(mig_257)
print(f"  saved mig_257 ({len(mig_257):,} chars)")


# ============================================================================
# mig_258 — N1a stage but LN_TOTAL_POSITIVE NULL/0
# ============================================================================
print("\n=== mig_258: N1a + LN_total_positive=0/NULL ===")
n_pos_breakdown, cols = query("""
SELECT
  AJCC8_N_STAGE,
  CASE WHEN LN_TOTAL_POSITIVE IS NULL THEN 'count_NULL'
       WHEN LN_TOTAL_POSITIVE = 0 THEN 'count_0'
       ELSE 'count_pos' END AS count_bucket,
  COUNT(*) AS n
FROM CANONICAL_PATIENT_MASTER_FLAT
WHERE IS_MALIGNANT = TRUE AND AJCC8_N_STAGE IN ('N1a', 'N1b')
GROUP BY 1, 2 ORDER BY 1, 2
""")

mig_258 = f"""# Cursor Composer Dispatch — mig_258: Reconcile N-stage assertion vs LN_TOTAL_POSITIVE count

**Generated:** 2026-05-01 by Cowork (Snowflake Cortex trial round 2).
**Lane:** mig_258 — 1,501 patients have `ajcc8_n_stage = 'N1a'` (or N1b) but `ln_total_positive` is NULL or 0. The N-staging encodes positivity that the structured count doesn't reflect. This breaks any Table 1 that summarizes both columns (e.g. M037 LN predictors).
**Recommended agent:** **Cursor Chat (Claude Sonnet 4 or GPT-5)** for the rule design pass first → **Cursor Composer** for the apply. Decision is clinical/structural: do we trust the N-stage label and forward-fill the count, or treat the count as truth and downgrade the N-stage when missing?
**Estimated runtime:** 75–90 min
**Triggered by:** Snowflake M037 Table 1 generation (round 2).
**Severity:** HIGH. Affects M037 (LN predictors), M044 (ETE — uses N-stage), and any LN-counting paper.
**Opens carry-forward:** CF-mig258-NSTAGE-LNCOUNT-RECONCILE.

---

## §0 — First message to paste into Cursor Chat (decision pass)

> mig_258 decision pass. 1,501 patients have `ajcc8_n_stage` ∈ {{'N1a', 'N1b'}} but `ln_total_positive` is NULL or 0. The two fields disagree at scale. Three candidate rules:
>
> **Rule A — N-stage is truth:** when N1a/b but count is NULL/0, set count=`ln_total_examined` if examined>0, else fall back to 1 (sentinel for "at least one positive"). Pro: preserves N-staging. Con: conflates "≥1" with actual count.
>
> **Rule B — count is truth:** when count=0, set N-stage to N0; when NULL but staging implies positive, leave N-stage as-is but flag `ln_count_unknown=TRUE` in a new column. Pro: structurally honest. Con: loses 1,501 N1+ patients from manuscripts that filter by N1.
>
> **Rule C — separate truth domains:** keep both as-is, add `ln_status_source` ∈ {{'staging','count','both'}} so manuscripts can pick. Pro: lossless. Con: every downstream query must declare which source it trusts.
>
> Run §2 probes; surface counts to Logan; pick A/B/C. Only then move to Composer.

---

## §1 — Distribution

{md_table(n_pos_breakdown, cols)}

## §2 — Pre-task probes

```sql
-- Probe 1: do these patients have a path-events source?
WITH disagreement AS (
  SELECT research_id FROM main.canonical_patient_master
  WHERE is_malignant = TRUE AND ajcc8_n_stage IN ('N1a', 'N1b')
    AND (ln_total_positive IS NULL OR ln_total_positive = 0)
)
SELECT
  COUNT(*) AS n_disagreement,
  COUNT_IF(ln_total_examined > 0) AS n_with_lns_examined,
  COUNT_IF(ln_total_examined IS NULL OR ln_total_examined = 0) AS n_no_lns_examined
FROM main.canonical_patient_master cpm
JOIN disagreement d USING (research_id);

-- Probe 2: which mig wrote N-stage and ln_total_positive?
SELECT column_name, last_signed_off_mig, verification_method
FROM main.canonical_column_verification_registry_v1
WHERE table_name = 'canonical_patient_master'
  AND column_name IN ('ajcc8_n_stage', 'ln_total_positive', 'ln_total_examined', 'ln_positive_flag');

-- Probe 3: any patients with both signals?
SELECT
  COUNT(*) AS n,
  ROUND(AVG(ln_total_positive), 2) AS mean_count
FROM main.canonical_patient_master
WHERE is_malignant = TRUE AND ajcc8_n_stage = 'N1a' AND ln_total_positive > 0;
```

## §3 — Apply (template; concrete SQL after rule pick)

```sql
-- Pre-snapshot
CREATE OR REPLACE TABLE archive_pub_v1_0.cpm_pre_mig258_20260501 AS
SELECT research_id, ajcc8_n_stage, ln_total_positive, ln_total_examined, ln_positive_flag
FROM main.canonical_patient_master
WHERE is_malignant = TRUE AND ajcc8_n_stage IN ('N1a', 'N1b');

-- Rule C example: add ln_status_source column
ALTER TABLE main.canonical_patient_master
  ADD COLUMN IF NOT EXISTS ln_status_source VARCHAR;
UPDATE main.canonical_patient_master
SET ln_status_source = CASE
  WHEN ajcc8_n_stage IN ('N1a','N1b') AND ln_total_positive > 0 THEN 'both'
  WHEN ajcc8_n_stage IN ('N1a','N1b') THEN 'staging'
  WHEN ln_total_positive > 0 THEN 'count'
  ELSE NULL
END
WHERE is_malignant = TRUE;
```

## §4 — Re-verify on Snowflake (post-mig)

```sql
-- Rule A or B: count should be 0
SELECT COUNT(*) FROM CANONICAL_PATIENT_MASTER_FLAT
WHERE IS_MALIGNANT = TRUE AND AJCC8_N_STAGE IN ('N1a', 'N1b')
  AND (LN_TOTAL_POSITIVE IS NULL OR LN_TOTAL_POSITIVE = 0);

-- Rule C: ln_status_source distribution
SELECT $1:ln_status_source::VARCHAR AS src, COUNT(*) FROM CANONICAL_PATIENT_MASTER GROUP BY 1;
-- (need to re-export CPM from MD post-mig and re-CTAS the Snowflake table)
```

## §5 — Carry-forwards
- CF-mig258-NSTAGE-LNCOUNT-RECONCILE (closed by chosen rule)
- CF-mig258-MANUSCRIPT-FILTER-UPDATE (M037 + M044 view definitions update to use chosen rule)

## §6 — Surgical git add paths
```
scripts/output/mig_258_*.md
scripts/output/mig_258_pre_snapshot_log.txt
```
"""
(OUT_DIR / f"CURSOR_PROMPT_MIG_258_NSTAGE_LNCOUNT_RECONCILE_{TODAY}.md").write_text(mig_258)
print(f"  saved mig_258 ({len(mig_258):,} chars)")


# ============================================================================
# Routing summary
# ============================================================================
summary = f"""# Snowflake Round-2 Findings — Cursor Routing Summary

**Generated:** 2026-05-01 by Cowork.

5 Cursor prompts were authored from the round-2 Snowflake validation findings.
Each maps to a specific tool. Run them in this order; later migs depend on earlier ones being applied.

| # | Mig | Title | Tool | Severity |
|---|---|---|---|---|
| 1 | mig_254 | M1 → Stage II reconcile | **Cursor Chat (Claude Sonnet 4 / GPT-5) → Composer** | HIGH |
| 2 | mig_255 | recurrence flag/timing reconcile | **Cursor Composer** | MEDIUM-HIGH |
| 3 | mig_256 | 6 benign + recurrence reconcile | **Cursor Composer** (case-by-case) | LOW |
| 4 | mig_257 | followup > survival repair | **Cursor Composer** (mig_101 pattern) | MEDIUM |
| 5 | mig_258 | N-stage vs LN count reconcile | **Cursor Chat (Claude Sonnet 4 / GPT-5) → Composer** | HIGH |

**When to use Chat-first vs straight-Composer:**
- **Chat-first (mig_254, mig_258):** the rule itself needs human/clinical adjudication. Composer would just guess. Get the rule ratified by Logan in Chat, *then* paste the §0 message into Composer with the chosen rule embedded.
- **Composer (mig_255, mig_256, mig_257):** the rule is mechanical (pattern-match a prior mig); Composer can run the whole dispatch.

**Why Cursor over VSC GPT-5.5:**
For your workflow, Cursor Composer wins on every mig because it has direct access to your file structure, your `cursor_prompts/` history, MotherDuck MCP, and Desktop Commander. VSC + GPT-5.5 (e.g. via Copilot Chat) is only better when you want the strongest reasoning model and don't need codebase integration — but Cursor's Claude Sonnet 4 / GPT-5 in Chat covers that case while keeping the codebase context. **Recommendation: do all 5 in Cursor.**

**After all 5 land in MotherDuck:**
- Re-export from MD via `snowflake_trial/scripts/01_export_md_to_parquet.py`
- Reload Snowflake via `02_load_to_snowflake.py`
- Rerun `06_prompt3_survival.py` and `07_prompt5_staging.py` — counts should drop to the expected disposition target (0 for Options A/B; declared residual for Option C on mig_258).

## File index (all in `cursor_prompts/`)

1. `CURSOR_PROMPT_MIG_254_M1_STAGEII_RECONCILE_{TODAY}.md`
2. `CURSOR_PROMPT_MIG_255_RECUR_FLAG_TIMING_{TODAY}.md`
3. `CURSOR_PROMPT_MIG_256_BENIGN_RECUR_RECONCILE_{TODAY}.md`
4. `CURSOR_PROMPT_MIG_257_FU_POST_DEATH_{TODAY}.md`
5. `CURSOR_PROMPT_MIG_258_NSTAGE_LNCOUNT_RECONCILE_{TODAY}.md`
"""

(OUT_DIR / f"SNOWFLAKE_ROUND2_CURSOR_ROUTING_{TODAY}.md").write_text(summary)
print(f"\n  saved routing summary")

ctx.close()
print("\n=== done ===")
print(f"Five mig prompts + routing summary in cursor_prompts/")
