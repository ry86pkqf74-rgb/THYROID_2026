# Cursor Composer Dispatch — mig_254b: Single-row residual cleanup (rid 9600, MTC + M1, NULL stage_group)

**Generated:** 2026-05-01 by Cowork (Snowflake re-verification post-mig_254).
**Lane:** mig_254b — single-row residual surfaced by mig_254 D3 sanity check. `research_id = '9600'` is MTC, T1b, N0, **M1**, age 63, surgery 2022-06-20, with `ajcc8_stage_group = NULL`. Per the mig_254 ratified house convention (MTC + M1 → IVB at CPM level), this row should be `IVB`.
**Recommended agent:** **Cursor Composer** — single-row UPDATE, trivial.
**Estimated runtime:** 5 min
**Triggered by:** Snowflake Prompt 5 re-verification after mig_254.
**Severity:** LOW (N=1) but a valid residual; flag for completeness.
**Closes carry-forward:** none new (resolves the rid-9600 residual called out in mig_254 §D3).

---

## §0 — First message to paste into Cursor Composer

> mig_254b dispatch. Single MTC patient (rid 9600) has M1 + NULL `ajcc8_stage_group` — should be IVB per mig_254 house convention. Snapshot to `"Thyroid 2026 UPdated".archive_pub_v1_0`, UPDATE, verify D1=0 D2=0, sign off.

---

## §1 — Why this lane exists

mig_254 D3 verify surfaced one residual:

| research_id | hist | T | N | M | stage_group | age | surgery_date |
|---|---|---|---|---|---|---|---|
| 9600 | MTC | T1b | N0 | M1 | (NULL) | 63 | 2022-06-20 |

The CPM stage-group column got `NULL` for this patient — likely because the mig_266b overlay logic returned NULL when the upstream mig_184 source produced `'IVC'` (a label not in the CPM allowed set {I, II, III, IVB}) and the collapse step dropped it instead of mapping IVC → IVB. This is the same root cause as the 40 mig_254 patients, just hitting a NULL instead of II.

This row is a sub-case of CF-mig254-MIG266B-OVERLAY-RE-DERIVE (deferred to mig_259). The point fix here is identical to mig_254's apply: set stage_group = 'IVB'.

## §2 — Probe

```sql
-- Confirm the row state
SELECT research_id, histology_final, ajcc8_t_stage, ajcc8_n_stage,
       ajcc8_m_stage, ajcc8_stage_group, age_at_surgery, first_surgery_date
FROM main.canonical_patient_master WHERE research_id = '9600';
-- Expected: stage_group IS NULL, m_stage = 'M1', histology_final = 'MTC'
```

## §3 — Apply

```sql
-- A. Pre-snapshot
CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.cpm_pre_mig254b_20260501 AS
SELECT research_id, ajcc8_m_stage, ajcc8_stage_group, histology_final
FROM main.canonical_patient_master WHERE research_id = '9600';

-- B. UPDATE
UPDATE main.canonical_patient_master
SET ajcc8_stage_group = 'IVB'
WHERE research_id = '9600'
  AND ajcc8_m_stage = 'M1'
  AND ajcc8_stage_group IS NULL
  AND (histology_final ILIKE 'MTC%' OR histology_final ILIKE '%medullary%');

-- C. Verify rid 9600 (should now show IVB)
SELECT research_id, ajcc8_stage_group
FROM main.canonical_patient_master WHERE research_id = '9600';

-- D. Verify house convention sanity
SELECT
  CASE WHEN histology_final ILIKE 'MTC%' OR histology_final ILIKE '%medullary%' THEN 'MTC'
       ELSE 'OTHER' END AS hist,
  ajcc8_stage_group, COUNT(*) AS n
FROM main.canonical_patient_master
WHERE is_malignant = TRUE AND ajcc8_m_stage = 'M1'
GROUP BY 1, 2 ORDER BY 1, 2;
-- Expected: MTC IVB = 60 (was 59 + this 1)
```

## §4 — Registry signoff

```sql
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
VALUES ('mig_254b', CURRENT_TIMESTAMP::TIMESTAMP, 'logan_via_cursor',
  'Single-row residual fix: rid 9600 (MTC + M1) had NULL stage_group; flipped to IVB per house convention (mig_254 ratified rule). Closes mig_254 D3 residual.');
```

## §5 — Surgical git add

```
scripts/output/mig_254b_apply_log.txt
```

## §6 — Re-verify on Snowflake

```bash
cd /Users/ros/THyroid\ 2026
source .venv/bin/activate
SNOWFLAKE_PAT='...' python snowflake_trial/scripts/01_export_md_to_parquet.py
SNOWFLAKE_PAT='...' python snowflake_trial/scripts/02_load_to_snowflake.py
SNOWFLAKE_PAT='...' python -c "
import sys; sys.path.insert(0,'snowflake_trial/scripts'); from _sf_client import get_cursor
ctx, cur = get_cursor()
cur.execute(\"SELECT \$1:ajcc8_stage_group::VARCHAR FROM CANONICAL_PATIENT_MASTER WHERE \$1:research_id::VARCHAR = '9600'\")
print(cur.fetchone())  # should print ('IVB',)
"
```
