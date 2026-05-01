# Cursor Composer Dispatch — mig_253: Fill `surg_procedure_type` / `surg_total_thyroidectomy` / `surg_hemithyroidectomy` for the 2,138 cohort-wide NULL cases

**Generated:** 2026-05-01 by Cowork at HEAD `0ae2881` (post-mig_250)
**Lane:** mig_253 — populate `canonical_patient_master.surg_procedure_type`, `surg_total_thyroidectomy`, `surg_hemithyroidectomy` for the 2,138 patients (19.7% of CPM) where all three are NULL despite `first_surgery_date`, `n_surgeries`, and (often) `gland_weight_final_g` being populated.
**Recommended agent:** Cursor Composer (vocabulary-mapping work + cross-source evidence reconciliation)
**Estimated runtime:** 60–90 min
**Triggered by:** Cowork audit 2026-05-01 during M038 planning. Logan: "surg_procedure_type should absolutely be present for every operative event."
**Severity:** MEDIUM-HIGH. Affects every manuscript that stratifies by procedure type (essentially all surgical manuscripts).
**Closes carry-forward:** CF-SURG-PROC-TYPE-NULL (newly opened in this dispatch).

---

## §0 — First message to paste into Cursor Composer

> mig_253 dispatch. Read `cursor_prompts/CURSOR_PROMPT_MIG_253_SURG_PROCEDURE_TYPE_FILL_20260501.md` end-to-end before any tool use. You have MotherDuck MCP authed to `logan.glosser.eras@gmail.com`; database is `thyroid_canonical_publication_v1_0`. GitHub repo at `/Users/loganglosser/THYROID_2026`. Use Desktop Commander for git ops (FileVault).
>
> **Critical:** this is a `main.*` UPDATE. Per `feedback_dryrun_signoff_before_build.md`, dry-run all gates against scratch TEMP tables and surface the diff to Logan for sign-off BEFORE running any `UPDATE main.canonical_patient_master`.

---

## §1 — Why this lane exists

During M038 planning Cowork found that 121/475 (25.5%) of the ≥200g focal cohort had NULL `surg_procedure_type` AND NULL `surg_total_thyroidectomy` AND NULL `surg_hemithyroidectomy`. Generalizing to CPM-wide:

- **2,138 / 10,871 patients (19.7%)** have all three flags NULL simultaneously.
- 2,138/2,138 have `first_surgery_date` populated.
- 2,138/2,138 have `n_surgeries` populated.
- 1,595/2,138 have `gland_weight_final_g` (cannot have a gland weight without thyroidectomy).
- 916/2,138 have `histology_final` populated.
- 348/2,138 have `nsqip_thyroidectomy_has_data = TRUE` AND a populated `nsqip_cpt_code` + `nsqip_cpt_description`.

**These are NOT cases without surgery — they're cases where procedure-type vocabulary mapping failed during the rollup that built the three flags on `canonical_patient_master`.**

The raw data needed is available:

- `main.canonical_operative_events_v1` — "[domain=operative; grain=event] — surgical encounter events; one row per (research_id, surgery_id) with date, procedure, complications, and surgeon metadata"
- `main.canonical_operative_patient_rollup_v1` — "patient-level rollup of canonical_operative_events_v1; first/last surgery dates, total counts, primary procedure flag"
- `main.canonical_operative_procedure_codes_v1` — 21,691 rows of procedure codes
- `canonical_patient_master.nsqip_cpt_code` + `nsqip_cpt_description` — populated for 348 of the 2,138 NULL cases with values like:

| cpt_code | cpt_description | n |
|---:|---|---:|
| 60240 | Thyroidectomy, total or complete | 181 |
| 60252 | Thyroidectomy, total or subtotal for malignancy; with limited neck dissection | 71 |
| 60271 | Thyroidectomy, including substernal thyroid; cervical approach | 48 |
| 60260 | Thyroidectomy, removal of all remaining thyroid tissue following previous removal of a portion of thyroid | 40 |
| 60254 | Thyroidectomy, total or subtotal for malignancy; with radical neck dissection | 7 |
| 60270 | Thyroidectomy, including substernal thyroid; sternal split or transthoracic approach | 1 |

These are unambiguous total-thyroidectomy mappings. The vocabulary-mapping work is done; the rollup just isn't reading them.

---

## §2 — Pre-task probes

```sql
-- Tip-state confirmation
SELECT * FROM semantic_publication.vw_publication_qc_status_VIEW_v1;

-- Confirm the gap
SELECT
  COUNT(*) AS n_total,
  SUM(CASE WHEN surg_procedure_type IS NULL THEN 1 ELSE 0 END) AS null_proc_type,
  SUM(CASE WHEN surg_procedure_type IS NULL
            AND surg_total_thyroidectomy IS NULL
            AND surg_hemithyroidectomy IS NULL THEN 1 ELSE 0 END) AS null_all_three
FROM main.canonical_patient_master;
-- Expected: 10871, 2138, 2138

-- Inspect canonical_operative_events_v1 structure & coverage of the NULL pts
SELECT COUNT(*) AS n_op_events,
       COUNT(DISTINCT research_id) AS n_distinct_pts
FROM main.canonical_operative_events_v1;

DESCRIBE main.canonical_operative_events_v1;
DESCRIBE main.canonical_operative_patient_rollup_v1;

-- For NULL-procedure-type pts: how many have records in canonical_operative_events_v1?
WITH null_pts AS (
  SELECT research_id FROM main.canonical_patient_master
  WHERE surg_procedure_type IS NULL
    AND surg_total_thyroidectomy IS NULL
    AND surg_hemithyroidectomy IS NULL
)
SELECT
  COUNT(DISTINCT n.research_id) AS n_null_pts,
  COUNT(DISTINCT op.research_id) AS n_with_op_event,
  COUNT(*) AS n_op_event_rows_for_null_pts
FROM null_pts n
LEFT JOIN main.canonical_operative_events_v1 op USING (research_id);

-- Distinct procedure values in canonical_operative_events_v1 for NULL-type pts
WITH null_pts AS (
  SELECT research_id FROM main.canonical_patient_master
  WHERE surg_procedure_type IS NULL
    AND surg_total_thyroidectomy IS NULL
    AND surg_hemithyroidectomy IS NULL
)
SELECT op.procedure, COUNT(*) AS n
FROM main.canonical_operative_events_v1 op
JOIN null_pts USING (research_id)
GROUP BY 1
ORDER BY n DESC;
```

---

## §3 — Task spec

### Step 3.1 — Locate the existing rollup-build script

Find the script/migration that built `canonical_patient_master.surg_procedure_type`, `surg_total_thyroidectomy`, `surg_hemithyroidectomy`. Likely candidates:

- A `patient_master_*_cluster` migration in `qc_framework_v1/migrations/`
- A Python script under `scripts/`
- The build that originally populated `canonical_operative_patient_rollup_v1` and pushed it forward to CPM

Identify the gap: does the rollup script handle the case where `canonical_operative_events_v1` has data but the procedure-type derivation returns NULL?

### Step 3.2 — Define the three-flag derivation

Standard mapping (consensus across `nsqip_cpt_description` and `canonical_operative_events_v1.procedure`):

| Source value | surg_total_thyroidectomy | surg_hemithyroidectomy | surg_procedure_type |
|---|:-:|:-:|---|
| CPT 60240 / "total thyroidectomy" / "total or complete" | TRUE | FALSE | `'total_thyroidectomy'` |
| CPT 60252 / 60254 / "total ... with neck dissection" | TRUE | FALSE | `'total_thyroidectomy'` |
| CPT 60260 / "removal of remaining tissue" / "completion" | TRUE | FALSE | `'total_thyroidectomy'` (completion → total in aggregate) |
| CPT 60271 / 60270 / "including substernal" | TRUE | FALSE | `'total_thyroidectomy'` |
| CPT 60220 / 60225 / "thyroid lobectomy" / "hemithyroidectomy" | FALSE | TRUE | `'hemithyroidectomy'` |
| CPT 60210 / 60212 / "partial thyroid lobectomy" | FALSE | TRUE | `'hemithyroidectomy'` (partial → hemi for analysis grain) |
| Isthmusectomy | FALSE | FALSE | `'isthmusectomy'` |
| Multiple distinct surgeries | depends | depends | use the most-extensive procedure as primary |

Multiple-event-per-patient logic:

- A patient with one hemi + one completion thyroidectomy → `surg_total_thyroidectomy = TRUE`, `surg_hemithyroidectomy = FALSE` (overall removed = total).
- A patient with one hemi only → `surg_hemithyroidectomy = TRUE`, `surg_total_thyroidectomy = FALSE`.
- A patient with two hemis → `surg_total_thyroidectomy = TRUE` (effectively total).

### Step 3.3 — Source-of-truth precedence

When both `canonical_operative_events_v1.procedure` and `nsqip_cpt_code` are present, define a precedence:

1. `canonical_operative_events_v1.procedure` (richer source; covers the most patients)
2. `nsqip_cpt_code` (structured CPT)
3. Free-text fallback from operative-report NLP (`note_entities_operative_detail`) if neither above resolves

Document the precedence used in the migration header.

### Step 3.4 — Dry-run

Build a TEMP table with proposed values for each of the 2,138 NULL pts. Surface to Logan:

```sql
-- expected-resolution counts (illustrative; refine to actual)
-- of 2,138 NULL pts:
--   resolved via canonical_operative_events_v1.procedure: ~XX
--   resolved via nsqip_cpt_code (where above is NULL):    ~XX
--   resolved via op-report NLP (last resort):             ~XX
--   STILL NULL after all three sources:                   ~XX (this should be small;
--                                                         flag for chart review)
```

Surface the breakdown of newly-populated values:

```sql
SELECT proposed_surg_procedure_type, COUNT(*) AS n_pts_resolved
FROM mig_253_temp_resolution
GROUP BY 1
ORDER BY n_pts_resolved DESC;
```

### Step 3.5 — Apply

`UPDATE main.canonical_patient_master SET surg_procedure_type = ..., surg_total_thyroidectomy = ..., surg_hemithyroidectomy = ...` for the 2,138 affected rows. Do NOT touch the other 8,733 rows (they already have correct values).

Update `signoff_registry` per `feedback_dryrun_signoff_before_build.md` with mig_253 provenance.

### Step 3.6 — Downstream cohort views

The cohort views in `manuscript_workspace.cohort_*` (including `cohort_m038_massive_goiter_v1` post-mig_251) will automatically pick up the new values via column passthrough.

Verify by re-running the M038 ≥200g distribution:

```sql
SELECT
  surg_procedure_type, surg_total_thyroidectomy, surg_hemithyroidectomy, COUNT(*) AS n
FROM manuscript_workspace.cohort_m038_massive_goiter_v1
WHERE gland_weight_final_g >= 200
GROUP BY 1,2,3
ORDER BY n DESC;
-- Expected post-mig_253: 121 NULLs in the ≥200g subset → 0 (or near-0)
```

### Step 3.7 — Update gates and dashboard

- Re-run `vw_publication_qc_status_VIEW_v1` and confirm gate1=218 unchanged.
- No `manuscript_feasibility_v1` re-score needed unless a feasibility row depends specifically on `surg_procedure_type` coverage (M033 had this in its gating_issues — re-check after mig_253 lands).

---

## §4 — Acceptance criteria

1. `canonical_patient_master.surg_procedure_type IS NULL AND surg_total_thyroidectomy IS NULL AND surg_hemithyroidectomy IS NULL` count drops from 2,138 → ≤50 (residual cases that genuinely lack source surgery data; flagged for chart review).
2. The three flags are consistent (no rows with `surg_procedure_type='total_thyroidectomy'` AND `surg_total_thyroidectomy = FALSE`).
3. M038 ≥200g cohort `surg_procedure_type IS NULL` count drops from 121 → ≤5.
4. `vw_publication_qc_status_VIEW_v1` gate1=218 unchanged, gates 2–5 = 0, cohort_parity TRUE.
5. Migration file `qc_framework_v1/migrations/253_surg_procedure_type_fill_20260501.sql` exists and is committed.
6. Cowork notified to verify M038 cohort flag distribution post-fix.

---

## §5 — Carry-forwards opened by this dispatch

| ID | Description |
|---|---|
| **CF-SURG-RESIDUAL-CHART-REVIEW** | Post-mig_253 residual NULL-procedure-type pts (~50 estimated) need chart-review remediation. List them in a follow-up CSV under `manuscript_outputs/audit/`. |
| **CF-SURG-CPT-VOCAB-REGISTRY** | Canonicalize the CPT → procedure-type mapping into a registry table so future raw-data refreshes don't re-introduce this gap. |

---

**End of mig_253 dispatch.**
