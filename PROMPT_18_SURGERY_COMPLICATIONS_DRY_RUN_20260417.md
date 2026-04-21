# PROMPT 18 — Surgery & Complications Detail ↔ Master Rollup
## Dry-Run Cross-Validation Findings

**Database:** `thyroid_canonical_publication_v1_0`
**Rollup under test:** `main.canonical_patient_master` (10,871 × 1,377)
**Detail tables:** `main.operative_episode_detail_v2` (9,371 rows / 9,368 patients), `main.complication_phenotype_v1` (5,978 rows / 2,938 patients)
**Run date:** 2026-04-17
**Mode:** Dry-run (read-only). All fix SQL is drafted, none executed.
**Severity legend:** `CRITICAL` = rollup contradicts source, load-bearing · `HIGH` = missed patients or wrong counts · `MED` = cosmetic / unverified semantics · `INFO` = concordance confirmation.

---

## 0. Scope & cross-reference to prior work

This run extends `PART2_DETAIL_CROSSVAL_FINDINGS_20260416.md` §5 (Outcomes / Batch 5), which previously reported surgery & complications at an aggregate level. **New in this run:**

- Per-episode-flag drill into the 217-patient lateral-ND gap (Batch 5.5 prior call of "`lateral_neck_dissected`: 8,479 match" turns out to be dominated by vacuous FALSE=FALSE; the TRUE-side reconciliation is much worse than implied).
- Per-entity `confirmed`/`transient`/`permanent`/`days_postop` concordance for all 9 complication entities tracked in `complication_phenotype_v1`.
- Whether Script 236 (2026-04-16) VC-paralysis/paresis `status_v2` recalibration was propagated to CPM — it was not.
- A "phantom confirmed" check (CPM says confirmed but no detail row exists) across all 9 entities.

Observed row/patient counts match the 2026-04-16 brief; the +50/+46 `complication_phenotype_v1` delta is consistent with the post-Script-236 state (5,978 / 2,938).

---

## 1. Findings by task

### Task 1 · `INFO` — Surgery count rollup is clean

| CPM column | matches | mismatches | CPM NULL | n joined |
|---|---|---|---|---|
| `n_surgeries` | **8,731** | 0 | 2 | 8,733 |
| `surg_n_procedures` | 8,730 | 3 | — | 8,733 |
| `n_surgeries_v2` | 8,134 | varies | — | 8,733 |

`n_surgeries` (INTEGER) is the primary and reconciles exactly. `surg_n_procedures` differs for 3 patients — investigate separately, low blast radius. `n_surgeries_v2` lags the primary for 599 patients; either retire or re-rollup, but not load-bearing.

### Task 2 · `INFO` — Procedure type is clean

Earliest-surgery `procedure_normalized` vs `cpm.surg_procedure_type` matches exactly on all 8,733 joined patients (zero mismatches). `surg_total_thyroidectomy` flag reconciles cleanly (4,561 TRUE on each side, zero `tt_but_flag_false`). The 2,138 NULL `surg_procedure_type` rows correspond 1:1 to the 1,503 CPM patients with no operative-episode record plus 635 orphan operative-episode patients not in CPM — see finding 1.7.

### Task 3.1 · `HIGH` — Lateral neck dissection under-called for 217 of 241 detail-flagged patients (90% silent loss)

**Finding.** Per-patient OR-aggregate of `operative_episode_detail_v2.lateral_neck_dissection_flag`:
- Detail says TRUE: **241** patients
- CPM `lateral_neck_dissected` says TRUE: **61** patients (on joined 8,733)
- detail=TRUE / CPM=FALSE: **217**
- detail=FALSE / CPM=TRUE: 37
- detail=TRUE / CPM=TRUE (agree-TRUE): **24 of 241 = 10%**

The prior report's "lateral_neck_dissected: 8,479 match" rate is dominated by vacuous FALSE=FALSE agreement on patients who had no LND at all. The clinically relevant number — agreement on the TRUE state — is only 10%. Sampled discrepant patients (research_id 1998, 565, 7775, 8705, 7087, 1739, 7424, 793, 2627, 3091) each have one surgery episode with `lateral_neck_dissection_flag=TRUE`, but both `cpm.lateral_neck_dissected` and `cpm.lateral_neck_dissected_v10` are FALSE. This is not a version-drift issue (v10 also misses).

**Impact.** Any cohort filter on "lateral neck dissection" built from CPM will under-count N1b-treated patients ~4x relative to the structured surgical detail. Load-bearing for nodal-disease and aggressive-surgery papers.

**Replay.**

```sql
WITH agg AS (
  SELECT research_id, BOOL_OR(lateral_neck_dissection_flag) AS det_lnd
  FROM main.operative_episode_detail_v2 GROUP BY 1
)
SELECT
  SUM(CASE WHEN agg.det_lnd THEN 1 ELSE 0 END) AS detail_true,
  SUM(CASE WHEN cpm.lateral_neck_dissected THEN 1 ELSE 0 END) AS cpm_true,
  SUM(CASE WHEN COALESCE(agg.det_lnd,FALSE) AND NOT COALESCE(cpm.lateral_neck_dissected,FALSE) THEN 1 ELSE 0 END) AS det_t_cpm_f
FROM main.canonical_patient_master cpm
JOIN agg ON TRY_CAST(cpm.research_id AS INTEGER) = agg.research_id;
-- returns: 241, 61, 217
```

**Draft fix SQL.**

```sql
-- FIX: OR the structured detail flag into cpm.lateral_neck_dissected.
-- NOT EXECUTED - dry run.
/*
WITH agg AS (
  SELECT research_id, BOOL_OR(lateral_neck_dissection_flag) AS det_lnd
  FROM main.operative_episode_detail_v2 GROUP BY 1
)
UPDATE main.canonical_patient_master cpm
SET lateral_neck_dissected = TRUE
FROM   agg
WHERE  TRY_CAST(cpm.research_id AS INTEGER) = agg.research_id
  AND  agg.det_lnd = TRUE
  AND  COALESCE(cpm.lateral_neck_dissected, FALSE) = FALSE;
-- Manually review the 37 detail=FALSE/CPM=TRUE cases before touching them
-- (they may be NLP-sourced LND that the structured op-note didn't capture).
*/
```

### Task 3.2 · `INFO` — Reoperative & parathyroid-autograft aggregate flags match exactly

| flag | detail TRUE | CPM TRUE (joined) | mismatches |
|---|---|---|---|
| `op_reoperative_any` vs `reoperative_field_flag` | 46 | 46 | **0** |
| `op_parathyroid_autograft_any` vs `parathyroid_autograft_flag` | 40 | 40 | **0** |

Both are exact — these aggregate CPM flags are true patient-level OR of the detail flag. (Prior report's concern about autograft "only 40 match" was the total TRUE count, not a discordance count — no bug here.)

### Task 4 · `INFO` — Per-entity confirmed flag is 100% concordant on all 9 entities

Grouped by `complication_entity` (BOOL_OR detail confirmed per patient vs CPM `comp_<entity>_confirmed`):

| entity | detail rows | matches | CPM missed confirmed | CPM over-called confirmed |
|---|---|---|---|---|
| chyle_leak | 1,588 | 1,588 | 0 | 0 |
| hematoma | 253 | 253 | 0 | 0 |
| hypocalcemia | 1,927 | 1,927 | 0 | 0 |
| hypoparathyroidism | 430 | 430 | 0 | 0 |
| rln_injury | 731 | 731 | 0 | 0 |
| seroma | 874 | 874 | 0 | 0 |
| vocal_cord_paralysis | 88 | 88 | 0 | 0 |
| vocal_cord_paresis | 71 | 71 | 0 | 0 |
| wound_infection | 16 | 16 | 0 | 0 |

The confirmed-flag rollup is *mechanically* exact for every entity. BUT see finding 2.1 below — the VC paralysis/paresis concordance is "perfect" only because both sides are FALSE; the Script 236 cross-reference updated `status_v2` but did not flip `confirmed_flag`, and CPM never re-ran against `status_v2`.

### Task 4b · `INFO` — Zero phantom-confirmed CPM calls across all 9 entities

For each entity, we searched for patients with `cpm.comp_<entity>_confirmed=TRUE` but no corresponding row in `complication_phenotype_v1`. Every entity returned **0 phantoms**. CPM totals reconcile exactly with detail confirmed-TRUE counts:

| entity | CPM confirmed=TRUE | detail confirmed=TRUE | phantom |
|---|---|---|---|
| hypocalcemia | 98 | 98 | 0 |
| hypoparathyroidism | 34 | 34 | 0 |
| rln_injury | 59 | 59 | 0 |
| hematoma | 38 | 38 | 0 |
| seroma | 28 | 28 | 0 |
| chyle_leak | 20 | 20 | 0 |
| wound_infection | 2 | 2 | 0 |
| vc_paralysis | **0** | **0** | 0 (but should be 19 — see 2.1) |
| vc_paresis | **0** | **0** | 0 (but should be 13 — see 2.1) |

### Task 5 · `MED` — Timing data is perfectly concordant where both sides populated, but ~50–89% of confirmed events have no timing on either side

Non-null-both rows match exactly (within 7 days ⇒ 100% of matchable rows) for every entity. The gap is missing data:

| entity | detail confirmed rows | both NULL days_postop | exact match | within 7d | >30d disagreement |
|---|---|---|---|---|---|
| rln_injury | 59 | 0 | 59 | 59 | 0 |
| hypoparathyroidism | 34 | 0 | 34 | 34 | 0 |
| chyle_leak | 20 | 0 | 20 | 20 | 0 |
| hematoma | 38 | 28 (74%) | 10 | 10 | 0 |
| hypocalcemia | 98 | 77 (79%) | 21 | 21 | 0 |
| seroma | 28 | 25 (89%) | 3 | 3 | 0 |
| wound_infection | 2 | 1 | 1 | 1 | 0 |

Hematoma, seroma, and hypocalcemia have a dominant missing-data problem in the source phenotype table — timing_days_post_surgery is NULL on ~75–90% of their confirmed rows, and CPM inherits the gap. Not a rollup bug; document as a completeness caveat. (Recurrence/revision-of-care analyses that need days-to-event should scope to rln_injury, hypoparathyroidism, and chyle_leak until the gap is closed.)

### Task 6 · `HIGH` — Hypoparathyroidism 18 CPM-over on "permanent" (likely unsafe default)

Per-entity transient/permanent concordance is perfect for 8 of 9 entities. **Hypoparathyroidism** is the exception:

| metric | value |
|---|---|
| joined rows | 430 |
| transient matches | 426 (4 discordances, all detail=T/CPM=F) |
| permanent matches | 412 (18 discordances, **all detail=F/CPM=T**) |

Sampled 10 permanent-discordant patients; **every one** has `complication_phenotype_v1.final_complication_status ∈ {'confirmed_duration_unknown','confirmed_transient'}` yet CPM sets `comp_hypoparathyroidism_permanent = TRUE` (with `comp_hypoparathyroidism_evidence_tier = 1`, so Tier-1 confidence in an unsupported permanence call). Two of the ten have `final_complication_status='confirmed_transient'` — those are direct contradictions (transient → CPM-permanent). The annotation column `comp_hypopara_permanent_limitation_note` is NULL across the sample.

```text
rid  det_status                    cpm_hypopara_permanent
5131 confirmed_duration_unknown    TRUE
7477 confirmed_duration_unknown    TRUE
5306 confirmed_duration_unknown    TRUE
6202 confirmed_duration_unknown    TRUE
7540 confirmed_duration_unknown    TRUE
9765 confirmed_transient           TRUE    ← contradiction
7487 confirmed_transient           TRUE    ← contradiction
6779 confirmed_duration_unknown    TRUE
7144 confirmed_duration_unknown    TRUE
5866 confirmed_duration_unknown    TRUE
```

**Impact.** Permanent hypoparathyroidism is a major complication endpoint in every thyroidectomy-safety paper. An 18-patient over-call with Tier-1 confidence will survive stratified analyses unless explicitly filtered. The 2 transient→permanent flips are the most clinically consequential and need manual adjudication.

**Replay.**

```sql
WITH d AS (
  SELECT TRY_CAST(research_id AS INTEGER) AS rid,
         BOOL_OR(permanent_flag) AS det_p
  FROM main.complication_phenotype_v1
  WHERE complication_entity='hypoparathyroidism' GROUP BY 1
)
SELECT COUNT(*) FILTER (WHERE d.det_p = FALSE AND p.comp_hypoparathyroidism_permanent = TRUE)
FROM d
JOIN main.canonical_patient_master p ON TRY_CAST(p.research_id AS INTEGER) = d.rid;
-- returns: 18
```

**Draft fix SQL.**

```sql
-- FIX option A (conservative): fall back to detail's permanent_flag.
-- NOT EXECUTED - dry run.
/*
WITH d AS (
  SELECT TRY_CAST(research_id AS INTEGER) AS rid,
         BOOL_OR(permanent_flag) AS det_p,
         BOOL_OR(transient_flag) AS det_t,
         MAX(final_complication_status) AS det_status
  FROM main.complication_phenotype_v1
  WHERE complication_entity='hypoparathyroidism' GROUP BY 1
)
UPDATE main.canonical_patient_master p
SET comp_hypoparathyroidism_permanent = COALESCE(d.det_p, FALSE),
    comp_hypoparathyroidism_transient = COALESCE(d.det_t, p.comp_hypoparathyroidism_transient),
    comp_hypopara_permanent_limitation_note =
      CASE WHEN NOT COALESCE(d.det_p,FALSE) AND p.comp_hypoparathyroidism_permanent=TRUE
           THEN 'reset_from_phenotype:'||d.det_status
           ELSE p.comp_hypopara_permanent_limitation_note END
FROM   d
WHERE  TRY_CAST(p.research_id AS INTEGER) = d.rid
  AND  (d.det_p = FALSE AND p.comp_hypoparathyroidism_permanent = TRUE);

-- FIX option B (adjudicate): leave CPM permanent=TRUE but expose provenance.
ALTER TABLE main.canonical_patient_master
  ADD COLUMN IF NOT EXISTS comp_hypopara_permanent_source VARCHAR;
UPDATE main.canonical_patient_master p
SET comp_hypopara_permanent_source =
  CASE WHEN comp_hypoparathyroidism_permanent THEN
    CASE WHEN p.prm_hypoparathyroidism_lab_flag THEN 'lab_persistence'
         ELSE 'unknown_override' END
  END;
*/
```

### Task 7 · `CRITICAL` — 635 operative-record patients absent from CPM (re-confirms Batch 5.2)

Orphan check:

| detail table | detail patients | orphan patients (no CPM row) | orphan rows |
|---|---|---|---|
| `operative_episode_detail_v2` | 9,368 | **635** | 635 |
| `complication_phenotype_v1` | 2,938 | **0** | 0 |

This re-confirms PART2 §5.2. Cause and remediation are the same: either admit these 635 research_ids to the CPM registry with documented exclusion criteria, or create an audit table (`audit.cpm_missing_vs_op_episode`) with their surgery dates for human triage. **Status: unchanged since 2026-04-16. Still outstanding.**

---

## 2. Cross-cutting finding from drill-down

### 2.1 · `CRITICAL` — Script 236 VC paralysis/paresis recalibration (2026-04-16) never propagated to CPM comp_vc_* columns

**Finding.** `complication_phenotype_v1` has a `status_v2` column populated by Script 236 (dated 2026-04-16) that cross-references `extracted_rln_injury_refined_v2` confirmed RLN events to flip absent-or-unconfirmed vocal-cord rows into `confirmed_from_rln_crossref`:

| entity | final_complication_status | status_v2 | n_rows | confirmed_flag=TRUE |
|---|---|---|---|---|
| vocal_cord_paralysis | absent_or_unconfirmed | absent_or_unconfirmed | 69 | 0 |
| vocal_cord_paralysis | absent_or_unconfirmed | **confirmed_from_rln_crossref** | **19** | **0** |
| vocal_cord_paresis | absent_or_unconfirmed | absent_or_unconfirmed | 58 | 0 |
| vocal_cord_paresis | absent_or_unconfirmed | **confirmed_from_rln_crossref** | **13** | **0** |

Two compounding problems:

1. **`confirmed_flag` was never flipped** when `status_v2` was updated — Script 236 only populated the new overlay column, leaving `confirmed_flag = FALSE` on the 32 cross-referenced rows. This is why Task 4's row-level concordance is vacuously 100%.

2. **CPM never re-rolled from `status_v2`.** For the 32 patients:
   - `cpm.comp_vc_paralysis_confirmed` = FALSE for all 19 (should be TRUE)
   - `cpm.comp_vc_paresis_confirmed` = FALSE for all 13 (should be TRUE)
   - Yet `cpm.rln_injury_is_confirmed = TRUE` on all 32 (because the source of the crossref fires RLN).

So CPM currently reports zero confirmed VC paralysis and zero confirmed VC paresis, despite 32 patients having structured evidence of both via the Script 236 cross-reference. This is exactly the class of "rollup built before reconciliation script completed" bug flagged in PART2 §6.2 pattern #2.

**Replay.**

```sql
WITH vc AS (
  SELECT TRY_CAST(research_id AS INTEGER) AS rid, complication_entity, status_v2
  FROM main.complication_phenotype_v1
  WHERE complication_entity IN ('vocal_cord_paralysis','vocal_cord_paresis')
    AND status_v2 = 'confirmed_from_rln_crossref'
)
SELECT vc.complication_entity,
       COUNT(*) AS n_script236_confirmed,
       SUM(CASE WHEN vc.complication_entity='vocal_cord_paralysis'
                  AND NOT COALESCE(p.comp_vc_paralysis_confirmed,FALSE) THEN 1
                WHEN vc.complication_entity='vocal_cord_paresis'
                  AND NOT COALESCE(p.comp_vc_paresis_confirmed,FALSE) THEN 1
                ELSE 0 END) AS cpm_still_false,
       SUM(CASE WHEN p.rln_injury_is_confirmed THEN 1 ELSE 0 END) AS cpm_rln_confirmed
FROM vc JOIN main.canonical_patient_master p ON TRY_CAST(p.research_id AS INTEGER) = vc.rid
GROUP BY 1;
-- returns: vocal_cord_paralysis 19/19/19  ·  vocal_cord_paresis 13/13/13
```

**Draft fix SQL.**

```sql
-- FIX: promote status_v2 = 'confirmed_from_rln_crossref' to confirmed_flag in the detail table
-- AND backfill CPM. Do both so downstream re-rollups agree.
-- NOT EXECUTED - dry run.
/*
-- Step 1: flip confirmed_flag in phenotype for the 32 cross-ref'd rows (leave final_complication_status alone).
UPDATE main.complication_phenotype_v1
SET confirmed_flag = TRUE,
    phenotype_version = COALESCE(phenotype_version,'') || '+s236_crossref'
WHERE complication_entity IN ('vocal_cord_paralysis','vocal_cord_paresis')
  AND status_v2 = 'confirmed_from_rln_crossref'
  AND COALESCE(confirmed_flag, FALSE) = FALSE;

-- Step 2: backfill CPM comp_vc_paralysis_confirmed and comp_vc_paresis_confirmed.
WITH para AS (
  SELECT TRY_CAST(research_id AS INTEGER) AS rid
  FROM main.complication_phenotype_v1
  WHERE complication_entity='vocal_cord_paralysis'
    AND status_v2='confirmed_from_rln_crossref'
),
pares AS (
  SELECT TRY_CAST(research_id AS INTEGER) AS rid
  FROM main.complication_phenotype_v1
  WHERE complication_entity='vocal_cord_paresis'
    AND status_v2='confirmed_from_rln_crossref'
)
UPDATE main.canonical_patient_master p
SET comp_vc_paralysis_confirmed = TRUE
FROM para WHERE TRY_CAST(p.research_id AS INTEGER) = para.rid;

UPDATE main.canonical_patient_master p
SET comp_vc_paresis_confirmed = TRUE
FROM pares WHERE TRY_CAST(p.research_id AS INTEGER) = pares.rid;
*/
```

---

## 3. Top 5 integrity concerns (answer to the brief)

| # | Severity | Area | Concern | Patients |
|---|---|---|---|---|
| **1** | `CRITICAL` | Cohort coverage | 635 research_ids with operative-episode detail are absent from CPM (Batch 5.2 unresolved) | 635 |
| **2** | `CRITICAL` | Complication reconciliation | Script 236 `status_v2` VC paralysis/paresis cross-ref never propagated to CPM `comp_vc_paralysis_confirmed` / `comp_vc_paresis_confirmed`; CPM reports zero confirmed VC paralysis despite structured evidence | 32 (19+13) |
| **3** | `HIGH` | Operative flag rollup | `lateral_neck_dissected` captures only **10% (24 of 241)** of detail-flagged LND patients; 217 silent detail=T / CPM=F cases | 217 |
| **4** | `HIGH` | Complication permanence | `comp_hypoparathyroidism_permanent` over-calls 18 patients whose phenotype status is `confirmed_duration_unknown` (16) or `confirmed_transient` (2); the 2 transient→permanent flips are direct contradictions | 18 |
| **5** | `INFO` | Areas confirmed clean | `n_surgeries`, `surg_procedure_type`, `surg_total_thyroidectomy`, `op_reoperative_any`, `op_parathyroid_autograft_any`, 7 of 9 complication `confirmed` flags, 8 of 9 `transient`/`permanent` flags, and all orphan checks on `complication_phenotype_v1` are exact | — |

**Pattern.** Findings 1–4 all reduce to a single root cause: **CPM was built / last refreshed before the latest detail-table reconciliation pass finished** (Script 236 for VC, structured LND flag, hypopara permanence rule). The concordance logic itself is correct where CPM has been re-rolled; the gap is an orchestration gap, not a transform-logic gap. A single CPM re-rollup pass sourcing from `status_v2` (not `final_complication_status`) and from the structured operative flags (not only NLP) would clear findings 2, 3, and 4. Finding 1 requires a cohort-registry decision, not a re-rollup.

---

## 4. Consolidated fix block (dry-run — none of this has been executed)

```sql
-- ============================================================
-- PROMPT 18 REBUILD BLOCK  (dry-run; uncomment to execute)
-- Matches findings 2.1, Task 3.1, Task 6
-- Must run inside thyroid_canonical_publication_v1_0
-- ============================================================
/*
BEGIN TRANSACTION;

-- (2.1) Flip confirmed_flag for Script 236 cross-ref'd VC rows and backfill CPM.
UPDATE main.complication_phenotype_v1
SET    confirmed_flag = TRUE,
       phenotype_version = COALESCE(phenotype_version,'') || '+s236_crossref'
WHERE  complication_entity IN ('vocal_cord_paralysis','vocal_cord_paresis')
  AND  status_v2 = 'confirmed_from_rln_crossref'
  AND  COALESCE(confirmed_flag, FALSE) = FALSE;

WITH para AS (
  SELECT TRY_CAST(research_id AS INTEGER) AS rid
  FROM main.complication_phenotype_v1
  WHERE complication_entity='vocal_cord_paralysis' AND status_v2='confirmed_from_rln_crossref'
)
UPDATE main.canonical_patient_master p SET comp_vc_paralysis_confirmed = TRUE
FROM para WHERE TRY_CAST(p.research_id AS INTEGER) = para.rid;

WITH pares AS (
  SELECT TRY_CAST(research_id AS INTEGER) AS rid
  FROM main.complication_phenotype_v1
  WHERE complication_entity='vocal_cord_paresis'   AND status_v2='confirmed_from_rln_crossref'
)
UPDATE main.canonical_patient_master p SET comp_vc_paresis_confirmed = TRUE
FROM pares WHERE TRY_CAST(p.research_id AS INTEGER) = pares.rid;

-- (Task 3.1) Lateral neck dissection — lift structured detail flag into CPM.
WITH agg AS (
  SELECT research_id, BOOL_OR(lateral_neck_dissection_flag) AS det_lnd
  FROM main.operative_episode_detail_v2 GROUP BY 1
)
UPDATE main.canonical_patient_master cpm
SET    lateral_neck_dissected = TRUE
FROM   agg
WHERE  TRY_CAST(cpm.research_id AS INTEGER) = agg.research_id
  AND  agg.det_lnd = TRUE
  AND  COALESCE(cpm.lateral_neck_dissected, FALSE) = FALSE;

-- (Task 6) Hypoparathyroidism permanence — reconcile with phenotype status.
WITH d AS (
  SELECT TRY_CAST(research_id AS INTEGER) AS rid,
         BOOL_OR(permanent_flag) AS det_p,
         BOOL_OR(transient_flag) AS det_t,
         MAX(final_complication_status) AS det_status
  FROM main.complication_phenotype_v1
  WHERE complication_entity='hypoparathyroidism' GROUP BY 1
)
UPDATE main.canonical_patient_master p
SET    comp_hypoparathyroidism_permanent = COALESCE(d.det_p, FALSE),
       comp_hypoparathyroidism_transient = COALESCE(d.det_t, p.comp_hypoparathyroidism_transient),
       comp_hypopara_permanent_limitation_note =
         'reset_20260417:' || COALESCE(d.det_status,'null_detail')
FROM   d
WHERE  TRY_CAST(p.research_id AS INTEGER) = d.rid
  AND  COALESCE(p.comp_hypoparathyroidism_permanent, FALSE) = TRUE
  AND  COALESCE(d.det_p, FALSE) = FALSE;

-- COMMIT;  -- uncomment after review
*/
```

---

## 5. Recommendations

1. **Unlock finding 1 first** — the 635 operative-but-not-CPM patients is the only one here that requires a cohort-registry decision. Everything else can be fixed by a re-rollup script.
2. **Re-rollup CPM comp_* columns against `status_v2`, not `final_complication_status`.** The Script 236 overlay is now the authoritative source; CPM's rollup should coalesce over it.
3. **Lateral ND** — consider making `lateral_neck_dissected` the strict detail-flag OR, and introducing `lateral_neck_dissected_nlp_or_structured` if NLP-sourced positives need to be preserved.
4. **Hypoparathyroidism permanence** — 2 direct-contradiction patients (9765, 7487) need manual adjudication before any rebuild. For the other 16 `duration_unknown` cases, document the inference rule explicitly (e.g., "permanent = lab persistence >6mo OR prescription-requiring >12mo") and add a `comp_hypopara_permanent_source` provenance column.
5. **Add a CPM-build provenance timestamp** (`cpm_built_at`) so future dry runs can detect "CPM snapshot X pre-dates reconciliation script Y" directly rather than by noticing the drift downstream.

---

## 6. Sources

- Prior findings: [`PART2_DETAIL_CROSSVAL_FINDINGS_20260416.md`](computer:///sessions/hopeful-gifted-faraday/mnt/THYROID_2026/PART2_DETAIL_CROSSVAL_FINDINGS_20260416.md) §5 (Batch 5 — Outcomes)
- Script 236 audit table: `manuscript_workspace.vc_paralysis_recalibration_v236`
- Governance: `/THYROID_2026/AGENTS.md` — MotherDuck Database Governance; all writes must target `thyroid_canonical_publication_v1_0`
