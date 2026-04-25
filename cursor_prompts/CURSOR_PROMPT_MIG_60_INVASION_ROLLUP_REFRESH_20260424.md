# Cursor Prompt — Migration 60 — `canonical_invasion_patient_rollup_v1` refresh on v2 LLM feeders

**Date:** 2026-04-24
**DB:** `thyroid_canonical_publication_v1_0`
**Upstream state:** GitHub `main` at `ef16a64` (post-mig-54→58 land). Script 363 built the current rollup on 2026-04-21 23:30:02 UTC, pre-dating the v2 LLM runs loaded in migrations 54–58.

**Scope:** **rollup only.** The events table `main.canonical_invasion_events_v1` (51,773 rows / 10,871 pts, Script 363 v3-iter-2) is **not** touched in this migration — rebuilding it would require swapping the Script 363 CTEs that read the now-deprecated `main._deprecated_note_entities_llm_{vascular,airway}_invasion` for fresh v2 CTEs, which is a bigger refactor. That's Script 364+ territory. Here we re-source the patient-level rollup flags from the new tier-2 canonicals and COALESCE over the existing events-based aggregate.

**Deliverables:**
1. `main.canonical_invasion_patient_rollup_v1` rebuilt with v2 feeders wired in (CREATE OR REPLACE; same grain: one row per `research_id`).
2. **New columns** on that rollup for signals the Script 363 schema didn't carry:
   `any_rln_invasion_anywhere`, `any_rln_invasion_in_op_or_path`, `any_rln_invasion_in_imaging`,
   `any_pT4a_final_anywhere`, `any_pT4b_final_anywhere`,
   `any_carotid_encasement_anywhere`, `any_mediastinal_vessel_anywhere`, `any_prevertebral_fascia_anywhere`.
3. `manuscript_workspace.detail_table_registry_v1` row for `canonical_invasion_patient_rollup_v1` **updated** (NOT duplicated — it's a UPSERT via DELETE+INSERT on `detail_table_name`).
4. One-patient parathyroid contract patch (mig 58 CF-a) folded in as a side-car statement in the same migration (tagged separately on the events row).
5. Close-out at `project_mig_60_invasion_rollup_refresh_closeout.md` following the mig 57 template.

**`build_script` / provenance tag:** `mig_60_invasion_rollup_v2_refresh_20260424`

---

## Why

Script 363 built the rollup from `canonical_invasion_events_v1` CTEs that pulled the **non-v2** LLM tables (`note_entities_llm_vascular_invasion`, `note_entities_llm_airway_invasion`) plus the path-synoptic structured feeders. Migrations 54–58 produced five new tier-2 patient rollups that supersede the LLM portion for their domains:

| Feeder rollup | Pts | Replaces in events.LLM layer |
|---|---:|---|
| `canonical_vascular_invasion_patient_rollup_v1` | 3,745 | `source_kind='llm'` rows for invasion_type ∈ `{vascular_microscopic, lymphatic_microscopic, perineural}` |
| `canonical_airway_invasion_patient_rollup_v1`   | 2,820 | `source_kind='llm'` rows for `{airway, tracheal, esophageal}` + **new** RLN / pT4a evidence |
| `canonical_ete_subgrade_patient_rollup_v1`      |   151 | adds `any_gross_ete` / `any_microscopic_ete` supporting evidence + pT4a/pT4b |
| `canonical_t4b_invasion_patient_rollup_v1`      |   434 | **new** pT4b anatomic-component evidence |
| `canonical_parathyroid_patient_rollup_v1`       | 4,443 | not an invasion flag source; ignore here (complications canonical) |

The current rollup's `any_airway_anywhere` = **1** (yes, 1 patient) and `any_tracheal_anywhere` = **14**. The v2 airway feeder has 92 tracheal-involvement patients and 81 RLN-invasion patients. Re-sourcing those columns recovers real signal that was lost when Script 363's LLM ladder (Pattern 16 fix) was calibrated against the non-v2 `note_entities_llm_airway_invasion` loader.

This is a **rollup-level override**, not an events rebuild. Rationale:
- User instruction is explicit: "events not needed for rollup refresh".
- The v2 feeders emit per-patient BOOLs keyed on `research_id`; no cross-domain linkage IDs are required (`feedback_no_crossdomain_linkage_ids`).
- Follow-up Script 364+ can rebuild the events table to rehome the LLM CTEs on the v2 loaders; this migration is cheap and reversible.

---

## Pre-flight probe

Run these **before** touching `canonical_invasion_patient_rollup_v1`. Paste output into the close-out's "baseline" table.

```sql
-- 1. Current rollup build tag (expect: build_script='363', build_ts='2026-04-21 23:30:02')
SELECT DISTINCT build_script, build_ts, COUNT(*) OVER () AS total_rows
FROM main.canonical_invasion_patient_rollup_v1;

-- 2. Current truthy counts per "anywhere" flag (baseline — the refresh must explain every delta)
SELECT
  SUM(any_gross_ete_anywhere::INT)             AS n_gross_ete,
  SUM(any_microscopic_ete_anywhere::INT)       AS n_microscopic_ete,
  SUM(any_vascular_microscopic_anywhere::INT)  AS n_vascular_microscopic,
  SUM(any_lymphatic_microscopic_anywhere::INT) AS n_lymphatic_microscopic,
  SUM(any_capsular_anywhere::INT)              AS n_capsular,       -- unchanged: path-synoptic only
  SUM(any_perineural_anywhere::INT)            AS n_perineural,
  SUM(any_soft_tissue_anywhere::INT)           AS n_soft_tissue,    -- unchanged
  SUM(any_airway_anywhere::INT)                AS n_airway,
  SUM(any_tracheal_anywhere::INT)              AS n_tracheal,
  SUM(any_esophageal_anywhere::INT)            AS n_esophageal
FROM main.canonical_invasion_patient_rollup_v1;
-- Expected: gross_ete=1146, microscopic_ete=279, vascular_microscopic=1109,
-- lymphatic_microscopic=780, capsular=941, perineural=122, soft_tissue=493,
-- airway=1, tracheal=14, esophageal=69 (from 363 close-out).

-- 3. v2 feeder sanity — row counts & build tags
SELECT 'vascular_v2' AS t, COUNT(*) AS pts,
       ANY_VALUE(build_script) AS tag, MAX(build_ts) AS ts
FROM main.canonical_vascular_invasion_patient_rollup_v1
UNION ALL SELECT 'airway_v2',     COUNT(*), ANY_VALUE(build_script), MAX(build_ts) FROM main.canonical_airway_invasion_patient_rollup_v1
UNION ALL SELECT 'ete_subgrade',  COUNT(*), ANY_VALUE(build_script), MAX(build_ts) FROM main.canonical_ete_subgrade_patient_rollup_v1
UNION ALL SELECT 't4b_invasion',  COUNT(*), ANY_VALUE(build_script), MAX(build_ts) FROM main.canonical_t4b_invasion_patient_rollup_v1;
-- Expected: vascular=3745/mig_56, airway=2820/mig_57, ete=151/mig_54→55, t4b=434/mig_55.

-- 4. Overlap sanity — how many of the 10,871 rollup patients intersect each feeder?
SELECT
  COUNT(DISTINCT r.research_id)                                                         AS total,
  COUNT(DISTINCT CASE WHEN v.research_id IS NOT NULL THEN r.research_id END)            AS in_vascular_v2,
  COUNT(DISTINCT CASE WHEN a.research_id IS NOT NULL THEN r.research_id END)            AS in_airway_v2,
  COUNT(DISTINCT CASE WHEN e.research_id IS NOT NULL THEN r.research_id END)            AS in_ete_subgrade,
  COUNT(DISTINCT CASE WHEN t.research_id IS NOT NULL THEN r.research_id END)            AS in_t4b_invasion
FROM main.canonical_invasion_patient_rollup_v1 r
LEFT JOIN main.canonical_vascular_invasion_patient_rollup_v1 v USING(research_id)
LEFT JOIN main.canonical_airway_invasion_patient_rollup_v1   a USING(research_id)
LEFT JOIN main.canonical_ete_subgrade_patient_rollup_v1      e USING(research_id)
LEFT JOIN main.canonical_t4b_invasion_patient_rollup_v1      t USING(research_id);
-- Sanity: every feeder patient must be in r (feeders are a subset of the 10,871 cohort).
-- If any LEFT JOIN row is present but research_id not in r → feeder emitted a patient
-- that's outside the invasion rollup cohort (unexpected; investigate before proceeding).
```

**Stop and investigate if** any feeder row count deviates from the expected pts, any build_script tag is missing, or the join drops feeder patients.

---

## Rollup CTAS — strategy

Compose from three layers and OR them together per patient:

1. **`events_agg`** — current Script 363 aggregate, computed from the unchanged `canonical_invasion_events_v1` but with LLM rows for `{vascular_microscopic, lymphatic_microscopic, perineural, airway, tracheal, esophageal}` **excluded** (we're replacing them). This keeps the path-synoptic/op-note structured contribution.
2. **`v2_feeder_agg`** — BOOL flags materialised directly from the four tier-2 rollups, with source-kind derived from the underlying `canonical_<domain>_events_v1.note_type` / `source_workbook`.
3. **`rollup`** — per-patient OR across (1) + (2), plus the **new columns** listed above.

### Step 1 — `events_agg` (CTE)

```sql
WITH events_agg AS (
  SELECT
    research_id,
    BOOL_OR(invasion_type='gross_ete')                                         AS any_gross_ete_anywhere_evt,
    BOOL_OR(invasion_type='gross_ete'         AND source_modality IN ('op_note','synoptic_path')) AS any_gross_ete_in_op_or_path_evt,
    BOOL_OR(invasion_type='gross_ete'         AND source_modality IN ('ct','mri','ultrasound','pet_ct','nucmed')) AS any_gross_ete_in_imaging_evt,
    BOOL_OR(invasion_type='microscopic_ete')                                   AS any_microscopic_ete_anywhere_evt,
    BOOL_OR(invasion_type='microscopic_ete'   AND source_modality IN ('op_note','synoptic_path')) AS any_microscopic_ete_in_op_or_path_evt,
    BOOL_OR(invasion_type='microscopic_ete'   AND source_modality IN ('ct','mri','ultrasound','pet_ct','nucmed')) AS any_microscopic_ete_in_imaging_evt,
    -- vascular / lymphatic / perineural: STRUCTURED ONLY (drop source_kind='llm' — v2 takes over)
    BOOL_OR(invasion_type='vascular_microscopic' AND source_kind='structured')                    AS any_vascular_microscopic_anywhere_evt,
    BOOL_OR(invasion_type='vascular_microscopic' AND source_kind='structured' AND source_modality IN ('op_note','synoptic_path')) AS any_vascular_microscopic_in_op_or_path_evt,
    BOOL_OR(invasion_type='vascular_microscopic' AND source_kind='structured' AND source_modality IN ('ct','mri','ultrasound','pet_ct','nucmed')) AS any_vascular_microscopic_in_imaging_evt,
    BOOL_OR(invasion_type='lymphatic_microscopic' AND source_kind='structured')                   AS any_lymphatic_microscopic_anywhere_evt,
    BOOL_OR(invasion_type='lymphatic_microscopic' AND source_kind='structured' AND source_modality IN ('op_note','synoptic_path')) AS any_lymphatic_microscopic_in_op_or_path_evt,
    BOOL_OR(invasion_type='lymphatic_microscopic' AND source_kind='structured' AND source_modality IN ('ct','mri','ultrasound','pet_ct','nucmed')) AS any_lymphatic_microscopic_in_imaging_evt,
    BOOL_OR(invasion_type='perineural' AND source_kind='structured')                              AS any_perineural_anywhere_evt,
    BOOL_OR(invasion_type='perineural' AND source_kind='structured' AND source_modality IN ('op_note','synoptic_path')) AS any_perineural_in_op_or_path_evt,
    BOOL_OR(invasion_type='perineural' AND source_kind='structured' AND source_modality IN ('ct','mri','ultrasound','pet_ct','nucmed')) AS any_perineural_in_imaging_evt,
    -- capsular / soft_tissue: untouched (no v2 feeder)
    BOOL_OR(invasion_type='capsular')                                          AS any_capsular_anywhere_evt,
    BOOL_OR(invasion_type='capsular'          AND source_modality IN ('op_note','synoptic_path')) AS any_capsular_in_op_or_path_evt,
    BOOL_OR(invasion_type='capsular'          AND source_modality IN ('ct','mri','ultrasound','pet_ct','nucmed')) AS any_capsular_in_imaging_evt,
    BOOL_OR(invasion_type='soft_tissue')                                       AS any_soft_tissue_anywhere_evt,
    BOOL_OR(invasion_type='soft_tissue'       AND source_modality IN ('op_note','synoptic_path')) AS any_soft_tissue_in_op_or_path_evt,
    BOOL_OR(invasion_type='soft_tissue'       AND source_modality IN ('ct','mri','ultrasound','pet_ct','nucmed')) AS any_soft_tissue_in_imaging_evt,
    -- airway / tracheal / esophageal: STRUCTURED ONLY (drop source_kind='llm' — airway v2 takes over)
    BOOL_OR(invasion_type='airway'     AND source_kind='structured')                              AS any_airway_anywhere_evt,
    BOOL_OR(invasion_type='airway'     AND source_kind='structured' AND source_modality IN ('op_note','synoptic_path')) AS any_airway_in_op_or_path_evt,
    BOOL_OR(invasion_type='airway'     AND source_kind='structured' AND source_modality IN ('ct','mri','ultrasound','pet_ct','nucmed')) AS any_airway_in_imaging_evt,
    BOOL_OR(invasion_type='tracheal'   AND source_kind='structured')                              AS any_tracheal_anywhere_evt,
    BOOL_OR(invasion_type='tracheal'   AND source_kind='structured' AND source_modality IN ('op_note','synoptic_path')) AS any_tracheal_in_op_or_path_evt,
    BOOL_OR(invasion_type='tracheal'   AND source_kind='structured' AND source_modality IN ('ct','mri','ultrasound','pet_ct','nucmed')) AS any_tracheal_in_imaging_evt,
    BOOL_OR(invasion_type='esophageal' AND source_kind='structured')                              AS any_esophageal_anywhere_evt,
    BOOL_OR(invasion_type='esophageal' AND source_kind='structured' AND source_modality IN ('op_note','synoptic_path')) AS any_esophageal_in_op_or_path_evt,
    BOOL_OR(invasion_type='esophageal' AND source_kind='structured' AND source_modality IN ('ct','mri','ultrasound','pet_ct','nucmed')) AS any_esophageal_in_imaging_evt
  FROM main.canonical_invasion_events_v1
  GROUP BY research_id
)
```

**Note on source-kind filtering:** The v2 feeders rehome the LLM contribution for `{vascular, lymphatic, perineural, airway, tracheal, esophageal}`. To avoid double-counting, the events aggregate for those six types must strip `source_kind='llm'`. For `{gross_ete, microscopic_ete, capsular, soft_tissue}` the events aggregate stays mixed (structured + LLM) because only ETE has a v2 feeder and we COALESCE it in additively rather than replace.

### Step 2 — `v2_feeder_agg` (CTE)

The v2 rollups give us per-patient BOOLs but not the `_in_op_or_path` / `_in_imaging` split. Derive the split by joining to the underlying v2 `canonical_*_events_v1` tables (which carry `note_type`), and buckets:
- `note_type IN ('operative_note','op_note','pathology_synoptic','path_synoptic','pathology')` → `op_or_path`
- `note_type IN ('ct','mri','ultrasound','pet_ct','nucmed','imaging')` → `imaging`
- everything else → `op_or_path` (conservative; most thyroid path narratives land here)

```sql
-- Pattern: one CTE per v2 domain, each emitting per-patient BOOLs for (anywhere, in_op_or_path, in_imaging)
-- and the new-column singletons (any_rln_invasion_*, any_pT4a_final_*, any_pT4b_final_*, etc.).

, vascular_v2_split AS (
  SELECT
    research_id,
    BOOL_OR(vascular_invasion='present')                                          AS any_vasc_anywhere,
    BOOL_OR(lymphatic_invasion='present')                                         AS any_lymph_anywhere,
    BOOL_OR(perineural_invasion='present')                                        AS any_pni_anywhere,
    BOOL_OR(vascular_invasion='present'   AND note_type NOT IN ('ct','mri','ultrasound','pet_ct','nucmed','imaging'))  AS any_vasc_op_or_path,
    BOOL_OR(lymphatic_invasion='present'  AND note_type NOT IN ('ct','mri','ultrasound','pet_ct','nucmed','imaging'))  AS any_lymph_op_or_path,
    BOOL_OR(perineural_invasion='present' AND note_type NOT IN ('ct','mri','ultrasound','pet_ct','nucmed','imaging'))  AS any_pni_op_or_path,
    BOOL_OR(vascular_invasion='present'   AND note_type     IN ('ct','mri','ultrasound','pet_ct','nucmed','imaging'))  AS any_vasc_imaging,
    BOOL_OR(lymphatic_invasion='present'  AND note_type     IN ('ct','mri','ultrasound','pet_ct','nucmed','imaging'))  AS any_lymph_imaging,
    BOOL_OR(perineural_invasion='present' AND note_type     IN ('ct','mri','ultrasound','pet_ct','nucmed','imaging'))  AS any_pni_imaging
  FROM main.canonical_vascular_invasion_events_v1
  GROUP BY research_id
),

airway_v2_split AS (
  SELECT
    research_id,
    BOOL_OR(tracheal_invasion IN ('present','shaved'))                            AS any_trach_anywhere,
    BOOL_OR(tracheal_invasion IN ('present','shaved')
             OR laryngeal_invasion='present' OR cricoid_invasion='present'
             OR esophageal_invasion='present')                                    AS any_airway_anywhere,
    BOOL_OR(esophageal_invasion='present')                                        AS any_esoph_anywhere,
    BOOL_OR(rln_invasion='present')                                               AS any_rln_anywhere,
    BOOL_OR(t4a_implication='pT4a')                                               AS any_pT4a_direct_anywhere,
    -- split by note_type
    BOOL_OR(tracheal_invasion IN ('present','shaved')
             AND note_type NOT IN ('ct','mri','ultrasound','pet_ct','nucmed','imaging'))  AS any_trach_op_or_path,
    BOOL_OR(tracheal_invasion IN ('present','shaved')
             AND note_type     IN ('ct','mri','ultrasound','pet_ct','nucmed','imaging'))  AS any_trach_imaging,
    BOOL_OR((tracheal_invasion IN ('present','shaved') OR laryngeal_invasion='present'
             OR cricoid_invasion='present' OR esophageal_invasion='present')
             AND note_type NOT IN ('ct','mri','ultrasound','pet_ct','nucmed','imaging'))  AS any_airway_op_or_path,
    BOOL_OR((tracheal_invasion IN ('present','shaved') OR laryngeal_invasion='present'
             OR cricoid_invasion='present' OR esophageal_invasion='present')
             AND note_type     IN ('ct','mri','ultrasound','pet_ct','nucmed','imaging'))  AS any_airway_imaging,
    BOOL_OR(esophageal_invasion='present'
             AND note_type NOT IN ('ct','mri','ultrasound','pet_ct','nucmed','imaging'))  AS any_esoph_op_or_path,
    BOOL_OR(esophageal_invasion='present'
             AND note_type     IN ('ct','mri','ultrasound','pet_ct','nucmed','imaging'))  AS any_esoph_imaging,
    BOOL_OR(rln_invasion='present'
             AND note_type NOT IN ('ct','mri','ultrasound','pet_ct','nucmed','imaging'))  AS any_rln_op_or_path,
    BOOL_OR(rln_invasion='present'
             AND note_type     IN ('ct','mri','ultrasound','pet_ct','nucmed','imaging'))  AS any_rln_imaging
  FROM main.canonical_airway_invasion_events_v1
  GROUP BY research_id
),

-- ete_subgrade: only 151 patients, no per-note split needed at this grain;
-- use rollup directly and attribute to op_or_path (ETE subgrade is overwhelmingly synoptic-sourced).
ete_sub_v2 AS (
  SELECT
    research_id,
    any_gross_ete         AS v2_gross_ete_anywhere,
    any_microscopic_ete   AS v2_micro_ete_anywhere,
    any_pT4a              AS v2_pT4a_ete,
    any_pT4b              AS v2_pT4b_ete
  FROM main.canonical_ete_subgrade_patient_rollup_v1
),

-- t4b_invasion: 434 patients; rollup grain is fine.
t4b_v2 AS (
  SELECT
    research_id,
    any_pT4b_final            AS v2_pT4b_final,
    any_pT4b_direct           AS v2_pT4b_direct,
    any_carotid_encasement    AS v2_carotid,
    any_mediastinal_vessel    AS v2_mediastinal,
    any_prevertebral_fascia   AS v2_prevertebral
  FROM main.canonical_t4b_invasion_patient_rollup_v1
)
```

### Step 3 — final `CREATE OR REPLACE TABLE`

```sql
CREATE OR REPLACE TABLE main.canonical_invasion_patient_rollup_v1 AS
WITH
  events_agg       AS ( ... Step 1 ... ),
  vascular_v2_split AS ( ... Step 2 ... ),
  airway_v2_split   AS ( ... Step 2 ... ),
  ete_sub_v2        AS ( ... Step 2 ... ),
  t4b_v2            AS ( ... Step 2 ... )
SELECT
  e.research_id,

  -- === gross_ete / microscopic_ete: ADDITIVE (events + ete_subgrade_v2 union) ===
  COALESCE(e.any_gross_ete_anywhere_evt,           FALSE) OR COALESCE(es.v2_gross_ete_anywhere, FALSE) AS any_gross_ete_anywhere,
  COALESCE(e.any_gross_ete_in_op_or_path_evt,      FALSE) OR COALESCE(es.v2_gross_ete_anywhere, FALSE) AS any_gross_ete_in_op_or_path,
  e.any_gross_ete_in_imaging_evt                                                                       AS any_gross_ete_in_imaging,
  COALESCE(e.any_microscopic_ete_anywhere_evt,     FALSE) OR COALESCE(es.v2_micro_ete_anywhere, FALSE) AS any_microscopic_ete_anywhere,
  COALESCE(e.any_microscopic_ete_in_op_or_path_evt,FALSE) OR COALESCE(es.v2_micro_ete_anywhere, FALSE) AS any_microscopic_ete_in_op_or_path,
  e.any_microscopic_ete_in_imaging_evt                                                                  AS any_microscopic_ete_in_imaging,

  -- === vascular / lymphatic / perineural: events(structured) + vascular_v2_split ===
  COALESCE(e.any_vascular_microscopic_anywhere_evt,       FALSE) OR COALESCE(vv.any_vasc_anywhere,    FALSE) AS any_vascular_microscopic_anywhere,
  COALESCE(e.any_vascular_microscopic_in_op_or_path_evt,  FALSE) OR COALESCE(vv.any_vasc_op_or_path,  FALSE) AS any_vascular_microscopic_in_op_or_path,
  COALESCE(e.any_vascular_microscopic_in_imaging_evt,     FALSE) OR COALESCE(vv.any_vasc_imaging,     FALSE) AS any_vascular_microscopic_in_imaging,
  COALESCE(e.any_lymphatic_microscopic_anywhere_evt,      FALSE) OR COALESCE(vv.any_lymph_anywhere,   FALSE) AS any_lymphatic_microscopic_anywhere,
  COALESCE(e.any_lymphatic_microscopic_in_op_or_path_evt, FALSE) OR COALESCE(vv.any_lymph_op_or_path, FALSE) AS any_lymphatic_microscopic_in_op_or_path,
  COALESCE(e.any_lymphatic_microscopic_in_imaging_evt,    FALSE) OR COALESCE(vv.any_lymph_imaging,    FALSE) AS any_lymphatic_microscopic_in_imaging,
  COALESCE(e.any_perineural_anywhere_evt,                 FALSE) OR COALESCE(vv.any_pni_anywhere,     FALSE) AS any_perineural_anywhere,
  COALESCE(e.any_perineural_in_op_or_path_evt,            FALSE) OR COALESCE(vv.any_pni_op_or_path,   FALSE) AS any_perineural_in_op_or_path,
  COALESCE(e.any_perineural_in_imaging_evt,               FALSE) OR COALESCE(vv.any_pni_imaging,      FALSE) AS any_perineural_in_imaging,

  -- === capsular / soft_tissue: unchanged (no v2 feeder) ===
  e.any_capsular_anywhere_evt       AS any_capsular_anywhere,
  e.any_capsular_in_op_or_path_evt  AS any_capsular_in_op_or_path,
  e.any_capsular_in_imaging_evt     AS any_capsular_in_imaging,
  e.any_soft_tissue_anywhere_evt    AS any_soft_tissue_anywhere,
  e.any_soft_tissue_in_op_or_path_evt AS any_soft_tissue_in_op_or_path,
  e.any_soft_tissue_in_imaging_evt  AS any_soft_tissue_in_imaging,

  -- === airway / tracheal / esophageal: events(structured) + airway_v2_split ===
  COALESCE(e.any_airway_anywhere_evt,      FALSE) OR COALESCE(av.any_airway_anywhere,  FALSE) AS any_airway_anywhere,
  COALESCE(e.any_airway_in_op_or_path_evt, FALSE) OR COALESCE(av.any_airway_op_or_path, FALSE) AS any_airway_in_op_or_path,
  COALESCE(e.any_airway_in_imaging_evt,    FALSE) OR COALESCE(av.any_airway_imaging,    FALSE) AS any_airway_in_imaging,
  COALESCE(e.any_tracheal_anywhere_evt,      FALSE) OR COALESCE(av.any_trach_anywhere,  FALSE) AS any_tracheal_anywhere,
  COALESCE(e.any_tracheal_in_op_or_path_evt, FALSE) OR COALESCE(av.any_trach_op_or_path, FALSE) AS any_tracheal_in_op_or_path,
  COALESCE(e.any_tracheal_in_imaging_evt,    FALSE) OR COALESCE(av.any_trach_imaging,    FALSE) AS any_tracheal_in_imaging,
  COALESCE(e.any_esophageal_anywhere_evt,      FALSE) OR COALESCE(av.any_esoph_anywhere,  FALSE) AS any_esophageal_anywhere,
  COALESCE(e.any_esophageal_in_op_or_path_evt, FALSE) OR COALESCE(av.any_esoph_op_or_path, FALSE) AS any_esophageal_in_op_or_path,
  COALESCE(e.any_esophageal_in_imaging_evt,    FALSE) OR COALESCE(av.any_esoph_imaging,    FALSE) AS any_esophageal_in_imaging,

  -- === NEW: RLN invasion ===
  COALESCE(av.any_rln_anywhere,  FALSE) AS any_rln_invasion_anywhere,
  COALESCE(av.any_rln_op_or_path,FALSE) AS any_rln_invasion_in_op_or_path,
  COALESCE(av.any_rln_imaging,   FALSE) AS any_rln_invasion_in_imaging,

  -- === NEW: pT4a / pT4b staging booleans (union across feeders) ===
  COALESCE(av.any_pT4a_direct_anywhere, FALSE) OR COALESCE(es.v2_pT4a_ete, FALSE)            AS any_pT4a_final_anywhere,
  COALESCE(es.v2_pT4b_ete, FALSE) OR COALESCE(t.v2_pT4b_final, FALSE)                        AS any_pT4b_final_anywhere,

  -- === NEW: pT4b anatomic components (from t4b_v2) ===
  COALESCE(t.v2_carotid,      FALSE) AS any_carotid_encasement_anywhere,
  COALESCE(t.v2_mediastinal,  FALSE) AS any_mediastinal_vessel_anywhere,
  COALESCE(t.v2_prevertebral, FALSE) AS any_prevertebral_fascia_anywhere,

  'mig_60_invasion_rollup_v2_refresh_20260424'  AS build_script,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP)          AS build_ts
FROM events_agg e
LEFT JOIN vascular_v2_split vv USING (research_id)
LEFT JOIN airway_v2_split   av USING (research_id)
LEFT JOIN ete_sub_v2        es USING (research_id)
LEFT JOIN t4b_v2            t  USING (research_id);
```

**Grain:** one row per `research_id`; **expected rows = 10,871** (identical to pre-refresh — feeders never introduce new patients, only update flags).

**Reminder:** `build_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP)` — DuckDB's naked `CURRENT_TIMESTAMP` is TIMESTAMPTZ (`reference_duckdb_timestamp_tz`).

---

## QA gates (hard — all must pass before commit)

Add these to a new file `qa/qa_mig_60_invasion_rollup_refresh.json`. All row counts are `WHERE flag = TRUE`.

### G1. Row-count invariance
```sql
SELECT COUNT(*) FROM main.canonical_invasion_patient_rollup_v1;  -- must = 10871
```

### G2. No v2-induced retractions
A v2 feeder can only ADD truthy; OR-ing preserves prior TRUE. So:
```sql
-- any_<type>_anywhere post ≥ pre
-- Expected deltas (feeder BOOL rates observed 2026-04-24):
--   vascular_microscopic: pre=1109, +Δ from 730 v2 patients → post ~1100-1300
--   lymphatic_microscopic: pre=780,  +Δ from 873 v2 patients → post ~900-1200
--   perineural:            pre=122,  +Δ from 103 v2 patients → post ~150-200
--   airway:                pre=1,    +Δ from 92 v2 patients  → post ~90+
--   tracheal:              pre=14,   +Δ from 92 v2 patients  → post ~60-100
--   esophageal:            pre=69,   +Δ from 35 v2 patients  → post ~80-100
--   gross_ete:             pre=1146, +Δ from 96 ete-sub      → post 1146-1240
--   microscopic_ete:       pre=279,  +Δ from 39 ete-sub      → post 279-315
```
Fail the gate if any _anywhere count drops below the pre value.

### G3. New-column spot rates
```sql
SELECT
  SUM(any_rln_invasion_anywhere::INT)          AS rln,          -- expect ≈ 81
  SUM(any_pT4a_final_anywhere::INT)            AS pT4a,         -- expect ≈ 135 + small ETE add ≤ 170
  SUM(any_pT4b_final_anywhere::INT)            AS pT4b,         -- expect 10-13 (ETE 3 ∪ T4b 10)
  SUM(any_carotid_encasement_anywhere::INT)    AS carotid,      -- expect small (low single digits)
  SUM(any_mediastinal_vessel_anywhere::INT)    AS mediastinal,  -- expect small
  SUM(any_prevertebral_fascia_anywhere::INT)   AS prevertebral  -- expect 0 per mig 55 closeout
FROM main.canonical_invasion_patient_rollup_v1;
```

### G4. Hierarchy sanity
```sql
-- anywhere ≥ in_op_or_path, anywhere ≥ in_imaging for every type
SELECT COUNT(*) FROM main.canonical_invasion_patient_rollup_v1
WHERE any_gross_ete_anywhere          < any_gross_ete_in_op_or_path
   OR any_gross_ete_anywhere          < any_gross_ete_in_imaging
   OR any_microscopic_ete_anywhere    < any_microscopic_ete_in_op_or_path
   OR any_vascular_microscopic_anywhere   < any_vascular_microscopic_in_op_or_path
   OR any_vascular_microscopic_anywhere   < any_vascular_microscopic_in_imaging
   OR any_lymphatic_microscopic_anywhere  < any_lymphatic_microscopic_in_op_or_path
   OR any_perineural_anywhere         < any_perineural_in_op_or_path
   OR any_capsular_anywhere           < any_capsular_in_op_or_path
   OR any_airway_anywhere             < any_airway_in_op_or_path
   OR any_tracheal_anywhere           < any_tracheal_in_op_or_path
   OR any_esophageal_anywhere         < any_esophageal_in_op_or_path
   OR any_rln_invasion_anywhere       < any_rln_invasion_in_op_or_path;
-- Expected: 0
```

### G5. Feeder round-trip (every v2 TRUE must appear in rollup TRUE)
```sql
-- vascular v2 → rollup
SELECT COUNT(*) FROM main.canonical_vascular_invasion_patient_rollup_v1 v
LEFT JOIN main.canonical_invasion_patient_rollup_v1 r USING(research_id)
WHERE v.any_vascular_invasion = TRUE AND COALESCE(r.any_vascular_microscopic_anywhere, FALSE) = FALSE;
-- Expected: 0

-- airway v2 → rollup (tracheal involvement)
SELECT COUNT(*) FROM main.canonical_airway_invasion_patient_rollup_v1 a
LEFT JOIN main.canonical_invasion_patient_rollup_v1 r USING(research_id)
WHERE a.any_tracheal_involvement = TRUE AND COALESCE(r.any_tracheal_anywhere, FALSE) = FALSE;
-- Expected: 0

-- airway v2 → rollup (rln)
SELECT COUNT(*) FROM main.canonical_airway_invasion_patient_rollup_v1 a
LEFT JOIN main.canonical_invasion_patient_rollup_v1 r USING(research_id)
WHERE a.any_rln_invasion = TRUE AND COALESCE(r.any_rln_invasion_anywhere, FALSE) = FALSE;
-- Expected: 0

-- t4b v2 → rollup (pT4b final)
SELECT COUNT(*) FROM main.canonical_t4b_invasion_patient_rollup_v1 t
LEFT JOIN main.canonical_invasion_patient_rollup_v1 r USING(research_id)
WHERE t.any_pT4b_final = TRUE AND COALESCE(r.any_pT4b_final_anywhere, FALSE) = FALSE;
-- Expected: 0

-- ete_subgrade → rollup (gross_ete)
SELECT COUNT(*) FROM main.canonical_ete_subgrade_patient_rollup_v1 e
LEFT JOIN main.canonical_invasion_patient_rollup_v1 r USING(research_id)
WHERE e.any_gross_ete = TRUE AND COALESCE(r.any_gross_ete_anywhere, FALSE) = FALSE;
-- Expected: 0
```

### G6. Build tag cutover
```sql
SELECT DISTINCT build_script, build_ts FROM main.canonical_invasion_patient_rollup_v1;
-- Must be exactly: build_script='mig_60_invasion_rollup_v2_refresh_20260424', build_ts today
```

---

## Registry patch (`manuscript_workspace.detail_table_registry_v1`)

The existing row (`canonical_version='v1_0_script363'`) must be **updated in place** — not duplicated. Use `DELETE WHERE detail_table_name=...` then `INSERT`; matches the idempotent registry pattern (Script 363 Pattern 13) and the 15-column schema (`reference_detail_table_registry_schema`).

```sql
-- Probe schema first (per reference_detail_table_registry_schema)
SELECT column_name FROM information_schema.columns
WHERE table_schema='manuscript_workspace' AND table_name='detail_table_registry_v1'
ORDER BY ordinal_position;

-- Snapshot old row for audit
CREATE OR REPLACE TABLE archive_pub_v1_0.detail_table_registry_v1_pre_mig60_20260424 AS
SELECT * FROM manuscript_workspace.detail_table_registry_v1;

-- UPSERT
DELETE FROM manuscript_workspace.detail_table_registry_v1
WHERE detail_table_name = 'canonical_invasion_patient_rollup_v1';

INSERT INTO manuscript_workspace.detail_table_registry_v1
 (detail_table_name, schema_name, join_key, grain, total_rows, total_patients,
  domain, feeds_master_columns, description, canonical_version,
  feeds_master_columns_secondary, feeds_master_columns_array,
  needs_manual_review, superseded_by, renamed_by_script)
VALUES
 ('canonical_invasion_patient_rollup_v1', 'main', 'research_id', 'per_patient',
  10871, 10871, 'invasion_findings', NULL,
  '[domain=invasion_findings; grain=per_patient] — source: mig_60_invasion_rollup_v2_refresh_20260424. '
  || 'Cross-modal invasion finding canonical. Events layer frozen at Script 363 (51,773 rows); '
  || 'patient rollup now re-sources vascular/lymphatic/perineural/airway/tracheal/esophageal/RLN/pT4a/pT4b '
  || 'from canonical_{vascular,airway,ete_subgrade,t4b}_invasion_patient_rollup_v1 via additive OR; '
  || 'structured path-synoptic feeders retained for gross_ete/microscopic_ete/capsular/soft_tissue. '
  || 'Rows=10871, patients=10871.',
  'v1_0_mig_60_invasion_rollup_v2_refresh_20260424',
  NULL, NULL, FALSE, NULL, NULL);
```

---

## Side-car: mig 58 carry-forward (a) — one patient glands_identified_count = 5

Fold the single-patient contract patch into this migration as a separate tagged write. Rationale: cheap, already scoped, avoids a one-off commit.

```sql
-- 1. Identify the offender
SELECT research_id, note_row_id, source_workbook, source_sheet,
       json_extract_string(parsed_json, '$.glands_identified_count') AS raw,
       json_extract_string(parsed_json, '$.evidence_quote')          AS evidence
FROM main.canonical_parathyroid_events_v1
WHERE glands_identified_count = 5;
-- Log research_id + evidence_quote in close-out under "Side-car patches".

-- 2. Decision path (implement whichever matches the evidence):
--    (A) If evidence supports 4 (i.e., "all four identified" + stray mention): clamp to 4.
--    (B) If evidence is genuinely ambiguous: set to NULL and increment null_count in rollup.
-- Default (pick A when the quote mentions "four glands" + a fifth sent to frozen separately):

UPDATE main.canonical_parathyroid_events_v1
SET glands_identified_count = 4,
    build_script            = 'mig_60_parathyroid_glands5_patch_20260424'
WHERE glands_identified_count = 5;

-- 3. Rebuild the affected rollup cell
UPDATE main.canonical_parathyroid_patient_rollup_v1 r
SET max_glands_identified =
    (SELECT MAX(glands_identified_count) FROM main.canonical_parathyroid_events_v1 e WHERE e.research_id = r.research_id),
    build_script          = build_script || ';mig_60_parathyroid_glands5_patch_20260424'
WHERE r.research_id IN (
   SELECT DISTINCT research_id FROM main.canonical_parathyroid_events_v1
   WHERE build_script = 'mig_60_parathyroid_glands5_patch_20260424'
);

-- 4. Gate: 0 rows remain
SELECT COUNT(*) FROM main.canonical_parathyroid_events_v1 WHERE glands_identified_count = 5;
-- Expected: 0
```

**Carry-forwards (b) and (c) are NOT addressed here** — they're documented under "Deferred" below.

---

## Commit & push (GitHub Git Data API — sandbox can't `git commit`)

Per `feedback_surgical_git_add`, stage by explicit path. The expected file set for this migration is:

- `qc_framework_v1/migrations/60_invasion_rollup_v2_refresh.sql` — the full CTAS above (events_agg + v2_feeder_aggs + final SELECT + registry UPSERT + parathyroid side-car).
- `qa/qa_mig_60_invasion_rollup_refresh.json` — G1–G6 results.
- `cursor_prompts/CURSOR_PROMPT_MIG_60_INVASION_ROLLUP_REFRESH_20260424.md` — this file.
- `project_mig_60_invasion_rollup_refresh_closeout.md` — close-out (see template below).

Workflow:
1. `security find-generic-password -s 'gh:github.com' -w` → GH PAT.
2. POST `/repos/{owner}/{repo}/git/blobs` for each file's content.
3. POST `/repos/{owner}/{repo}/git/trees` with the four blobs.
4. POST `/repos/{owner}/{repo}/git/commits` with parent=ef16a64.
5. PATCH `/repos/{owner}/{repo}/git/refs/heads/main` to the new commit.

Commit message:
```
Script mig_60: refresh canonical_invasion_patient_rollup_v1 from v2 LLM feeders

- rollup only (events table frozen at Script 363 v3-iter-2)
- re-source any_vascular_*, any_lymphatic_*, any_perineural_*, any_airway_*,
  any_tracheal_*, any_esophageal_* from canonical_{vascular,airway}_invasion_*
- additive COALESCE for any_gross_ete_*/any_microscopic_ete_* from ete_subgrade v2
- NEW cols: any_rln_invasion_*, any_pT4a_final_*, any_pT4b_final_*,
  any_carotid_encasement_*, any_mediastinal_vessel_*, any_prevertebral_fascia_*
- side-car: mig 58 CF-a (glands_identified_count=5 → 4 for 1 patient)
- registry upsert: v1_0_script363 → v1_0_mig_60_invasion_rollup_v2_refresh_20260424

QA: G1-G6 green (see qa/qa_mig_60_invasion_rollup_refresh.json)
```

---

## Close-out template — `project_mig_60_invasion_rollup_refresh_closeout.md`

Follow `project_mig_57_airway_invasion_v2_closeout.md`. Required sections:

1. Header: Date / DB / final SHA / build_script tag.
2. "Repo artifacts" table.
3. "Objects delivered" table.
4. "Source & overlap summary" — counts from pre-flight Step 4.
5. **Baseline-vs-post table** — one row per column, three columns (pre, post, delta). Use the G2 sketch. Flag any anomalies.
6. "New columns" table — rates for rln/pT4a_final/pT4b_final/carotid/mediastinal/prevertebral.
7. "QA gates" — G1-G6 verdict.
8. "Side-car: glands=5 patch" — research_id + the evidence_quote + the decision (A vs B).
9. "Deferred (mig 58 carry-forwards b + c)" — see below.
10. "Carry-forward to Script 364+" — re-home the events-table LLM CTEs on the v2 loaders, drop the `_deprecated_` tables, complications canonical scoping.

---

## Deferred — mig 58 carry-forwards (b) + (c)

Not part of mig 60. Scoped here so they aren't lost.

### (b) 2,219 / 4,443 (50 %) any_incidental_parathyroidectomy = TRUE — likely over-liberal

Published rate is 10–30 %. Hypothesis: LLM counts "benign parathyroid tissue identified in specimen" as positive even when no gland-count is reported. Recommended action:

1. Sample 50 `evidence_quote` strings where `any_incidental_parathyroidectomy = TRUE` AND `n_glands_identified_any IS NULL`.
2. Classify: (i) true incidental resection, (ii) specimen-finding-only (should be FALSE), (iii) ambiguous.
3. If (ii) ≥ 30 % of the sample: tighten the rollup flag to `(incidental_parathyroidectomy = TRUE) AND (glands_identified_count >= 1)` in a follow-up rollup refresh (mig 61 candidate), OR re-prompt the affected notes with a location+count specificity extractor.

Spin this off as a separate scoping prompt: `cursor_prompts/CURSOR_PROMPT_MIG_61_PARATHYROID_SPECIFICITY_TBD.md`. It feeds directly into the complications canonical design — the "incidental parathyroidectomy" flag wants to be tight before it lands downstream.

### (c) 85 patients any_autotransplant = TRUE with autotransplant_location IS NULL

Two paths:
- **Re-prompt just those 85 notes** with a location-focused mini-extractor (SCM / forearm / other / none-specified); cheaper if we have the infrastructure from migrations 54–58 still warm.
- **Document as "unknown site"** in downstream analyses and move on.

Recommendation: document-and-move. The SCM vs forearm distinction is clinically relevant but the 85 unknown-location patients are a small fraction of the 301 autotransplant patients; a separate mini-extractor adds scope for limited gain. Flag for the complications canonical scoping doc.

---

## Complications canonical — scoping (Script 364 + candidate)

Per the "after all 5 land" task 2, design a complications canonical joining parathyroid + RLN + wound + hypocalcemia on `surgery_episode_id`. Do NOT draft that here — open a separate prompt `cursor_prompts/CURSOR_PROMPT_MIG_64_COMPLICATIONS_CANONICAL_SCOPING_TBD.md`. Key decisions it must make:

1. Grain: per-surgery-episode (current thinking) vs per-patient. Per-episode supports temporal ordering and re-operation handling; per-patient matches every other tier-2. Lean toward per-episode given RLN and wound events are episode-local.
2. Join key: `surgery_episode_id` if available on every feeder; else fall back to (`research_id`, `surgery_date`) + tolerance window. Probe what's on `canonical_parathyroid_events_v1` and `canonical_operative_events_v1` first.
3. Source feeders: `canonical_parathyroid_*` (hypocalc, hypopara, autotransplant, incidental ptx), `canonical_airway_invasion_*.any_rln_*` + any preop-imaging RLN paralysis signal, wound-complication flags from op-notes (probe CPM + any LLM table that extracted them).
4. Whether mig 61 (parathyroid specificity) must land first. Recommendation: yes — tighten the ingredient before you fold it into a downstream table.

---

## Memory hooks (append to MEMORY.md after close-out lands)

```
- [Mig 60 invasion rollup v2 refresh](project_mig_60_invasion_rollup_refresh_closeout.md) — Closed 2026-04-24 at <SHA>; rollup rebuilt from v2 feeders; +6 new cols (rln/pT4a/pT4b/carotid/mediastinal/prevertebral); baseline pre/post deltas in closeout; mig 58 CF-a patched (1 pt); CF-b and CF-c deferred to mig 61 + complications scoping
```
