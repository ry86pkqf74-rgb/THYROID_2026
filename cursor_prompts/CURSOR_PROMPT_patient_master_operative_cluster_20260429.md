# Cursor Agent Task — `canonical_patient_master` OPERATIVE CLUSTER slice (Protocol v2)

**Generated:** 2026-04-29 (Cowork session, post mig_127 audit refinement)
**Working dir:** `/Users/ros/THyroid 2026`
**Tip of `origin/main`:** at handoff time — `git fetch && git pull` before starting
**Estimated effort:** 2-3 hours (~125 cols; first thematic slice of patient_master verification)
**Run order:** Lane 22 of next 3-prompt batch (run last — biggest scope; sets pattern for future patient_master slices)

---

## 1. Goal

Begin the **`canonical_patient_master` verification effort** by closing one thematic cluster: **the operative cluster** (~125 cols matching `op_*`, `surg*`, `*surgery*`, `operative_*`, `*procedure*`).

`canonical_patient_master` has 1,592 cols total / 1,588 not_started — far too large for a single Cursor lane. Verifying by thematic clusters keeps each lane bounded and checkpoints incremental progress.

This lane sets the pattern: future lanes can take pathology-cluster (~82 cols), lymph-node cluster (~80), labs cluster (~65), pmh_psh cluster (~64), us_imaging cluster (~44), rai cluster (~36), recurrence cluster (~30), fna cluster (~25), ete cluster (~20), survival cluster (~18), medications cluster (~17), molecular cluster (~7), complications (~5), frozen_section (~3), demographics (~2), and the residual `other` (~975).

---

## 2. Operative-cluster scope (probe at start)

```sql
SELECT column_name, data_type 
FROM information_schema.columns
WHERE table_catalog='thyroid_canonical_publication_v1_0' AND table_schema='main'
  AND table_name='canonical_patient_master'
  AND (column_name LIKE 'op_%' OR column_name LIKE 'surg%' 
       OR column_name LIKE '%surgery%' OR column_name LIKE 'operative_%' 
       OR column_name LIKE '%procedure%' OR column_name LIKE 'nlp_ne_procedures%'
       OR column_name LIKE 'nsqip_%')
ORDER BY column_name;
```

Expected ~125 cols. Sub-clusters:
- **`op_*`** (~80 cols): operative findings (gross_ete, drain_placed, esophageal_inv, local_invasion, etc.) — derive from `canonical_operative_events_v1` (verified mig_362) + LLM operative_detail entities
- **`op_nlp_*`** (~30 cols): NLP extracted operative findings (berry_ligament, drain, ebl, esophageal, gross_invasion, etc.) — derive from `note_entities_operative_detail` (raw upstream)
- **`surg*` / `*surgery*`** (~10 cols): surgery dates, counts, time-between — derive from `canonical_operative_events_v1`
- **`*procedure*`** (~5 cols): procedure code references — derive from `canonical_operative_procedure_codes_v1` (verified mig_118)
- **`nsqip_*`** (~4 cols): NSQIP registry data — derive from external NSQIP source (likely raw)
- **`first_surgery_date` + `_v2`** (special): TIMESTAMP vs DATE pair — flag CF-mig<N>-PM-FIRST-SURGERY-DATE-RETYPE if applicable

---

## 3. Methodology — derivation re-derivation against verified upstream

Pattern reference: `qc_framework_v1/migrations/106_parathyroid_patient_rollup_signoff.sql` (cohort-wide derivation).

### 3a. Per-col derivation map

For each not_started col, identify upstream + derivation:

**op_* derived cols (BOOL_OR / MIN(date) / COUNT pattern):**
- `op_drain_placed_any` → `BOOL_OR(canonical_operative_events_v1.drain_placed)`
- `op_esophageal_inv_any` → `BOOL_OR(canonical_operative_events_v1.esophageal_invasion_flag)` or via UNION with operative_detail entity
- `op_esophageal_inv_first_date` → `MIN(date)` per pt
- `op_n_surgeries_with_findings` → `COUNT(*) FILTER (WHERE has_finding)` per pt
- `op_intraop_gross_ete_any` → cross-source: ete_subgrade_events (mig_114) + operative_events
- `op_local_invasion_any` → cross-source: invasion_events (mig_95) + operative_events

**op_nlp_* (extraction-faithfulness vs note_entities_operative_detail):**
- `op_nlp_berry_ligament_*` → from note_entities_operative_detail entity rows tagged 'berry_ligament'
- `op_nlp_drain_*` → entity tag 'drain'
- `op_nlp_ebl_ml` → numeric extraction from operative_detail
- `op_nlp_extraction_method` → uniform value (extraction_method col on upstream)

**surg/surgery date cols:**
- `first_surgery_date` (TIMESTAMP) → `CAST(MIN(canonical_operative_events_v1.surgery_date_native) AS TIMESTAMP)` ⚠️ TIMESTAMP — file CF-mig<N>-PM-FIRST-SURGERY-DATE-RETYPE
- `first_surgery_date_v2` (DATE) → `MIN(canonical_operative_events_v1.surgery_date_native)::DATE` ✓ clean
- `days_between_first_second_surgery` → DATE_DIFF; check for both v1 and v2 variants

**procedure cols:**
- references to `canonical_operative_procedure_codes_v1` (verified mig_118)

### 3b. Cohort parity probe
```sql
SELECT 
  (SELECT COUNT(*) FROM main.canonical_patient_master) = 10871 AS cohort_match,
  (SELECT COUNT(DISTINCT research_id) FROM main.canonical_patient_master) = 10871 AS pts_match;
```

### 3c. Per-col drift probes
For each operative-cluster not_started col, run a per-col drift probe vs the derivation:
```sql
WITH derived AS (
  SELECT research_id, BOOL_OR(<flag_col>) AS expected
  FROM main.canonical_operative_events_v1
  GROUP BY research_id
)
SELECT COUNT(*) FILTER (WHERE pm.<col> IS DISTINCT FROM d.expected) AS drift
FROM main.canonical_patient_master pm
LEFT JOIN derived d ON pm.research_id = d.research_id;
```
Drift > 0 → indicates patient_master is stale relative to events; document, don't block.

### 3d. ⚠️ Date type CFs
- `first_surgery_date` is TIMESTAMP → flag CF-mig<N>-PM-FIRST-SURGERY-DATE-RETYPE (joins CF-100/117/119/120/mig122 batch)
- `first_surgery_date_v2` is DATE → confirm v2 was added as the clean replacement; check whether downstream consumers should migrate to v2
- Other `op_*_date` cols: many are DATE — verify each per col

### 3e. Sign-off SQL
File: `qc_framework_v1/migrations/<next-N>_patient_master_operative_cluster_signoff.sql`
- ~125 col flips with per-cluster verification_method labels
- table_signoff_registry NOT updated to verified (because thousands of cols still not_started in other clusters)
- Update n_verified count on signoff registry
- Document explicitly: "Operative cluster verified; pathology + lymph_node + labs + pmh_psh + us_imaging + rai + recurrence + fna + ete + survival + medications + molecular + complications + frozen_section + demographics + other clusters remain"

---

## 4. Acceptance gates

- ~125 operative-cluster cols flipped (or document why fewer)
- 0 drift on derivation re-derivation per col (or document expected drift with rationale)
- Cohort parity 10,871 confirmed
- CF rows recorded for date violations
- 5-gate audit re-run: gate 1 unchanged at 66 (patient_master STILL not_started overall — only its operative cluster is closed). gate 4 must remain 0 (every newly verified col has full metadata).

⚠️ **The 5-gate audit query treats `canonical_patient_master` as a single table** — gate 1 won't flip until ALL ~1,588 not_started cols are closed. Document partial progress in the close-out so future Cursor lanes know which clusters are done.

---

## 5. Don't touch (active parallel lanes)

- `canonical_recurrence_v1` Script 203 rebuild — Cursor lane 19 (paused at Logan-approval gate; if it lands, will affect 30-ish recurrence_* cols on patient_master — DEFER recurrence cluster to later lane)
- 5 tier3_extraction tables — Sibling lane 20
- `manuscript_workspace.*` tier3_helper batch — Sibling lane 21

---

## 6. Reference reading

Required:
- Auto-memory: `project_op_procedure_consolidation_script_362_closeout.md` (operative spine context)
- Auto-memory: `project_op_path_consolidation_script_361_closeout.md` (path family context — useful for cross-cluster handoff)
- Auto-memory: `project_op_procedure_codes_mig_118_closeout.md` (hybrid pattern for operative procedure codes)
- Auto-memory: `project_medications_parathyroid_families_complete_2026-04-29.md` (3 reusable patterns)
- Auto-memory: `feedback_clinical_dates_calendar_only.md`
- Auto-memory: `feedback_motherduck_direct_check.md`
- Auto-memory: `feedback_surgical_git_add.md`
- Auto-memory: `feedback_audit_regex_word_boundary.md` (mig_117 + mig_127 audit refinements)
- Repo: `qc_framework_v1/migrations/106_parathyroid_patient_rollup_signoff.sql` (cohort-wide derivation template)
- Repo: `qc_framework_v1/migrations/118_operative_procedure_codes_signoff_20260429.sql` (operative-family hybrid template)
- Repo: `qc_framework_v1/migrations/127_audit_refinement_na_filter_20260429.sql` (latest audit template)

---

## 7. File / commit conventions

- Author: `Logan Glosser <logan.glosser@gmail.com>`
- Single commit closing the operative cluster
- DuckDB CAST CURRENT_TIMESTAMP AS TIMESTAMP
- Surgical git add (no `-A`, no `scripts/output/` sweeps)

---

## 8. If something unexpected surfaces

- Patient count != 10,871 → cohort drift; STOP and reconcile vs canonical_patient_master.research_id
- Drift > 5% on any col → likely stale rollup; flag for rebuild and CF; do not block this cluster
- A col references a NOT YET VERIFIED upstream (e.g., `canonical_invasion_events_v1` is verified but `canonical_invasion_resolved_v1` may not be) → use the verified source if available; CF if no verified source exists
- More than 30 cols can't be cleanly mapped to an upstream → STOP, surface to Logan: those cols may need a build-script rerun before verification
- `first_surgery_date_v2` is identical to `first_surgery_date` (same values, just different types) → confirms the v2 was added as a DATE-typed parallel; verify both, retain v1 with CF until consumer migration is done

---

End of prompt. Lane 22 of new 3-prompt batch. Begins the patient_master verification effort with the operative cluster (~125 cols). Sets the pattern for future thematic-cluster lanes (pathology, lymph_node, labs, etc.). Update `MEMORY.md` with close-out entry — note this is the FIRST patient_master slice; future Cursor work continues by cluster.
