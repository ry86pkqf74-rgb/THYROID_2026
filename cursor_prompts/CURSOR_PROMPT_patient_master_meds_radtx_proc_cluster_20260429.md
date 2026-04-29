# Cursor Agent Task — `canonical_patient_master` MEDS + RADTX + PROCEDURES CLUSTER (Protocol v2)

**Generated:** 2026-04-29 (Cowork session)
**Estimated effort:** 2-3 hours (~39 cols)
**Run order:** Lane 41 of next 4-prompt batch (mig_151)

---

## 1. Goal

Continue patient_master verification with **3-bucket bundle** (~39 cols):

- **Medications** (~15 cols): `med_*` / `medications_*`
- **Radiation therapy (external beam, NOT RAI)** (~10 cols): `radtx_*`
- **Procedures** (~14 cols): `proc_*` / `procedure_*`

Probe scope:

```sql
SELECT column_name, data_type,
  CASE
    WHEN column_name LIKE 'med_%' OR column_name LIKE 'medications_%' THEN 'medications'
    WHEN column_name LIKE 'radtx_%' OR column_name LIKE 'radiation_%' THEN 'radtx'
    WHEN column_name LIKE 'proc_%' OR column_name LIKE 'procedure_%' THEN 'procedures'
  END AS bucket
FROM information_schema.columns
WHERE table_catalog='thyroid_canonical_publication_v1_0' AND table_schema='main'
  AND table_name='canonical_patient_master'
  AND (column_name LIKE 'med_%' OR column_name LIKE 'medications_%'
       OR column_name LIKE 'radtx_%' OR column_name LIKE 'radiation_%'
       OR column_name LIKE 'proc_%' OR column_name LIKE 'procedure_%')
  AND column_name NOT IN (
    SELECT column_name FROM main.canonical_column_verification_registry_v1
    WHERE table_name='canonical_patient_master' AND verification_status <> 'not_started'
  )
ORDER BY bucket, column_name;
```

Confirm count ~39 before proceeding.

---

## 2. Methodology

### 2a. Medications bucket

Anchor: `canonical_medications_events_v1` (per `project_medications_parathyroid_families_complete_2026-04-29.md`, closed at mig_105).

Likely cols: med counts, med-class flags (levothyroxine, calcium, vit-D, anticoag, beta-blocker), STRING_AGG of med names per pt. Use list_sort for set-equal probes (per `feedback_no_crossdomain_linkage_ids.md`).

CF carry-forward: `CF-mig58-STRING-AGG-ORDER` (medications STRING_AGG ordering) — re-confirm pattern.

Method: `derivation_vs_canonical_medications_events_v1`, `patient_level_aggregate_medications_per_class`.

### 2b. RadTx (external beam radiation) bucket

NOT RAI — this is external beam radiation therapy (XRT) for advanced/recurrent disease.

Anchor: depends on what canonical exists. Probe `information_schema.tables WHERE table_name LIKE '%radtx%' OR table_name LIKE '%radiation%' OR table_name LIKE '%xrt%'`. May derive from notes via `note_entities_llm_radtx` (Tier-1).

Likely cols: `radtx_received_flag`, `radtx_first_date`, `radtx_dose_gy`, `radtx_field_anatomic`, `radtx_indication`, `radtx_n_courses`.

If no canonical exists, use extraction-faithfulness vs Tier-1 LLM. Open `CF-mig151-RADTX-UPSTREAM-PENDING` if upstream unverified — but proceed if NLP-Tier-1 is the SSOT (per the mig_142 precedent — agent stopped on RAI upstream gate, but here a Tier-1-only chain is acceptable).

### 2c. Procedures bucket

Anchor: `canonical_op_procedure_codes_v1` / `canonical_operative_procedure_codes_v1` (per `project_op_procedure_codes_mig_118_closeout.md`, closed at mig_118).

Likely cols: procedure-code counts per pt, procedure category (lobectomy / total / completion / neck dissection central / neck dissection lateral / etc.), STRING_AGG of codes.

Method: `derivation_vs_canonical_op_procedure_codes_v1`, `patient_level_aggregate_procedure_per_code`.

### 2d. ⚠️ Cohort-uniformity sanity check

For every BOOLEAN flipped: §2c sweep.
- Levothyroxine flag → ~70-90% TRUE (most thyroid surgery pts go on T4)
- Calcium / Vit-D supplementation → 30-50% TRUE
- Beta-blocker → low-to-moderate
- RadTx received → rare (~5-10%)
- Procedure category flags → spread across the cohort

Flag any near-uniform-FALSE OR near-uniform-TRUE.

### 2e. ⚠️ Calendar-only dates

`radtx_first_date` etc. → DATE not TIMESTAMP. CF-mig151-DATE-RETYPE if violation.

### 2f. Sign-off SQL

File: `qc_framework_v1/migrations/151_patient_master_meds_radtx_proc_cluster_signoff_20260429.sql`

```
batch_id = 'mig_151_patient_master_meds_radtx_proc_cluster_20260429'
```

---

## 3. Acceptance gates

- ~39 cols flipped
- 0 drift on derivation
- Cohort-uniformity sweep clean
- gate 4 = 0
- PM advances by cluster count

---

## 4. Don't touch (active parallel lanes)

- mig_142 RAI (BLOCKED), imaging mig_145/146/147 (in flight), mig_148 RAI upstream
- Sibling lanes 39, 40, 42 (synoptic, parathyroid+postop+TP, NLP)

---

## 5. Reference reading

Required:
- Auto-memory: `project_medications_parathyroid_families_complete_2026-04-29.md`
- Auto-memory: `project_op_procedure_codes_mig_118_closeout.md`
- Auto-memory: `feedback_no_crossdomain_linkage_ids.md` (set-equal probe with list_sort)
- Auto-memory: `feedback_clinical_dates_calendar_only.md`
- Repo: `qc_framework_v1/migrations/118_operative_procedure_codes_signoff_20260429.sql`

---

## 6. File / commit conventions

Same as siblings.

---

## 7. If something unexpected surfaces

- No radtx canonical AND no Tier-1 NLP source → STOP, ask Logan; radtx may need an upstream-build lane first
- Levothyroxine flag < 50% → suspicious low (most post-thyroidectomy pts get T4); investigate
- More than 5 cols can't be cleanly mapped → STOP, ask Logan

---

End of prompt. Lane 41 of 4-prompt batch.
