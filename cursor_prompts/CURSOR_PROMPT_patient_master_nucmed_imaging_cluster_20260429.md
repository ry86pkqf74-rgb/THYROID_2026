# Cursor Agent Task — `canonical_patient_master` NUCMED IMAGING CLUSTER (Protocol v2)

**Generated:** 2026-04-29 (Cowork session)
**Working dir:** `/Users/ros/THyroid 2026`
**Estimated effort:** 3 hours (~27 cols)
**Run order:** Lane 37 of new 4-prompt batch (mig_147)

---

## 1. Goal

Continue patient_master verification with the **nuclear medicine (nucmed) cluster** (~27 unverified cols covering nucmed scan metadata, RAI uptake, RAI-stimulated Tg/TgAb/TSH labs, dose tracking, and overall assessment).

Probe scope:

```sql
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_catalog='thyroid_canonical_publication_v1_0' AND table_schema='main'
  AND table_name='canonical_patient_master'
  AND column_name LIKE 'nucmed%'
  AND column_name NOT IN (
    SELECT column_name FROM main.canonical_column_verification_registry_v1
    WHERE table_name='canonical_patient_master' AND verification_status <> 'not_started'
  )
ORDER BY column_name;
```

Confirm count is **exactly 27** before proceeding. Note `nucmed_has_rai_scan` is in mig_142 RAI scope (in flight) — if it appears in this lane's probe, EXCLUDE and document collision.

Sub-clusters:

- **Nucmed exam metadata** (~5 cols): `nucmed_n_scans`, `nucmed_scan_types`, `nucmed_indication_first`, `nucmed_indication_last`, `nucmed_n_with_indication`
- **Nucmed impressions/findings** (~3 cols): `nucmed_findings_last`, `nucmed_impression_last`, `nucmed_n_with_impression`
- **Nucmed dose tracking** (~3 cols): `nucmed_cumulative_therapeutic_dose`, `nucmed_dose_max_parsed`, `nucmed_n_doses_parsed`
- **Nucmed-stimulated lab linkage** (~10 cols): `nucmed_first_scan_with_labs`, `nucmed_last_scan_with_labs`, `nucmed_lab_source`, `nucmed_n_tg_values`, `nucmed_n_tgab_values`, `nucmed_n_tsh_values`, `nucmed_tg_max`, `nucmed_tg_min`, `nucmed_tgab_max`, `nucmed_tgab_max_source`, `nucmed_tsh_is_stimulated`, `nucmed_tsh_max`
- **Nucmed RAI uptake quantification** (~2 cols): `nucmed_uptake_24hr_max`, `nucmed_uptake_pct_max`
- **Nucmed overall assessment** (~1 col): `nucmed_overall_assessment`

---

## 2. Methodology

### 2a. ⚠️ Upstream dependency check (FIRST STEP)

Verify nucmed upstream chain:
- `canonical_nucmed_*` family — check `information_schema.tables`
- Tier-1 LLM: `note_entities_llm_nucmed_*` or `note_entities_llm_imaging_nucmed_*`
- Lab linkage: `canonical_labs_tg_v1` (Tg+TgAb shared per `project_lab_consolidation_script_347_closeout.md`) and `canonical_labs_tsh_v1` for the Tg/TgAb/TSH cols
- RAI overlap: `canonical_rai_*` (Lane 31 mig_142 in-flight)

Document SSOT chain in each col's `verification_method`.

### 2b. Per-col derivation map (representative)

- `nucmed_n_scans` → COUNT(DISTINCT scan_id) per pt
- `nucmed_scan_types` → STRING_AGG distinct types per pt; list_sort (per `project_medications_parathyroid_families_complete_2026-04-29.md`)
- `nucmed_indication_first` / `_last` → indication from earliest/latest scan
- `nucmed_findings_last` / `nucmed_impression_last` → text from latest scan
- `nucmed_cumulative_therapeutic_dose` → SUM(dose_mci) for therapy scans per pt; cross-check vs `canonical_rai_*.rai_total_cumulative_dose_mci` (Lane 31). **Drift flag**: if nucmed and RAI canonicals disagree on cumulative dose by >5%, document precedence
- `nucmed_dose_max_parsed` → MAX(dose_mci) per pt
- `nucmed_uptake_24hr_max` / `nucmed_uptake_pct_max` → MAX uptake per pt (RAI uptake scans)
- `nucmed_first_scan_with_labs` / `_last_scan_with_labs` → MIN/MAX scan_date among scans with non-null Tg/TgAb/TSH lab linkage
- `nucmed_n_tg_values` → COUNT lab values linked to nucmed scans
- `nucmed_tg_max` / `_min` → MAX/MIN Tg from nucmed-linked labs (post-stimulation Tg)
- `nucmed_tgab_max` / `_tgab_max_source` → MAX TgAb + which lab provided it
- `nucmed_tsh_is_stimulated` → BOOL: any TSH measurement was stimulated (TSH > 30 mIU/L typical threshold)
- `nucmed_tsh_max` → MAX TSH per pt
- `nucmed_overall_assessment` → severity ladder per pt across nucmed scans

### 2c. ⚠️ Cohort-uniformity sanity check (CRITICAL)

For every BOOLEAN: §2c sweep. Most nucmed cols will be NULL for ~50-70% of cohort (only RAI-receiving pts get nucmed). Within the receiving subset:
- `nucmed_tsh_is_stimulated` should be near-100% TRUE for stimulated-Tg pts (the whole point of stim Tg is TSH > 30)
- `nucmed_overall_assessment` distribution should span the severity ladder

### 2d. ⚠️ Cross-cluster overlap with mig_142 RAI

Several nucmed cols overlap conceptually with RAI:
- `nucmed_cumulative_therapeutic_dose` vs `rai_total_cumulative_dose_mci`
- `nucmed_n_doses_parsed` vs `rai_n_episodes_with_dose`
- `nucmed_uptake_24hr_max` vs `rai_avidity` / `rai_avid_flag`

If mig_142 has already landed when you start, cross-validate. If still in-flight, do NOT touch RAI cols (`rai_*`); flag any drift in this lane's notes.

### 2e. ⚠️ Calendar-only dates

`nucmed_first_scan_with_labs`, `_last_scan_with_labs` MUST be DATE. CF-mig147-PM-NUCMED-DATE-RETYPE if TIMESTAMP.

### 2f. Sign-off SQL

File: `qc_framework_v1/migrations/147_patient_master_nucmed_imaging_cluster_signoff_20260429.sql`

```
batch_id = 'mig_147_patient_master_nucmed_imaging_cluster_20260429'
verification_method options:
  - 'derivation_vs_canonical_nucmed_<source>_v1'
  - 'extraction_faithfulness_vs_note_entities_llm_nucmed'
  - 'patient_level_aggregate_nucmed_per_scan'
  - 'cross_validate_dose_nucmed_vs_rai'
  - 'derivation_vs_canonical_labs_tg_v1' (nucmed_tg_*)
  - 'derivation_vs_canonical_labs_tsh_v1' (nucmed_tsh_*)
```

---

## 3. Acceptance gates

- ~27 cols flipped (or report scope adjustment if `nucmed_has_rai_scan` collides with mig_142)
- Cross-validation against mig_142 RAI cols if available
- All nucmed date cols are DATE
- gate 4 = 0
- PM `n_verified` advances by exactly the cluster count

---

## 4. Don't touch (active parallel lanes)

- RAI mig_142 — in flight; do NOT touch `rai_*` cols
- SmallClusters mig_143 — in flight
- US+imaging-generic mig_144, CT mig_145, MRI+PET mig_146 — sibling lanes

---

## 5. Reference reading

Required:
- Auto-memory: `feedback_clinical_dates_calendar_only.md`
- Auto-memory: `feedback_extraction_faithfulness_llm_canonical.md`
- Auto-memory: `feedback_motherduck_direct_check.md`
- Auto-memory: `project_lab_consolidation_script_347_closeout.md` (Tg+TgAb shared canonical)
- Auto-memory: `project_lab_ingestion_refactor_script_348_closeout.md`
- Repo: `qc_framework_v1/migrations/96_fna_days_to_surgery_recompute.sql`
- Repo: most-recent labs sign-off SQL (mig_134)

---

## 6. File / commit conventions

Same as siblings.

---

## 7. If something unexpected surfaces

- No upstream nucmed canonical → STOP, ask Logan
- Cumulative dose drift between nucmed and RAI canonicals > 10% → STOP, ask Logan; precedence is a clinical decision (RAI canonical is the more aggregated SSOT typically)
- TSH stimulated-threshold ambiguous (30 vs 25 mIU/L) → confirm with Logan
- Tg/TgAb lab linkage incomplete (some scans had labs but linkage table missing rows) → propose CF-mig147-LAB-LINKAGE-INCOMPLETE
- More than 5 cols can't be cleanly mapped → STOP, ask Logan

---

End of prompt. Lane 37 of 4-prompt batch (target: PM `n_verified` advances by 27).

After this batch closes, the remaining unverified PM cols are the **~446 "residual" bucket** which needs further sub-clustering investigation. Top sub-prefixes (by frequency) for that planning round: nlp_* (120), syn_* (34), n_* (18), prm_* (17), med_* (15), proc_* (14), gm_* (13), any_* (13), para_* (13), postop_* (12), tp_* (10), radtx_* (10), and miscellaneous.
