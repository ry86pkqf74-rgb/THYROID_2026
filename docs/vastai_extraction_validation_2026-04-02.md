# VastAI Extraction Validation And Fleet Status 2026-04-02

Fresh live audit taken on 2026-04-02 after retiring unstable H200_F, handing off its unfinished work to H200_G, and revalidating completed parquet outputs against the source note corpus.

## Validation scope

- Source parquet: `processed/remaining/clinical_notes_long.parquet`
- Completed extraction artifacts checked: all parquet files under `processed/output/v2_parquets/`
- Validation basis: normalized join on `note_row_id` plus field-level checks for `research_id`, `note_date`, `source_workbook`, `source_sheet`, and `source_column`

## Linkage validation result

All completed parquet artifacts passed the core provenance and source-linkage checks.

- `UNMATCHED=0` for every validated parquet
- `MISMATCH_RESEARCH_ID=0` for every validated parquet
- `MISMATCH_NOTE_DATE=0` for every validated parquet
- `MISMATCH_WORKBOOK=0` for every validated parquet
- `MISMATCH_SHEET=0` for every validated parquet
- `MISMATCH_COLUMN=0` for every validated parquet
- `INVALID_JSON=0` for every validated parquet

This confirms that the completed extracted rows still resolve back to the original source note rows and preserve the expected source linkage metadata.

## Validation caveats

These did not break source linkage, but they remain schema-quality observations:

- Older completed artifacts missing `preprocessed_at_utc`: `combined`, `complications`, `genetics`, `imaging`, `labs`, `medications`, `pathology`, `physical_exam`, `problem_list`, `procedures`
- Some `result_json` payloads do not contain an `entities` key and instead represent valid negative or alternate payload shapes. This is not a provenance failure, but downstream consumers should not assume `entities` is always present.

## Current live fleet

Input corpus size per active domain: `11,037` notes.

### Primary H200

- Vast instance ID: `33534710`
- Direct SSH verified: `ssh -p 43384 -o StrictHostKeyChecking=no root@107.206.71.138`
- Vast broker status: `ssh1.vast.ai:14710` is currently closing during key exchange
- Active worker: `past_medical_hx`
- Current queue: `past_medical_hx rad_treatment synoptic_pathology_enrichment`
- Current counts:
  - `dynamic_risk_response`: `11037/11037`
  - `past_medical_hx`: `5438/11037`
  - `rad_treatment`: `10/11037`
  - `synoptic_pathology_enrichment`: `0/11037`
- GPU at audit: `100%`

### H200_G

- Vast instance ID: `33964874`
- SSH: `ssh -p 14874 -o StrictHostKeyChecking=no root@ssh5.vast.ai`
- Active worker: `past_surgical_hx`
- Current queue: `past_surgical_hx operative_details patient_decision_adherence`
- Current counts:
  - `functional_outcomes`: `11037/11037`
  - `past_surgical_hx`: `2174/11037`
  - `operative_details`: `0/11037`
  - `patient_decision_adherence`: `425/11037`
- GPU at audit: `92%`
- Operational note: this host now owns the partial `patient_decision_adherence` checkpoint copied from retired H200_F.

### H200_H2

- Vast instance ID: `33968613`
- SSH: `ssh -p 18612 -o StrictHostKeyChecking=no root@ssh9.vast.ai`
- Active worker: `complications_rln_laryngoscopy`
- Current queue: `complications_rln_laryngoscopy vascular_invasion tg_kinetics presenting_symptoms molecular_thyroseq_afirma`
- Current counts:
  - `complications_rln_laryngoscopy`: `1200/11037`
  - `vascular_invasion`: `9424/11037`
  - `tg_kinetics`: `681/11037`
  - `presenting_symptoms`: `587/11037`
  - `molecular_thyroseq_afirma`: `0/11037`
- GPU at audit: `100%`

## Balanced domain ledger

### Completed locally and validated

- `airway_invasion`: `11037/11037`
- `combined`: `11037/11037`
- `complications`: `11037/11037`
- `dynamic_risk_response`: `11037/11037`
- `functional_outcomes`: `11037/11037`
- `genetics`: `11037/11037`
- `imaging`: `11037/11037`
- `labs`: `11037/11037`
- `medication_management`: `11037/11037`
- `medications`: `11037/11037`
- `operative_v2_enrichment`: `11037/11037`
- `parathyroid_per_gland`: `11037/11037`
- `pathology`: `11037/11037`
- `physical_exam`: `11037/11037`
- `problem_list`: `11037/11037`
- `procedures`: `11037/11037`
- `rai_detailed`: `11037/11037`
- `recurrence`: `11037/11037`
- `recurrence_detailed`: `11037/11037`
- `staging`: `11037/11037`
- `survival_followup`: `11037/11037`
- `tirads_granular`: `11037/11037`

### Remaining active domains with one owner each

- `Primary_H200`
  - `past_medical_hx`: `5438/11037`
  - `rad_treatment`: `10/11037`
  - `synoptic_pathology_enrichment`: `0/11037`
- `H200_G`
  - `past_surgical_hx`: `2174/11037`
  - `operative_details`: `0/11037`
  - `patient_decision_adherence`: `425/11037`
- `H200_H2`
  - `complications_rln_laryngoscopy`: `1200/11037`
  - `vascular_invasion`: `9424/11037`
  - `tg_kinetics`: `681/11037`
  - `presenting_symptoms`: `587/11037`
  - `molecular_thyroseq_afirma`: `0/11037`

No overlapping live ownership was observed across the three active hosts at the time of this audit.

## Retired host disposition

- H200_F instance `33939816` was destroyed after local backup of its completed `survival_followup` artifacts and partial `patient_decision_adherence` checkpoint.
- `patient_decision_adherence` was seeded onto H200_G before destruction.
- Post-destroy verification: the instance disappeared from `vastai show instances` and stopped accepting SSH.

## Local residue cleanup

- The stray local repo-root `output/` residue was non-canonical and safe to remove after this report was recorded.
- Canonical validated artifacts remain under `processed/output/v2_parquets/`.