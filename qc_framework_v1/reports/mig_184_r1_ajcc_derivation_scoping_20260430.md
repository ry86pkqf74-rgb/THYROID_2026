# mig_184 — R1 AJCC derivation rule scoping

**Run ID:** `mig184_r1_ajcc_derivation_scoping_20260430`  
**Run timestamp (UTC):** `2026-04-30T01:15:12.404404+00:00`  
**Posture:** read-only MotherDuck scoping; no production DDL/DML executed.  
**Target DB:** `thyroid_canonical_publication_v1_0`  
**Carry-forward:** `CF-87-AJCC` closure track; Logan ratified R1 as a future apply strategy, but rules remain pending ratification.  

## Executive summary

- Queried **6,689** rows from `main.canonical_path_malignant_events_v1` and **10,871** rows from `main.canonical_patient_master` with read-only SELECTs.
- Authored a non-executable skeleton apply SQL with explicit `LOGAN MUST RATIFY RULES BEFORE EXECUTION` markers and `<TBD_LOGAN_RATIFIED_RULE_*>` placeholders.
- Exported four new Logan-adjudication CSVs in `exports/mig184_r1_adjudication_20260430/`; existing r1a was preserved. New row counts: r1b_n1_unspecified_no_location.csv=2,378, r1c_size_unavailable_t_uncalculable.csv=601, r1d_t4_invasion_evidence_review.csv=1,385, r1e_stage_group_age_cutoff_review.csv=1,255.
- Draft shift counts below are **scoping estimates only** using conservative placeholder derivation rules; they are not apply instructions.

## 1. Full AJCC7/8 rule spec

### AJCC8 T component (DTC/MTC path-event derivation)

- `T1a`: tumor ≤1.0 cm, limited to thyroid, no gross/anatomic ETE.
- `T1b`: tumor >1.0 cm and ≤2.0 cm, limited to thyroid, no gross/anatomic ETE.
- `T2`: tumor >2.0 cm and ≤4.0 cm, limited to thyroid, no gross/anatomic ETE.
- `T3a`: tumor >4.0 cm limited to thyroid. Prompt text also listed a strap-muscle alternate under T3a, but the AJCC8 operational rule assigns gross strap-muscle ETE to `T3b`; Logan should ratify final wording.
- `T3b`: gross ETE invading only strap muscles (sternohyoid, sternothyroid, thyrohyoid, omohyoid) regardless of size.
- `T4a`: gross ETE invading subcutaneous soft tissues, larynx, trachea, esophagus, or recurrent laryngeal nerve.
- `T4b`: gross ETE invading prevertebral fascia, mediastinal vessels, or encasing carotid artery.
- Microscopic/minimal ETE does **not** upstage T1/T2 in AJCC8.

### AJCC7 T component (DTC/MTC path-event derivation)

- `T1a`: tumor ≤1.0 cm, limited to thyroid.
- `T1b`: tumor >1.0 cm and ≤2.0 cm, limited to thyroid.
- `T2`: tumor >2.0 cm and ≤4.0 cm, limited to thyroid.
- `T3`: tumor >4.0 cm limited to thyroid **or** minimal/microscopic ETE into sternothyroid muscle/perithyroid soft tissues.
- `T4a`: moderately advanced disease with gross ETE into subcutaneous soft tissues, larynx, trachea, esophagus, or recurrent laryngeal nerve.
- `T4b`: very advanced disease invading prevertebral fascia, mediastinal vessels, or encasing carotid artery.

### AJCC8 N component

- `N0`: no nodal metastases; path-event proxy is `ln_examined > 0` and `ln_involved = 0`.
- `N0a`: cytologically/histologically confirmed benign nodes.
- `N0b`: no radiologic/clinical evidence of nodal disease.
- `N1a`: level VI or VII central-compartment/upper-mediastinal nodal metastases.
- `N1b`: unilateral, bilateral, contralateral lateral cervical levels I–V or retropharyngeal nodal metastases.
- Current path-event grain lacks anatomic nodal level/location, so `N1a`/`N1b` splitting requires Logan-ratified supplemental-source rules.

### AJCC7 N component

- `N0`: no regional nodal metastases.
- `N1a`: level VI central compartment metastases (pretracheal, paratracheal, prelaryngeal/Delphian).
- `N1b`: unilateral/bilateral/contralateral cervical or superior mediastinal metastases.
- AJCC7 vs AJCC8 differs for upper mediastinal/level VII handling; ratification must explicitly encode edition-specific mapping.

### M component

- `M0`: no distant metastases.
- `M1`: distant metastases present.
- Path-event rows currently carry copied `m_stage_ajcc7`/`m_stage_ajcc8`; the verified-finding source for a true re-derivation must be ratified before apply.

### AJCC8 stage group — differentiated thyroid carcinoma (DTC)

- Age <55: `I` for any T/any N/M0; `II` for any T/any N/M1.
- Age ≥55: `I` for T1–T2/N0 or NX/M0; `II` for T1–T2/N1/M0 or T3a–T3b/any N/M0; `III` for T4a/any N/M0; `IVA` for T4b/any N/M0; `IVB` for any T/any N/M1.

### AJCC7 stage group — differentiated thyroid carcinoma (DTC)

- Age <45: `I` for any T/any N/M0; `II` for any T/any N/M1.
- Age ≥45: `I` for T1/N0/M0; `II` for T2/N0/M0; `III` for T3/N0/M0 or T1–T3/N1a/M0; `IVA` for T4a/any N/M0 or T1–T3/N1b/M0; `IVB` for T4b/any N/M0; `IVC` for any T/any N/M1.

### MTC and ATC stage grouping

- MTC uses age-independent stage grouping: I=T1/N0/M0; II=T2–T3/N0/M0; III=T1–T3/N1a/M0; IVA=T4a/any N/M0 or T1–T3/N1b/M0; IVB=T4b/any N/M0; IVC=M1. Logan must ratify mixed `MTC | PTC` precedence.
- ATC is stage IV by definition; broad grouping should preserve IVA/IVB/IVC where T/M detail is available, otherwise `IV`.

## 2. Adjudication-gap enumeration

| rule                             | required_inputs                                               | availability                                                                  |   live_gap_count | adjudication_needed           | disposition                                                                        |
|:---------------------------------|:--------------------------------------------------------------|:------------------------------------------------------------------------------|-----------------:|:------------------------------|:-----------------------------------------------------------------------------------|
| T1/T2/T3 size cutoffs            | size_greatest_dimension_cm                                    | available as DOUBLE on canonical_path_malignant_events_v1                     |              601 | YES for size-unavailable rows | Export r1c; leave resolved T NULL unless Logan ratifies feeder fallback.           |
| ETE text to none/micro/gross/T4  | extrathyroidal_extension, gross_ete                           | available but messy (33 distinct non-null ETE strings)                        |             6158 | YES                           | Use r1a and report vocabulary; Logan ratifies text mapping before apply.           |
| T3b gross ETE strap muscles      | muscle-specific gross ETE evidence                            | not directly available on path-event; gross_ete is unlocalized                |             1572 | YES                           | Logan decides gross_ete -> T3b vs ambiguous bucket.                                |
| T4a/T4b anatomic invasion        | airway/trachea/esophagus/RLN/prevertebral/carotid/mediastinal | partially available via canonical_invasion_events_v1 plus path-event ETE text |             1464 | YES                           | Export r1d; Logan maps invasion_type/status/qualifier to T4a/T4b/exclude.          |
| N1a/N1b split                    | nodal level/location                                          | not available on path-event; only ln_involved/counts and ENE                  |             2378 | YES                           | Export r1b; ratify defer-as-N1 or join separate cervical LN sources.               |
| Stage group age cutoff           | age_at_surgery                                                | available on CPM only                                                         |             1255 | YES                           | Export r1e; recommend patient-grain stage group only using PM age.                 |
| DTC vs MTC vs ATC stage grouping | primary_histology / histology_final                           | available but mixed/unknown cases remain                                      |             6937 | YES                           | Logan ratifies mixed histology precedence; use DTC/MTC/ATC-specific CASE branches. |

### ETE draft bucket distribution

| ete_rule_bucket   |   n_events |
|:------------------|-----------:|
| present_ungraded  |       4586 |
| gross_unlocalized |       1572 |
| missing           |        445 |
| microscopic       |         86 |

### Patient histology-class draft distribution

| histology_class_for_stage_draft   |   n_patients |
|:----------------------------------|-------------:|
| UNKNOWN_REVIEW                    |         6898 |
| DTC                               |         3780 |
| MTC                               |          167 |
| ATC                               |           26 |

## 3. Cross-source drift cohort under proposed R1 draft derivation

### Patient grain (malignant CPM patients only)

| component                        | current_column    |   rows_total |   current_non_null |   draft_non_null |   paired_non_null |   paired_changes |   paired_change_pct |   draft_uncalculable |
|:---------------------------------|:------------------|-------------:|-------------------:|-----------------:|------------------:|-----------------:|--------------------:|---------------------:|
| t_stage_ajcc8_resolved_draft     | ajcc8_t_stage     |         4137 |               4128 |             3993 |              3991 |              428 |               10.72 |                  144 |
| n_stage_resolved_coarse_draft    | ajcc8_n_stage     |         4137 |               4077 |             2363 |              2363 |             1165 |               49.3  |                 1774 |
| m_stage_resolved_draft           | ajcc8_m_stage     |         4137 |               4137 |             4137 |              4137 |                0 |                0    |                    0 |
| stage_group_ajcc8_resolved_draft | ajcc8_stage_group |         4137 |               4128 |             3835 |              3833 |              366 |                9.55 |                  302 |
| t_stage_ajcc7_resolved_draft     | ajcc7_t_stage     |         4137 |               4127 |             4106 |              4099 |             2514 |               61.33 |                   31 |
| n_stage_resolved_coarse_draft    | ajcc7_n_stage     |         4137 |               4137 |             2363 |              2363 |             1218 |               51.54 |                 1774 |
| m_stage_resolved_draft           | ajcc7_m_stage     |         4137 |               4137 |             4137 |              4137 |             1798 |               43.46 |                    0 |
| stage_group_ajcc7_resolved_draft | ajcc7_stage_group |         4137 |               3882 |             3944 |              3740 |             2456 |               65.67 |                  193 |

### Path-event grain

| component                     | current_column   |   rows_total |   current_non_null |   draft_non_null |   paired_non_null |   paired_changes |   paired_change_pct |   draft_uncalculable |
|:------------------------------|:-----------------|-------------:|-------------------:|-----------------:|------------------:|-----------------:|--------------------:|---------------------:|
| t_stage_ajcc8_resolved_draft  | t_stage_ajcc8    |         6689 |               6443 |             6195 |              6182 |             1781 |               28.81 |                  494 |
| n_stage_resolved_coarse_draft | n_stage_ajcc8    |         6689 |               6632 |             3666 |              3666 |             1967 |               53.66 |                 3023 |
| m_stage_resolved_draft        | m_stage_ajcc8    |         6689 |               6689 |             6689 |              6689 |                0 |                0    |                    0 |
| t_stage_ajcc7_resolved_draft  | t_stage_ajcc7    |         6689 |               6443 |             6606 |              6406 |             5189 |               81    |                   83 |
| n_stage_resolved_coarse_draft | n_stage_ajcc7    |         6689 |               6632 |             3666 |              3666 |             1967 |               53.66 |                 3023 |
| m_stage_resolved_draft        | m_stage_ajcc7    |         6689 |               6689 |             6689 |              6689 |                0 |                0    |                    0 |

Interpretation: these are draft estimates with unratified rules. The apply lane must not use them until Logan resolves the adjudication CSVs and ratifies source precedence for ETE, anatomic invasion, nodal location, M-stage source, age broadcast, and histology class.

## 4. Logan adjudication CSVs

| CSV | rows | purpose |
|---|---:|---|
| r1a_ete_t_stage_upgrade_review.csv | preserved existing | Cowork-generated ETE→T-stage upgrade candidates. |
| r1b_n1_unspecified_no_location.csv | 2,378 | Generated by this mig_184 scoping lane. |
| r1c_size_unavailable_t_uncalculable.csv | 601 | Generated by this mig_184 scoping lane. |
| r1d_t4_invasion_evidence_review.csv | 1,385 | Generated by this mig_184 scoping lane. |
| r1e_stage_group_age_cutoff_review.csv | 1,255 | Generated by this mig_184 scoping lane. |

Minimum common columns include domain identifiers, relevant clinical inputs, `proposed_action`, blank `logan_decision`, blank `logan_notes`, and `staging_source_note`.

## 5. Recommended Logan dispositions before apply lane

1. **Ratify ETE text mapping**: decide which raw `extrathyroidal_extension` values map to absent/microscopic/gross/T4 and whether unlocalized `gross_ete=1` should default to AJCC8 `T3b` or remain ambiguous.
2. **Ratify T4 source mapping**: map `canonical_invasion_events_v1.invasion_type` + `finding_status` + `evidence_qualifier` to `T4a`, `T4b`, or exclude. Do not count absent/boilerplate entries.
3. **Ratify N-stage policy**: either keep positive path-event rows as coarse `N1` when location is absent, or define a governed supplemental-source join for central/lateral level mapping.
4. **Ratify M-stage source**: identify verified distant-metastasis source; copied legacy `m_stage_*` is acceptable only if Logan declares it the source of truth for R1.
5. **Ratify stage-group grain**: compute stage group at patient grain using CPM `age_at_surgery`; avoid broadcasting age-derived stage groups back to tumor rows unless explicitly needed.
6. **Ratify histology precedence**: define DTC/MTC/ATC class for mixed histologies such as `MTC | PTC`; route unresolved classes to manual review.

## Governance boundary

This lane did not run `ALTER`, `UPDATE`, `CREATE`, `DROP`, registry mutation, or provenance insert against MotherDuck. The skeleton SQL is a placeholder artifact only and intentionally contains unresolved `<TBD_LOGAN_RATIFIED_RULE_*>` markers.
