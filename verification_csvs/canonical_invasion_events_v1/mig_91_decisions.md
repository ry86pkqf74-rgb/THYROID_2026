# mig_91 final decisions — canonical_invasion_events_v1 cancer orphans

Date: 2026-04-28 (Cowork session, post-Logan partial review)
Inputs:
- Logan's filled .xlsx (HIGH_POS: 13 FLIP; VOCAL_OR_NOT: 3 dispositioned; AMBIG_EC: noted "Tumor capsular invasion")
- Logan's stated rules:
  - Rule #1 (cancer-only): goiter ETE != tumor ETE
  - LN extranodal extension is NOT thyroid-tumor ETE; separate domain
  - Vocal cord paralysis is a different column from airway invasion
  - Many key-word-only rows need source-note context before deciding
- path_synoptics structured columns (`tumor_1_extrathyroidal_extension`,
  `tumor_1_extranodal_extension`, `tumor_1_capsule`, `tumor_1_capsular_invasion`,
  `tumor_1_histologic_type`)
- path_synoptics free text (`synoptic_diagnosis`, `path_diagnosis_summary`,
  `microscopic_description`)

## Summary

47 cancer orphans split into final dispositions:

| Disposition | n | Notes |
|---|---|---|
| FLIP_TO_PRESENT (keep invasion_type) | 16 | 13 HIGH_POS + 3 KEYWORD source-confirmed |
| RECLASS to `capsular` + FLIP_TO_PRESENT | 4 | KEYWORD encapsulated-tumor pattern |
| REJECT (LN ENE — different domain) | 17 | 8 original LN_ENE + 4 AMBIG_EC + 5 KEYWORD source-confirmed |
| RECLASS to vocal-cord / mass-effect col | 2 | Logan dispositioned (5048 vocal cord, 11862 mass effect) |
| REJECT (incidental finding) | 1 | Logan dispositioned (12129 laryngocele) |
| ACCEPT downgrade (correct weakening) | 4 | 9174 vasc-suspicious, 9209 N/A, 9224 CAP echo, 9829 microcarcinoma no ETE |
| NEEDS_CONTEXT — CT rows pending | 3 | 5186 arytenoid sclerosis, 4107 ETE on PET/CT, 5048 ETE on CT |
| **TOTAL** | **47** | |

Of the 3 NEEDS_CONTEXT, all are radiology-modality findings; need ct_imaging
note text for the next round.

## Per-row dispositions

### FLIP_TO_PRESENT — 16 rows

**Logan's HIGH_POS (13):**
- 6062 airway ct: "extension through the left cricothyroid membrane with loss of the fat plane"
- 6062 airway ct: "involvement of left cricothyroid membrane"
- 6869 airway ct: "cartilage erosion by the mass"
- 10932 airway ct: "cricoid cartilage involvement"
- 1908 gross_ete ct: "mild extension into the tracheoesophageal groove" (×2)
- 5531 gross_ete ct: "circumferential soft tissue within the upper mediastinum surrounding the trachea and esophagus"
- 6062 gross_ete ct: "extrathyroidal extension identified on CT: left cricothyroid membrane"
- 6869 gross_ete ct: "extrathyroidal extension into the right tracheoesophageal groove"
- 11388 gross_ete ct: "Aggressive appearing right thyroid gland mass which extends into the right brachiocephalic vein and the SVC causing near complete vascular occlusion" (conf=1.0)
- 9683 soft_tissue op_note: "strap muscle invasion"
- 10258 soft_tissue synoptic_path: "strap muscle invasion"
- 12043 soft_tissue synoptic_path: "extrathyroidal extension (ETE), strap muscle invasion"

**KEYWORD source-text confirmed positive (3):**
- 2073 soft_tissue synoptic_path: "FOLLICULAR VARIANT OF PAPILLARY CARCINOMA, WITH FOCAL EXTENSION TO ADJACENT PERITHYROIDAL TISSUES" → microscopic ETE present
- 9636 soft_tissue synoptic_path: "microscopic carcinoma involves THYROID capsule & abuts skeletal muscle" → minimal/microscopic ETE present
- 10872 soft_tissue synoptic_path: "Carcinoma focally invades into hyoid bone" → gross ETE present (TGDC PTC)

### RECLASS to `capsular` + FLIP_TO_PRESENT — 4 rows

Per Logan's rule: "extracapsular extension" in encapsulated-tumor context = tumor capsule invasion → invasion_type='capsular', finding_status='present'.

- 2641 PTC `tumor_1_capsule`='encapsulated': "extrathyroidal extension (ETE)"
- 5048 PTC `tumor_1_capsule`='totally encapsulated' synoptic_path: "extrathyroidal extension"
- 8825 PTC `tumor_1_capsule`='encapsulated': "extrathyroidal extension"
- 11201 FTUMP (encapsulated by definition): "focal area of pseudopodal tumor extension" — pseudopod invasion is the canonical capsular-invasion pattern in encapsulated FTUMP

### REJECT — LN extranodal extension (17 rows)

Per Logan's rule: LN ENE is a separate domain, not thyroid-tumor ETE. These rows should be removed from canonical_invasion_events_v1's `soft_tissue` / `perineural` invasion types.

**Original LN_ENE bucket (8):**
- 512 perineural synoptic_path: "focal pericapsular invasion identified in one lymph node"
- 495 soft_tissue synoptic_path: "extranodal extension"
- 5832 soft_tissue synoptic_path: "extranodal extension present"
- 9020 soft_tissue synoptic_path: "extranodal extension is present"
- 10691 soft_tissue synoptic_path: "extranodal extension (less than 1 mm)"
- 10691 soft_tissue synoptic_path: "extranodal extension (at least 1 mm)"
- 10691 soft_tissue synoptic_path: "focal microscopic extranodal extension (less than 1 mm)"
- 11111 soft_tissue synoptic_path: "focal extranodal tumor extension present"

**AMBIG_EC reclassified per structured `tumor_1_histologic_type`='metastatic PTC' + `tumor_1_extranodal_extension`='present' (4):**
- 1085: "extracapsular extension is present" — metastatic PTC, t1_ene='present'
- 1266: "focal extracapsular extension" — metastatic PTC, t1_ene='present\nright neck dissection'
- 1909: "extracapsular extension present" — no structured row available; pattern matches; defer-or-reject (ambiguous)
- 4122: "focal extracapsular extension" — metastatic PTC, t1_ene='present'

**KEYWORD reclassified per structured `tumor_1_histologic_type` starts with 'metastatic' + `tumor_1_extranodal_extension`='present' (4):**
- 495: "widely infiltrative" — metastatic PTC follicular, t1_ene='present'
- 1970: "extrathyroidal extension" — metastatic/recurrent PTC, t1_ene='present'
- 2667: "extrathyroidal extension" — metastatic PTC, t1_ene='present'
- 4015: "extrathyroidal extension" — metastatic MTC, t1_ene='present'

**KEYWORD source-text confirmed LN ENE (1):**
- 2627: "extrathyroidal extension (ETE)" — synoptic shows "ONE IN SEVEN LYMPH NODES POSITIVE FOR METASTATIC SQUAMOUS CELL CARCINOMA WITH EXTRANODAL EXTENSION" + "FOCUS OF INTRANODAL THYROID TISSUE, CONSISTENT WITH METASTATIC PAPILLARY CARCINOMA" — the LLM extracted "ETE" from the LN ENE phrase, not thyroid-tumor ETE.

### RECLASS to vocal-cord / mass-effect column (2)

- 5048 airway ct: "medialization of the right true vocal cord and arytenoid" → vocal cord paralysis column (Logan's rule)
- 11862 airway ct: "superior deviation towards the left" → mass effect column

### REJECT — incidental finding (1)

- 12129 airway ct: "Small laryngoceles bilaterally" — incidental, not invasion (Logan)

### ACCEPT — downgrade was correct (4)

- 9174 vascular_microscopic synoptic_path: "focus suspicious but not diagnostic" → arc=present → live=suspected. Correct weakening (focus suspicious is exactly the 'suspected' threshold).
- 9209 soft_tissue synoptic_path: "N/A" → arc=absent → live=indeterminate. LLM saw nothing meaningful; downgrade defensible.
- 9224 PTC: "Extrathyroidal extension:" appears as bare CAP template field header followed by "Regional Lymph Nodes: Not identified" — classic CAP template echo, no actual ETE finding. Same pattern as airway mig_82 (18 CAP echoes caught).
- 9829 PTC microcarcinoma: "Papillary thyroid microcarcinoma, oncocytic subtype" + "coalescing thyroid Follicular nodular disease... no evidence of invasion". No actual ETE evidence; microcarcinoma + nodular goiter context. The "extrathyroidal extension" qualifier was a CAP template field echo.

### NEEDS_CONTEXT — pending CT-modality source text (3)

These are CT-imaging-modality KEYWORD rows; need ct_imaging note text or
LLM source extraction (note_entities_llm_*_v2 with parsed_json evidence_quote)
to decide.

- 5186 airway ct: "sclerosis of the left arytenoid cartilage" — could be cancer-related or age-related; need full CT report context
- 4107 gross_ete ct: "extrathyroidal extension identified on PET/CT" — bare keyword, need CT/PET report
- 5048 gross_ete ct: "extrathyroidal extension identified on CT" — bare keyword, need CT report

## BENIGN orphans (54)

All confirmed benign per `canonical_path_benign_events_v1` presence (no
malignant pathology). Per Rule #1 (cancer-only), Script 363 correctly
downgraded these — they are massive goiter / MNG / multinodular substernal
extension being mis-extracted as malignant ETE. **Default disposition =
ACCEPT all (audit-confirm via spot check).**

## evidence_qualifier / evidence_span_hash / confidence (76 rows)

Mostly row-pair swaps between near-duplicate rows on patients 5986, 9846 +
1 typo fix ("minimally invasvie" → "minimally invasive"). Informational —
no decision needed.

## Linkage cluster

0 diffs vs pre-363 across linkage_method, n_candidate_episodes,
linkage_ambiguous_multi_episode. The 759-group ambiguous-linkage CSV is
unchanged from verified pre-363 state — defer multi-finding rename via
CF-91-LINKAGE-COL-NAME.

## Sign-off path

mig_91b (next session, after Logan confirms):

1. Apply UPDATEs:
   - 16 rows: SET finding_status='present' WHERE invasion_event_id IN (...)
   - 4 rows: SET invasion_type='capsular', finding_status='present' WHERE ...
   - 17 rows: DELETE FROM main.canonical_invasion_events_v1 WHERE invasion_event_id IN (...) — these are LN ENE, not thyroid-tumor ETE
   - 2 rows: handle vocal-cord and mass-effect reclass (target column TBD; may need a new invasion_type or move to a different canonical)
   - 1 row: DELETE (incidental laryngocele)
   - 4 ACCEPT rows: no change (already 'indeterminate' or 'suspected')

2. After applying, re-pull the 3 NEEDS_CONTEXT CT rows with surrounding ct_imaging note text; one more mini-review.

3. Flag all 11 not_started cols in canonical_column_verification_registry_v1 with verification_status='verified', signoff_migration='qc_framework_v1/migrations/91b_invasion_events_apply_decisions.sql'.

4. Refresh canonical_table_signoff_registry_v1 (table_status='verified').

5. Push canonical_invasion_events_v1 to verified count: **8/184 tables, ~238 cols.**
