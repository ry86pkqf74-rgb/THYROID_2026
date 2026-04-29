# mig_175 — mig_136 Days-Semantic Adjudication Package

**Date:** 2026-04-29
**Lane:** 64 / mig_175
**Batch:** `mig_175_mig136_days_semantic_adjudication_20260429`
**Posture:** read-only MotherDuck profile and Logan decision package; no data writes.
**Target DB:** `thyroid_canonical_publication_v1_0`
**Target table:** `main.canonical_patient_master`
**Replay SQL:** `qc_framework_v1/migrations/175_days_semantic_probes_20260429.sql`

## Executive summary

The `CF-mig136-DAYS-SEMANTIC` carry-forward cluster is confirmed at **58 registry columns**. The profile shows that only **6 columns are actual numeric day-offset metrics** whose anchor needs adjudication. The remaining **52 columns are not themselves day offsets**:

- 21 boolean PMH/family-history flags.
- 20 count metrics.
- 4 categorical/provenance text columns.
- 1 confidence score.
- 6 source first/event date columns that feed the day metrics but are not themselves anchored day offsets.

Across the 6 actual day-offset columns, current stored values exactly match **Option B: `DATE_DIFF('day', first_surgery_date, event_date)`**. There were **0 patient-level and 0 patient-column mismatches** when recalculating against `first_surgery_date`. Re-anchoring to event-start (Option A) would change **914 patients / 1,668 patient-column cells** relative to current values. Re-anchoring to last-contact/LKA (Option C) would change **877 patients / 1,609 patient-column cells** relative to current values.

**Recommendation:** Ratify **Option B** for the 6 actual `*_days_from_surg` metrics, because it is already the implemented formula, matches the column names, and preserves the existing PM/manuscript convention where negative values indicate pre-operative PMH mentions and positive values indicate post-operative mentions. Open a follow-up cleanup lane to drop or reclassify `CF-mig136-DAYS-SEMANTIC` from the 46 clearly non-date columns, and optionally move the 6 source date columns to a paired-source-date provenance note rather than treating them as anchor-decision columns.

## Live scope profile

### Registry scope

Read-only probe:

```sql
SELECT column_name, data_type, COALESCE(verification_status,'unknown') AS status
FROM main.canonical_column_verification_registry_v1
WHERE notes ILIKE '%CF-mig136-DAYS-SEMANTIC%'
ORDER BY column_name;
```

Result: **58 rows**, all `verification_status = verified`.

### Sub-type counts

| Sub-type | n_cols | Examples |
|---|---:|---|
| Boolean PMH/family-history flag; no date anchor | 21 | `pmhx_nlp_afib`, `pmhx_nlp_asthma`, `pmhx_nlp_autoimmune_thyroid_hx`, `pmhx_nlp_breast_cancer`, `pmhx_nlp_cad` |
| Categorical/provenance text; no date anchor | 4 | `pmhx_nlp_comorbidity_list`, `pmhx_nlp_extraction_method`, `pmhx_nlp_note_types`, `pmhx_nlp_smoking_status` |
| Confidence score; no date anchor | 1 | `pmhx_nlp_radiation_exposure_confidence` |
| Count metric; no date anchor | 20 | `pmhx_nlp_afib_n_mentions`, `pmhx_nlp_asthma_n_mentions`, `pmhx_nlp_autoimmune_thyroid_hx_n_mentions`, `pmhx_nlp_breast_cancer_n_mentions`, `pmhx_nlp_cad_n_mentions` |
| Date-derived days-from-surgery metric | 6 | `pmhx_nlp_diabetes_first_days_from_surg`, `pmhx_nlp_hypertension_first_days_from_surg`, `pmhx_nlp_hyperthyroidism_first_days_from_surg`, `pmhx_nlp_hypothyroidism_first_days_from_surg`, `pmhx_nlp_obesity_first_days_from_surg` |
| Source first/event date; paired source, not day offset | 6 | `pmhx_nlp_diabetes_first_date`, `pmhx_nlp_hypertension_first_date`, `pmhx_nlp_hyperthyroidism_first_date`, `pmhx_nlp_hypothyroidism_first_date`, `pmhx_nlp_obesity_first_date` |

## Full 58-column profile

| Column | Type | Status | Sub-type | Non-null | Distinct | True rows |
|---|---|---|---|---:|---:|---:|
| `pmhx_nlp_afib` | BOOLEAN | verified | boolean PMH/family-history flag | 3,895 | 2 | 174 |
| `pmhx_nlp_afib_n_mentions` | BIGINT | verified | count metric | 3,895 | 4 |  |
| `pmhx_nlp_asthma` | BOOLEAN | verified | boolean PMH/family-history flag | 3,895 | 2 | 475 |
| `pmhx_nlp_asthma_n_mentions` | BIGINT | verified | count metric | 3,895 | 5 |  |
| `pmhx_nlp_autoimmune_thyroid_hx` | BOOLEAN | verified | boolean PMH/family-history flag | 290 | 2 | 80 |
| `pmhx_nlp_autoimmune_thyroid_hx_n_mentions` | BIGINT | verified | count metric | 290 | 4 |  |
| `pmhx_nlp_breast_cancer` | BOOLEAN | verified | boolean PMH/family-history flag | 3,895 | 2 | 425 |
| `pmhx_nlp_breast_cancer_n_mentions` | BIGINT | verified | count metric | 3,895 | 5 |  |
| `pmhx_nlp_cad` | BOOLEAN | verified | boolean PMH/family-history flag | 3,895 | 2 | 224 |
| `pmhx_nlp_cad_n_mentions` | BIGINT | verified | count metric | 3,895 | 4 |  |
| `pmhx_nlp_ckd` | BOOLEAN | verified | boolean PMH/family-history flag | 3,895 | 2 | 221 |
| `pmhx_nlp_ckd_n_mentions` | BIGINT | verified | count metric | 3,895 | 5 |  |
| `pmhx_nlp_coagulopathy` | BOOLEAN | verified | boolean PMH/family-history flag | 290 | 2 | 13 |
| `pmhx_nlp_comorbidity_list` | VARCHAR | verified | categorical/provenance text | 3,895 | 762 |  |
| `pmhx_nlp_copd` | BOOLEAN | verified | boolean PMH/family-history flag | 3,895 | 2 | 107 |
| `pmhx_nlp_copd_n_mentions` | BIGINT | verified | count metric | 3,895 | 3 |  |
| `pmhx_nlp_depression` | BOOLEAN | verified | boolean PMH/family-history flag | 3,895 | 2 | 399 |
| `pmhx_nlp_depression_n_mentions` | BIGINT | verified | count metric | 3,895 | 4 |  |
| `pmhx_nlp_diabetes` | BOOLEAN | verified | boolean PMH/family-history flag | 3,895 | 2 | 1,466 |
| `pmhx_nlp_diabetes_first_date` | DATE | verified | source first/event date | 527 | 457 |  |
| `pmhx_nlp_diabetes_first_days_from_surg` | INTEGER | verified | date-derived days-from-surgery metric | 396 | 334 |  |
| `pmhx_nlp_diabetes_n_mentions` | BIGINT | verified | count metric | 3,895 | 5 |  |
| `pmhx_nlp_extraction_method` | VARCHAR | verified | categorical/provenance text | 3,895 | 1 |  |
| `pmhx_nlp_family_hx_cancer` | BOOLEAN | verified | boolean PMH/family-history flag | 290 | 2 | 16 |
| `pmhx_nlp_family_hx_thyroid_n_mentions` | BIGINT | verified | count metric | 290 | 4 |  |
| `pmhx_nlp_gerd` | BOOLEAN | verified | boolean PMH/family-history flag | 3,895 | 2 | 478 |
| `pmhx_nlp_gerd_n_mentions` | BIGINT | verified | count metric | 3,895 | 4 |  |
| `pmhx_nlp_hypertension` | BOOLEAN | verified | boolean PMH/family-history flag | 3,895 | 2 | 1,775 |
| `pmhx_nlp_hypertension_first_date` | DATE | verified | source first/event date | 680 | 509 |  |
| `pmhx_nlp_hypertension_first_days_from_surg` | INTEGER | verified | date-derived days-from-surgery metric | 373 | 316 |  |
| `pmhx_nlp_hypertension_n_mentions` | BIGINT | verified | count metric | 3,895 | 5 |  |
| `pmhx_nlp_hyperthyroidism` | BOOLEAN | verified | boolean PMH/family-history flag | 3,895 | 2 | 1,163 |
| `pmhx_nlp_hyperthyroidism_first_date` | DATE | verified | source first/event date | 665 | 487 |  |
| `pmhx_nlp_hyperthyroidism_first_days_from_surg` | INTEGER | verified | date-derived days-from-surgery metric | 246 | 152 |  |
| `pmhx_nlp_hyperthyroidism_n_mentions` | BIGINT | verified | count metric | 3,895 | 4 |  |
| `pmhx_nlp_hypothyroidism` | BOOLEAN | verified | boolean PMH/family-history flag | 3,895 | 2 | 1,962 |
| `pmhx_nlp_hypothyroidism_first_date` | DATE | verified | source first/event date | 1,066 | 706 |  |
| `pmhx_nlp_hypothyroidism_first_days_from_surg` | INTEGER | verified | date-derived days-from-surgery metric | 672 | 554 |  |
| `pmhx_nlp_hypothyroidism_n_mentions` | BIGINT | verified | count metric | 3,895 | 5 |  |
| `pmhx_nlp_lung_cancer` | BOOLEAN | verified | boolean PMH/family-history flag | 3,895 | 2 | 156 |
| `pmhx_nlp_lung_cancer_n_mentions` | BIGINT | verified | count metric | 3,895 | 4 |  |
| `pmhx_nlp_men_syndrome` | BOOLEAN | verified | boolean PMH/family-history flag | 290 | 2 | 6 |
| `pmhx_nlp_n_comorbidities` | BIGINT | verified | count metric | 3,895 | 10 |  |
| `pmhx_nlp_n_source_notes` | BIGINT | verified | count metric | 3,895 | 4 |  |
| `pmhx_nlp_note_types` | VARCHAR | verified | categorical/provenance text | 3,895 | 51 |  |
| `pmhx_nlp_obesity` | BOOLEAN | verified | boolean PMH/family-history flag | 3,895 | 2 | 523 |
| `pmhx_nlp_obesity_first_date` | DATE | verified | source first/event date | 223 | 203 |  |
| `pmhx_nlp_obesity_first_days_from_surg` | INTEGER | verified | date-derived days-from-surgery metric | 152 | 133 |  |
| `pmhx_nlp_obesity_n_mentions` | BIGINT | verified | count metric | 3,895 | 4 |  |
| `pmhx_nlp_osteoporosis` | BOOLEAN | verified | boolean PMH/family-history flag | 290 | 2 | 22 |
| `pmhx_nlp_prior_cancer_hx` | BOOLEAN | verified | boolean PMH/family-history flag | 290 | 2 | 162 |
| `pmhx_nlp_prior_cancer_hx_n_mentions` | BIGINT | verified | count metric | 290 | 7 |  |
| `pmhx_nlp_radiation_exposure` | BOOLEAN | verified | boolean PMH/family-history flag | 290 | 2 | 33 |
| `pmhx_nlp_radiation_exposure_confidence` | DOUBLE | verified | confidence score | 33 | 9 |  |
| `pmhx_nlp_radiation_exposure_date` | DATE | verified | source first/event date | 29 | 29 |  |
| `pmhx_nlp_radiation_exposure_days_from_surg` | INTEGER | verified | date-derived days-from-surgery metric | 29 | 28 |  |
| `pmhx_nlp_radiation_exposure_n_mentions` | BIGINT | verified | count metric | 290 | 5 |  |
| `pmhx_nlp_smoking_status` | VARCHAR | verified | categorical/provenance text | 20 | 12 |  |

## Actual day-offset columns and anchor verification

The six actual day-offset columns are paired with six source date columns:

| Source date | Day-offset column |
|---|---|
| `pmhx_nlp_diabetes_first_date` | `pmhx_nlp_diabetes_first_days_from_surg` |
| `pmhx_nlp_hypertension_first_date` | `pmhx_nlp_hypertension_first_days_from_surg` |
| `pmhx_nlp_hyperthyroidism_first_date` | `pmhx_nlp_hyperthyroidism_first_days_from_surg` |
| `pmhx_nlp_hypothyroidism_first_date` | `pmhx_nlp_hypothyroidism_first_days_from_surg` |
| `pmhx_nlp_obesity_first_date` | `pmhx_nlp_obesity_first_days_from_surg` |
| `pmhx_nlp_radiation_exposure_date` | `pmhx_nlp_radiation_exposure_days_from_surg` |

### Per-column live impact

| Day-offset column | Event dates | Current day values | Pre-surgery event dates | On/after-surgery event dates | Option A cells changed vs current | Option B mismatches vs recalculated first-surgery anchor | Option C cells changed vs current | Current min | Current max | LKA min | LKA max |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `pmhx_nlp_diabetes_first_days_from_surg` | 527 | 396 | 203 | 324 | 374 | 0 | 346 | -11,454 | 6,589 | -12,671 | 0 |
| `pmhx_nlp_hypertension_first_days_from_surg` | 680 | 373 | 211 | 469 | 351 | 0 | 343 | -11,072 | 6,258 | -12,593 | 0 |
| `pmhx_nlp_hyperthyroidism_first_days_from_surg` | 665 | 246 | 198 | 467 | 157 | 0 | 152 | -11,610 | 5,875 | -12,388 | 0 |
| `pmhx_nlp_hypothyroidism_first_days_from_surg` | 1,066 | 672 | 259 | 807 | 623 | 0 | 601 | -11,647 | 6,589 | -12,656 | 0 |
| `pmhx_nlp_obesity_first_days_from_surg` | 223 | 152 | 48 | 175 | 135 | 0 | 140 | -11,392 | 4,958 | -12,711 | 0 |
| `pmhx_nlp_radiation_exposure_days_from_surg` | 29 | 29 | 8 | 21 | 28 | 0 | 27 | -12,755 | 3,464 | -14,736 | 3,464 |

### Representative distribution samples

Each sample is the 10 lowest observed non-null values by day-offset value.

| Column | Lowest-value sample |
|---|---|
| `pmhx_nlp_diabetes_first_days_from_surg` | -11454:1, -11447:1, -11327:1, -11317:2, -11242:1, -11228:1, -11072:1, -11010:1, -10926:1, -10877:1 |
| `pmhx_nlp_hypertension_first_days_from_surg` | -11072:1, -10815:1, -10370:1, -10141:1, -9526:1, -8552:1, -8026:1, -3031:1, -1211:1, -854:1 |
| `pmhx_nlp_hyperthyroidism_first_days_from_surg` | -11610:1, -11437:1, -11326:1, -11317:1, -11228:1, -11129:1, -11116:1, -11072:1, -11054:1, -10952:1 |
| `pmhx_nlp_hypothyroidism_first_days_from_surg` | -11647:1, -11610:1, -11454:1, -11392:1, -11327:1, -11317:2, -11292:1, -11204:1, -11167:1, -11147:1 |
| `pmhx_nlp_obesity_first_days_from_surg` | -11392:1, -10688:1, -10648:1, -10370:1, -9526:1, -8607:1, -7468:1, -597:1, -158:1, -100:1 |
| `pmhx_nlp_radiation_exposure_days_from_surg` | -12755:1, -8706:1, -8057:1, -5960:1, -4690:1, -2705:1, -196:1, -33:1, 0:1, 1:1 |

## Three-option Logan decision package

The impact counts below are computed relative to the current stored `*_days_from_surg` values across the six actual day-offset columns. They are **not** database writes.

| Option | Anchor formula | Registry cols in CF cluster | Actual value-semantic cols | Patients impacted vs current | Patient-column cells impacted vs current | Pros | Cons |
|---|---|---:|---:|---:|---:|---|---|
| A | `days = event_date - event_start`; for first event date, this collapses to `0` | 58 | 6 | 914 | 1,668 | Per-event semantics; avoids negative PMH day offsets | Removes currently meaningful pre-/post-surgery timing; day offsets become mostly zero for first-date fields; cross-col comparisons lose surgical anchor |
| B | `days = event_date - first_surgery_date` | 58 | 6 | 0 | 0 | Already implemented exactly; matches `*_days_from_surg` names; aligns with PM/manuscript convention; negative = pre-op and positive = post-op | Chronic PMH conditions often become negative, which needs clear documentation |
| C | `days = event_date - last_contact_date` | 58 | 6 | 877 | 1,609 | Aligns with LKA/censoring-style anchor vocabulary | Clinically confusing for PMH; LKA moves with follow-up; nearly all PMH events become negative relative to last contact; not implied by column names |

### Hybrid options surfaced for Logan

| Hybrid | Proposed handling | Impact | Recommendation |
|---|---|---|---|
| A+B per sub-type | Keep PMH boolean/count/category/date columns as event-level facts; use first-surgery anchor only for actual `*_days_from_surg` columns | Equivalent to Option B for the 6 day-offset columns, plus CF cleanup on non-anchor columns | **Recommended framing**. It separates factual PMH fields from day-offset fields and avoids treating counts/booleans as anchor-dependent. |
| B with sign-flipped convention | Convert PMH day offsets to positive `days_pre_surg` for pre-op events | Would require new columns or semantic rename; current 6 columns would no longer match `*_days_from_surg` | Not recommended for mig_175b. Consider only if Logan wants a separate manuscript-friendly derived layer, not a canonical replacement. |

## Sub-type decision table

| Sub-type | n_cols | Example col | Proposed anchor/status | Rationale |
|---|---:|---|---|---|
| Boolean PMH/family-history flag | 21 | `pmhx_nlp_diabetes` | Anchor not applicable; reclass/drop `CF-mig136-DAYS-SEMANTIC` | Boolean presence flags are not day offsets. |
| Count metric | 20 | `pmhx_nlp_diabetes_n_mentions` | Anchor not applicable; reclass/drop `CF-mig136-DAYS-SEMANTIC` | Mention counts do not encode dates. |
| Categorical/provenance text | 4 | `pmhx_nlp_extraction_method` | Anchor not applicable; reclass/drop `CF-mig136-DAYS-SEMANTIC` | Text provenance and status fields do not need date anchors. |
| Confidence score | 1 | `pmhx_nlp_radiation_exposure_confidence` | Anchor not applicable; reclass/drop `CF-mig136-DAYS-SEMANTIC` | Confidence score is not a temporal offset. |
| Source first/event date | 6 | `pmhx_nlp_diabetes_first_date` | Keep as source event date; no day-anchor transformation; optionally retag as paired source date | The date itself is the event/mention date. The anchor decision applies to derived day-offset columns only. |
| Date-derived days-from-surgery metric | 6 | `pmhx_nlp_diabetes_first_days_from_surg` | **Option B: first_surgery_date** | Current values already match `DATE_DIFF('day', first_surgery_date, event_date)` with 0 mismatches. |

## Carry-forwards and proposed registry disposition

| Carry-forward | Proposed status from mig_175 | Notes |
|---|---|---|
| `CF-mig136-DAYS-SEMANTIC` | Keep open pending Logan ratification | mig_175 supplies evidence only; no registry mutation. |
| `CF-mig175-DAYS-ANCHOR-PROPOSAL-RECOMMEND-OPTION-B` | Informational recommendation | Option B has 0 recalculation mismatches and matches column names. |
| `CF-mig175-NA-RECLASS-CANDIDATE-COLS` | Informational recommendation | At least 46 columns are clearly non-anchor fields: booleans, counts, categorical/provenance text, confidence. The 6 source date fields are source dates rather than offsets and should be handled separately from the 6 day metrics. |

## Recommended mig_175b apply scope after Logan ratification

1. Do **not** rewrite current day values if Logan ratifies Option B; the live data already matches this formula.
2. Document formula for the 6 day metrics as: `DATE_DIFF('day', CAST(first_surgery_date AS DATE), event_date)`.
3. Reclass/drop `CF-mig136-DAYS-SEMANTIC` from non-anchor columns after registry governance approval.
4. Keep source date columns as event-date provenance for their paired day metric; do not convert them to day offsets.
5. If Logan wants PMH-friendly positive values, add new derived fields named `*_days_pre_surg` in a later analytics layer rather than changing `*_days_from_surg` semantics.

## Out-of-scope actions not performed

- No `UPDATE`, `ALTER`, `CREATE`, or registry mutation was performed.
- No canonical patient master values were modified.
- No survival/follow-up day columns were touched.
- No `query_rw` action was used.

## Logan ratification request

Please ratify one of the following:

1. **Option B / recommended:** Preserve current `first_surgery_date` anchoring for the 6 actual `*_days_from_surg` metrics; reclass non-anchor fields out of `CF-mig136-DAYS-SEMANTIC` in mig_175b.
2. **Option A:** Re-anchor the 6 day metrics to event-start, effectively zeroing first-event day offsets and requiring a value rewrite or replacement columns.
3. **Option C:** Re-anchor the 6 day metrics to last-contact/LKA, changing 877 patients / 1,609 patient-column cells and requiring strong censoring-language documentation.
4. **Hybrid:** Ratify Option B for canonical storage and request separate manuscript-layer `days_pre_surg` derived variables for PMH readability.
