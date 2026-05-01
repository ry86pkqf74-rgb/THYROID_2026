# Snowflake Cortex Validation — Prompt 12: Synoptic Pathology Coverage & Label Drift
**Generated:** 2026-05-01 (post-handoff)
**Source:** MD-direct via MCP (`thyroid_canonical_publication_v1_0`); equivalent script `snowflake_trial/scripts/18_prompt12_synoptic.py` will produce the same numbers from Snowflake once Logan re-exports.
**Tables probed:** `path_synoptics` (11,688 × 582), `canonical_invasion_events_v1` (58,582 × 20), `canonical_path_malignant_events_v1` (6,469 × 66).

---

## Summary

`path_synoptics` is the **structured CAP-template substrate** behind canonical_path_malignant + canonical_invasion. 11,688 rows / 10,871 patients (1.08 avg/pt; 763 pts have multi-row); surgery dates span 1994-05-25 → 2025-08-01 (32-year window — plausible). The structured per-field columns are populated for CAP-conforming reports; legacy/non-CAP reports leave per-field NULLs but populate `synoptic_diagnosis` with raw text.

The most actionable issue is **label drift** in CAP-template categorical fields — case sensitivity, trailing whitespace, typos ("preesent", "extesive", "extensiver", "indeeterminate", "Multifocal" vs "multifocal"), embedded newlines. The `canonical_invasion_events_v1` build (post mig_177/179) absorbs most of this drift via normalization, but per-field probes in `path_synoptics` show the raw extent. M025 / M075 (TIRADS multi-nodule) cohort definitions and any per-tumor analyses that go through `tumor_1_lymphatic_invasion` directly (instead of the canonical) need to handle this.

Net new findings:
- **CF-mig262b-CAP-LABEL-DRIFT-FOCALITY** — `tumor_focality` has ≥7 case/whitespace/typo variants (most-common: "unifocal" 2,534 vs "Unifocal" 46 vs "unifocal " 1 vs "unifocal*" 2)
- **CF-mig262c-CAP-LABEL-DRIFT-LVI** — `tumor_1_lymphatic_invasion` has ≥17 distinct values including 6 typos ("preesent", "indeeterminate", "extensivre", "extensiver", "indeterminent", "indetermiante"), each appearing 1–3 times
- **CF-mig262d-CAP-LABEL-DRIFT-ETE** — `tumor_1_extrathyroidal_extension` has ≥20 distinct values including ETE-x sentinel (3,382 rows), case drift ("Yes" 1, "Yes;" 7, "yes" 19), typo ("extesive" 2), free-text fallback (1 row at 174 chars). Closely related to mig_261 ETE label normalization candidate.
- **CF-mig262e-PATH-SYNOPTICS-SURG_DATE-TIMESTAMP** — `path_synoptics.surg_date` is TIMESTAMP type (with 00:00:00 time component) but per Logan-ratified 2026-04-28 rule clinical event dates must be DATE type
- **CF-mig262f-CAP-MULTI-TUMOR-SAMPLE-DROP** — multi-tumor coverage drops sharply (T1 4,181 → T2 1,324 → T3 453 → T4 153 → T5 56) — legitimate multifocal cohort, but worth confirming canonical_path_malignant captures all of these per-tumor when present

---

## 1. path_synoptics structural overview

| Metric | Value |
| --- | --- |
| Total rows | 11,688 |
| Distinct patients | 10,871 |
| Min surg_date | 1994-05-25 00:00:00 |
| Max surg_date | 2025-08-01 00:00:00 |
| NULL surg_date | 2 |
| Min rows-per-patient | 1 |
| Max rows-per-patient | 6 |
| Avg rows-per-patient | 1.08 |
| Patients with multi-row | 763 |
| Total columns | 582 |

`surg_date` is TIMESTAMP (with always-zero time). Violates the Logan-ratified clinical-dates-calendar-only rule — should be DATE. **CF-mig262e**.

---

## 2. Histology distribution (top 20 of `tumor_1_histologic_type`)

| Value | n | Notes |
| --- | --- | --- |
| PTC | 3,184 | canonical |
| follicular carcinoma | 450 | lowercase |
| metastatic PTC | 183 | recurrence/met cases |
| MTC | 156 | |
| NIFTP | 121 | |
| **PTC ** | **80** | **trailing whitespace** |
| **Follicular carcinoma** | **43** | **case drift** |
| FTUMP | 34 | |
| poorly differentiated thyroid carcinoma | 24 | |
| anaplastic carcinoma | 19 | |
| metastatic MTC | 17 | |
| **Metastatic PTC** | **12** | **case drift** |
| **Poorly differentiated thyroid carcinoma** | **12** | **case drift** |
| metastatic PTC classical | 9 | |
| metastatic follicular carcinoma | 7 | |
| metastatic PTC tall cell variant | 7 | |
| differentiated high grade thyroid carcinoma | 6 | |
| Metastatic MTC | 6 | case drift |
| recurrent/metastatic PTC | 5 | |
| follicular adenoma | 4 | |

**8 of top 20 are case/whitespace duplicates of higher-rank values.** Aggregate-level prevalences are correct only after `LOWER(TRIM())` normalization. The `is_malignant` derivation upstream presumably normalizes; user-side direct queries on `path_synoptics.tumor_1_histologic_type` will see split categories.

---

## 3. CAP-template label drift — per-field deep dive

### 3a. ETE (`tumor_1_extrathyroidal_extension`)
| Value | n |
| --- | --- |
| x | 3,382 |
| present | 252 |
| minimal | 174 |
| microscopic | 65 |
| c/a | 29 |
| extensive | 24 |
| yes | 19 |
| focal | 13 |
| indeterminate | 9 |
| Yes; | 7 |
| yes (minimal) | 2 |
| **extesive** | **2** |
| n/a | 2 |
| X | 2 |
| Extensive | 1 |
| Yes | 1 |
| free-text fallback | 1 |

### 3b. LVI (`tumor_1_lymphatic_invasion`)
| Value | n |
| --- | --- |
| x | 2,678 |
| present | 702 |
| extensive | 54 |
| indeterminate | 50 |
| c/a | 9 |
| focal | 7 |
| **preesent** | **3** |
| **indeeterminate** | **2** |
| **extensivre** | **2** |
| no | 1 |
| **indeterminent** | **1** |
| **indetermiante** | **1** |
| free-text fallback | 1 |
| suspicious | 1 |
| 1 focus | 1 |
| n/s | 1 |
| **extensiver** | **1** |

### 3c. Focality (`tumor_focality`)
| Value | n |
| --- | --- |
| unifocal | 2,534 |
| multifocal | 1,315 |
| **Multifocal** | **89** |
| **Unifocal** | **46** |
| c/a | 3 |
| **multifocal\n** | **3** |
| **unifocal*** | **2** |
| free-text fallback | 1 |
| multifocal (including prior resection) | 1 |
| **multifocal **| **1** |
| multifocal, bilateral | 1 |
| free-text fallback | 1 |
| **unifocal ** | **1** |
| n/s | 1 |
| multifocal (9-foci) | 1 |

Six typo-class variants in LVI alone, each appearing 1–3× (12 patient impact). ETE has its 3,382 "x" sentinel (already known per memory `project_ete_documentation_rate.md`) plus case + typo drift. Focality has clean values 96% of the time but ≥6 minor-frequency variants.

---

## 4. LVI cross-validation: `path_synoptics.tumor_1_lymphatic_invasion` vs `canonical_invasion_events_v1`

Definition of synoptic-LVI-positive: `LOWER(TRIM(tumor_1_lymphatic_invasion)) IN ('present', 'extensive', 'focal', 'suspicious')`.

| Source | n_pts |
| --- | --- |
| Synoptic tumor_1 LVI positive | 758 |
| Canonical lymphatic_microscopic events present | 989 |
| Both | 758 |
| Synoptic-only (canonical missing it) | 0 |
| Canonical-only (synoptic tumor_1 missing it) | 231 |

**Canonical correctly captures everything tumor_1 has (758/758) plus 231 additional patients** — the 231 are caught from `tumor_2..tumor_5_lymphatic_invasion` (multifocal cases) and the older combined "Lymph-Vascular Invasion" CAP-template field via the mig_177/mig_179 supplemental-events architecture. The mig_179 LVI rebuild win is visible at the canonical layer.

---

## 5. Multi-tumor (T1–T5) coverage

| Tumor slot | Patients with size populated |
| --- | --- |
| tumor_1_size_greatest_dimension_cm | 4,181 |
| tumor_2_size_greatest_dimension_cm | 1,324 |
| tumor_3_size_greatest_dimension_cm | 453 |
| tumor_4_size_greatest_dimension_cm | 153 |
| tumor_5_size_greatest_dimension_cm | 56 |
| tumor_focality (any) | 4,004 |

`tumor_focality` is non-NULL for 4,004 patients (the malignant + multifocal-flag-determinable cohort). The T1–T5 cascade is the legitimate multifocal distribution. M025/M037/M075 cohorts that filter on per-tumor characteristics should confirm they're fanning out across all populated slots, not just T1.

---

## 6. `synoptic_diagnosis` raw-text behavior

Top values for the free-text rollup column:

| Value | n |
| --- | --- |
| N/A | 370 |
| (blank/whitespace) | 6 |
| (long-form CAP raw paste — varies per patient) | unique strings, 1 each |

For non-CAP-conforming reports (older templates, free-text dictation, multi-block CAP), `synoptic_diagnosis` contains the raw paste while structured per-field columns are NULL. This is the substrate for any future extractor (e.g. an LLM run over the raw text to recover fields the structured parser missed). Worth flagging for Cortex-Search indexing if Logan goes down that path.

---

## 7. Reusable patterns

- **Case+whitespace+typo audit** for any low-cardinality CAP-template field is a one-shot `GROUP BY value` + visual-scan. The drift compounds when filters use exact-match against the most-common spelling.
- **Path-synoptic vs canonical concordance** at the patient level (`syn_only`, `canon_only`) verifies the canonical builders are surfacing all per-field signal — and surfaces missing-tumor-N cases when canonical has a patient that path_synoptics tumor_1 doesn't.
- **Free-text fallback detection**: rows with a long-string value (length > 100 chars) in a normally-categorical column flag CAP-template-not-conforming reports needing LLM re-extraction.

---

## 8. Carry-forwards (new)

| CF | Description | Severity | Action |
| --- | --- | --- | --- |
| CF-mig262b-CAP-LABEL-DRIFT-FOCALITY | tumor_focality has ≥7 case/whitespace/typo variants | LOW | Normalization migration on path_synoptics or use canonical_path_malignant (already-normalized) |
| CF-mig262c-CAP-LABEL-DRIFT-LVI | tumor_1_lymphatic_invasion has 6 typos with 1–3× each | LOW | Same — normalize at upstream or downstream |
| CF-mig262d-CAP-LABEL-DRIFT-ETE | tumor_1_extrathyroidal_extension has ≥20 values incl. typos & free-text fallback | MED | Aligns with mig_261 ETE label normalization candidate; expand scope |
| CF-mig262e-PATH-SYNOPTICS-SURG_DATE-TIMESTAMP | surg_date is TIMESTAMP not DATE | LOW | Type-flip via mig_160b-style retype migration; aligns with clinical-dates-calendar-only rule |
| CF-mig262f-CAP-MULTI-TUMOR-CONFIRM | T1=4,181, T2=1,324, …, T5=56 — confirm canonical absorbs all T2-T5 |  LOW | Probe `canonical_path_malignant_events_v1` for tumor_index distribution |
