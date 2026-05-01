# Snowflake Cortex Validation — Prompt 9: NLP Tier-1 Flag Coverage & Concordance
**Generated:** 2026-05-01 (post-handoff)
**Source:** MD-direct via MCP (`thyroid_canonical_publication_v1_0`); equivalent script `snowflake_trial/scripts/15_prompt9_nlp.py` will produce the same numbers from `CANONICAL_PATIENT_MASTER_FLAT` once Logan re-exports.
**Scope:** 128 unique `nlp_*` flags on CPM; cross-validate against canonical detail tables (PMH, PSH, invasion, US exam, synoptic, complications, parathyroid, frozen-section, molecular).

---

## Summary

The Tier-1 NLP flags on CPM are **reliable for ETE (94.6% overlap) and complications (slight surplus, expected)** but **systematically under-fire for vascular invasion, LN positivity, PMH, imaging, and recurrence.** The largest gap is `nlp_path_vasc_inv_mentioned` (569 vs 1,178 canonical, 36% miss) — directly tied to the mig_177/mig_179 LVI rebuild that added 989 lymphatic + 1,178 vascular present-events from CAP-template parsing the NLP didn't see.

3 net-new findings worth surfacing:
- **CF-mig260b-NLP-VASCINV-UNDERFIRE** — `nlp_path_vasc_inv_mentioned` misses 749 canonical vasc-positive patients (post-mig_179)
- **CF-mig260c-NLP-LN-DISCORDANCE** — `nlp_path_ln_positive_mentioned` discordant with `canonical_path_malignant_events_v1.ln_involved>0` in 1,105 patients (453 NLP-only / 652 canon-only)
- **CF-mig260d-NLP-REC-PRESURGERY** — 8 patients have `nlp_rec_earliest_days_from_surg < 0` (recurrence "before" surgery, an extraction bug)

ETE is solid. Most others are coverage gaps, not data corruption.

---

## 1. NLP domain coverage (top 32 `nlp_*_has_data` flags)

| Domain flag | n pts | % of cohort |
| --- | --- | --- |
| nlp_synoptic_has_data | 4,835 | 44.5% |
| nlp_ne_procedures_has_data | 4,722 | 43.4% |
| nlp_ne_problemlist_has_data | 4,036 | 37.1% |
| nlp_ne_operative_has_data | 4,031 | 37.1% |
| nlp_parathyroid_has_data | 3,585 | 33.0% |
| nlp_path_has_data | 3,382 | 31.1% |
| nlp_survfu_has_data | 2,911 | 26.8% |
| nlp_frozensec_has_data | 2,855 | 26.3% |
| nlp_ne_complications_has_data | 2,840 | 26.1% |
| nlp_ne_medications_has_data | 2,070 | 19.0% |
| nlp_pshx_has_data | 1,864 | 17.1% |
| nlp_imaging_has_data | 1,728 | 15.9% |
| nlp_tirads_has_data | 1,715 | 15.8% |
| nlp_cervln_has_data | 1,643 | 15.1% |
| nlp_ne_staging_has_data | 1,639 | 15.1% |
| nlp_airway_has_data | 1,634 | 15.0% |
| nlp_funcoutcome_has_data | 1,623 | 14.9% |
| nlp_ln_has_data | 868 | 8.0% |
| nlp_labs_has_data | 791 | 7.3% |
| nlp_vasc_has_data | 776 | 7.1% |
| nlp_raidetail_has_data | 620 | 5.7% |
| nlp_ne_genetics_has_data | 605 | 5.6% |
| nlp_physexam_has_data | 512 | 4.7% |
| nlp_ptdecision_has_data | 367 | 3.4% |
| nlp_pmhx_has_data | 290 | 2.7% |
| nlp_radtx_has_data | 210 | 1.9% |
| nlp_rec_has_data | 133 | 1.2% |
| nlp_symptoms_has_data | 116 | 1.1% |
| nlp_esoph_has_data | 60 | 0.6% |
| nlp_tg_has_data | 49 | 0.5% |
| nlp_dynrisk_has_data | 25 | 0.2% |
| nlp_usnodule_has_data | 18 | 0.2% |

Cohort denominator: 10,871 CPM rows.

---

## 2. NLP flag vs Tier-2 canonical detail table — coverage cross-check

For each NLP domain, compare `nlp_<dom>_has_data` patient set vs the corresponding canonical detail table patient set.

| Domain | NLP flag n | Canonical detail n | Overlap | Notes |
| --- | --- | --- | --- | --- |
| pmhx (vs canonical_pmh_events_v1) | 290 | 4,158 | 290 | NLP flag is a strict subset of canonical PMH (4,158 patients have PMH events from broader sources beyond NLP-PMHx-section extraction) |
| pshx (vs canonical_psh_events_v1) | 1,864 | 1,878 | 1,864 | Near-perfect (≈99% NLP-driven) |
| path (vs canonical_path_malignant_events_v1) | 3,382 | 4,022 | 1,822 | 1,560 nlp-only (likely benign-path coverage) + 2,200 canon-only — expected; NLP path covers all path, canonical_path_malignant only catches malignant |
| imaging (vs canonical_us_exam_master_VIEW_v2) | 1,728 | 4,385 | 1,344 | NLP imaging severely under-covers — only 30.6% of patients with US exams have nlp_imaging fired |
| synoptic (vs path_synoptics) | 4,835 | 10,871 | 4,835 | path_synoptics has at least one row for every patient; NLP flag fires for 44.5% — large gap, possibly synoptic_row_ix-driven |
| complications (vs canonical_complications_events_v1) | 2,840 | 2,481 | 2,450 | NLP slight surplus (391 NLP-only, 31 canon-only) — expected, NLP catches mentions that don't make confirmed events |
| parathyroid (vs canonical_parathyroid_events_v1) | 3,585 | 4,443 | 2,483 | Both directions leak: 1,102 NLP-only, 1,960 canon-only |
| frozen_section (vs canonical_frozen_section_events_v1) | 2,855 | 4,116 | 2,855 | NLP perfect subset; 1,261 canonical-only patients (NLP missed; canonical likely sourced from synoptic CAP) |
| molecular (vs canonical_molecular_genetics_v2) | 605 | 1,151 | 372 | Both directions leak; 233 NLP-only, 779 canon-only |

---

## 3. NLP path-mention flags vs canonical_invasion_events_v1 (key finding-level cross-validation)

| NLP flag | NLP n | Canonical n | Overlap | NLP-only | Canon-only |
| --- | --- | --- | --- | --- | --- |
| nlp_path_ete_mentioned | 1,215 | 1,211 | 1,149 | 66 | 62 |
| nlp_path_vasc_inv_mentioned | 569 | 1,178 | 429 | 140 | **749** |
| nlp_vasc_positive_mentioned (vasc OR LVI) | 776 | 1,350 | 753 | 23 | **597** |
| nlp_path_ln_positive_mentioned | 963 | 1,162 | 510 | **453** | **652** |

**ETE is concordant** (94.6% overlap, 66+62 = 128 disagreements out of 1,277 union-set, ~10%).

**Vascular invasion is severely under-fired** at the NLP level: 749 canonical-confirmed vasc-positive patients have `nlp_path_vasc_inv_mentioned = FALSE`. Direct legacy of mig_177/mig_179 — the LVI rebuild added events from CAP-template `Lymph-Vascular Invasion: Present` and the newer separate `Lymphatic Invasion: Present` lines that the existing NLP path-section extractor didn't pick up.

**LN-positive flag is the most discordant**: 453 NLP-only + 652 canon-only = 1,105 patients where the two sources disagree about LN positivity. This is **not** a simple under/over-fire — both directions leak. Worth a manuscript-methods footnote when M037 (LN predictors) re-runs against `canonical_path_malignant_events_v1.ln_involved` versus the NLP-path Tier-1 flag.

---

## 4. Confidence-tier distribution (Round-2 LLM annotated domains)

| Domain | Tier | n |
| --- | --- | --- |
| nlp_cervln | round2_gpt_oss_120b_v1 | 1,643 |
| nlp_esoph | round2_gpt_oss_120b_v1 | 60 |
| nlp_path | round2_gpt_oss_120b_v1 | 3,382 |
| nlp_rec | below_80pct_concordance | 133 |
| nlp_vasc | below_80pct_concordance | 776 |

Two domains (`nlp_rec`, `nlp_vasc`) are stamped `below_80pct_concordance` — they have known reliability issues vs the Round-2 gpt-oss-120B annotations. This matches the §3 finding that `nlp_vasc_*` is the most under-firing flag and `nlp_rec_*` has the highest discordance with `recurrence_flag_v2` (see §5).

---

## 5. NLP recurrence vs `recurrence_flag_v2` (post-mig_255)

| Metric | n |
| --- | --- |
| Cohort | 10,871 |
| nlp_rec_any_mentioned | 133 |
| recurrence_flag_v2 (post-mig_255) | 189 |
| Both | 31 |
| NLP-only | 102 |
| Canonical-only | 158 |
| nlp_rec_disease_free_mentioned | 17 |
| nlp_rec_earliest_date IS NOT NULL | 91 |
| **nlp_rec_earliest_days_from_surg < 0** | **8** |

**Massive discordance**: only 31/260 union-set (12%) overlap between NLP-detected recurrence mentions and the canonical `recurrence_flag_v2` set. mig_255 was sourced from path_proven recurrence events + B′/A′ rule, not NLP — so the two are nearly independent. The 102 NLP-only patients are candidates for a manuscript footnote: 102 patients have follow-up note evidence of recurrence the canonical flag missed.

**8 patients** have `nlp_rec_earliest_days_from_surg < 0`, meaning the NLP extracted a recurrence date *before* the surgery date. These are obvious extraction bugs — file as **CF-mig260d-NLP-REC-PRESURGERY** for the next NLP-extraction refresh.

---

## 6. Reusable patterns

- **NLP-vs-canonical concordance scoring**: every Tier-1 `nlp_*_has_data` flag should pair with a Tier-2 canonical detail table for cross-validation. Where both exist, "overlap, NLP-only, canon-only" is the audit triple. Tight overlap = NLP is reliable proxy; large canon-only = NLP under-fires; large NLP-only = NLP over-fires (or canonical filters tighter).
- **Confidence-tier surfacing**: `*_confidence_tier='below_80pct_concordance'` is a structured warning baked into CPM. Worth a one-shot SQL during every validation round.
- **Days-from-surg sign sanity**: any `nlp_*_days_from_surg < 0` for a forward-only event (recurrence, follow-up) is an extraction bug. Cheap to probe across all date-bearing NLP flags.

---

## 7. Carry-forwards (new)

| CF | Description | Severity | Action |
| --- | --- | --- | --- |
| CF-mig260b-NLP-VASCINV-UNDERFIRE | `nlp_path_vasc_inv_mentioned` misses 749 canonical-confirmed vasc-positive patients | MED | NLP refresh against post-mig_179 CAP-template patterns |
| CF-mig260c-NLP-LN-DISCORDANCE | NLP and `canonical_path_malignant_events_v1.ln_involved` disagree in 1,105 patients | MED | Manuscript-methods footnote; consider canonical as SoT for M037 |
| CF-mig260d-NLP-REC-PRESURGERY | 8 patients with negative `nlp_rec_earliest_days_from_surg` | LOW | Extraction bug; fix in next NLP refresh |

Existing CF-mig255-RECUR-RESOURCING-FROM-EVENTS aligns with the 158 canon-only recurrence patients in §5.
