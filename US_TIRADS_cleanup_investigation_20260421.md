# US / TIRADS section — cleanup investigation

**Database:** `thyroid_canonical_publication_v1_0`
**Date:** 2026-04-21
**Scope:** Every table and view touching ultrasound, TIRADS, US nodule, US gland size, and US lymph node findings.

---

## TL;DR

You already have most of the pieces for the three-table model you want, but they are scattered across 15+ tables with three overlapping "canonical" nodule masters, no dedicated US-gland table, and no dedicated US lymph-node table. The fix is consolidation, not re-extraction, for the nodule layer — plus two net-new parsed tables for thyroid gland and US LN findings. About 4,733 patients are stuck in a legacy wide-format feeder and need LLM re-extraction to join the canonical view.

---

## 1. Current landscape — every US / TIRADS object in `main`

| Table | Rows | Patients | Grain | Role |
|---|---|---|---|---|
| `us_nodules_tirads` | 10,859 | 10,859 | one row per patient (wide, up to 14 nodules, single us_1_date) | LEGACY Excel ingest — widest patient reach, only first US per patient |
| `ultrasound_reports` | 6,793 | 4,074 | one row per US exam | Per-report structured ingest — rich (gland dims, LN text, impressions), but only covers 4,074 patients |
| `tirads_v2_nodules_raw` | 11,914 | 3,021 | one LLM-nodule per report | Qwen2.5-32B extraction over US report text (directly from us_nodules_tirads.us_1_impression) |
| `tirads_llm_extracted_v2` | 5,636 | — | per nodule (earlier LLM) | Superseded by tirads_v2_nodules_raw |
| `extracted_tirads_validated_v1` | 3,439 | — | per nodule | Per-component validated TIRADS (feeds tirads_best_score_v12 on CPM) |
| `tirads_reextraction_queue_v1` | 4,363 | — | per patient | Queue for LLM TIRADS re-extraction |
| `note_entities_llm_tirads_granular` | 11,037 | — | one note | LLM JSON entities from **h&p / op_note / endocrine_note** (NOT US reports) |
| `note_entities_llm_us_nodule_dynamics` | 11,037 | — | one note | LLM JSON entities from same clinical notes — tracks nodule changes over time |
| `note_entities_llm_cervical_ln_detail` | 11,037 | — | one note | LLM JSON entities from same clinical notes — **pathology/surgical LN, not US LN** |
| `imaging_nodule_master_v1` | 37,016 | 6,126 | per nodule per exam | BASE nodule table; 25 cols; feeds cunm/cunc |
| `canonical_us_nodule_master_v1` (cunm_v1) | 37,016 | 6,126 | per nodule per exam | Merged 5-source nodule master; 30 cols; source-tagged |
| `canonical_us_nodule_characteristics_v1` (cunc_v1) | 37,016 | 6,126 | per nodule per exam | Parallel characteristics master; 39 cols; only master that has `us_exam_id` |
| `canonical_us_exam_master_v1` | 13,347 | 6,126 | per exam | Per-exam rollup (worst TIRADS, n_nodules, bilateral flag, exam_rank_for_patient) |
| `canonical_us_patient_master_v1` | 6,126 | 6,126 | per patient | Patient rollup (first/last US, max TIRADS ever, preop available) |
| `serial_imaging_us` | 4,162 | — | per US exam | Legacy MA-ingest feeder — now deprecated by canonical stack |
| `thyroid_sizes` | 11,675 | — | per surgery | **Pathology gross sizes (surg_date) — NOT US-derived** |

Plus seven `manuscript_workspace` helpers (tirads_granular_parsed_v1 = 181 rows, us_nodule_dynamics_parsed_v1 = 49 rows, imaging_nodule_master_clean_v1 = 34,946 rows, tirads_llm_haiku_vs_qwen_v1, tirads_v1_v2_discordance_v1, us_nodules_tirads_vs_inm_v1_discordance_v1, cohort_m025_tirads_performance_v1). These are audit/discordance queues, small and single-purpose.

---

## 2. The three overlapping nodule masters — which one wins?

All three are exactly 37,016 rows × 6,126 patients. But they differ materially on populated structured features:

| Feature | `imaging_nodule_master_v1` | `canonical_us_nodule_characteristics_v1` | `canonical_us_nodule_master_v1` |
|---|---|---|---|
| composition | 19,891 | 19,891 | **27,963** |
| echogenicity | 19,891 | 19,891 | **25,466** |
| shape | 19,891 | 19,891 | **21,586** |
| margins | 19,891 | 19,891 | **22,468** |
| echogenic foci | — | 5,149 | **30,921** |
| size (cm) | 19,891 | 19,891 | **33,081** |
| TIRADS score (2017 pts) | 27,903 | 4,396 | 4,396 |
| TIRADS category | 28,222 | 3,404 | 3,404 |
| has `us_exam_id` | no | **yes (hash)** | no |
| has source-tagging (5 flags) | no | no | **yes** |
| has `dominant_nodule_flag` | yes | no | no |
| build date | Script 245/271 | Script 246/271 | newer merge of the other two + tirads_v2 + dynamics_llm + fna_linkage |

Verdict: `canonical_us_nodule_master_v1` is the most complete on structured features but is missing `us_exam_id` and the ACR component points breakdown. `canonical_us_nodule_characteristics_v1` is the only one with a stable exam hash. `imaging_nodule_master_v1` is the base feeder. **None of the three alone is what you want.**

The row-level mismatch between cunm and cunc is small but real: 35,073 of 37,016 nodules match on `(research_id, exam_date, nodule_index_within_exam)`; 1,943 do not (1.5% — likely nodule-index reassignment between builds).

---

## 3. The patient coverage gap — 4,733 patients missing from canonical

| Source | Unique patients |
|---|---|
| `us_nodules_tirads` (legacy wide ingest, any US info) | 10,859 |
| `canonical_us_nodule_master_v1` (structured) | 6,126 |
| **Patients in legacy but not in canonical** | **4,733** |

These 4,733 patients have only free-text nodule descriptions in `us_nodules_tirads.nodule_1`..`nodule_14` and were never LLM-parsed. `tirads_v2_nodules_raw` (3,021 patients) covers some of them — so a re-extraction run over the remaining ~1,700 patients would close most of the gap.

---

## 4. Multi-US-per-patient and multi-nodule-per-US handling today

- The canonical table **does** support this today: `canonical_us_nodule_characteristics_v1` has `(research_id, us_exam_id, exam_date, nodule_index_within_exam)` as its natural key. 6,126 patients × 13,347 exams × 37,016 nodules — so on average 2.18 US exams per patient, 2.8 nodules per exam.
- What is **broken** in the current shape: it's long-format, not wide. You asked for `US1`, `US1_date`, `US1_nodule1_*`, `US1_nodule2_*`, `US2_*`… — that's a post-query pivot, not a storage change. Long-format is correct for storage; we build a wide convenience view on top.
- Duplicate/aggregate rows exist: patient 10734's 2013-10-06 exam has 4 real nodules (rows 1–4) plus 2 aggregate rows (5–6) that concatenate earlier text. These need a dedup pass.

---

## 5. US thyroid gland (non-nodule) findings — NO canonical table exists

The data is there but only in free text, covering only 4,074 of 6,126 US patients:

- `ultrasound_reports.right_lobe_dimensions`, `left_lobe_dimensions`, `isthmus_thickness`, `total_thyroid_size`, `total_thyroid_volume_ml` — populated for all 6,793 reports as text strings like "4.3 x 1.6 x 1.6 cm".
- No vascularity, parenchyma, echogenicity, Hashimoto pattern, etc. at the gland level.
- `thyroid_sizes` table is **pathology gross-specimen**, not US.
- CPM has only `us_isthmus_thickness_mm` and `gland_weight_isthmus_g` — very thin.
- **Gap: need a `canonical_us_thyroid_gland_v1` table keyed on (research_id, us_exam_id, exam_date) with parsed gland measurements + parenchyma descriptors.**

---

## 6. US lymph node findings — NO canonical table exists; CPM coverage is essentially zero

- `ultrasound_reports.lymph_node_assessment` exists as free text but only for 4,074 patients' reports.
- `us_nodules_tirads.lymph_node_assessment` does **not exist** in that table (I verified — it's only on `ultrasound_reports`).
- `note_entities_llm_cervical_ln_detail` (11,037 rows) is from **op_note / h_p / endocrine_note**, NOT US reports. This is pathology/surgical lymph-node content.
- CPM `lnus_n_exams > 0` → only 61 patients. `lnus_abnormal_ln_any = TRUE` → only 19 patients. Effectively unusable.
- `tirads_v2_any_suspicious_ln_on_us` is on CPM but derived from tirads_v2 extractions, not a true per-LN record.
- **Gap: need a `canonical_us_lymph_node_v1` table keyed on (research_id, us_exam_id, exam_date, ln_index_within_exam), parsed via LLM from `ultrasound_reports.lymph_node_assessment` and from the LN-pass run over US report text.**

---

## 7. Other items that can be consolidated or retired

| Table | Recommendation |
|---|---|
| `tirads_llm_extracted_v2` (5,636) | RETIRE — superseded by `tirads_v2_nodules_raw` (Qwen2.5-32B, 0 parse errors) |
| `serial_imaging_us` (4,162) | RETIRE — the 4,745 patient-only-in-legacy gap makes this redundant once cunm_v1 replaces it |
| `imaging_nodule_master_v1` (37,016) | KEEP as feeder; drop from "canonical" namespace; it's a staging table |
| `extracted_tirads_validated_v1` (3,439) | KEEP — it feeds `tirads_best_score_v12` on CPM |
| `note_entities_llm_tirads_granular`, `note_entities_llm_us_nodule_dynamics` | KEEP raw JSON — these are the LLM source of truth for **clinical-note** mentions, complementary to ultrasound report extractions |
| `manuscript_workspace.tirads_granular_parsed_v1` (181), `us_nodule_dynamics_parsed_v1` (49) | RETIRE — stale partial parses, superseded by the full parse we'll build |
| `manuscript_workspace.imaging_nodule_master_clean_v1` (34,946) | RETIRE — it's a view wrapper |
| Discordance/audit tables | KEEP — they're chart-review queues |

---

## 8. Proposed final architecture — 3 canonical tables + 2 rollups + retirements

```
main.canonical_us_nodule_v1                 ← ONE master per-nodule table  (target ~38–42K rows, 7,800+ patients)
 ├─ Keys: research_id, us_exam_id (hash), exam_date, nodule_index_within_exam, nodule_id
 ├─ Identity: laterality, pole, position, location_detail, size_cm_max, length/width/height_mm, volume_ml
 ├─ Sonography: composition, echogenicity, shape, margins, calcifications, echogenic_foci, halo, vascularity,
 │              extrathyroidal_extension_on_us, chammas_type, elastography_category
 ├─ TIRADS: tirads_score_2017 + 5 component points, tirads_category_v2, tirads_level_2017,
 │          tirads_reported (from report text), tirads_concordant_flag, suspicious_flag
 ├─ Dynamics: interval_growth_flag, prior_size_mm_max, fna_recommended_this_nodule,
 │            fna_performed_prior_or_concurrent
 ├─ Provenance: source_base, source_tirads_v2, source_tirads_llm, source_dynamics_llm, source_fna_linkage,
 │              data_completeness_pct, resolution_rule, tirads_score_component_complete
 └─ Dedup: explicit pass removing the aggregate-concatenation rows we saw in patient 10734 today

main.canonical_us_thyroid_gland_v1           ← NEW  (target ~13,000 exam-rows, 6,100+ patients)
 ├─ Keys: research_id, us_exam_id, exam_date
 ├─ Measurements: rl_length_cm, rl_width_cm, rl_depth_cm, rl_volume_ml  (same for ll_, isthmus_, pyramidal_, total_)
 ├─ Parenchyma: background_echogenicity, heterogeneity, hashimoto_pattern, vascularity_overall,
 │              calcifications_parenchymal, goiter_flag, substernal_extension_flag
 ├─ Findings: clinical_impression_text (from ultrasound_reports.source_us_impression),
 │            recommendation_text, radiologist, study_indication
 └─ Provenance: source (ultrasound_reports | us_nodules_tirads-parsed | tirads_v2_raw-parsed),
                extracted_at, llm_model

main.canonical_us_lymph_node_v1              ← NEW  (target ~15–25K rows once LLM-parsed)
 ├─ Keys: research_id, us_exam_id, exam_date, ln_index_within_exam, ln_id
 ├─ Location: laterality, neck_level (I–VII), level_subregion, region (central|lateral_left|lateral_right|other)
 ├─ Measurements: size_cm_max, short_axis_mm, long_axis_mm
 ├─ Sonographic features: shape (round vs oval), echogenicity, hilum_preserved, calcifications,
 │                        cystic_component, vascularity_pattern (hilar|peripheral|mixed),
 │                        extranodal_extension_on_us
 ├─ Assessment: suspicious_flag, suspicion_level (benign|indeterminate|suspicious), biopsy_recommended,
 │              evidence_text
 └─ Provenance: source_note_type, llm_model, confidence

main.canonical_us_exam_master_VIEW_v2             ← REBUILD keyed to new nodule + gland + LN tables
main.canonical_us_patient_master_VIEW_v2          ← REBUILD  (first/last US date, multi-US flag, max TIRADS ever,
                                                          any_abnormal_ln_on_us_ever, goiter_ever, etc.)
```

**Retirements (moved to an archive schema — same pattern as the molecular v2 consolidation from 2026-04-21):**
- `canonical_us_nodule_master_v1` → merged into `canonical_us_nodule_v1`
- `canonical_us_nodule_characteristics_v1` → merged into `canonical_us_nodule_v1`
- `imaging_nodule_master_v1` → archive (feeder only)
- `tirads_llm_extracted_v2` → archive (superseded by tirads_v2_nodules_raw)
- `serial_imaging_us` → archive
- Small manuscript_workspace parsed tables (`tirads_granular_parsed_v1`, `us_nodule_dynamics_parsed_v1`) → archive

**Keep (raw/feeder, do not touch):**
- `us_nodules_tirads` — raw Excel ingest, feeder
- `ultrasound_reports` — raw per-report ingest, feeder
- `tirads_v2_nodules_raw` — LLM raw output, feeder
- `note_entities_llm_tirads_granular`, `note_entities_llm_us_nodule_dynamics` — LLM JSON, feeder
- `extracted_tirads_validated_v1` — feeds CPM TIRADS columns

---

## 9. Wide convenience view (what you asked for — "US1, US1 date, US1 Nodule 1…")

Storage stays long-format. We expose a wide view for manuscript / export purposes:

```sql
CREATE OR REPLACE VIEW views_readable.US_Nodules_Wide AS
SELECT
  research_id,
  MAX(CASE WHEN exam_rank = 1 THEN exam_date END) AS us_1_date,
  MAX(CASE WHEN exam_rank = 1 AND nodule_index_within_exam = 1 THEN size_cm_max END) AS us_1_nodule_1_size_cm,
  MAX(CASE WHEN exam_rank = 1 AND nodule_index_within_exam = 1 THEN laterality END) AS us_1_nodule_1_laterality,
  MAX(CASE WHEN exam_rank = 1 AND nodule_index_within_exam = 1 THEN tirads_category_v2 END) AS us_1_nodule_1_tirads,
  -- …nodule 2–8, exam 2–5, etc.
FROM canonical_us_nodule_v1 n
JOIN canonical_us_exam_master_VIEW_v2 e USING (research_id, us_exam_id)
GROUP BY research_id;
```

The wide view scales as far as the manuscript's cap (e.g., 5 exams × 8 nodules = 40 columns) — anything beyond that stays in the long table.

---

## 10. Outstanding questions before I write the Cursor prompt

1. **Inclusion of the 4,733 "legacy-only" patients.** Do we (a) leave them out of v2 and note it as "NLP TODO," (b) LLM-extract them as part of this cleanup (Qwen2.5-32B over their `us_nodules_tirads.nodule_X` free-text cells — ~1,700 patients not yet in `tirads_v2_nodules_raw`), or (c) punt to a v1_1 follow-up?
2. **Source ranking for nodule characteristics conflicts.** Today cunm_v1 implicitly prefers `source_tirads_v2` over `source_tirads_llm` over `source_base`. Keep that order, or change it?
3. **Scope of the LLM pass for gland + LN.** I suggest a new dedicated Qwen prompt run over `ultrasound_reports.source_us_impression` + `ultrasound_reports.lymph_node_assessment` + `us_nodules_tirads.us_1_impression`, parsing gland-level and LN-level entities. Acceptable, or do you want to re-use the existing `note_entities_llm_cervical_ln_detail` model?
4. **Archive target.** Same pattern as molecular v2 (`"Thyroid 2026 UPdated".us_legacy_20260421`)?
5. **Are we versioning as v1 (replace) or v2 (parallel + cutover)?** The memory notes molecular used parallel. I'd suggest parallel again for safety.

Once you land these five, I'll write the Cursor prompt with (i) the three canonical CREATE TABLE scripts, (ii) the LLM parse pass for the 4,733-patient gap + gland + LN, (iii) the CPM column backfill to point to v2, (iv) the view pivot for US_Nodules_Wide, and (v) the registry + archive moves.

---

## Appendix: evidence from today's MotherDuck deep dive

- 15 raw/feeder tables in `main`, 7 parsed/audit tables in `manuscript_workspace` + 6 `views_readable` wrappers — 28 objects total currently serving the US/TIRADS domain.
- cunm_v1 ↔ cunc_v1: exact row count match (37,016), 35,073 key-matched, 1,943 diverged per side.
- CPM carries 149 US/TIRADS/imaging-related columns today (counted from `information_schema.columns`).
- CPM coverage: 6,126 patients with any US exam; only 3,474 with TIRADS best/worst; only 61 with lnus_n_exams > 0.
- Patient 10734 spot-check shows same-exam duplicate/aggregate rows (rows 5–6 of 2013-10-06 concatenate earlier nodule text) — systemic dedup needed before v2.
