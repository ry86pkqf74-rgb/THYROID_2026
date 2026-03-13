# Phase 5: Targeted Top-5 Variable Refinement — Kickoff

_Generated: 2026-03-12_

## MotherDuck Business Pro Connection

- **Status:** Connected
- **DuckDB version:** 1.4.4
- **Database:** thyroid_research_2026

## Baseline Counts

### 1. ETE Sub-grading (Priority #1)
| Metric | Value |
|--------|-------|
| patient_refined_staging_flags_v3 | 10,871 rows |
| extracted_ete_refined_v1 total | 3,879 rows |
| present_ungraded | **3,558** (target for sub-grading) |
| gross | 27 |
| microscopic | 265 |
| none | 29 |
| Source of ungraded | All from `path_report` structured |
| Ungraded patients with op notes | 1,842 |
| Ungraded patients with any notes | 2,186 / 3,558 (61%) |

**Raw values in path_synoptics:** "x"=3,384, "present"=252, "minimal"=174 (→microscopic), "microscopic"=65 (→microscopic), "extensive"=25 (→gross), "yes"=20, "focal"=13 (→microscopic), "indeterminate"=9, "yes;"=7, "yes (minimal)"=2 (→microscopic), plus edge cases

### 2. TERT Promoter (Priority #2)
| Source | TERT Positive | Patients |
|--------|--------------|----------|
| molecular_test_episode_v2 (tert_flag) | 79 episodes | **76 patients** |
| NLP note_entities_genetics | 121 present mentions | 43 patients |
| recurrence_risk_features_mv | **1** | 1 |
| patient_refined_staging_flags_v3 | **1** | 1 |

**Gap:** 76 TERT+ patients in molecular episodes vs 1 in staging flags → 75 patients lost in translation. Critical fix needed.

### 3. Post-op PTH/Calcium (Priority #3)
| Metric | Value |
|--------|-------|
| Existing PTH/calcium table | **NONE** |
| Patients with PTH/calcium note mentions | 2,492 |
| Notes with PTH/calcium mentions | 3,434 |
| Lab-related tables | thyroglobulin_labs (30,245), anti_thyroglobulin_labs, lab_timeline, thyroseq_followup_labs |

### 4. RAI Dose/Avidity (Priority #4)
| Metric | Value |
|--------|-------|
| RAI episodes total | 1,857 |
| With dose_mci | **55** (3.0%) |
| With date | 1,272 (68.5%) |
| Dose range | 98.7–449.0 mCi |
| Has source_note_type column | Yes |
| Has assertion_status | Yes |

### 5. Extranodal Extension (Priority #5)
| Metric | Value |
|--------|-------|
| path_synoptics total | 11,688 |
| With ENE data | **1,374** (11.8%) |
| "x" placeholder | 903 |
| "present" (some with location) | 377 |
| With level/location detail | ~15 (present + level annotation) |

## Clinical Notes Distribution
| Note Type | Count |
|-----------|-------|
| op_note | 4,680 |
| h_p | 4,221 |
| other_history | 525 |
| endocrine_note | 519 |
| ed_note | 498 |
| history_summary | 249 |
| dc_sum | 185 |
| other_notes | 160 |

## Plan
1. Build `extraction_audit_engine_v3.py` with new parser classes
2. Execute each variable in priority order with intrinsic evaluation
3. Materialize refined tables to MotherDuck
4. Update staging flags, advanced features, and validation
5. Re-run H1/H2 sensitivity
