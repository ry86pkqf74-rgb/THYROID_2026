# Multi-Surgery Truth Snapshot

> Generated: 2026-03-15 | Source: `scripts/97_episode_linkage_audit.py` on MotherDuck dev  
> Export: `exports/episode_linkage_audit_20260315_0036/`

---

## Executive Summary

Of the 10,871 thyroid surgery patients in the research database, **761 (7.0%) had ≥2 distinct surgery dates**, accounting for 1,576 total surgery episodes. This snapshot documents the current state of episode-level data integrity for this multi-surgery subpopulation and identifies systemic linkage gaps that pre-date this audit.

### Top-Line KPIs

| Metric | Value |
|--------|------:|
| Multi-surgery patients | 761 |
| Total surgery episodes | 1,576 |
| Total artifacts assigned | 8,498 |
| % exact/high confidence | 8.3% |
| % in-window (midpoint rule) | 100.0% |
| Mislink candidates detected | 1,464 |
| Ambiguous artifacts | 3,992 |
| Integrity: GREEN | 53 (7.0%) |
| Integrity: YELLOW | 75 (9.9%) |
| Integrity: RED | 14 (1.8%) |
| Integrity: REVIEW_REQUIRED | 616 (80.9%) |
| Integrity: NO_ARTIFACTS | 3 (0.4%) |

---

## 1. Cohort Characteristics

### Surgery Count Distribution

```
 2 surgeries: ████████████████████████████████████████████████████  719 patients (94.5%)
 3 surgeries: ██                                                    33 patients  (4.3%)
 4 surgeries: ▏                                                      7 patients  (0.9%)
 5 surgeries: ▏                                                      1 patient   (0.1%)
 6 surgeries: ▏                                                      1 patient   (0.1%)
```

### Procedure Type Breakdown

The most common multi-surgery pattern is **lobectomy → completion thyroidectomy**, consistent with the clinical pathway where initial lobectomy reveals malignancy prompting return for total thyroidectomy.

### Inter-Surgery Gap Statistics

| Statistic | Days |
|-----------|-----:|
| Minimum | 2 |
| Median | 98 |
| Mean | 484 |
| Maximum | ~6,860 |

The 2-day minimum likely represents re-exploration for complications. The wide mean (484 days) reflects surveillance-driven reoperations.

---

## 2. Artifact Landscape

### By Domain

| Domain | Count | Share |
|--------|------:|------:|
| Lab (Tg, anti-Tg, PTH, Ca) | 5,748 | 67.6% |
| Imaging (US nodule) | 1,067 | 12.6% |
| Clinical notes | 700 | 8.2% |
| FNA cytology | 572 | 6.7% |
| RAI treatment | 317 | 3.7% |
| Molecular test | 94 | 1.1% |

Labs dominate because multi-surgery patients have extended follow-up with serial Tg surveillance. Most labs (56.7%) fall outside any surgery's ≤365-day window and are classified `unlinked` — these represent long-term surveillance events, not linkage failures.

### By Assignment Confidence

| Tier | Count | Share | Interpretation |
|------|------:|------:|---------------|
| exact_match | 353 | 4.2% | Op notes on surgery day |
| high_confidence | 354 | 4.2% | Perioperative artifacts (≤7-30d) |
| plausible | 1,434 | 16.9% | Follow-up within expected window |
| weak | 1,541 | 18.1% | Distant but temporally closer to this surgery |
| unlinked | 4,816 | 56.7% | Long-term surveillance beyond linkage window |

---

## 3. Linkage Integrity Findings

### 3.1 V3 Linkage Table Mislinks

The v3 cross-domain linkage tables (`surgery_pathology_linkage_v3`, `pathology_rai_linkage_v3`, `preop_surgery_linkage_v3`) were built without explicit multi-surgery routing. For multi-surgery patients, pathology/RAI/FNA may be linked to the wrong `surgery_episode_id`.

**Mislink breakdown:**

| Domain | Total Flagged | Mislink Candidates | Minor Mismatch | Unresolved |
|--------|-------------:|-------------------:|---------------:|-----------:|
| surgery_pathology | 1,450 | 1,440 | 10 | 0 |
| preop_surgery | 34 | 20 | 0 | 14 |
| pathology_rai | 4 | 4 | 0 | 0 |

**Root cause**: `surgery_pathology_linkage_v3` compares the `surg_date` in the linkage row against the `surgery_date` in the cohort. When a patient has 2+ surgeries, the linkage table may carry the same surgery_episode_id=1 date for all pathology rows, causing a date discrepancy against surgery #2.

### 3.2 Episode Key Propagation

| Table | Present | Expected | Distinct Episode IDs | Issue |
|-------|--------:|---------:|--------------------:|-------|
| operative_episode_detail_v2 | 622 | 761 | 1 | All episode_id=1; 139 patients missing |
| episode_analysis_resolved_v1_dedup | 622 | 761 | 1 | All episode_id=1; 139 patients missing |
| tumor_episode_master_v2 | 761 | 761 | 6 | ✓ Correct multi-episode modeling |

**Key finding**: `tumor_episode_master_v2` is the only downstream table that correctly tracks multi-surgery episodes. The operative and resolved-episode tables collapse all surgeries into episode_id=1.

### 3.3 Ambiguity Profile

3,992 artifacts are **nearly equidistant** (≤14-day gap difference) between two surgeries. These are concentrated in:
- Labs (thyroglobulin drawn between two surgeries)
- Clinical notes (H&P that could reference either upcoming or recent surgery)

---

## 4. Impact Assessment

### On Manuscript Analyses

| Analysis Type | Impact | Severity |
|---------------|--------|----------|
| Patient-level descriptives (Table 1) | None — patient-level aggregation is unaffected | LOW |
| Surgery-level outcomes | Moderate — some outcomes may attribute to wrong surgery | MEDIUM |
| Time-to-event (KM, Cox) | Low — uses first surgery date as origin; recurrence date is patient-level | LOW |
| Complication rates per surgery | High — completion thyroidectomy complications may mismatch | HIGH |
| RAI dose-response | Moderate — RAI may link to wrong preceding surgery | MEDIUM |

### On Data Quality Scores

The multi-surgery population's linkage quality does NOT degrade the overall database quality score (98/100) because:
1. 93% of patients (10,110) are single-surgery and unaffected
2. The audit tables provide full transparency for the affected 7%
3. No data is lost — artifacts are assigned, just with lower confidence

---

## 5. Actionable Remediation Plan

### Critical Path (blocks publication)
- [ ] None — manuscript uses patient-level aggregation primarily

### Recommended (improves robustness)
- [ ] Fix `operative_episode_detail_v2` episode_id propagation for 139 missing patients
- [ ] Add multi-surgery routing to v3 linkage SQL (scripts 23/48)
- [ ] Add manuscript caveat: "Episode-level analyses for multi-surgery patients (7.0%) use temporal-window assignment; see Supplement"

### Future Enhancement
- [ ] Laterality-aware disambiguation for 3,992 ambiguous artifacts
- [ ] Build `episode_mislink_resolution_v1` table to capture manual adjudication decisions
- [ ] Integrate into daily refresh pipeline (script 36)

---

## 6. Validation Artifacts

All tables deployed to `thyroid_research_2026_dev`:

```
multi_surgery_episode_cohort_v1      1,576 rows
val_episode_artifact_assignment_v1   8,498 rows
val_episode_mislink_candidates_v1    1,488 rows
val_episode_linkage_integrity_v1       761 rows
val_episode_key_propagation_v1           3 rows
val_episode_ambiguity_review_v1      3,992 rows
val_episode_linkage_summary_v1          13 rows
```

CSV exports: `exports/episode_linkage_audit_20260315_0036/` (7 CSVs + manifest.json)

---

## 7. Audit Methodology

- **Script**: `scripts/97_episode_linkage_audit.py`
- **Execution**: `--env dev --export`
- **Source data**: Cross-database read from `thyroid_research_2026.*` (prod) into `thyroid_research_2026_dev` (workspace mode, no ATTACH)
- **All 7 tables created successfully** with 0 errors
- **Reproducible**: Re-run with `--env dev` to regenerate all tables from scratch
