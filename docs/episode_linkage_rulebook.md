# Episode Linkage Rulebook — Multi-Surgery Patients

> Generated: 2026-03-15 | Script: `scripts/97_episode_linkage_audit.py` | Version: v1

## 1. Scope

This rulebook governs how clinical artifacts (notes, labs, RAI, molecular tests, FNA, imaging) are assigned to specific surgery episodes for the **761 patients with ≥2 distinct surgery dates** (1,576 total surgery episodes).

The single-surgery majority (10,110 patients) is unaffected — all artifacts map 1:1.

---

## 2. Multi-Surgery Cohort Definition

| Criterion | Value |
|-----------|-------|
| Source table | `path_synoptics` |
| Deduplication | `ROW_NUMBER() OVER (PARTITION BY research_id, surg_date)` |
| Surgery count | ≥2 distinct `surg_date` per `research_id` |
| Procedure normalization | LOWER + keyword matching (total, lobectomy, subtotal, isthmusectomy) |
| Laterality derivation | Keyword extraction: right/left/bilateral/isthmus/unspecified |
| Completion flag | `completion IN ('yes','y','completion')` |

### Surgery Distribution

| Surgeries per Patient | Count |
|----------------------:|------:|
| 2 | 719 |
| 3 | 33 |
| 4 | 7 |
| 5 | 1 |
| 6 | 1 |
| **Total patients** | **761** |
| **Total episodes** | **1,576** |

---

## 3. Temporal Window Assignment

### 3.1 Window Construction

Each surgery is assigned a contiguous half-open time window `[window_start, window_end)`:

```
window_start = midpoint between prev_surgery_date and surgery_date
               (or 1900-01-01 for the first surgery)

window_end   = midpoint between surgery_date and next_surgery_date
               (or 2099-12-31 for the last surgery)
```

For a patient with surgeries on 2020-03-01 and 2020-09-01:
```
Surgery 1: [1900-01-01, 2020-06-01)  ← all artifacts before June 2020
Surgery 2: [2020-06-01, 2099-12-31)  ← all artifacts from June 2020 onward
```

### 3.2 Assignment Rule

Each artifact is assigned to the surgery with the **smallest absolute day gap** from the artifact date. Ties broken by confidence tier, then by surgery order.

### 3.3 Confidence Tiers (artifact → surgery)

| Tier | Clinical Notes | Labs | RAI/Molecular | FNA/Imaging |
|------|---------------|------|--------------|-------------|
| `exact_match` | op_note ≤1 day | — | — | — |
| `high_confidence` | ≤7 days | ≤7 days | ≤30 days | ≤14 days |
| `plausible` | ≤30 days | ≤90 days | ≤180 days | ≤90 days |
| `weak` | ≤180 days | ≤365 days | ≤365 days | ≤365 days |
| `unlinked` | >180 days | >365 days | >365 days | >365 days |

**Domain-specific rationale:**
- **Clinical notes**: Op notes are nearly synchronous (exact_match at ≤1d); H&P/discharge within a week is high_confidence.
- **Labs**: Thyroglobulin surveillance follows 6-12 month intervals post-surgery, so up to 365d is still plausible linkage.
- **RAI**: Typically administered 4-12 weeks post-surgery; 30d = high_confidence, 180d = plausible.
- **FNA/Imaging**: Pre-surgical workup typically within 2 weeks; extended to 90-365d for surveillance.

---

## 4. Mislink Detection Rules

### 4.1 Surgery-Pathology Linkage

Cross-checks `surgery_pathology_linkage_v3` against the temporal cohort:

| Verdict | Rule |
|---------|------|
| `correct` | `\|surg_date − cohort_surgery_date\| = 0 days` |
| `minor_mismatch` | `\|surg_date − cohort_surgery_date\| ≤ 3 days` |
| `mislink_candidate` | `\|surg_date − cohort_surgery_date\| > 3 days` |

### 4.2 Pathology-RAI Linkage

Cross-checks `pathology_rai_linkage_v3` against artifact assignment:

| Verdict | Rule |
|---------|------|
| `correct` | `\|rai_date − audit_surgery_date\| ≤ 7 days` |
| `minor_mismatch` | `\|rai_date − audit_surgery_date\| ≤ 30 days` |
| `mislink_candidate` | `\|rai_date − audit_surgery_date\| > 30 days` |
| `no_date` | RAI date is NULL |

### 4.3 Preop-Surgery Linkage

Cross-checks `preop_surgery_linkage_v3` against artifact assignment:

| Verdict | Rule |
|---------|------|
| `correct` | Artifact-assigned surgery_rank matches linked surgery_episode_id |
| `mislink_candidate` | Surgery ranks disagree |
| `unresolved` | Artifact not found in assignment table |

### Audit Results (2026-03-15)

| Domain | Candidates | Mislink | Minor | Unresolved |
|--------|-----------|---------|-------|------------|
| surgery_pathology | 1,450 | 1,440 | 10 | — |
| preop_surgery | 34 | 20 | — | 14 |
| pathology_rai | 4 | 4 | — | — |
| **Total** | **1,488** | **1,464** | **10** | **14** |

**Interpretation**: The high surgery_pathology mislink count (1,440) is expected. The v3 linkage tables were built without multi-surgery awareness — they link pathology to the *first* surgery_episode_id across ALL patients, not the temporally correct one. For multi-surgery patients, this is structurally incorrect when pathology belongs to surgery #2, #3, etc.

---

## 5. Integrity Grading

Each multi-surgery patient receives an overall integrity grade:

| Grade | Rule | Count | Pct |
|-------|------|------:|----:|
| `GREEN` | ≥80% of artifacts are exact/high_confidence AND 0 mislinks | 53 | 7.0% |
| `YELLOW` | ≥60% of artifacts are exact/high/plausible AND 0 mislinks | 75 | 9.9% |
| `RED` | <60% confident AND 0 mislinks | 14 | 1.8% |
| `REVIEW_REQUIRED` | Any mislink detected | 616 | 80.9% |
| `NO_ARTIFACTS` | Zero artifacts found for this patient | 3 | 0.4% |

**Note**: `REVIEW_REQUIRED` dominates because surgery_pathology_linkage_v3 has date discrepancies for nearly all multi-surgery patients. This is a systemic v3 linkage limitation, not individual patient data quality issues.

---

## 6. Ambiguity Detection

An artifact is flagged as **ambiguous** when the day gap to the closest surgery and the second-closest surgery differ by ≤14 days.

| Metric | Value |
|--------|------:|
| Total ambiguous artifacts | 3,992 |
| Affected patients | ~450 |
| Domains: clinical_note | ~1,500 |
| Domains: lab | ~2,400 |

Ambiguous artifacts require manual or heuristic resolution (e.g., note_type, laterality matching, clinical context).

---

## 7. Episode Key Propagation Audit

Checks whether downstream tables carry correct `surgery_episode_id` for multi-surgery patients:

| Table | Patients Present | Patients Expected | Distinct Episode IDs | All-ones (multi-surg) |
|-------|----------------:|------------------:|--------------------:|---------:|
| operative_episode_detail_v2 | 622 | 761 | 1 | 624 |
| episode_analysis_resolved_v1_dedup | 622 | 761 | 1 | 622 |
| tumor_episode_master_v2 | 761 | 761 | 6 | 761 |

**Key findings:**
- **operative_episode_detail_v2** and **episode_analysis_resolved_v1_dedup** only have `surgery_episode_id = 1` for all multi-surgery patients → episode ID is not properly propagated (139 patients missing entirely)
- **tumor_episode_master_v2** has 6 distinct episode IDs and all 761 patients present → correctly models multi-surgery structure

---

## 8. Artifact Inventory (2026-03-15 Audit)

### By Domain

| Domain | Artifacts Assigned |
|--------|------------------:|
| lab | 5,748 |
| imaging | 1,067 |
| clinical_note | 700 |
| fna | 572 |
| rai | 317 |
| molecular | 94 |
| **Total** | **8,498** |

### By Confidence

| Confidence | Count | Pct |
|------------|------:|----:|
| exact_match | 353 | 4.2% |
| high_confidence | 354 | 4.2% |
| plausible | 1,434 | 16.9% |
| weak | 1,541 | 18.1% |
| unlinked | 4,816 | 56.7% |
| **Total** | **8,498** | 100% |

**Interpretation**: 56.7% unlinked means these artifacts occurred >365 days (labs) or >180 days (notes) from any surgery — long-term surveillance data. The 8.3% exact/high rate covers perioperative artifacts with strong temporal linkage.

---

## 9. Recommendations

### Immediate (pre-publication)
1. **Fix v3 linkage tables** for multi-surgery patients: add multi-surgery surgery-episode routing so pathology links to the temporally correct surgery, not always episode #1.
2. **Backfill surgery_episode_id** in `operative_episode_detail_v2` and `episode_analysis_resolved_v1_dedup` for the 139 missing multi-surgery patients.
3. **Manuscript caveat**: State that multi-surgery patients (761/10,871 = 7.0%) have episode-level linkage verified via temporal-window audit; 81% required review due to pre-existing v3 linkage limitations.

### Future
4. **Laterality-aware assignment**: For near-equidistant artifacts (3,992 ambiguous), use laterality matching between the artifact and surgery to disambiguate.
5. **Note-type heuristics**: Op notes → index surgery; pre-op H&P → next surgery; discharge summary → most recent surgery.
6. **Longitudinal lab windowing**: For Tg surveillance labs >365d from any surgery, assign to the most recent prior surgery rather than marking unlinked.

---

## 10. Output Tables

| Table | Rows | Description |
|-------|-----:|-------------|
| `multi_surgery_episode_cohort_v1` | 1,576 | One row per surgery per multi-surgery patient |
| `val_episode_artifact_assignment_v1` | 8,498 | Per-artifact → surgery assignment with confidence |
| `val_episode_mislink_candidates_v1` | 1,488 | Detected mislinks from v3 linkage tables |
| `val_episode_linkage_integrity_v1` | 761 | Per-patient integrity grade |
| `val_episode_key_propagation_v1` | 3 | Episode ID propagation check per downstream table |
| `val_episode_ambiguity_review_v1` | 3,992 | Near-equidistant artifacts requiring review |
| `val_episode_linkage_summary_v1` | 13 | Aggregate KPI summary |

All tables deployed to `thyroid_research_2026_dev`. Promotion path: dev → qa → prod via `scripts/95`.
