# Reviewer Defense: How Were Duplicates Prevented?

## Patient-Level Deduplication

The patient spine originates from `path_synoptics`, which contains **11,688 pathology records** for **10,871 unique patients**. The one-to-many relationship exists because multi-pathology-per-surgery events and multiple surgeries are legitimate clinical occurrences.

The manuscript cohort (`manuscript_cohort_v1`) enforces `DISTINCT research_id` at construction time. Verification confirms **0 duplicate patients** across 10,871 rows — every `research_id` appears exactly once.

## Episode-Level Deduplication

`episode_analysis_resolved_v1` initially contained **146 duplicate episodes** arising from multi-pathology JOIN fan-out when linking canonical tables. These were resolved deterministically into `episode_analysis_resolved_v1_dedup` (**9,368 unique surgery episodes**) using the following priority cascade:

| Priority | Criterion | Rationale |
|----------|-----------|-----------|
| 1 | `analysis_eligible = TRUE` | Prefer episodes with complete data |
| 2 | T-stage severity (descending) | Worst pathology governs staging |
| 3 | N-stage severity (descending) | Worst nodal status governs staging |
| 4 | Tumor size (descending) | Largest tumor is index lesion |
| 5 | LN positive count (descending) | Highest burden retained |
| 6 | Linkage score (descending) | Best-linked episode preferred |

### Duplicate Profile (Pre-Resolution)

| Group Size | Count | Total Excess Rows |
|------------|-------|-------------------|
| 2 | 114 | 114 |
| 3 | 22 | 44 |
| 4 | 4 | 12 |
| 5 | 2 | 6 |
| 8 | 3 | 21 |
| 9 | 1 | 8 |

All 146 groups were classified in `episode_duplicate_profile_v1`: 144 as `other_join_fanout` (identical rows from V2 linkage fan-out) and 2 as `join_artifact_v2_linkage` (different V2 confidence tiers). Excluded rows are preserved in `episode_duplicate_review_v1` (207 rows) for audit.

## Master Clinical Table Caveat

`patient_refined_master_clinical_v12` contains **2,015 duplicate `research_id` values** from multi-pathology joins. This table is **not** used as the manuscript spine. It serves as a wide enrichment lookup; the canonical deduplicated patient table is `manuscript_cohort_v1`.

## Readiness Gate Evidence

| Gate | Test | Result |
|------|------|--------|
| G1 | Patient-level duplicates in `manuscript_cohort_v1` | **PASS** (0 duplicates) |
| G2 | Episode-level duplicates in `episode_analysis_resolved_v1_dedup` | **PASS** (0 duplicates) |

`readiness_assessment.json` records: `episode_dupes_before=146, episode_dupes_after=0`.

## Key References

- **Dedup logic**: `scripts/56_readiness_gate.py` — readiness gate assertions G1 and G2
- **Episode dedup table**: `episode_analysis_resolved_v1_dedup` (MotherDuck, 9,368 rows)
- **Duplicate profile**: `episode_duplicate_profile_v1` (MotherDuck, 146 rows)
- **Excluded rows audit**: `episode_duplicate_review_v1` (MotherDuck, 207 rows)
- **Readiness report**: `exports/FINAL_PUBLICATION_BUNDLE_20260313/readiness_assessment.json`
- **Manuscript cohort**: `manuscript_cohort_v1` (MotherDuck, 10,871 rows, 0 duplicates)
