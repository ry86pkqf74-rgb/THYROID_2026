# Episode Linkage Non-Regression Report v2 — 20260315

## Single-Surgery Patient Safety Check

Overall: **PASS**

| Table | Total | EP=1 | Non-EP1 | Status |
|-------|-------|------|---------|--------|
| episode_analysis_resolved_v1_dedup | 8110 | 8110 | 0 | PASS |
| operative_episode_detail_v2 | 8111 | 8111 | 0 | PASS |
| preop_surgery_linkage_v3 | 3263 | 3263 | 0 | PASS |
| surgery_pathology_linkage_v3 | 8113 | 8113 | 0 | PASS |

## Interpretation

All single-surgery patients (10,109) retain surgery_episode_id=1.
No single-surgery patient was reassigned to a different episode.