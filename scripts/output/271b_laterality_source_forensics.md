# Script 271b — cpm.laterality Source Forensics

**Run:** 2026-04-18T02:13:24Z

## cpm.laterality distribution

| Value | n |
|---|---:|
| `bilateral` | 5571 |
| `left` | 2428 |
| `right` | 2339 |
| `None` | 533 |

## Candidate sources, ranked by 3-way agreement (bilateral/left/right)

| Candidate | n in 3-way buckets | matches | agreement |
|---|---:|---:|---:|
| `canonical_patient_master.path_laterality` | 10338 | 10338 | 100.0% |
| `path_synoptics.tumor_laterality` | 626 | 619 | 98.9% |
| `operative_episode_detail_v2.laterality` | 453 | 417 | 92.1% |
| `tumor_pathology.tumor_laterality_overall` | 3653 | 1680 | 46.0% |
| `path_synoptics.tumor_1_site_laterality` | 3496 | 1168 | 33.4% |
| `path_synoptics.bilateral_neck_dissection` | 0 | 0 | 0.0% |
| `operative_episode_detail_v2.lateral_neck_dissection_flag` | 0 | 0 | 0.0% |

**Verdict:** identified

**Proposed COMMENT for cpm.laterality (applied in Step 6):**

```
Patient-level laterality. Forensics in Script 271b found this column to be functionally identical to cpm.path_laterality (100.0% 3-way agreement, identical NULL pattern), strongly suggesting they are duplicate or copy columns from the same upstream feeder. Vocabulary: bilateral/left/right/NULL. Predates Script 271; documented retroactively. For new analyses prefer tumor_pathology_laterality_v271b (rebuilt from tumor_pathology under documented rules). Script 271b, 2026-04-18.
```

## Best-candidate confusion (cpm.laterality × bucketed candidate)

Candidate: `canonical_patient_master.path_laterality`

| cpm.laterality | candidate bucket | n |
|---|---|---:|
| `bilateral` | `bilateral` | 5571 |
| `left` | `left` | 2428 |
| `right` | `right` | 2339 |
| `None` | `None` | 533 |

