# Schema notes (live MotherDuck audit)

Generated: 2026-03-26T04:15:27.410520+00:00

## Catalog

- Database: `thyroid_research_2026` (read-write connection used for SELECT only in this study).

## Discrepancy vs repo docs (AGENTS / pipeline_architecture)

| Doc expectation | Live finding |
|-----------------|--------------|
| `imaging_fna_linkage_v2` | **Missing** — use `imaging_fna_linkage_v3` (9024 rows). |
| `preop_surgery_linkage_v2` | Present but **0 rows** — use `preop_surgery_linkage_v3` (3591 rows). |
| `surgery_pathology_linkage_v2` | **Missing** — use `surgery_pathology_linkage_v3`. |
| `fna_molecular_linkage_v2` | 0 rows in live — prefer `fna_molecular_linkage_v3` where populated. |

## Canonical preop nodule table for this study

**Primary:** `imaging_nodule_long_v2`

- Row count: 10866
- Rationale: episode-level grain with `size_cm_max`, `resolved_exam_date`, `laterality`, `tirads_score`, `linked_fna_episode_id`, `linked_molecular_episode_id`, `suspicious_node_flag`, aligned with `imaging_exam_summary_v2`.

**Secondary / cross-check:** `imaging_nodule_master_v1` (higher row count; `max_dimension_cm`, `fna_link_score_v3`) for size fallback when long table lacks a row.

## Inventory file

See `source_inventory.csv` for all candidate objects, row counts, and errors.
