# MotherDuck multi-tumor pathology audit

**Generated:** 2026-03-27T05:55:47.633549+00:00
**Connection:** local:memory+path_synoptics.parquet(synthetic tumor_episode + aggregate)
**Token mode:** n/a (local)

## Lineage (canonical)

| Layer | Object | Role |
|-------|--------|------|
| Source-derived wide | `path_synoptics` | One row per pathology/synoptic specimen (slots 1–5 wide) |
| Long foci | `synoptic_tumor_long_v1` (or `md_*`) | One row per slot with any field nonempty (script 108, SLOT_MAP) |
| Episode spine | `tumor_episode_master_v2` | **Only** `tumor_ordinal = 1` per surgery (script 22) |
| Patient rollup | `extracted_multi_tumor_aggregate_v1` | Latest PS row per patient; `n_tumors` = histology-nonempty slots; worst ETE/margin/size |
| Lesion export | `lesion_analysis_resolved_v1` | Derived from `tumor_episode_master_v2` → inherits single ordinals |
| Proposal 2 | `ptc_cohort` → `exports/ptc_full.csv` | `tumor_1_extrathyroidal_ext`, `largest_tumor_cm` — **tumor_1 / tumor_pathology** centric |

## Resolved dependencies

- `synoptic_tumor_long_v1`: **synoptic_tumor_long_v1**
- `tumor_episode_master_v2`: **tumor_episode_master_v2**
- `extracted_multi_tumor_aggregate_v1`: **extracted_multi_tumor_aggregate_v1**
- `tumor_pathology`: **MISSING**
- `lesion_analysis_resolved_v1`: **MISSING**

## Key counts

- Pathology rows: **11,688**
- Specimens with ≥2 nonempty slots (any SLOT_MAP field): **1,379**
- Specimens with ≥2 histology slots: **1,366**
- `tumor_1_multiple_tumor` text multifocal flag: **1**
- Long-table rows: **11,103** (table ` synoptic_tumor_long_v1 `)

### Distribution `n_slots_any` (nonempty OR across SLOT_MAP columns)

|   n_populated_tumor_slots |   n_pathology_records |
|--------------------------:|----------------------:|
|                         0 |                  2630 |
|                         1 |                  7679 |
|                         2 |                   921 |
|                         3 |                   305 |
|                         4 |                    98 |
|                         5 |                    55 |

### Distribution `n_slots_histology_only`

|   n_histology_slots |   n_pathology_records |
|--------------------:|----------------------:|
|                   0 |                  7240 |
|                   1 |                  3082 |
|                   2 |                   911 |
|                   3 |                   308 |
|                   4 |                    95 |
|                   5 |                    52 |

## Discrepancy flags (from `_mt_disc`)

- Rows with canonical-episode design limit (≥2 slots but TE max ordinal ≤1): **1,379** (expected — not a drop)
- Long undercount vs slots: **0**
- Max size across slots > tumor_1 slot size: **107**
- Max slot size > `tumor_pathology.histology_1_largest_tumor_cm`: **0**

Exported **109** high-signal discrepancy rows to `motherduck_multi_tumor_discrepant_cases.csv`.

## Completeness verdict

Multi-tumor completeness is **proven** only if: (1) `synoptic_tumor_long_v1` exists on MotherDuck, (2) `n_long_rows` equals `n_slots_any` for every pathology row key `(research_id, surg_d)`, and (3) the table was built from the same `path_synoptics` snapshot as production. Canonical `tumor_episode_master_v2` remains **single-ordinal-by-design** (tumor 1 spine); absence of additional ordinals is not a load bug. **`ptc_cohort` / `exports/ptc_full.csv` / proposal2** use **tumor_1** ETE and pathology-linked largest size; secondary-foci ETE or larger focus in slots 2–5 can differ from `tumor_1_*` — see `max_size_exceeds_tumor1_slot` and discrepancy export.

**This run used `--local`** (parquet + synthetic tumor_episode/aggregate). Re-run without `--local` against MotherDuck for production lineage.

## Proposal2 (ETE staging) impact

Variables in `proposal2_ete_analysis.py` come from **`ptc_full.csv`**: `tumor_1_extrathyroidal_ext`, `largest_tumor_cm`, staging from `tumor_pathology`. They do **not** automatically incorporate worst-of-all-foci from slots 2–5. When `max_size_exceeds_tumor1_slot` or secondary-site ETE is present, **reported ETE/size can be incomplete relative to full synoptic multi-tumor data** unless augmented from `extracted_multi_tumor_aggregate_v1` or `synoptic_tumor_long_v1`.

## SQL artifacts

- Skeleton: `sql/motherduck_multi_tumor_audit.sql`
- Generated run: `sql/motherduck_multi_tumor_audit_generated.sql`
