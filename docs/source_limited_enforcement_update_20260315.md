# Source-Limited Field Enforcement — Registry Update

**Date:** 2026-03-15  
**Script:** `scripts/103_source_limited_enforcement.py`  
**Target:** MotherDuck `thyroid_research_2026` (prod)

## Purpose

Extends the source-limited field registry from a flat CSV into a governed
MotherDuck table with analysis-tier eligibility, limitation categories, and
manuscript-safe wording. Provides automated validation assertions.

## Output Tables

| Table | Rows | Description |
|-------|------|-------------|
| `source_limited_enforcement_registry_v2` | 35 | Per-field governance metadata |
| `source_limited_enforcement_summary_v1` | 10 | Per-tier × limitation-category summary |
| `val_source_limited_enforcement_v1` | 6 | Validation assertions (ALL PASS) |

## Tier Rules

| Tier | Analysis Tier | Table 1 | Regression | Survival | Caveat Required |
|------|--------------|---------|------------|----------|-----------------|
| **CANONICAL** | primary_and_secondary | ✓ | ✓ | ✓ | No |
| **SOURCE_LIMITED** | secondary_with_caveat | ✓ | ✓ (caveat) | ✓ (caveat) | Yes |
| **DERIVED_APPROXIMATE** | exploratory_only | No | No | No | Yes |
| **MANUAL_REVIEW_ONLY** | prohibited_for_population | No | No | No | Yes |

## Limitation Categories

Each field is classified into one of 4 limitation categories:

- **source_feed** (11 fields): Data source absent from institution (e.g., nuclear
  medicine reports, structured lab orders)
- **template** (5 fields): Operative note template records risk discussion boilerplate
  as default values (e.g., `FALSE` meaning "not parsed", not "confirmed negative")
- **pipeline** (7 fields): V2 extractor output exists but not materialized to
  MotherDuck (e.g., berry_ligament_flag, frozen_section_flag)
- **review** (12 fields): Fields requiring clinical adjudication before population-level
  denominators are valid

## Field Distribution

| Status | Count | Categories |
|--------|-------|-----------|
| SOURCE_LIMITED | 22 | source_feed (11), template (5), pipeline (6) |
| CANONICAL | 10 | review (6), source_feed (2), pipeline (2) |
| DERIVED_APPROXIMATE | 2 | pipeline (1), review (1) |
| MANUAL_REVIEW_ONLY | 1 | review (1) |

## Safe Manuscript Wording (per tier)

- **CANONICAL**: No special wording needed.
- **SOURCE_LIMITED**: "Field coverage limited by absence of institutional data
  feed; reported values represent available subset only."
- **DERIVED_APPROXIMATE**: "Values derived from approximate heuristic; not
  suitable for primary or sensitivity analyses."
- **MANUAL_REVIEW_ONLY**: "Field requires manual clinical adjudication; do not
  use for population-level denominators without expert review."

## Validation Assertions

| Assertion | Expected | Result |
|-----------|----------|--------|
| minimum_total_fields | ≥30 | PASS (35) |
| minimum_canonical_count | ≥5 | PASS (10) |
| minimum_source_limited_count | ≥10 | PASS (22) |
| no_null_field_names | 0 nulls | PASS |
| no_null_statuses | 0 nulls | PASS |
| all_tiers_have_wording | 0 missing | PASS |

## Exports

`exports/source_limited_enforcement_20260315_0746/`

- `enforcement_registry.csv`
- `enforcement_summary.csv`
- `enforcement_validation.csv`
- `manifest.json`

## Promotion Gate

Gate **G9 (source_limited_registry)** validates:
- `source_limited_enforcement_registry_v2` exists with ≥30 rows
- Both CANONICAL and SOURCE_LIMITED tiers present
- `val_source_limited_enforcement_v1` has 0 FAIL assertions

Status: **PASS** (verified 2026-03-15)

## Source CSV

`exports/final_release_hardening_20260314/source_limited_field_registry.csv` (35 fields)
