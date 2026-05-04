# Lymph Node Procedure Investigation & Resolution

**Date:** 2026-05-04
**Investigator:** Claude (Cowork) + Logan Glosser
**Database:** thyroid_canonical_publication_v1_0

## Summary

Investigated and resolved three data pipeline gaps in the canonical operative layer:
1. **Completion thyroidectomy** — 0 in rollup → 931 resolved, 346 events backfilled
2. **Central neck dissection (CND)** — 2,495 → 2,831 events (2,897 in resolution)
3. **Lateral neck dissection (LND)** — 241 → 476 events (529 in resolution, including 186 MRND)
4. **LND sidedness** — added left/right/bilateral/unspecified to all 529 LND patients

## Root Cause

Procedure codes NLP correctly extracted procedure mentions but these were never propagated
to the canonical operative events layer or the patient rollup. The same pattern affected
all three procedure types.

## MotherDuck Tables Created

### manuscript_workspace

| Table | Rows | Description |
|-------|------|-------------|
| completion_thyroidectomy_resolved_v1 | 953 | Multi-source completion thyroidectomy resolution with confidence tiers |
| ln_dissection_cnd_resolved_v1 | 2,897 | CND resolution from 4 sources: operative events, CPM, proc codes, NSQIP |
| ln_dissection_lnd_resolved_v1 | 529 | LND resolution v1 (no sidedness) |
| ln_dissection_lnd_resolved_v2 | 529 | LND resolution v2 with sidedness (left/right/bilateral/unspecified) |
| lnd_level_by_side_crosstab_v1 | 529 | Patient-level cross-tab: LND type × side × per-level LN examined/positive |

### Views

| View | Description |
|------|-------------|
| operative_patient_rollup_completion_fixed_VIEW_v1 | Full 931-patient completion view with confidence tiers |

### archive_pub_v1_0

| Table | Rows | Description |
|-------|------|-------------|
| canonical_operative_events_v1_pre_completion_fix_20260504 | 708 | Pre-fix snapshot of operative events |
| canonical_operative_patient_rollup_v1_pre_completion_fix_20260504 | 10,871 | Pre-fix snapshot of patient rollup |
| canonical_operative_events_v1_pre_ln_fix_20260504 | 704 | Pre-LN-fix snapshot |
| ln_dissection_lnd_resolved_v1_pre_side_20260504 | 529 | Pre-sidedness snapshot |

### main schema tables MODIFIED

- `canonical_operative_events_v1`: 346 completion, 336 CND, 235 LND events backfilled
- `canonical_operative_patient_rollup_v1`: corresponding rollup counts updated

## LND Sidedness Resolution

### Method (priority order)
1. `lateral_side_v10` — explicit side from v10 operative extraction (94 patients)
2. `ln_pathology_counts` — right/left lateral LN examined/positive > 0 (273 patients)
3. `ops_tumor_side` — tumor laterality as fallback (8 patients)
4. Remaining 154 → unspecified

### Distribution

| Side | Total | MRND | Selective LND | LND Unspecified |
|------|-------|------|---------------|-----------------|
| Right | 213 (40.3%) | 82 | 85 | 46 |
| Left | 118 (22.3%) | 42 | 63 | 13 |
| Bilateral | 44 (8.3%) | 24 | 18 | 2 |
| Unspecified | 154 (29.1%) | 38 | 86 | 30 |

### Per-Level LN Data Coverage (among LND patients)

| Level | Patients with examined data | Patients with positive nodes |
|-------|---------------------------|------------------------------|
| Level II | 293 | 181 |
| Level III | 236 | 180 |
| Level IV | 240 | 175 |
| Level V | 172 | 136 |
| Level VI | 1,806 | 780 |

## Confidence Tiers

- **Tier 1** (3+ sources agree): highest confidence
- **Tier 2** (2 sources agree): moderate confidence
- **Tier 3** (single source): lower confidence, flagged for review

## Reproducibility

All resolution tables can be regenerated from source data. Archive tables preserve
pre-modification state for audit trail. Table comments document provenance and build dates.
