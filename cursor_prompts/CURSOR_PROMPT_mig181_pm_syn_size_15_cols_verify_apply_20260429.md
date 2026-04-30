# Cursor Prompt — mig_181 PM syn_*_size 15 not_started cols verify + apply

**Date:** 2026-04-29 (very late evening)
**Lane:** mig_181 / pm_syn_size_15_cols_verify
**Batch (proposed):** `mig_181_pm_syn_size_cols_verify_apply_20260429`
**Predecessor:** mig_173b (CLOSED — added 15 typed cols + 3 legacy_raw on PM via syn size dtype reform); §8 retro audit verified clean
**Posture:** Read-only audit + SQL-only authoring + commit + push. **DO NOT execute against MotherDuck.** Cowork applies via Path C.
**Target DB:** `thyroid_canonical_publication_v1_0`
**Touches:** `main.canonical_column_verification_registry_v1` (registry-only)

---

## Mission

After mig_180 application, PM has **16 not_started cols** remaining. **15 of them are the syn_*_size cols added by mig_173b** (right_lobe + left_lobe + isthmus × length_cm/width_cm/height_cm/volume_cc/parse_status = 15). 1 other is a misc col. mig_181 verifies these 15 against their derivation logic and flips status, dropping PM not_started 16 → 1.

**Live MD probed by Cowork 2026-04-29 (post-mig_180):**
- 15 cols at `not_started` with `batch_id='mig_173_syn_size_cm_dtype_reform_20260429'`
- 3 cols at `na` with `batch_id='mig_173_syn_size_cm_dtype_reform_20260429'` (legacy_raw — skip; already na'd)
- Sample parse_status distribution on `syn_right_lobe_size_parse_status`: parsed_3axis 6,787 / NULL 3,813 / unparsed 224 / sentinel 39 / parsed_partial 8 (multi-valued enum, not Type-A/B placeholder)

---

## Required scope

### §1 Verify the 5-metric × 3-lobe parse pipeline

For each of right_lobe / left_lobe / isthmus:
1. Read `syn_<lobe>_size_cm_legacy_raw` (raw VARCHAR with multi-axis dimensions like "3.8 x 1.7 x 1.1")
2. Verify parse logic: when parse_status = `parsed_3axis`, length_cm/width_cm/height_cm should be 3 distinct DOUBLE values from the raw string
3. Verify volume_cc = length_cm × width_cm × height_cm (rectangular formula per Cursor's mig_173b design)
4. Spot-check 5 random patients per lobe; verify each parsed value matches the legacy_raw text

### §2 Cohort-uniformity sweep on parse_status (3 cols)

For each parse_status col:
- Type-A near-uniform-TRUE check: is one value > 95% of nonnulls? (parsed_3axis is dominant but not 95%; multi-valued OK)
- Type-B placeholder check: is parse_status only NULL or only one non-NULL value? (No — has 5 distinct values)
- Standard verified status

### §3 Derivative col semantic checks (3 lobes × 4 metrics = 12 cols)

For each (length_cm, width_cm, height_cm, volume_cc) per lobe:
- Population coverage (n_nonnull) should match parse_status='parsed_3axis' or 'parsed_partial' count
- Volume_cc should equal length × width × height when all 3 are non-null
- length/width/height should be DOUBLE > 0

### §4 Author apply SQL

`qc_framework_v1/migrations/181_pm_syn_size_cols_verify_apply_20260429.sql` with:
- §A pre-snapshot 18 registry rows (15 syn_*_size + 3 legacy_raw, but legacy already na — so 15 in scope)
- §B Path-C stamp on 15 cols (verified_by + batch_id + verification_method=`derivation_vs_syn_size_legacy_raw_parse_pipeline` + verified_ts + notes)
- §C status flips: 15 cols → verified
- §D registry note appendix for the parse_status cols documenting the 5-state enum
- §E resync `canonical_table_signoff_registry_v1` for canonical_patient_master (n_verified 1,575 → 1,590; n_not_started 16 → 1)

### §5 Audit/report

`qc_framework_v1/reports/mig_181_syn_size_audit_20260429.md` with:
- Per-lobe parse coverage table (parse_status × n_pts)
- 5-patient spot-checks per lobe
- Volume formula verification table
- New CF tags if any (e.g., CF-mig181-SYN-SIZE-VOLUME-FORMULA-RECTANGULAR informational confirming Cursor's choice; pairs with existing CF-mig173b-VOLUME-FORMULA-CONVENTION)

---

## Governance reminders

- Read-only audit + SQL authoring only. Cowork applies via Path C.
- Author = `Logan Glosser <logan.glosser@gmail.com>` for all commits.
- Surgical git add only.

---

## Deliverables

1. `qc_framework_v1/migrations/181_pm_syn_size_cols_verify_apply_20260429.sql`
2. `qc_framework_v1/reports/mig_181_syn_size_audit_20260429.md`

Commit message: `qc: mig_181 PM syn_*_size 15 cols verify + apply authoring`

---

End of prompt.
