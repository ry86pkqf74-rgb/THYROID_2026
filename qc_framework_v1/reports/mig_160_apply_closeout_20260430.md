# mig_160 apply close-out — global clinical-date retype

**Date:** 2026-04-30
**Lane:** mig_160 / global_clinical_date_retype
**Cowork applied:** 2026-04-30 via Path C (Cowork-direct, not Cursor)
**Outcome:** 21 clinical-date cols retyped from TIMESTAMP/VARCHAR → DATE; 1 dependent VIEW patched; gate5 closed for the in-scope cols.

---

## §1 Executive summary

- **21 cols retyped to DATE** across 5 base canonicals (1 ETE event + 14 frozen rollup + 2 molecular + 2 path-malignant rollup + 2 recurrence)
- **5 pre-snapshots** to `archive_pub_v1_0` (rollback-safe)
- **0 parse failures** across all 5 VARCHAR cols (probed pre-apply)
- **1 dependent VIEW patched**: `manuscript_workspace.canonical_molecular_genetics_v2_date_clean` had `length(trim(resolved_test_date))` which fails on DATE input. Re-authored to use `IS NOT NULL` semantics. All 12 other dependent VIEWs auto-recompiled clean.
- **gate5: 46 → 25** (the 21 in-scope cols closed; 25 PM date cols remain — out of mig_160 scope, addressed by future mig_160b)
- **gate1: 172 → 172** (no change — base tables still verified)

---

## §2 Path-C apply trace

| Step | Action | Result |
|---|---|---|
| §A.1-5 | Pre-snapshots × 5 (ete / frozen / molecular / path_malignant_rollup / recurrence) | 6,689 + 4,116 + 1,384 + 4,137 + 10,871 rows ✓ |
| §B.1 | ALTER canonical_ete_event_resolved_v1.last_known_alive_date TIMESTAMP→DATE | ✓ |
| §B.2-15 | ALTER canonical_frozen_section_patient_rollup_v1 14 cols VARCHAR→DATE | ✓ all 14 |
| §B.16-17 | ALTER canonical_molecular_genetics_v2 (test_date_native TIMESTAMP→DATE; resolved_test_date VARCHAR→DATE) | ✓ |
| §B.18-19 | ALTER canonical_path_malignant_patient_rollup_v1 (earliest_, latest_malignant_path_date) TIMESTAMP→DATE | ✓ |
| §B.20-21 | ALTER canonical_recurrence_v1 (recurrence_date, first_surgery_date) TIMESTAMP→DATE | ✓ |
| VIEW.1 | CREATE OR REPLACE manuscript_workspace.canonical_molecular_genetics_v2_date_clean (DATE-aware logic) | ✓ |
| §D.1-21 | Registry note appendix on 21 col rows | 1+14+2+2+2 = 21 ✓ |

---

## §3 Dependent VIEW recompile audit

12 of 13 VIEWs recompiled cleanly without intervention. 1 patched.

| VIEW | Recompile result |
|---|---|
| views_readable.Genetics_Testing | ✓ clean (1,384 rows) |
| views_readable.Recurrence_Status | ✓ clean (10,871 rows) |
| views_readable.path_malignant_patient_rollup_VIEW_v1 | ✓ clean (4,137 rows) |
| main.molecular_fusions_unnested_VIEW_v2 | ✓ clean (60 rows) |
| main.molecular_variants_unnested_VIEW_v2 | ✓ clean (936 rows) |
| manuscript_workspace.canonical_molecular_genetics_v2_braf_variant | ✓ clean (1,384) |
| manuscript_workspace.canonical_molecular_genetics_v2_status_clean | ✓ clean (1,384) |
| manuscript_workspace.canonical_molecular_genetics_v2_fna_rebind | ✓ clean (1,384) |
| manuscript_workspace.canonical_molecular_genetics_v2_platform_clean | ✓ clean (1,384) |
| manuscript_workspace.molecular_episode_uid_v1 | ✓ clean (1,384) |
| manuscript_workspace.specimen_genomic_assay_v1_relinked | ✓ clean (10,370) |
| manuscript_workspace.specimen_genomic_assay_v1_rebound | ✓ clean (10,370) |
| **manuscript_workspace.canonical_molecular_genetics_v2_date_clean** | **PATCHED** — `length(trim(resolved_test_date))` removed; replaced with `IS NOT NULL`; returns 1,384 rows post-patch |
| manuscript_workspace.ete_manuscript_analytic_v1 | pre-existing broken (depends on missing `path_malignant_event_fingerprint_v1`) — NOT mig_160-related |

---

## §4 CFs

**Closed by mig_160:**
- `CF-mig121-ETE-EVENT-LAST-ALIVE-RETYPE` (canonical_ete_event_resolved_v1)
- `CF-119-FROZEN-ROLLUP-DATE-RETYPE` (14 frozen cols)
- `CF-100-DATE-RETYPE` (path_malignant_patient_rollup)
- `CF-90-DATE-FORMAT` (canonical_recurrence_v1; partial — also affects PM)
- `CF-mig137-PM-MOL-DATE-RETYPE` on canonical_molecular_genetics_v2 source
- `CF-mig160-GATE-5-CLOSURE` for 21-col scope

**Carry-forward (informational):**
- `CF-mig160-PM-DATE-COLS-REMAINING` — 25 PM date cols still TIMESTAMP/VARCHAR (became visible to gate5 after mig_183 flipped PM to verified). Cohort-eligible for a future `mig_160b` follow-up. Not blocking manuscript.
- `CF-mig160-2DIGIT-YEAR-NORMALIZATION-APPLIED` — `%m/%d/%y` arm fired on edge cases per Protocol v2 convention (00→2000, 25→2025).

---

## §5 5-gate audit before/after

| | Before | After |
|---|---:|---:|
| gate1 | 172 | 172 |
| gate2/3/4 | 0/0/0 | 0/0/0 |
| **gate5** | **46** | **25** (delta = 21 cols closed) |
| Cohort | 10,871/10,871 | 10,871/10,871 |

---

End of close-out.
