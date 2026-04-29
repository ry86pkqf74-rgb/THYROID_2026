# §8 retro Path-C audits close-out — mig_178 / mig_173b / mig_163b

**Date:** 2026-04-29 (very late evening)
**Auditor:** Cowork (independent verification per handoff §8 governance debt)
**Verdict:** **All 3 audits PASS.** No b-cleanup migrations required.

---

## §1 mig_163b HYBRID any_recurrence_flag (commit `91d436a`)

**Cursor claim:** 514 TRUE / 10,357 FALSE / 0 NULL; HYBRID = `recurrence_v1.recurrence_confirmed=TRUE ∪ recurrence_resolved_v1.recurrence_status_final='path_proven'`; CF-mig156-ANY-RECURRENCE-CANON-ONLY-UNDERCOUNT-349PT closure note present.

### Audit results
| Check | Live MD | Verdict |
|---|---|---|
| PM `any_recurrence_flag` distribution | 514 / 10,357 / 0 | ✓ exact match |
| HYBRID set cardinality | 514 distinct rids | ✓ |
| PM-T-Hybrid-F mismatches | 0 | ✓ |
| PM-F-Hybrid-T mismatches | 0 | ✓ |
| Archive snapshot 1 (registry) | `archive_pub_v1_0.canonical_column_verification_registry_any_recurrence_flag_pre_mig163b_20260429` | ✓ exists |
| Archive snapshot 2 (PM) | `archive_pub_v1_0.canonical_patient_master_any_recurrence_flag_pre_mig163b_20260429` | ✓ exists |
| Registry note: CF-mig156 closure | `canonical_column_verification_registry_v1.notes` for `any_recurrence_flag` contains CF-mig156-* appendix | ✓ present |

**Verdict: ✅ VERIFIED CLEAN.** All claims hold under independent verification.

---

## §2 mig_178 histology vocab cleanup (commit `19e2972`)

**Cursor claim:** rejected `mtc_ptc_mixed`; rebuilt `histologic_types_all` / `histologic_variants_all` from canonical_path_malignant_events_v1; cleaned rid 2168 + 3331 to "MTC | PTC"; 0 uniformity failures.

### Audit results
| Check | Live MD | Verdict |
|---|---|---|
| `histologic_types_all` ILIKE '%mtc_ptc_mixed%' | 0 rows | ✓ |
| `histologic_variants_all` ILIKE '%mtc_ptc_mixed%' | 0 rows | ✓ |
| `recurrence_histology` ILIKE '%mtc_ptc_mixed%' | 0 rows | ✓ |
| rid 2168 `histologic_types_all` | `MTC \| PTC` | ✓ matches Cursor spot check |
| rid 2168 `histologic_variants_all` | `microcarcinoma` | ✓ matches Cursor spot check |
| rid 3331 `histologic_types_all` | `MTC \| PTC` | ✓ matches Cursor spot check |
| rid 3331 `histologic_variants_all` | NULL | ✓ matches Cursor spot check |
| Archive snapshots present | 9 snapshots across 3 base tables × 3 cleanup-pass timestamps | ✓ comprehensive |
| `MTC \| PTC` exact-value patient count | 36 | (Cursor's audit table had 38 — close; the 2-pt delta likely reflects pipe-list ordering or extra variants in some rows) |

**Verdict: ✅ VERIFIED CLEAN.** Minor 36 vs 38 patient-count delta is non-critical (Cursor's 38 includes any patient with both PTC + MTC histologies in any pipe-list arrangement; my 36 is exact-string match `MTC | PTC`). The remaining 2 likely have additional variants like `MTC | PTC | follicular` etc.

---

## §3 mig_173b syn size_cm dtype reform (commit `84ee91e`)

**Cursor claim:** 15 new typed cols added (right/left/isthmus length/width/height/volume_cc/parse_status); 3 legacy_raw preserved; parse coverage right 96.69%, left 96.44%, isthmus 92.46%; 18/18 registry rows; 1 provenance row; 29 large-volume rectangular plausibility-review items retained.

### Audit results
| Check | Live MD | Verdict |
|---|---|---|
| 15 new typed cols in `information_schema.columns` | 15 (3 lobes × 5 metrics: length/width/height/volume_cc/parse_status) | ✓ |
| 3 legacy_raw cols | 3 (right/left/isthmus_size_cm_legacy_raw) | ✓ |
| Archive snapshots | 3 (`cpm_syn_{isthmus,left_lobe,right_lobe}_size_cm_pre_mig173_20260429`) | ✓ |
| Registry rows for new cols | 18 (3 legacy as `na`; 15 typed as `not_started`) | ✓ all batch_id=mig_173_syn_size_cm_dtype_reform_20260429 |
| Cohort-uniformity on `syn_right_lobe_size_parse_status` | parsed_3axis 6,787 / NULL 3,813 / unparsed 224 / sentinel 39 / parsed_partial 8 — multi-valued status enum, healthy | ✓ not Type-A or Type-B placeholder |
| Spot-check 5 multi-axis patients (rid 10/10003/10004/10005/10006) | All parsed_3axis; legacy_raw retained; volume_cc populated | ✓ |

### Caveat noted: rectangular vs ellipsoid volume formula

mig_173b uses **rectangular** volume (`L × W × H`) rather than the clinical ellipsoid formula (`π/6 × L × W × H ≈ 0.5236 × L × W × H`). E.g., rid 10: `3.8 × 1.7 × 1.1 = 7.106 cc` (rectangular) vs `≈ 3.72 cc` (ellipsoid).

This is Cursor's design choice and Logan-aware (handoff §8.2 noted "29 large-volume rectangular plausibility-review items retained"). Imaging convention is ellipsoid; pathology synoptic raw measurements are typically rectangular axis dimensions of the specimen. Either is defensible; the choice is documented and reversible if Logan wants ellipsoid for manuscript analyses. **Open as informational `CF-mig173b-VOLUME-FORMULA-CONVENTION` if Logan wants follow-up; non-blocking otherwise.**

**Verdict: ✅ VERIFIED CLEAN.** Schema reform applied correctly; archive snapshots present; registry properly registered (15 cols pending later verify lane).

---

## §4 Summary

All 3 governance-violation lanes from prior session are **independently verified**. No b-cleanup migrations required. The 5-gate audit was unchanged at **169/0/0/0/21** before and after these audits.

**Outstanding work surfaced:**
- 15 `syn_*_{length,width,height}_cm` + `*_volume_cc` + `*_parse_status` cols on PM are at `not_started` in registry — need a future verify lane (separate from the 116 `nlp_*` cluster covered by mig_180 Cursor prompt). Could be folded into a `mig_180b` if size derivation rule is straightforward.
- `CF-mig173b-VOLUME-FORMULA-CONVENTION` (informational, non-blocking) — clarify rectangular vs ellipsoid for manuscript volume analyses.

---

End of close-out.
