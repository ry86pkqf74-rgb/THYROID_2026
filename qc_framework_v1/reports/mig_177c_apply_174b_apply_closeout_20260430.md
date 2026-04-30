# mig_177c_apply Option A + mig_174b apply close-out (combined)

**Date:** 2026-04-30
**Lanes:** mig_174b cnln_img_laterality per-side BOOLEANs + mig_177c_apply Option A clear-only
**Cursor authored:** mig_174b at `d6d0c62`; mig_177c_apply at `c8efeb8`
**Cowork applied:** 2026-04-30 via Path C
**Outcome:** 2 lanes complete; PM signoff = 1,596 / 24 / 0 / 1,620 (5 new BOOLEANs added by mig_174b — mig_177c_apply was data-write only, no col change)

---

## §1 mig_174b — cnln_img_laterality per-side BOOLEAN apply

Logan-ratified Option A on 2026-04-29 (preserve raw VARCHAR; add 5 per-side BOOLEAN cols).

### Apply trace
| Step | Action | Result |
|---|---|---|
| §A | Pre-snapshot 272 nonnull cnln_img_laterality rows | 272 ✓ |
| §B | ALTER TABLE ADD 5 BOOLEAN cols (left/right/central/bilateral/lateral_neck_present) | 5 ✓ |
| §C | UPDATE token-parse populate (10,871 rows) | 10,871 ✓ |
| §D | INSERT 5 new col registry rows (verified status) | 5 ✓ |
| §F | UPDATE legacy cnln_img_laterality note appendix | 1 ✓ |
| §G | Resync canonical_table_signoff_registry_v1 for PM | 1 ✓ |
| §H | Provenance row insert | 1 ✓ |

### Cohort uniformity (matches Cursor's prediction exactly)

| Col | TRUE | FALSE | NULL |
|---|---:|---:|---:|
| cnln_img_left_present | 85 | 187 | 10,599 |
| cnln_img_right_present | 87 | 185 | 10,599 |
| cnln_img_central_present | 32 | 240 | 10,599 |
| cnln_img_bilateral_present | 116 | 156 | 10,599 |
| cnln_img_lateral_neck_present | 7 | 265 | 10,599 |

### Closures
- `CF-mig174-CPM-CNLN-IMG-LATERALITY-MULTILABEL` → closed
- 5 informational `CF-mig174b-COHORT-UNIFORM-*` opened (sparse population — true presence flags, not Type-B)

---

## §2 mig_177c_apply — Option A clear-only

Logan-ratified Option A on 2026-04-30 (clear stale derivative cells on 5,082 mig_177b TRUE→FALSE flippers).

### Apply trace
| Step | Action | Result |
|---|---|---|
| §1 | Pre-snapshot 10,871 rows × 18 cols to archive | 10,871 ✓ |
| §2 | UPDATE LVI clear (lvi_grade=NULL, lvi_ordinal_worst=NULL, n_tumors_lvi_present=0) on 2,502 flippers | 2,502 ✓ |
| §3 | UPDATE VI clear (12 cols cleared) on 2,580 flippers | 2,580 ✓ |
| §4 | UPDATE registry note appendix on 15 derivative col rows | 15 ✓ |
| §5 | Provenance row insert | 1 ✓ |

### Post-state residuals (must all be 0)

| Family | Col | Residual |
|---|---|---:|
| LVI | lvi_grade | 0 ✓ |
| LVI | lvi_ordinal_worst | 0 ✓ |
| LVI | n_tumors_lvi_present>0 | 0 ✓ |
| VI | vasc_grade | 0 ✓ |
| VI | vasc_grade_final_v13 | 0 ✓ |
| VI | vascular_invasion_final | 0 ✓ |
| VI | vi_ordinal_worst | 0 ✓ |
| VI | n_tumors_vi_present>0 | 0 ✓ |

### Closures
- `CF-mig177b-LVI-VI-DERIVATIVES-PENDING-RECLEAN` → closed (5,082 flippers cleaned)
- `CF-mig177c-EXTENT-MISSING-FOR-NEW-FLIPPERS` opened (159 patients flipped FALSE/NULL→TRUE in mig_177b lack derivatives — future Option B lane needs grade/count cols on canonical_invasion_events_v1)

---

## §3 Combined post-state

- PM total cols: 1,615 → **1,620** (+5 from mig_174b)
- PM signoff: 1,591 / 24 / 0 / 1,615 → **1,596 / 24 / 0 / 1,620** (table_status remains `verified`)
- Cohort parity: 10,871 / 10,871 ✓
- Pre-snapshots present:
  - `archive_pub_v1_0.canonical_patient_master_pre_mig174b_cnln_laterality_20260429`
  - `archive_pub_v1_0.canonical_patient_master_lvi_vi_derivatives_pre_mig177c_apply_20260430`

---

End of close-out.
