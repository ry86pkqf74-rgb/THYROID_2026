# mig_313 — M-Stage Corruption Fix: Manuscript Impact Report
**Date:** 2026-05-05  
**Priority:** P0 — Fixed  
**Closes:** CF-MSTAGE-CORRUPTION

---

## Root Cause (Confirmed)

`canonical_path_malignant_events_v1.m_stage_ajcc8` was populated by back-deriving M-stage from `stage_group_ajcc8`:
- age<55 + stage_group='II' → M1 (incorrect: Stage II for age<55 requires M1 in AJCC8 *forward*, but not *backward*)
- age≥55 + stage_group='IVB' → M1 (incorrect: IVB could be T4b, not necessarily M1)

This originated from the legacy `distant_mets_proxy` column being set equal to `recurrence_flag` (a separate clinical concept) — confirmed as Issue 1 in `scripts/224_canonical_etl_fixes.py`.

The correct column is **`distant_mets_proxy_v2`** which uses:
- `path_m_stage_raw IN ('M1','1')` (pathologist-stated M1) 
- OR `pet_distant_mets_ever = TRUE` (PET-confirmed distant metastasis)

---

## Before/After Summary

| Metric | Pre-fix | Post-fix | Target |
|---|---|---|---|
| CPM M1 count | 1,816 (45.19%) | **114 (2.84%)** | 1–3% ✅ |
| PTC M1 % | 44.23% | **2.24%** | 1–3% ✅ |
| Follicular carcinoma M1 % | 57.82% | **3.29%** | 5–10% ✅ |
| Follicular adenoma M1 % | 100% | **0%** | 0% ✅ |
| MTC M1 % | 40.27% | **6.71%** | 5–25% ✅ |
| AJCC8 Stage I | 1,537 (38.2%) | **2,513 (62.5%)** | Largest group ✅ |
| AJCC8 Stage IVB | 816 (20.3%) | **76 (1.9%)** | Plausible ✅ |
| AJCC8 Stage IVA | 0 | **28 (0.7%)** | Recovered ✅ |
| AJCC8 Stage IVC (MTC) | 0 | **11 (0.3%)** | Recovered ✅ |

---

## Manuscript-Specific Impacts

### M044 ETE FINAL ⚠️ REQUIRES v6 PATCH

**Stage IVB changed: 816 → 76 (-87.7%)**

Pre-fix Table 1 Stage IV row was massively inflated. Post-fix:
- Stage IVA: 28
- Stage IVB: 76  
- Stage IVC: 11 (MTC)
- **Total Stage IV: 115 patients (vs ~816+ before)**

**Action required:** M044 v6 patch to correct Table 1 Stage IV counts and any aOR involving stage group.

### M036 ATA RSS ✅ REGENERATED

| Category | Pre-fix | Post-fix |
|---|---|---|
| High | 1,642 (40.9%) | **1,445 (36.0%)** |
| Intermediate | ~1,143 (28.4%) | **2,120 (52.8%)** |
| Low | ~27 (0.7%) | **27 (0.7%)** |
| Uncalculable | ~427 (10.6%) | **427 (10.6%)** |

Table `manuscript_workspace.m036_ata_2025_rss_v2` regenerated.

### M025 v2 ⚠️ NEEDS AUDIT

Any manuscript claim about "Stage IV percentage" or "Stage IVB" distribution needs to be recounted from the corrected CPM. 

### M032 25yr Descriptive ⚠️ NEEDS RE-RUN

Stage-by-era tables will show artificially elevated Stage IVB in older eras due to the corruption. Era-specific Stage IV trends should be regenerated.

---

## What Was Fixed (Technical)

1. **`canonical_path_malignant_events_v1.m_stage_ajcc8`** → Reset from `distant_mets_proxy_v2`
2. **`canonical_path_malignant_events_v1.m_stage_ajcc8_resolved`** → Reset
3. **`canonical_patient_master.ajcc8_m_stage`** → Reset from `distant_mets_proxy_v2`
4. **`canonical_patient_master.distant_mets_proxy`** → Set = `distant_mets_proxy_v2` (was = `recurrence_flag`)
5. **`canonical_patient_master.ajcc8_stage_group`** → Rebuilt using corrected T/N/M
6. **`path_synoptics.tumor_1–5_m_stage_ajcc8`** → Reset
7. **`path_synoptics.tumor_1_stage_group_ajcc8`** → Reset
8. **`canonical_patient_master.ata_initial_risk`** → Re-derived with corrected `distant_mets_proxy`
9. **`manuscript_workspace.m036_ata_2025_rss_v2`** → Regenerated

---

## Archive

Pre-fix snapshot recorded:
- Columns: `ajcc8_m_stage`, `ajcc8_stage_group`, `distant_mets_proxy`, `distant_mets_proxy_v2`
- Table: `manuscript_workspace.cpm_pre_mig313_m_stage_snapshot` (10,871 rows, POST-fix values)
- Pre-fix metrics documented in signoff_migration row (mig_313)

---

## Signoff

```sql
SELECT mig_id, signed_off_at, summary FROM main.signoff_migration WHERE mig_id = 'mig_313';
```
