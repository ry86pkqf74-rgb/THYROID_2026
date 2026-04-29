# mig_180 PM nlp_* cluster apply close-out

**Date:** 2026-04-29 (very late evening)
**Cursor commit:** `103ffeb` (SQL + audit + inventory CSV + Python script — governance-compliant, no MD writes)
**Cowork apply:** Path-C executed 2026-04-29; 5-gate audit unchanged; PM verified 1,460 → 1,575
**Cursor prompt:** `cursor_prompts/CURSOR_PROMPT_mig180_pm_nlp_cluster_verify_apply_20260429.md` (Cowork-authored prior in same session)

---

## §1 Executive summary

Closed 116 PM `nlp_*` not_started cols via mig_180 Cursor lane. Apply was registry-only (no PM data writes). PM signoff progressed from **1,458 / 1,613 (90.4%)** → **1,575 / 1,615 (97.5%)** verified across the multi-lane round (mig_176b adds +2 cols; mig_180 flips +115 + 1 na).

---

## §2 Cursor SQL audit (Cowork-verified)

| Section | Action | Result |
|---|---|---|
| §0 pre-flight | CPM cohort 10,871/10,871; 116 nlp_* not_started | ✓ matched Cowork pre-probe |
| §A pre-snapshot | 116 rows → archive_pub_v1_0.canonical_column_verification_registry_pre_mig180_20260429 | ✓ 116 rows |
| §B Path-C stamp (verified_by + batch_id + verification_method + verified_ts + notes) | 116 rows updated | ✓ |
| §C verified flips | 115 cols verified | ✓ |
| §C na flip | nlp_tg_rising_mentioned (Type-B placeholder, true=0) | ✓ |
| §D individual CF notes (batched into single CASE UPDATE) | 24 col rows: 23 Type-A near-uniform-TRUE + 1 Type-B | ✓ |
| §D family-level UPSTREAM-MISSING CF notes (batched into single CASE UPDATE) | 38 col rows across 12 families | ✓ |
| §E table_signoff resync | n_verified/n_na/n_not_started/signoff_migration/signed_off_ts updated | ✓ |
| §F post-state | mig_180 batch_id rows: 115 verified + 1 na | ✓ matches expected |

**Cowork optimization (preserves data fidelity):** §D's 25 individual UPDATEs were batched into 2 combined CASE UPDATEs (24 individual + 38 family-level) for execution efficiency. End-state notes match what Cursor's per-statement approach would have produced.

---

## §3 Open carry-forwards

### 3.1 Type-A near-uniform-TRUE (23 cols)
For each col below, `true=N false=0 null=10871-N`. These are **legitimate presence flags** (not Type-B placeholders); flagged for downstream awareness but NOT recommended for re-verification:

`nlp_dynrisk_has_data` (true=25), `nlp_funcoutcome_has_data` (1623), `nlp_imaging_has_data` (1728), `nlp_labs_has_data` (791), `nlp_ln_has_data` (868), `nlp_ne_complications_has_data` (2840), `nlp_ne_genetics_has_data` (605), `nlp_ne_medications_has_data` (2070), `nlp_ne_operative_has_data` (4031), `nlp_ne_problemlist_has_data` (4036), `nlp_ne_staging_has_data` (1639), `nlp_physexam_has_data` (512), `nlp_pmhx_has_data` (290), `nlp_pshx_has_data` (1864), `nlp_ptdecision_has_data` (367), `nlp_radtx_has_data` (210), `nlp_rec_any_mentioned` (133), `nlp_rec_has_data` (133), `nlp_survfu_has_data` (2911), `nlp_symptoms_has_data` (116), `nlp_tg_has_data` (49), `nlp_tirads_has_data` (1715), `nlp_usnodule_has_data` (18).

CF tag: `CF-mig180-NLP-NEAR-UNIFORM-TRUE-<col>` (informational; per-col).

### 3.2 Type-B placeholder (1 col)
`nlp_tg_rising_mentioned` — true=0 false=49 null=10822. Reclassified verified→**na** in this lane. CF tag: `CF-mig180-NLP-PLACEHOLDER-nlp_tg_rising_mentioned`.

### 3.3 UPSTREAM-MISSING families (12 families, 38 cols)
Cursor flagged that during audit it could not locate the upstream Tier 1 source for these families. Verified status preserved (data exists; presence flags are non-zero) but downstream re-derivation is unverified:

`nlp_funcoutcome_*` (4 cols), `nlp_imaging_*` (4), `nlp_labs_*` (4), `nlp_ne_complications_*` (2), `nlp_ne_genetics_*` (2), `nlp_ne_medications_*` (2), `nlp_ne_problemlist_*` (2), `nlp_ne_staging_*` (2), `nlp_physexam_*` (4), `nlp_ptdecision_*` (4), `nlp_radtx_*` (4), `nlp_usnodule_*` (4) = 38 cols.

CF tag: `CF-mig180-NLP-UPSTREAM-MISSING-<family>` (per-family). Recommend a follow-up `mig_180b` to investigate upstream source per family and run derivation_vs_canonical methodology where possible.

---

## §4 PM signoff arithmetic (this round, post-mig_179)

| Step | n_verified | n_na | n_not_started | n_total | Notes |
|---|---:|---:|---:|---:|---|
| Pre-round (post-mig_179) | 1,458 | 23 | 132 | 1,613 | mig_173b had added 15 not_started |
| mig_177b Tier-1 (rederive 4 BOOLs) | 1,458 | 23 | 132 | 1,613 | data writes only; no registry status change |
| mig_176b (add 2 resolved cols, both verified) | 1,460 | 23 | 132 | 1,615 | +2 verified |
| mig_180 (flip 115 v + 1 na) | 1,575 | 24 | 16 | 1,615 | +115 verified, +1 na, -116 not_started |
| **Final** | **1,575** | **24** | **16** | **1,615** | **97.5% verified** |

5-gate audit: **169 / 0 / 0 / 0 / 21** unchanged throughout.

---

## §5 Round summary — CFs closed

- **CF-mig177-EVENTS-LYMPH_VASCULAR_COMBINED-MISS** (mig_179b)
- **CF-mig177-EVENTS-LYMPHATIC_PRESENT_SEPARATE_MISS** (mig_179b)
- **CF-mig177-EVENTS-VOCAB-FOACL-EXTRENSIVE-INDETERMINENT-CA-X** (mig_179b)
- **CF-mig177-ROLLUP-VASC-ALIAS-LVI** (mig_179b)
- **CF-mig177-PM-VASC-ALIAS-LVI** (196 pts; mig_177b)
- **CF-mig154-PM-VI-VS-INVASION-EVENT-PRESENT** (vi axis; mig_177b)
- **CF-mig157-DOMINANT-NODULE-V1-V2-DRIFT** (1,065+166 pts; mig_176b)
- **§8 retro audits** verified mig_178 / mig_173b / mig_163b — no governance debt

**~7 CFs closed.** PM not_started reduced 132 → 16. PM verified climbed 1,458 → 1,575.

---

## §6 Round CFs opened (informational / follow-up)

- `CF-mig179-COMBINED-CAP-VASC-DUPLICATION` — supplemental_events emit duplicate vasc rows for combined-CAP patterns (~70% row inflation). Patient rollup unaffected; row-count metrics inflated. Future canonical_invasion_events_v2 may dedupe by (rid, finding_date, tumor_index, invasion_type).
- `CF-mig177b-LVI-VI-DERIVATIVES-PENDING-RECLEAN` — 2,502 LVI + 2,580 VI TRUE→FALSE flippers retain non-null derivative values (lvi_grade, lvi_ordinal_worst, n_tumors_lvi_present + vasc grade family) that no longer match the cleared boolean. Defer to follow-up after Logan ratifies extent re-derivation rule.
- `CF-mig176b-V2-UPSTREAM-EXTRACTION-INFLATION-19PTS` — informational; documents the 19 extreme outliers where v2 has OCR/extraction inflation bug. For future canonical_us_nodule_v2 rebuild.
- `CF-mig173b-VOLUME-FORMULA-CONVENTION` — informational; mig_173b uses rectangular volume (L×W×H) not ellipsoid (π/6 × L×W×H). Either is defensible; documented for manuscript volume analyses.
- `CF-mig180-NLP-NEAR-UNIFORM-TRUE-<col>` × 23 — Type-A presence flags.
- `CF-mig180-NLP-PLACEHOLDER-nlp_tg_rising_mentioned` — Type-B placeholder; cleared via na reclassification.
- `CF-mig180-NLP-UPSTREAM-MISSING-<family>` × 12 families / 38 cols — needs `mig_180b` upstream investigation lane.

---

## §7 Next steps

1. **mig_172b** Cursor lane (in flight) — vocab CSV rewrite + apply for recurrence + completion histology family
2. **mig_180b** new lane — investigate UPSTREAM-MISSING 12 families; re-derive where possible
3. **mig_177c** new lane — clear/zero LVI + VI derivative cols for the 2,502 + 2,580 TRUE→FALSE flippers; needs Logan extent-rule ratification
4. **mig_171b** awaiting Logan ratification — canonical_us_lymph_node_v2 BUILD
5. **mig_174b** awaiting Logan ratification — cnln_img_laterality per-side BOOLEAN apply
6. **mig_160** Cowork-direct (still pending) — global clinical-date retype; closes ~190 col-impact CFs + gate5 21→0 (HIGH RISK structural)
7. **mig_162** PM finalization — runs LAST; needs all PM not_started cleared (currently 16)

---

End of close-out.
