# Targeted remediation plan (2026-04-13 audit baseline)

**Inputs:** `studies/20260413_source_truth_completeness_audit/`, `studies/20260413_us_nodule_tirads_linkage_audit/`, `studies/20260413_us_lymph_node_audit/`, `studies/20260413_fna_bethesda_audit/`.

**Principle:** Separate **deterministic** pipeline fixes from **human-review-only** adjudication. Do not auto-promote imaging→FNA links or Bethesda numbers when sources disagree.

---

## 1. Deterministic extraction fixes

| ID | Evidence | Fix |
|----|----------|-----|
| E1 | `20260413_us_nodule_tirads_linkage_audit/verdict.md`: COMPLETE workbook **0** missing vs `imaging_nodule_master_v1`; total source inventory **60,519** rows with **0** unmatched. | **No additional extraction pass required** for structured COMPLETE / scored / Imaging_12 corpora already ingested. |
| E2 | `20260413_source_truth_completeness_audit/executive_verdict.md` B3: non-COMPLETE narrative US not exhaustively in `imaging_nodule_master_v1`. | **Out of scope for “smallest safe” pack** unless expanding `scripts/50_multinodule_imaging.py` + raw ingest; document as residual limitation (§6). |
| E3 | US LN audit **PASS** (`20260413_us_lymph_node_audit/verdict.md`): 0 rows in `positive_ln_misses.csv`, 0 in `negative_ln_capture_gaps.csv`. | **No deterministic LN extraction patch** from this audit. |

---

## 2. Deterministic linkage fixes

| ID | Failing rows (citable) | Fix |
|----|------------------------|-----|
| L1 | **`linkage_gap_worklist_unresolved_20260413_174900.csv` lines 2–129** (128 nodules): `linkage_reason_code=candidate_fna_in_90d_window_but_no_mm_link`. Same nodules appear in `imaging_fna_linkage_audit.csv` and `unresolved_exceptions.csv` as `no_primary_linkage_row`. | **Re-run / extend** `scripts/129_imaging_fna_linkage_mm_v1.py` so pairs that satisfy **calendar 0–90d US-before-FNA** and appear in the wide pre-table but fail `side_ok AND (specimen_match OR size_ok)` get a **reviewable primary** via existing relaxed tiers (`singleton_lateral_temporal_only`, `lateral_discord_specimen_match`, `size_drift_20_40pct_temporal`) — or add a **staged** `INSERT` from a join of `v_imaging_nodule_linkage_classification_v1` × `fna_episode_master_v2` restricted to these 128 `nodule_id` values **after** human confirmation of lateral/specimen rules. |
| L2 | `executive_verdict.md`: `linked_fna_episode_id` on `imaging_nodule_master_v1` all NULL; linkage in `imaging_fna_linkage_mm_v1` — **do not** backfill `imaging_nodule_master_v1` without updating script 50 contract. | Prefer **downstream consumers** using `imaging_fna_linkage_mm_v1` + `is_primary_link` (already assumed in audits). |

**Not deterministic without chart review:** ~**17.5k** rows in `unresolved_exceptions.csv` with `no_primary_linkage_row` where **no** FNA exists in 0–90d window — classify as `no_eligible_fna`, not bugs.

---

## 3. Deterministic normalization fixes

| ID | Evidence | Fix |
|----|----------|-----|
| N1 | `20260413_us_nodule_tirads_linkage_audit`: `missing_canonical_tirads_despite_sufficient_source` = **0**; `tirads_scoring_audit.csv` shows `missing_canonical_despite_sufficient_source=False` for sampled COMPLETE rows. | **No TR normalization patch** required for sufficient-source nodules. |
| N2 | `tirads_recompute_comparison.csv`: radiologist **reported** vs **ACR recalculated** can differ by design (e.g. `1529|2018-11-21|*` reported 3 vs recalc 4); DB stores both. | **Do not overwrite** reported TR with ACR without policy; optional **documentation** only. |
| N3 | Bethesda: deploy / consume `v_fna_episode_bethesda_resolved_v1` (`scripts/sql/source_truth_confirmation_v1.sql`) so downstream uses **one** resolved column with explicit `bethesda_unscorable_reason`. | Run `scripts/151_source_truth_confirmation_v1.py --md` after SQL review. |

---

## 4. Human-review-only cases

| Bucket | Row IDs / file | Why |
|--------|------------------|-----|
| Bethesda cross-table conflicts | **`fna_bethesda_conflicts.csv` lines 2–1900** (1899 rows): e.g. lines 2–7 `research_id` 790, 794, … — `unresolved_numeric_mismatch` across `fna_episode_master_v2` vs `fna_history` vs `fna_cytology`. | Institution must pick **gold source** (cytology report vs index episode vs Excel melt). |
| Imaging→FNA “gap” candidates | **`linkage_gap_worklist_unresolved_20260413_174900.csv` lines 2–129** (128 nodules) | Calendar says FNA in window; **script 129** excluded pair (laterality / size / specimen). Clinician confirms same lesion. |
| Executive NOT_CONFIRMED | `executive_verdict.md` questions 1–5 | Fail-closed scope; broader narrative US and manuscript claims need **scope shrink** or more ingest — not auto-fixed. |
| US LN `source_ambiguous` | `us_lymph_node_audit/verdict.md`: **8126** exams | Text tension across layers; no missed positives per strict lists — **optional** spot review, not blocking. |

Full machine-readable queue: **`human_review_packet.csv`** (128 linkage + 1899 Bethesda = 2027 rows).

---

## 5. Post-fix re-audit steps

1. MotherDuck: deploy SQL views (`151`) and, if applied, rebuilt `imaging_fna_linkage_mm_v1` (`129`).
2. Re-run **only**:
   - `studies/20260413_source_truth_completeness_audit/run_source_truth_audit.py`
   - `studies/20260413_us_nodule_tirads_linkage_audit/run_us_nodule_tirads_linkage_audit.py`
   - `studies/20260413_us_lymph_node_audit/run_us_lymph_node_audit.py`
   - `studies/20260413_fna_bethesda_audit/run_fna_bethesda_audit.py`
3. Pass/fail: compare **`linkage_gap_worklist_unresolved_*` row count** (expect **0** or documented exceptions), **`fna_bethesda_conflicts.csv` row count** (expect drop only after manual adjudication table is applied), **`unmatched_source_nodules.csv`** remains **0**, **`missing_canonical_despite_sufficient_source`** remains **0**.

See **`post_fix_reaudit_prompt.txt`**.

---

## 6. Residual irreducible source limitations

- **Narrative-only ultrasound** (`Imaging_12_1_25` free text, non-index reports) may never achieve 1:1 structured nodule rows without full NLP extraction (`executive_verdict.md` B3).
- **`imaging_fna_linkage_mm_v1`**: specimen_match rows **0** in audit snapshot (`executive_verdict.md` A2) — linkage dominated by **temporal** paths; improving specimen key fill in US + FNA metadata is a **data acquisition** problem.
- **Bethesda**: **456** episodes “not scorable from source” per FNA audit verdict — justified; not all are errors.
- **Pathology linkage** (nodule→surgery/path): not the primary failure mode in these four audit folders; use existing `imaging_pathology_concordance_review_v2` when expanding scope.

---

*Generated for `studies/20260413_targeted_remediation_pack/`.*
