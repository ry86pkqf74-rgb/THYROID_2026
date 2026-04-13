# Repo-scoped standard (A) vs strong user standard (B)

## Standard A — April 13–style scoped confirmation

**Definition (inferred from checked-in `studies/20260413_source_truth_completeness_audit/`):**

1. **COMPLETE workbook → canonical `imaging_nodule_master_v1`:** deterministic key parity for `research_id|exam_date|nodule_number` with no missing *source* keys.
2. **TI-RADS:** for COMPLETE source rows with sufficient ACR criteria (≥5), no `missing_canonical_despite_sufficient_source=true`.
3. **Linkage view:** `v_imaging_nodule_linkage_classification_v1` has 0 rows in `unresolved_linkage_gap`.
4. **US LN audit:** `studies/20260413_us_lymph_node_audit/verdict.md` strict PASS (heuristic) for positive/suspicious/negative capture.
5. **Bethesda:** either no `NULL` `bethesda_category` in `fna_episode_master_v2`, or `v_fna_episode_bethesda_resolved_v1` has 0 “fixable” gaps (NULL numeric with unscorable reason not in the allowed documentation set).

**This rerun (2026-04-13 UTC):**

| Criterion | Result | Evidence |
|-----------|--------|----------|
| COMPLETE key parity | **PASS** | 19,891 / 19,891 keys matched (`us_nodule_coverage_audit.csv`) |
| TI-RADS sufficient missing (COMPLETE) | **PASS** | 0 rows (`tirads_completeness_audit.csv` sum of `missing_canonical_despite_sufficient_source`) |
| `unresolved_linkage_gap` | **PASS** | 0 (`linkage_unresolved_gap_only.csv`) |
| LN heuristic audit | **PASS** (not re-run; prior artifact unchanged) | `studies/20260413_us_lymph_node_audit/verdict.md` still contains **PASS (heuristic)** |
| Bethesda fixable gap | **PASS** | `fna_bethesda_fixable_gap.csv` fixable_gap_count = 0; `fna_bethesda_resolved_summary.csv` shows 23 NULL numeric with 23 documented reason rows |

**Operator release gate (orthogonal):** `scripts/119_md_formalization_validate.py --md --release-mode` → **PASS WITH WARNINGS** (0 FAIL), report `studies/20260413_motherduck_formalization/validation_report.md`.

**Standard A verdict:** **PASS** (same narrow scope as the April 13 confirmation; not disproved by this rerun).

---

## Standard B — strong clinical completeness (user’s five pillars)

1. **All ultrasound nodules from *all* available corpora** in canonical structured data with deterministic key parity.
2. **All nodules with sufficient ACR feature detail** have TI-RADS in canonical output (across corpora, not COMPLETE-only).
3. **Provenance + downstream linkage:** exact provenance; linkage state in {linked_to_fna, linked_to_pathology, no_eligible_fna, unresolved}; unresolved = 0; **every** `no_eligible_fna` documented.
4. **US lymph-node data** fully recorded in **structured per-level / laterality / size** form, not only narrative preservation.
5. **All FNA episodes** carry **numeric** Bethesda in `fna_episode_master_v2` (semantic “resolved with reason” does **not** satisfy this bar).

**This rerun:**

| Pillar | Meets strong bar? | Evidence |
|--------|-------------------|----------|
| 1 | **NO** | Scored workbook: **527** / 19,367 keys unmatched to `imaging_nodule_master_v1` keys. Imaging_12 inferred: **620** / 20,910 unmatched. (`us_nodule_coverage_audit.csv`, `us_nodule_unmatched_source_keys_sample.csv`) |
| 2 | **NOT DEMONSTRATED** | TI-RADS completeness CSV is recomputed for **COMPLETE** corpus only (0 missing); scored and Imaging_12 corpora are **not** given the same per-row ACR sufficiency vs canonical column audit in this artifact. |
| 3 | **PARTIAL** | `unresolved_linkage_gap` = 0; `no_eligible_fna` = 30,657 with **non-null** `linkage_reason_code` for every row sampled (0 null reasons). Downstream pathology link state is **not** enumerated in `v_imaging_nodule_linkage_classification_v1` (only FNA-oriented states observed). |
| 4 | **NO** | No per-level LN structured table; April audit is **PASS (heuristic)** at exam/combined-text level only. |
| 5 | **NO** | `fna_episode_master_v2`: **23** / **8,119** rows have `bethesda_category` NULL (`fna_bethesda_audit_expanded.csv`). Resolved view explains all 23, but strong standard requires numeric **in episode master** for all episodes. |

**Standard B verdict:** **FAIL**

---

## Bottom line

- **Standard A:** PASS — repo’s April 13 scoped gates still hold on fresh MotherDuck + local sources.
- **Standard B:** FAIL — multi-corpus nodule coverage gaps, LN structure gap, and numeric Bethesda requirement not met.
