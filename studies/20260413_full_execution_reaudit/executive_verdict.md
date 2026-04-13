# Executive Verdict
overall_status: SCOPED_CONFIRMED_ONLY

## Repo scoped standard
status: PASS
rationale:
The April 13–style gates recomputed on live MotherDuck (2026-04-13 UTC) still pass: COMPLETE workbook deterministic keys 19,891/19,891 present in `imaging_nodule_master_v1`; zero COMPLETE rows with sufficient ACR criteria but missing canonical TI-RADS columns; `v_imaging_nodule_linkage_classification_v1` has zero `unresolved_linkage_gap`; prior US LN audit remains **PASS (heuristic)** on strict miss lists; `v_fna_episode_bethesda_resolved_v1` has zero fixable Bethesda gaps (NULL numeric only with documented unscorable reasons). Operator `119_md_formalization_validate.py --md --release-mode` returned 0 FAIL (35 PASS, 4 WARN).

exact counts:
- COMPLETE key match: 19,891 / 19,891
- `missing_canonical_despite_sufficient_source` (COMPLETE corpus): 0 / 19,891
- `unresolved_linkage_gap`: 0 / 37,016 classified nodules
- `fna_episode_master_v2` rows: 8,119; `v_fna_episode_bethesda_resolved_v1` fixable gap: 0
- `119 --release-mode`: FAIL=0, WARN=4

## Strong user standard
status: FAIL
rationale:
(1) **Strict** `rid|exam_date|nodule` keys from **scored** and **Imaging_12** corpora do not all match `imaging_nodule_master_v1` row-for-row (527 + 620 “strict” misses), **but** under the same **±30d dedup policy** as `scripts/50_multinodule_imaging.py`, those misses are **fully aligned** with existing canonical rows for the same `(research_id, nodule_number)` — see `us_nodule_coverage_audit_policy_aligned.csv` (`true_gap_after_30d_policy` = 0). The strong standard still fails on other grounds. (2) TI-RADS sufficiency audit in this run is applied to the **COMPLETE** corpus only; it does not prove cross-corpus TI-RADS completeness. (3) Linkage classification view shows only FNA-oriented states; `no_eligible_fna` rows all have non-null `linkage_reason_code`, but pathology linkage is not part of this view’s states. (4) US lymph nodes are not fully documented at structured per-level/laterality/size granularity — only exam-level/heuristic audit exists. (5) Numeric Bethesda is missing for 23/8,119 rows in `fna_episode_master_v2` (resolved view documents reasons; strong standard still requires numeric in episode master for every episode).

exact counts:
- Scored corpus unmatched keys (strict triple-key compare): 527 / 19,367
- Imaging_12 inferred unmatched keys (strict): 620 / 20,910
- After script 50 ±30d policy alignment: **true_gap_after_30d_policy** = **0** for both corpora (`us_nodule_coverage_audit_policy_aligned.csv`)
- `imaging_nodule_master_v1` total rows: 37,016
- Primary FNA-linked nodules (`linked_to_fna`): 6,359
- `no_eligible_fna` with documented `linkage_reason_code`: 30,657 (null reason: 0)
- `fna_episode_master_v2` NULL `bethesda_category`: 23 / 8,119
- `serial_imaging_us` on MotherDuck: **table present**, **0 rows** (placeholder DDL via `scripts/155_md_serial_imaging_us_placeholder.py` — no institutional serial-US feed yet)

## Question-by-question answers
1. All ultrasound nodules from all corpora extracted? **NO** if interpreted as **strict** triple-key parity without dedup policy; **YES** for scored + Imaging_12 supplements **under** `scripts/50` ±30d dedup (`true_gap_after_30d_policy` = 0 — see policy-aligned CSV).
2. All scoreable nodules TI-RADS scored (all corpora)? **NO** (COMPLETE-only sufficiency audit passed; other corpora not proven)
3. All nodules provenance-linked and downstream linkage-complete? **NO** (unresolved=0; strict key list differs from canonical by design; pathology linkage not in observed view)
4. All ultrasound lymph-node data fully recorded and documented in structured detail? **NO**
5. All FNA episodes numerically Bethesda scored? **NO** (23/8,119 NULL in `fna_episode_master_v2`)

## Important distinctions
- technical release readiness: `119 --release-mode` **PASS WITH WARNINGS** (0 FAIL) — not equivalent to strong clinical completeness.
- scoped confirmation: **PASS** — April 13 criteria still hold on recomputation.
- strong completion: **NOT MET** — structured LN detail and universal numeric Bethesda in episode master still fail; **strict triple-key nodule “gaps” vs scored/Imaging_12 are not material under script 50 ±30d policy** (`true_gap_after_30d_policy` = 0).
- human-review/manuscript readiness: Out of scope for automation; operator WARNs (e.g. specimen-adjacent review burden) remain in `119` report.

## Exact blockers

| ID | Blocker | Count |
|----|---------|------:|
| B1 | Strict triple-key misses (scored vs `imaging_nodule_master_v1`) — **not a true coverage gap** under script 50 ±30d dedup | 527 strict; **0** after policy (`us_nodule_coverage_audit_policy_aligned.csv`) |
| B2 | Strict triple-key misses (Imaging_12 vs canonical) — same | 620 strict; **0** after policy |
| B3 | `bethesda_category` NULL in `fna_episode_master_v2` | 23 |
| B4 | `serial_imaging_us` **empty** (schema only; no serial US feed) | 0 rows |
| B5 | No demonstrated per-level structured US LN table | N/A |

## Residual ambiguities

| ID | Description |
|----|-------------|
| A1 | **Resolved:** strict “unmatched” keys are explained by **±30d dedup** vs `scripts/50_multinodule_imaging.py` supplements — see `FAILURE_REMEDIATION_20260413.md` and `us_nodule_coverage_audit_policy_aligned.csv`. |
| A2 | `imaging_nodule_long_v2` row count (19,891) equals COMPLETE-only; relationship to scored/Imaging_12 supplements is asymmetric by design. |

## What would need to change to become FULLY_EXECUTED_BY_STRONG_STANDARD

1. ~~Reconcile strict keys~~ **Done for policy purposes:** supplements are reconciled with `scripts/50` ±30d logic; `true_gap_after_30d_policy` = 0. Optional: **also** store identical strict keys in canonical if product requires row-for-row workbook parity (would duplicate exam dates within the dedup window).
2. Extend TI-RADS sufficiency auditing to **every** corpus row with ≥5 ACR features (or prove insufficient detail) and eliminate any `missing_unexplained` rows.
3. Add manuscript-grade **pathology** linkage state per nodule (or justify exclusion) in addition to FNA linkage.
4. Implement and populate **structured** US lymph-node fields (level, laterality, size, suspicion) beyond exam-level text.
5. Backfill **numeric** `bethesda_category` for all `fna_episode_master_v2` rows or redefine episode grain so “episode” matches rows that always have cytology/path numeric Bethesda (requires approved writes — not done in this audit).
6. **Schema done** (placeholder 0 rows). **Populate** `serial_imaging_us` from an institutional feed if serial-US completeness is required for claims.
