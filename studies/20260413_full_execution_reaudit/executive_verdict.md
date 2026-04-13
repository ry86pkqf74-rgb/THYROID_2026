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
(1) Not all nodule keys from **scored** and **Imaging_12** corpora have deterministic matches in `imaging_nodule_master_v1` (527 + 620 unmatched keys). (2) TI-RADS sufficiency audit in this run is applied to the **COMPLETE** corpus only; it does not prove cross-corpus TI-RADS completeness. (3) Linkage classification view shows only FNA-oriented states; `no_eligible_fna` rows all have non-null `linkage_reason_code`, but pathology linkage is not part of this view’s states. (4) US lymph nodes are not fully documented at structured per-level/laterality/size granularity — only exam-level/heuristic audit exists. (5) Numeric Bethesda is missing for 23/8,119 rows in `fna_episode_master_v2` (resolved view documents reasons; strong standard still requires numeric in episode master for every episode).

exact counts:
- Scored corpus unmatched keys: 527 / 19,367
- Imaging_12 inferred unmatched keys: 620 / 20,910
- `imaging_nodule_master_v1` total rows: 37,016
- Primary FNA-linked nodules (`linked_to_fna`): 6,359
- `no_eligible_fna` with documented `linkage_reason_code`: 30,657 (null reason: 0)
- `fna_episode_master_v2` NULL `bethesda_category`: 23 / 8,119
- `serial_imaging_us` on MotherDuck: table absent (catalog error)

## Question-by-question answers
1. All ultrasound nodules from all corpora extracted? **NO**
2. All scoreable nodules TI-RADS scored (all corpora)? **NO** (COMPLETE-only sufficiency audit passed; other corpora not proven)
3. All nodules provenance-linked and downstream linkage-complete? **NO** (unresolved=0; multi-corpus key gaps remain; pathology linkage not in observed view)
4. All ultrasound lymph-node data fully recorded and documented in structured detail? **NO**
5. All FNA episodes numerically Bethesda scored? **NO** (23/8,119 NULL in `fna_episode_master_v2`)

## Important distinctions
- technical release readiness: `119 --release-mode` **PASS WITH WARNINGS** (0 FAIL) — not equivalent to strong clinical completeness.
- scoped confirmation: **PASS** — April 13 criteria still hold on recomputation.
- strong completion: **NOT MET** — multi-corpus nodule parity, LN detail, and universal numeric Bethesda fail.
- human-review/manuscript readiness: Out of scope for automation; operator WARNs (e.g. specimen-adjacent review burden) remain in `119` report.

## Exact blockers

| ID | Blocker | Count |
|----|---------|------:|
| B1 | Scored-workbook nodule keys not in `imaging_nodule_master_v1` | 527 |
| B2 | Imaging_12 inferred nodule keys not in canonical | 620 |
| B3 | `bethesda_category` NULL in `fna_episode_master_v2` | 23 |
| B4 | No `serial_imaging_us` table on MotherDuck | 1 (object missing) |
| B5 | No demonstrated per-level structured US LN table | N/A |

## Residual ambiguities

| ID | Description |
|----|-------------|
| A1 | Unmatched keys may include date-normalization or dedup-policy differences vs `scripts/50_multinodule_imaging.py` supplements — not root-caused in this read-only pass. |
| A2 | `imaging_nodule_long_v2` row count (19,891) equals COMPLETE-only; relationship to scored/Imaging_12 supplements is asymmetric by design. |

## What would need to change to become FULLY_EXECUTED_BY_STRONG_STANDARD

1. Reconcile **all** scored and Imaging_12 source nodules into `imaging_nodule_master_v1` with deterministic keys **or** formally scope them out with proof of duplicate/superset logic (not done here).
2. Extend TI-RADS sufficiency auditing to **every** corpus row with ≥5 ACR features (or prove insufficient detail) and eliminate any `missing_unexplained` rows.
3. Add manuscript-grade **pathology** linkage state per nodule (or justify exclusion) in addition to FNA linkage.
4. Implement and populate **structured** US lymph-node fields (level, laterality, size, suspicion) beyond exam-level text.
5. Backfill **numeric** `bethesda_category` for all `fna_episode_master_v2` rows or redefine episode grain so “episode” matches rows that always have cytology/path numeric Bethesda (requires approved writes — not done in this audit).
6. Materialize `serial_imaging_us` or replace its function with an explicit institutional imaging feed if required for completeness claims.
