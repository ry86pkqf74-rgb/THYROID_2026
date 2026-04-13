# Failure remediation (2026-04-13)

Follow-up to the read-only full execution re-audit. This documents what was **addressed** vs what remains **out of scope** without new data or product decisions.

## Resolved or reclassified

### B1 / B2 — “Unmatched” scored and Imaging_12 keys

**Reclassification:** Not missing canonical coverage under the pipeline’s **dedup policy**.

- `scripts/50_multinodule_imaging.py` supplements scored/Imaging_12 rows against existing `(research_id, nodule_number)` using a **±30 day** window, so the canonical table does **not** duplicate `rid|exam_date|nodule` when the **date differs within the window** from the COMPLETE row already ingested.
- The re-audit compares **strict** triple keys (`rid|date|nodule`) from workbooks to `imaging_nodule_master_v1` **without** that window; 527 + 620 “misses” are therefore **expected** under that comparison.
- Evidence: `us_nodule_coverage_audit_policy_aligned.csv` — for both corpora, `aligned_with_script50_30d_dedup_rule` equals the strict miss count and **`true_gap_after_30d_policy` = 0**.

### B4 — `serial_imaging_us` missing on MotherDuck

**Addressed (schema):** `scripts/155_md_serial_imaging_us_placeholder.py --md` creates `serial_imaging_us` with the six columns referenced by `scripts/22_canonical_episodes_v2.py` (empty table until an institutional feed is loaded).

### A1 — Root cause of strict key mismatches

**Resolved:** Differences between strict keys and canonical rows are **explained** by the script 50 ±30d dedup rule and asymmetric corpus design (`imaging_nodule_long_v2` COMPLETE-only row count vs supplement corpora), not by an undiagnosed ingestion bug in this pass.

## Addressed as documentation / analyst VIEW (not silent data fixes)

### B3 — NULL `bethesda_category` in episode master

- **Use** `v_fna_episode_bethesda_resolved_v1` for numeric Bethesda completeness (`COALESCE` episode + `fna_cytology`).
- **Deploy** `v_fna_bethesda_episode_vs_resolved_v1` via `scripts/156_md_bethesda_episode_vs_resolved_view.py --md` (pairs episode column vs resolved; requires `151` deployed first).
- Details: `open_items_b3_b5.md`.

### B5 — No per-level US LN table

- Documented in **`open_items_b3_b5.md`** — exam-level fields only; future work needs a governed extraction feed.

## Still open (unchanged at “strong standard” level)

| ID | Item | Notes |
|----|------|--------|
| B3 (strong wording) | “Every episode row has numeric Bethesda in `fna_episode_master_v2` alone” | Still **false**; analysis should use resolved view + buckets above. |
| B5 (strong wording) | Full structured LN level/laterality/size | Still **not in repo**; see `open_items_b3_b5.md`. |
| Strong standard | Cross-corpus TI-RADS sufficiency, pathology linkage in view, etc. | See `executive_verdict.md` — **strong** standard remains **FAIL** for these reasons. |

## Artifacts

- Policy-aligned coverage: `us_nodule_coverage_audit_policy_aligned.csv`
- Re-audit runner: `run_full_execution_reaudit_readonly.py` (helpers + CSV emit)
- Placeholder DDL: `scripts/155_md_serial_imaging_us_placeholder.py`
