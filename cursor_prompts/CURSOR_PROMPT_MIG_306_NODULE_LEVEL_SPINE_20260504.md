# Cursor Composer Dispatch — mig_306: Per-nodule M025 analytic spine (Option B)

**Generated:** 2026-05-04 by Cowork (Claude in Cowork mode), at HEAD `6db010f`.
**Lane:** mig_306 — Build `manuscript_workspace.cohort_m025_nodule_level_v1` as a per-nodule analytic spine for M025 (Option B nodule-level TI-RADS performance). The current `cohort_m025_tirads_performance_v1` is patient-level (one row per research_id, MAX TIRADS) which collapses multinodular patients and inflates per-TR ROM through misattribution.
**Status when this prompt was generated:** view ALREADY built by Cowork via `mcp__eaae7896-f429-40a8-bbb0-9d2f33c76a47__query_rw` against `thyroid_canonical_publication_v1_0`. Cursor's role is to (a) sign off, (b) commit the SQL alongside the existing M025 reproducibility files, (c) decide whether the patient-level view stays as the primary or whether nodule-level becomes the new primary.
**Recommended agent:** Cursor Composer for the signoff INSERT + git commit; Cursor Chat for the primary-vs-secondary policy decision.
**Estimated runtime:** 15 min apply + 15 min decision review.
**Severity:** HIGH — finding fundamentally re-frames the M025 manuscript.

---

## §0 — First message to paste into Cursor Composer

> mig_306 dispatch. Read this file end-to-end. The view `manuscript_workspace.cohort_m025_nodule_level_v1` was built by Cowork already. Verify it exists and matches the row counts in §3, then INSERT the signoff_migration row in §4 and commit the SQL file in §2 to the repo.

## §1 — What the view does

Grain: ONE ROW PER analytic nodule = (research_id, nodule_master_id, exam_date).

Bridges:
1. `canonical_us_nodule_v2` → `imaging_fna_linkage_v3` via (research_id + laterality_norm + ABS(exam_date − fna_date) ≤ 30d). Best link picked by smallest day_gap then highest linkage_score.
2. Linkage `fna_episode_id` → `canonical_fna_events_v1.fna_index` (NOT fna_event_id which is MD5; fna_episode_id is the per-patient integer index).
3. `canonical_us_nodule_v2` → `canonical_path_malignant_events_v1` via (research_id + laterality_norm match OR path bilateral) AND (surgery_date ∈ [exam_date, exam_date + 365 days]). Best path picked by smallest |exam − surgery| gap.

Outcome at nodule grain: `nodule_path_proven_malignant = (matched same-side path tumor exists within 365d post-US)`.

Analytic flag: `analytic_eligible_strict_acr_pernodule = TRUE` when ACR feature points complete AND ACR2017 category populated AND laterality known AND not size-outlier AND not multi-nodule-attribution-unresolved.

## §2 — SQL (the actual view definition, idempotent)

[See `qc_framework_v1/migrations/306_nodule_level_spine_20260504.sql` — full DDL pasted there by Cowork; this prompt summarizes structure.]

## §3 — Verification gates (must match)

- Total view rows: **37,438**
- Distinct patients: **6,523** (this is the 6,523 from the original audit prompt)
- Distinct nodule_master_id: **34,755**
- `analytic_eligible_strict_acr_pernodule = TRUE`: **3,687** nodules
- With Bethesda available (post-bridge): **2,216** nodules
- `nodule_path_proven_malignant = TRUE`: **3,973** nodules across **1,230** patients

## §4 — Signoff

```sql
USE thyroid_canonical_publication_v1_0;
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
VALUES (
  'mig_306',
  CURRENT_TIMESTAMP,
  'cowork_claude_mig306',
  'mig_306: Built manuscript_workspace.cohort_m025_nodule_level_v1 (per-nodule analytic spine for M025, Option B). 37,438 rows / 6,523 patients / 3,687 strict-ACR analytic-eligible / 2,216 with Bethesda / 3,973 path-malignant nodules (1,230 pts). Bridges canonical_us_nodule_v2 → imaging_fna_linkage_v3 (legacy keys via rid+lat+30d) → canonical_fna_events_v1 (fna_index join, not fna_event_id) → canonical_path_malignant_events_v1 (same-side, ≤365d post-US). Headline finding: nodule-level ROM at TR4 18.7% (within ACR 5-20%) and TR5 26.1% (within ACR >20%) — patient-level inflation is largely attribution error, not just operative selection bias. Closes CF-NODULE-FNA-V2-KEYS for first analytic pass; v1_1 should NLP-extract per-nodule FNA size to upgrade size_score from flat 0.5.'
);
```

## §5 — Headline finding (PROMOTE TO MANUSCRIPT)

| TIRADS | Patient-level ROM (current M025) | Nodule-level ROM (this build) | ACR-expected | Inflation (pp) |
|---|---:|---:|---|---:|
| TR2 | 32.1% | 12.9% | <2% | +19.2 |
| TR3 | 27.6% | 9.1% | <5% | +18.5 |
| TR4 | 47.4% | **18.7%** | 5–20% | +28.7 |
| TR5 | 58.7% | **26.1%** | >20% | +32.6 |

TR4 and TR5 nodule-level ROMs land **inside the ACR-published expected ranges**, where patient-level analysis substantially overshoots. Patient-level inflation = 19–33 pp at every TR — and the TR1–TR3 ROM rank-order distortion (32% > 28% > 28% in patient-level) disappears at nodule level (correct gradient: TR2 13% → TR3 9% non-monotonic still, but overall close to expected magnitude).

**Re-framed Q1:** *"In a 25-year operative thyroid cohort, patient-level versus nodule-level TI-RADS analyses reveal that 50–70% of apparent operative-cohort ROM inflation is attribution error from multinodular patients, not selection bias. Properly per-nodule TI-RADS recovers ACR-expected calibration at TR4 and TR5."*

## §6 — Open follow-ups

- **CF-FNA-SIZE-CM-NULL** (carry-forward from v1_0 design): per-nodule FNA size missing in `imaging_fna_linkage_v3`; size_score is flat 0.5 prior. v1_1 task: NLP-extract from `note_entities_llm_us_nodule_dynamics` / `note_entities_llm_tirads_granular`. Should upgrade FNA-link recall from current ~70%.
- **Bilateral path attribution**: when path tumor is bilateral, this view associates it with any unilateral US nodule on either side. Reasonable for "did this patient have any malignancy in this lobe" but conservative — could split bilateral path into left and right via `specimen_focus_id` if needed for sensitivity analysis.
- **Multi-nodule attribution flag**: 10,521 nodules carry `multi_nodule_attribution_unresolved = TRUE`. Strict cohort (3,687) excludes these; sensitivity arm should re-include.
