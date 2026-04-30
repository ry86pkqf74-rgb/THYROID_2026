# Lane E continuation apply (mig_219 / mig_220 / mig_221) — close-out

**Date:** 2026-04-30
**Round:** v13 (post v12 handoff at `8b6de0b`)
**Apply HEAD:** `097eca0` (typo-fix commit; pushed `4f4f979..097eca0`)
**Applier:** Cowork-direct via MotherDuck `query_rw` (account `logan.glosser.eras@gmail.com`)
**Source SQL:** Cursor-authored, committed as `4ac2dbe` + `d98f535`

---

## Why this round happened

The v12 handoff (`COWORK_HANDOFF_PROMPT_2026-04-30_v12.md`) listed Lane E continuation (mig_219/220/221) as **PENDING — Cursor composer**. First-action probing during v13 found that Cursor had **authored and git-committed** the SQL files but **never executed them against MotherDuck**.

Five independent live-MD probes were all empty:

1. No batch_ids `mig_219_*` / `mig_220_*` / `mig_221_*` in `main.canonical_column_verification_registry_v1`.
2. No pre-snapshot tables matching `pre_mig219` / `pre_mig220` / `pre_mig221` in `archive_pub_v1_0`.
3. No run_ids `mig_219_*` / `mig_220_*` / `mig_221_*` in `manuscript_workspace.cpm_reconciliation_provenance_v1`.
4. The 4 expected E4 views (`vw_us_nodule_tirads_*_VIEW_v1`) absent from `manuscript_workspace`.
5. The mig_220 ALTER TABLE col `tirads_conflict_resolution_source` absent from `canonical_us_nodule_v2`.

This is the **committed-but-not-applied** scenario. Logan ratified Cowork-direct apply as Path B from §6 of the handoff.

---

## Pre-apply Cursor typo

`mig_219` SQL had `priority_tier='tier2_canalytic'` — not in the registry's `priority_tier` enum distribution (existing values: `tier2_canonical`=22, `tier2_canonical_view`=4, `tier2_rollups`=19, `tier3_helper`=91, `tier3_extraction`=17, etc.). Logan ratified the fix to `tier2_canonical_view` (the standard for VIEW-type objects). Committed as `097eca0` before apply.

---

## mig_219 — Lane E4 (4 manuscript-facing TIRADS cohort VIEWs)

**Source:** `qc_framework_v1/migrations/219_tirads_cohort_views_20260430.sql`
**Base:** `manuscript_workspace.canonical_us_nodule_v2_filtered` (61 cols)
**Naming:** `_VIEW` infix per `reference_view_naming_convention.md`

| View | Filter (informal) | Row count |
|---|---|---:|
| `vw_us_nodule_tirads_strict_acr2017_VIEW_v1` | non-aggregate ∧ non-shell ∧ `acr2017_feature_points_complete=TRUE` ∧ points NOT NULL ∧ category NOT NULL | **5,121** |
| `vw_us_nodule_tirads_any_reported_VIEW_v1` | non-aggregate ∧ non-shell ∧ (any of `tirads_reported_in_text` / `acr2017_tirads_category` / `updated_tirads_category`) | **29,504** |
| `vw_us_nodule_tirads_reported_not_fully_parsed_VIEW_v1` | same as `any_reported` ∧ `acr2017_feature_points_complete=FALSE` | **24,371** |
| `vw_us_nodule_tirads_unresolved_or_excluded_VIEW_v1` | aggregate ∨ shell ∨ `nlp_backfill_pending=TRUE` | **3,366** |

**Registry deltas:**

| Object | Count |
|---|---:|
| signoff_registry rows inserted (`tier2_canonical_view`) | 4 |
| col_registry rows inserted (4 views × 61 cols, batch_id `mig_219_tirads_cohort_views_20260430`) | 244 |
| archive snapshot `archive_pub_v1_0.canonical_table_signoff_registry_v1_pre_mig219_20260430` | 189 rows |
| provenance row `mig_219_tirads_cohort_views_20260430` | 1 |

**Acceptance:** all 4 views queryable; signoff math green (`n_verified+n_na=n_columns_total`); 5-gate gate1 +4 (186 → 190).

---

## mig_220 — Lane E5 (auto-resolve high-priority TIRADS conflicts)

**Source:** `qc_framework_v1/migrations/220_tirads_high_pri_conflict_resolution_20260430.sql`
**Logan-locked rule:** prefer `value_tirads_v2` from queue for high-priority `tirads_reported` / `tirads_category_v2` / `tirads_score_2017` fields; skip rows where `value_tirads_v2 IS NULL OR blank` (deferred manual).

**Apply steps (all executed, in order):**

1. Pre-snapshot affected nodule rows → `archive_pub_v1_0.canonical_us_nodule_v2_pre_mig220_conflict_resolution_20260430` (2,506 rows).
2. `ALTER TABLE main.canonical_us_nodule_v2 ADD COLUMN tirads_conflict_resolution_source VARCHAR;` + `COMMENT ON COLUMN`.
3. INSERT 1 col_registry row (batch_id `mig_220_tirads_high_pri_conflict_resolution_20260430`).
4. UPDATE `tirads_reported_in_text` ← `value_tirads_v2` (where `field_name='tirads_reported'`).
5. UPDATE `updated_tirads_category` ← `value_tirads_v2` (where `field_name='tirads_category_v2'`).
6. UPDATE `acr2017_tirads_points` ← `value_tirads_v2` (where `field_name='tirads_score_2017'`).
7. Re-derive `acr2017_tirads_category` from points (TR1=0, TR2=2, TR3=3, TR4=4-6, TR5≥7) where points were touched.
8. Refresh `acr2017_vs_updated_concordant` where both category cols present.
9. INSERT provenance row.

**Per-field deltas vs queue scope:**

| Queue field | Queue rows | Resolved rows | Δ |
|---|---:|---:|---:|
| `tirads_reported` | 2,494 | **2,491** | -3 |
| `tirads_category_v2` | 123 | **120** | -3 |
| `tirads_score_2017` | 23 | **23** | 0 |
| Total field-resolutions | 2,640 | **2,634** | -6 |
| Distinct nodule rows touched | — | **2,506** | (compound rows tagged with pipe-separated source) |

**Carry-forward:** **CF-mig220-QUEUE-CURRENT-V2-DRIFT** — 6 high-pri queue rows referenced `(research_id, us_exam_id, nodule_index_within_exam)` tuples not present in current `canonical_us_nodule_v2`. Queue is a snapshot vs current build; investigate whether queue should be re-built post-mig_177c_apply or these are valid orphans for manual review. Non-blocking for v1.0.

---

## mig_221 — Lane E6 (`acr2017_feature_points_complete` semantic documentation)

**Source:** `qc_framework_v1/migrations/221_acr_completeness_flag_clarification_20260430.sql`
**Companion files (already committed in `4ac2dbe`):**

- `memory/feedback_acr2017_feature_points_complete_semantic.md`
- `docs/methods_acr2017_feature_points_complete_20260430.md`

**What was applied:**

1. `COMMENT ON COLUMN` on `main.canonical_us_nodule_v2.acr2017_feature_points_complete` clarifying the flag = legacy CUNC `tirads_score_component_complete` (Script 271): TRUE iff all five ACR feature **descriptor** fields non-NULL on `canonical_us_nodule_characteristics_v1` upstream — NOT equivalent to "all five `*_pts` cols non-NULL" because Script 376 imputes `*_pts` from normalized feature strings post-merge.
2. UPDATE existing col_registry row (batch_id `mig_117`) appending mig_221 documentation to `notes` and `verification_method`.
3. INSERT provenance row.

**Net effect:** documents the 21,454-vs-5,149 (4×) gap noted in ChatGPT TIRADS plan §"Completeness flag needs clarification". Strict ACR cohort (`vw_us_nodule_tirads_strict_acr2017_VIEW_v1`) correctly filters on `acr2017_feature_points_complete=TRUE`.

---

## Incidental: mig_222 (Lane F, Cline) discovered already-applied

While probing for parallel-lane race, found commit `4f4f979 mig_222: triage multi-nodule LLM attribution queues` had landed between v12 handoff and v13 first-action probe. Cline followed lane F prompt rigorously (audit CSVs in `qc_framework_v1/reports/.../mig_222_*` + summary.json + 11 audit CSV families).

**Cline's apply (verified live):**

- Added `multi_nodule_attribution_unresolved` BOOLEAN to `canonical_us_nodule_v2` (no collision with mig_220's `tirads_conflict_resolution_source`)
- Conservative publication-safe policy: 0 absorbed; all 448 candidate exams + 825 deferred patients documented as limitation; 10,570 affected canonical nodule rows flagged
- Durable triage ledger `manuscript_workspace.us_multi_nodule_attribution_triage_v1` created
- 1 col_registry row + 1 provenance row in MD ✓

**Lane F is therefore CLOSED.** Only Lane G (mig_223 — `semantic_publication` schema + manuscript-safe views, Cline GPT-5.5) remains pending.

---

## Final state post-apply

**Verification suite (all green):**

| Check | v12 baseline | v13 final | Δ |
|---|---:|---:|---:|
| §1 gate1 (verified tables) | 186 | **190** | +4 (mig_219 views) |
| §1 gates 2-5 | 0/0/0/0 | **0/0/0/0** | unchanged |
| §2 cohort parity | 10871/10871/10871 | **10871/10871/10871** | unchanged |
| §12 governance gap | 0 | **0** | unchanged |
| §14 v2 clinical date type | 0 | **0** | unchanged |
| §15 newest registry batch | mig_209 | mig_220 | mig_222/219/220 visible |

**Hard data invariants:** all unchanged.

---

## Carry-forwards

1. **CF-mig220-QUEUE-CURRENT-V2-DRIFT** (non-blocking) — 6 high-pri queue rows didn't map to current `canonical_us_nodule_v2`. Investigate post-mig_177c_apply.
2. **CF-mig219-NOT-FULLY-PARSED-COUNT-DRIFT** (manuscript-facing) — `vw_us_nodule_tirads_reported_not_fully_parsed_VIEW_v1` returned 24,371 rows vs ChatGPT's count of 8,243 (~3× delta). Filter logic is exactly what ChatGPT specified; counts may reflect a snapshot-time difference. Sanity-check before manuscript Methods reference.

---

## Reusable patterns added

1. **Committed-but-not-applied detection** — when SQL files exist on disk + git but the 5 standard probes (col_registry batch_id / archive snapshots / provenance run_id / target view existence / target ALTER COLUMN existence) all return empty, the lane was authored without execution. Apply Cowork-direct with pre-snapshots + provenance.
2. **Pre-apply typo-fix commit** — fix Cursor SQL bugs in their own commit BEFORE the apply (here `097eca0` for mig_219's `tier2_canalytic→tier2_canonical_view`); audit trail clean.
3. **Parallel-lane race detection during first-action checklist** — git log shows new commits between v12 handoff and v13 chat start (mig_222 here); probe immediately for state collision before any mutating apply.

---

## ChatGPT TIRADS plan coverage (uploaded `us_nodules_tirads_comprehensive_assessment_plan.md`)

| ChatGPT phase | Status | Migration |
|---|---|---|
| Phase 1 — 4 manuscript-facing TIRADS cohort views | ✅ APPLIED | mig_219 (this round) |
| Phase 2 — Resolve high-pri TIRADS conflicts | ✅ APPLIED | mig_220 (this round) |
| Phase 3 — Multi-nodule under-explosion (448+825) | ✅ APPLIED | mig_222 (Cline Lane F) |
| Phase 4 — Clarify `acr2017_feature_points_complete` semantics | ✅ APPLIED | mig_221 (this round) |
| Phase 5 — `bi_us_*_v1` Power BI semantic layer | ⏸ DEFERRED | Future H |

Every actionable item in the ChatGPT TIRADS doc that targets v1.0 cleanup is now in MotherDuck. Phase 5 is correctly deferred to the Power BI Desktop migration trigger.

---

## Manuscript readiness

**READY.** Logan's manuscript writing can proceed in parallel with Lane G (Cline mig_223 — `semantic_publication` schema + 8 vw_*_safe_VIEW_v1 manuscript-safe views). None of the carry-forwards block v1.0.
