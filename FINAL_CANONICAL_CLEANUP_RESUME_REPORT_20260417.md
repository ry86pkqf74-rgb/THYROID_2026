# FINAL CANONICAL CLEANUP RESUME REPORT — 2026-04-17

**Engagement:** `canonical_cleanup_resume_20260417`
**Database:** `thyroid_canonical_publication_v1_0` (canonical publication DB)
**Cohort:** 10,871 patients in `canonical_patient_master` (CPM)
**Run window:** 2026-04-18 02:51 UTC (preflight) → 05:12 UTC (Phase 5 verification)
**Token handling:** all DB access via `scripts/_md_connect.connect_locked()`; token never printed, never committed
**Invariant:** `canonical_patient_master = 10,871 rows / 10,871 distinct research_id` re-asserted at every checkpoint

---

## Executive summary

| phase | description | status |
|:---|:---|:---|
| 0 | Preflight | ✅ all 16 invariants green |
| 1 | Hypoparathyroidism adjudication (4 rids) | ✅ all 4 → `(C) indeterminate_requires_chart_review`; CPM untouched |
| 2 | Tg-lab orphan classification (403 rids) | ✅ flag-and-retain (Logan's Option 2); zero rows deleted |
| 3 | Archive / deprecate / delete pass | ✅ classifier ran clean (0 DELETE / 0 DEPRECATE / 0 ARCHIVE); audit table refreshed (115 → 120 rows, signal-based) |
| 4 | Documentation MEDs (4.1, 4.2, 4.3) | ✅ comments applied; invariant view created (returned 80 rows → escalated to 4(ii)) |
| 4 (ii) | 80-rid invariant violation trace + classify | ✅ inversion finding: `tumor_size_cm_max` is the broken column for **80 patients**; correction queues built; CPM untouched |
| 5 | Final aggregator + replay verification | ✅ all 13 replay queries pass; final invariant 10,871/10,871 |

**Critical findings cleared this run:** 0 (none introduced this run; prior critical findings were closed by the 04-17 work)
**High findings cleared this run:** 0
**Medium findings cleared this run:** 3 (4.1 path_tumor_size_cm comment, 4.2 invariant view created, 4.3 worst_bethesda_num comment)
**Items HELD FOR ADJUDICATION:** **84** (4 Phase 1 hypopara + 80 Phase 4(ii) F-bucket)

---

## Phase 1 — Hypoparathyroidism adjudication outcomes (4 patients)

Strict bar applied as agreed: **(B) permanent persists** requires PTH<15 pg/mL after day 180, active replacement med after day 180, AND no resolution evidence. PROMPT 18 holds (rids 7487, 9765) default to **(C)** unless evidence is overwhelming.

| rid | first surgery | hypo days postop | phen says | CPM says | action | basis |
|---:|:---|---:|:---|:---|:---|:---|
| 6447 | 2018-02-06 | 2,232 | confirmed_transient | permanent=TRUE | **(C)** | NSQIP "calcium-only oral pills" + 1 calcitriol/calcium NLP mention at d 2232; **zero** PTH and calcium labs ever recorded |
| 7487 | 2019-07-23 | 2,051 | confirmed_transient | permanent=TRUE | **(C)** | PROMPT 18 hold. Med NLP at d 140 (BEFORE d 180); zero PTH labs; calcium 8.9 mg/dL at d 2051 (normal) |
| 9765 | 2022-09-06 | 713 | confirmed_transient | permanent=TRUE | **(C)** | PROMPT 18 hold. PTH 79 pg/mL at d 713 (normal/high) suggests transient, BUT 6 calcitriol + 6 calcium NLP mentions with NULL dates → med-active status `unknown` |
| 10743 | 2024-07-16 | 27 | confirmed_transient | permanent=TRUE | **(C)** | Surgery only ~9 mo before CPM build; no data exists past day 180 |

### Items still HELD FOR ADJUDICATION — Phase 1

| rid | the question Logan needs to answer |
|---:|:---|
| **6447** | Is the patient still on calcium / calcitriol >6 yrs after surgery, OR did they discontinue? Any PTH lab anywhere in the chart? |
| **7487** | After day 180 post-op (2020-01-19+), any lab PTH or any active calcium/calcitriol replacement, or did everything resolve by then? |
| **9765** | Were the 6 calcitriol + 6 calcium NLP mentions all early-postop (transient), or do any extend past day 180? Need med dates. |
| **10743** | Wait for >180d follow-up data and re-adjudicate, OR chart-review the most recent visit notes for current PTH / med status now? |

**DB writes:** created `manuscript_workspace.cpm_hypopara_adjudication_log_v1` (4 rows, JSON evidence summary per rid); updated `manuscript_workspace.cpm_hypopara_adjudication_queue_v1.status` for all 4 rids → `indeterminate_requires_chart_review`. **No CPM modifications.**

---

## Phase 2 — Tg-lab orphan cohort decision (403 patients)

| classification | recommendation | n |
|:---|:---|---:|
| `likely_non_cancer_flagged_retained` | FLAG (don't delete) | **403** |
| `likely_dropped_from_CPM` | n/a | 0 |
| `ambiguous` | n/a | 0 |

All 403 had operative episodes (`has_op = TRUE`) but ZERO evidence in any of the 5 cancer-evidence tables (FNA, tumor episode, synoptic tumor, path synoptic, imaging nodule). `lab_first_after_cohort_freeze_flag = 0` for all → not a post-freeze feed-drift problem; benign-thyroidectomy patients on Tg surveillance.

### Logan's overrule of the delete recommendation → flag-and-retain

Logan's Option 2 was applied: preserve scientific value of benign comparator cohort, avoid 18.5% silent deletion of `thyroglobulin_lab_canonical_v1` and 18.4% of `longitudinal_lab_canonical_v1`, encode the cancer-cohort filter in schema rather than via deletion.

**DB writes:**

- `ALTER TABLE main.thyroglobulin_lab_canonical_v1 ADD COLUMN is_in_canonical_cancer_cohort BOOLEAN` → 60,385 TRUE, 13,873 FALSE, 0 NULL (matches expected splits exactly).
- `ALTER TABLE main.longitudinal_lab_canonical_v1 ADD COLUMN is_in_canonical_cancer_cohort BOOLEAN` → 61,374 TRUE, 13,873 FALSE, 0 NULL (matches expected splits exactly).
- `COMMENT ON COLUMN` for both flag columns documenting the 403-patient classification + provenance link to study.
- `CREATE OR REPLACE VIEW main.thyroglobulin_lab_canonical_cancer_only_v1` (60,385 rows) and `..._longitudinal_..._cancer_only_v1` (61,374 rows).
- `manuscript_workspace.lab_orphan_audit_v1` reclass: `classification = 'likely_non_cancer_flagged_retained'`; new column `resolution = 'flagged_is_in_canonical_cancer_cohort_FALSE_20260417'`.
- 1 MED finding cleared (lab orphan documented + exposable via flag, no longer invisible).

---

## Phase 3 — Archive / deprecate / delete pass

### Classifier outcome — **ran clean**

| action | n |
|:---|---:|
| DELETE | 0 |
| DEPRECATE | 0 |
| ARCHIVE | 0 |
| KEEP_REVIEW | 0 |
| LIVE | **118** |

Stop-gate **not tripped** (gates: >10 DELETEs, >5 ARCHIVEs).

The 04-17 Phase 4.6 work and the 266c Phase 5 archive sweep already moved every deprecation/archive candidate to `"Thyroid 2026 UPdated".archive_pub_v1_0`. The canonical DB is in its end state for archive/deprecate/delete. No cross-DB writes were needed.

### Audit table refresh

- **Snapshot** of pre-refresh state preserved as `manuscript_workspace.canonical_cleanup_audit_v1_snapshot_20260417` (115 rows + `snapshotted_at` column + COMMENT explaining the placeholder content and stale `data_dictionary_v240` row).
- **DROP & recreate** `manuscript_workspace.canonical_cleanup_audit_v1` with richer schema: `(object_name, object_type, status, destination, reason, row_count, n_distinct_research_id, is_referenced_by_view, is_referenced_by_script, is_identical_to_twin, n_view_refs, n_script_refs, has_version_twin, twin_name, classifier_version, last_modified_in_db, notes, classified_at)`.
- **120 rows inserted**: 118 main objects (status=`LIVE` for all, signal-based reasons) + 2 manuscript_workspace audit-trail rows (`lab_orphan_audit_v1`, `lab_orphan_cohort_review_v1`) with `notes = 'audit trail for 2026-04-17 Tg orphan cohort decision (Phase 2)'`.
- Lineage encoded: `data_dictionary_v266a` row carries `notes = 'replaces data_dictionary_v240 (archived to "Thyroid 2026 UPdated".archive_pub_v1_0 by 266c Phase 5 archive sweep 2026-04-18; lineage preserved here)'`.
- `last_modified_in_db` populated for 26 of 118 main tables (those with a `*_at` TIMESTAMP column).
- `classifier_version = 'v2_signal_based_20260417'` on every row.

---

## Phase 4 — Documentation MEDs

| step | result |
|:---|:---|
| 4.1 `COMMENT ON COLUMN main.canonical_patient_master.path_tumor_size_cm` | ✅ initial comment applied 2026-04-18; **superseded** by 4(ii) refinement |
| 4.2 `CREATE OR REPLACE VIEW manuscript_workspace.path_tumor_size_invariant_v1` | ✅ created; **returned 80 rows** (escalation to 4(ii)) |
| 4.3 `COMMENT ON COLUMN main.canonical_patient_master.worst_bethesda_num` | ✅ applied verbatim with 672 CPM-over caveat referencing PART2 §2.2 |

---

## Phase 4 (ii) — 80-rid invariant violation: trace + classify (read-only on CPM)

### 5-bucket → 6-bucket reclassification

The original 5-bucket spec (A unit-error, B wrong-source, C NLP-contamination, D enum-drift, E unresolvable) assumed `path_tumor_size_cm` was the suspect column. The trace showed **the broken column is `tumor_size_cm_max`**, not `path_tumor_size_cm`.

#### Framing inversion — what happened

Original hypothesis (Logan): `path_tumor_size_cm = 12.7` on a patient whose largest focus is 1.6cm is contamination of `path_tumor_size_cm`. The trace inverted this:

For rid 2378 (top extreme), TEM has two surgery rows: `surg_episode=1, ord=1: 1.6 cm` and `surg_episode=2, ord=1: 12.7 cm`. `synoptic_tumor_long_v1`, CTC, `path_synoptics`, `tumor_pathology` — **all only carry the first-surgery focus (1.6 cm)**. `path_tumor_size_cm = 12.7` is **correct** (it sees the second surgery's tumor). `tumor_size_cm_max = 1.6` is **broken** because the max-aggregator reads from the first-surgery-only feeder set. Same pattern across all 13 extremes and many moderates.

The agent surfaced this inversion to Logan, who confirmed the read.

### Final bucket counts (after Step 1 generalized scope check + lock-in reclassification)

| bucket | description | broken column | n |
|:---|:---|:---|---:|
| A | Unit / decimal error | `path_tumor_size_cm` | 0 |
| B | Wrong source (matches anatomic value) | `path_tumor_size_cm` | 0 |
| C | NLP contamination | `path_tumor_size_cm` | 0 |
| D (semantic overlay) | Multifocal enumeration drift documented | neither | 13 |
| E | Unresolvable | unknown | 0 |
| **F** | `tumor_size_cm_max` under-reports | **`tumor_size_cm_max`** | **80** |
| of which F1 | TEM-confirmed | | 75 |
| of which F2 | non-TEM feeder mismatch | | 5 |

### Step 1 generalized scope check

`hidden_both_under = 0` ✓ — no patients escape the `path_tumor_size_invariant_v1` view. The TEM-based scope check found 75 multi-surgery patients where TEM's true max exceeds CPM's `tumor_size_cm_max`; combined with 5 non-TEM-feeder mismatches in the original Phase 4(ii) classifier, the true F-bucket scope is **80 = entire invariant violation set**.

### Items HELD FOR ADJUDICATION — Phase 4 (ii)

| queue table | n | next action |
|:---|---:|:---|
| `manuscript_workspace.path_tumor_size_correction_queue_v1` | **80** | row-by-row approval of `proposed_corrected_value`; for F1 (75) the proposal = TEM `true_max_across_all_surgeries`; for F2 (5) the proposal = `observed_max_tumor_focus` from cross-feeder rollup. `upstream_fix_target` column documents the source-table fix needed. |
| `manuscript_workspace.path_tumor_size_multifocal_enumeration_notes_v1` | 13 | semantic overlay only; these 13 are also in correction queue (subbucket=F1) but additionally have legitimate dominant ≠ max even after correction — they have different valid foci across surgeries. Documentation, not held. |
| `manuscript_workspace.path_tumor_size_chart_review_queue_v1` | 0 | none; all originally-E rids reclassified to F1 once the TEM scope check ran. |

### Column comments updated (twice)

`main.canonical_patient_master.tumor_size_cm_max` now warns explicitly:

> Maximum tumor focus size across surgeries. KNOWN BUG (2026-04-18): for multi-surgery patients, feeder tables that populate this column include surgery-1 tumors only. N=80 patients identified where later-surgery foci exceed this value — see `manuscript_workspace.path_tumor_size_correction_queue_v1` (75 TEM-confirmed under-reports as subbucket=F1; 5 non-TEM-feeder under-reports as subbucket=F2; status='awaiting_approval'). Until corrections are applied, use `GREATEST(path_tumor_size_cm, tumor_size_cm_max)` for true-max queries, OR join to the correction queue for the authoritative value (`proposed_corrected_value` column). Dominant-focus queries should use `path_tumor_size_cm`. 13 of the 80 are documented as semantic overlay in `path_tumor_size_multifocal_enumeration_notes_v1` where dominant!=max even after the bug is fixed (different valid foci across surgeries). Generalized scope check 2026-04-18 confirmed `hidden_both_under = 0`.

`main.canonical_patient_master.path_tumor_size_cm` updated symmetrically to point at the queue.

### NO CPM value modifications

Per Logan's explicit gating: patient-data value corrections require a separate row-by-row approval step, not in a cleanup-run with other phases already committed. The correction queue is the deliverable; the actual UPDATE on `canonical_patient_master.tumor_size_cm_max` is a separate engagement Logan approves. CPM invariant 10,871 / 10,871 holds.

---

## Phase 5 — Provenance + replay verification

### Replay queries — all 13 PASS

| Q | check | result |
|:---|:---|:---|
| Q1 | CPM cardinality | 10,871 / 10,871 ✓ |
| Q2 | `cpm_built_at` non-null for all rows | 0 NULL ✓ |
| Q3 | `ajcc8_t_stage` + `..._with_microete_t3b_DEPRECATED` both present | ✓ |
| Q4 | `lateral_neck_dissected` + `..._structured_or_nlp` both present | ✓ |
| Q5 | 6 per-entity `comp_*_confirmed` columns present | ✓ |
| Q6 | `vc_paralysis_recalibration_v236` exists in mw | ✓ |
| Q7 | TG lab flag split (true/false/null) | 60,385 / 13,873 / 0 ✓ |
| Q7b | LONG lab flag split | 61,374 / 13,873 / 0 ✓ |
| Q8 | `tg_cancer_only_v1` view | 60,385 ✓ |
| Q8b | `long_cancer_only_v1` view | 61,374 ✓ |
| Q9 | audit table distribution | LIVE × 120 ✓ |
| Q9b | v266a lineage notes | populated ✓ |
| Q10 | `path_tumor_size_invariant_v1` rows | 80 (held) ✓ |
| Q11 | correction queue scope | F1=75, F2=5 ✓ |
| Q12 | hypopara queue status | indeterminate_requires_chart_review × 4 ✓ |
| Q13 | provenance ledger | 7 rows total ✓ |

### Drill-down row-count floors — all PASS

| table | observed | range | result |
|:---|---:|:---|:---|
| `operative_episode_detail_v2` | 9,371 | 9,366..9,376 | ✓ |
| `complication_phenotype_v1` | 5,978 | 5,928..6,028 | ✓ |
| `fna_episode_master_v2` | 8,119 | 5,000..30,000 | ✓ |
| `rai_treatment_episode_v2` | 1,857 | 1..100,000 | ✓ |
| `synoptic_tumor_long_v1` | 11,103 | 5,000..30,000 | ✓ |
| `thyroglobulin_lab_canonical_v1` | 74,258 | 73,758..74,758 | ✓ |
| `longitudinal_lab_canonical_v1` | 75,247 | 73,000..76,000 | ✓ |

### Provenance ledger after Phase 5 (7 rows)

```
canonical_cleanup_20260417                        (placeholder from prior runs)
canonical_cleanup_resume_20260417_phase1          held=4
canonical_cleanup_resume_20260417_phase2          med=1, held=0
canonical_cleanup_resume_20260417_phase3          all=0  (classifier_clean__audit_refreshed)
canonical_cleanup_resume_20260417_phase4          med=2, held=1
canonical_cleanup_resume_20260417_phase4ii       held=80  (F80 under-report queued)
canonical_cleanup_resume_20260417                 (final aggregator)
                                                  med=3, held=84  (4 hypopara + 80 F-bucket)
```

---

## Items still HELD FOR ADJUDICATION — combined

| n | source | next-action queue |
|---:|:---|:---|
| **4** | hypopara contradictions (rids 6447, 7487, 9765, 10743) | `manuscript_workspace.cpm_hypopara_adjudication_queue_v1` (status=`indeterminate_requires_chart_review`) + `..._log_v1` (evidence JSON per rid) |
| **80** | `tumor_size_cm_max` under-report bug | `manuscript_workspace.path_tumor_size_correction_queue_v1` (status=`awaiting_approval`; F1=75 TEM-confirmed, F2=5 non-TEM); per-row `proposed_corrected_value` |
| 13 (documented, not held) | Multifocal enumeration semantic overlay | `manuscript_workspace.path_tumor_size_multifocal_enumeration_notes_v1` (these are ALSO in correction queue) |
| 0 | Tg-lab orphans | resolved via flag-and-retain Phase 2 |

---

## Framing inversion note (audit trail honesty)

The Phase 4 prompt assumed `path_tumor_size_cm` was the suspect column for the 80 invariant violations. The agent's Phase 4(ii) trace, executed strictly read-only with full per-rid feeder dumps, demonstrated that `path_tumor_size_cm` is correct for the 80 rids and `tumor_size_cm_max` is the broken column (max-aggregator reads from a feeder set that excludes second-surgery TEM rows). Logan reviewed the trace, confirmed the inversion, and authorised the F-bucket framing. The generalized scope check Logan requested (Step 1) confirmed `hidden_both_under = 0` — no patients escape the invariant view. This audit trail is preserved so any reviewer asking "why did the prompt say one thing and the conclusion say another" finds the answer here.

---

## Files manifest (under `studies/canonical_cleanup_20260417_resume/`)

### Phase scripts (executable, idempotent)

- `preflight.py`, `preflight.json`, `preflight.log`
- `phase1_adjudicate.py`, `phase1_outcomes.md`, `phase1_evidence.json`, `phase1_run.log`
- `phase1_provenance.py`
- `phase2_orphan_classify.py`, `tg_orphan_decisions.md`, `tg_orphan_decisions.csv`, `phase2_summary.json`, `phase2_run.log`
- `phase2_apply_flags.py`, `phase2_apply.log`
- `phase3_inventory_and_classify.py`, `phase3_object_signals.json`, `phase3_proposed_actions.csv`, `phase3_preview.md`, `phase3_run.log`
- `phase3_refresh_audit.py`, `phase3_refresh.log`
- `phase4_doc_meds.py`, `phase4_invariant_violations.csv`, `phase4_run.log`
- `phase4ii_trace_classify.py`, `phase4ii_classification.{md,csv,json}`, `phase4ii_run.log`
- `phase4ii_apply_queues.py`, `phase4ii_apply.log`
- `phase4ii_step1_scope_check.py`, `phase4ii_scope_check.{json,csv}`, `phase4ii_scope_check.log`
- `phase4ii_lock_ins.py`, `phase4ii_lock_ins.log`
- `phase5_aggregate_and_verify.py`, `verification.md`, `verification.json`, `phase5_run.log`

### Probes (read-only diagnostics)

- `_phase1_probe_schema.py`, `_phase3_probe.py`, `_phase3_probe2.py`, `_phase3_diagnose.py`, `_probe_phase2.py`, `_phase4ii_probe.py`, `_phase4ii_inspect_E.py`, `_phase4ii_reconcile.py`, `_phase4_invariant_dump.py`, `_phase2_has_op_analysis.py`, `_probe_ajcc.py`

### Final report (this file)

- `FINAL_CANONICAL_CLEANUP_RESUME_REPORT_20260417.md` (repo root)

---

## DB objects created / modified (audit summary)

### `main` schema

| object | change |
|:---|:---|
| `canonical_patient_master` | NO row-value modifications. 2 column COMMENTs added/refined: `path_tumor_size_cm`, `tumor_size_cm_max`. CPM invariant 10,871 / 10,871 holds. |
| `thyroglobulin_lab_canonical_v1` | ADD COLUMN `is_in_canonical_cancer_cohort BOOLEAN` (60,385 TRUE / 13,873 FALSE / 0 NULL); COMMENT ON COLUMN added. |
| `longitudinal_lab_canonical_v1` | ADD COLUMN `is_in_canonical_cancer_cohort BOOLEAN` (61,374 TRUE / 13,873 FALSE / 0 NULL); COMMENT ON COLUMN added. |
| `thyroglobulin_lab_canonical_cancer_only_v1` | NEW VIEW (60,385 rows). |
| `longitudinal_lab_canonical_cancer_only_v1` | NEW VIEW (61,374 rows). |

### `manuscript_workspace` schema

| object | change |
|:---|:---|
| `cpm_hypopara_adjudication_queue_v1` | UPDATE status × 4 → `indeterminate_requires_chart_review` |
| `cpm_hypopara_adjudication_log_v1` | NEW TABLE (4 rows, JSON evidence per rid) |
| `lab_orphan_audit_v1` | UPDATE classification × 403 → `likely_non_cancer_flagged_retained`; NEW COLUMN `resolution` (populated) |
| `canonical_cleanup_audit_v1_snapshot_20260417` | NEW TABLE (115 rows; pre-refresh snapshot) |
| `canonical_cleanup_audit_v1` | DROPPED & RECREATED (120 rows, signal-based, richer schema) |
| `path_tumor_size_invariant_v1` | NEW VIEW (80 rows; canary for `tumor_size_cm_max` under-report) |
| `path_tumor_size_correction_queue_v1` | NEW TABLE (80 rows; awaiting_approval; F1=75 + F2=5) |
| `path_tumor_size_multifocal_enumeration_notes_v1` | NEW TABLE (13 rows; semantic overlay) |
| `path_tumor_size_chart_review_queue_v1` | NEW TABLE (0 rows; reserved for future) |
| `cpm_reconciliation_provenance_v1` | INSERT × 6 (5 per-phase rows + 1 final aggregator) |

---

## Sign-off

CPM invariant 10,871 / 10,871 holds. All replay queries pass. 84 items held for adjudication, each with a specific question or queue; no patient-data values modified anywhere in `canonical_patient_master`. The single most consequential finding is the `tumor_size_cm_max` under-report bug for 80 multi-surgery patients — that warrants a separate row-by-row approval engagement before any manuscript figures or tables that depend on it ship.
