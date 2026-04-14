> **HISTORICAL / SUPERSEDED:** This document is a point-in-time snapshot from its generation date. For current canonical state, see [`docs/final_source_of_truth_contract.md`](../../docs/final_source_of_truth_contract.md) and [`studies/CURRENT_MOTHERDUCK_REPO_STATE.md`](../CURRENT_MOTHERDUCK_REPO_STATE.md). Row counts cited here may no longer match live MotherDuck.

# Manuscript blocker rebaseline — post specimen/FHIR structural posture

**Generated (UTC):** 2026-04-08T14:45:00Z (report); **MotherDuck triage run:** `exports/review_queue_triage_20260408_144229/` (gitignored; mirrored under `evidence/` here).

**Purpose:** Re-baseline what still blocks a *signed manuscript* after structural specimen/FHIR hardening, and separate **structural** vs **technical promotability** vs **governance** vs **source-limited** gaps.

---

## 1. Three readiness lenses (do not conflate)

| Lens | Meaning | Live snapshot (this rebaseline) |
|------|---------|----------------------------------|
| **Structural specimen + analytic FHIR** | Diagnostic tables/views; `119 --release-mode` **Check 13** surface; no FAIL on broken FHIR refs at last signed prod capture | See [`studies/specimen_fhir_release_truth_20260408T141710Z/report.md`](../specimen_fhir_release_truth_20260408T141710Z/report.md). Structural blockers are **not** the current manuscript story once Check 13 is green. |
| **Technical release readiness** | `119 --release-mode`: pending MRQ = 0; synthetic-placeholder MRQ = 0 (CHECK **5b**); manifests / presentation views / domain parity per validator | **PASS posture on MRQ structure:** 5,622 rows, **0** `verification_status` NULL; **0** rows in synthetic-placeholder class. |
| **Manuscript-governance readiness** | Publication policy: distinguish **tier-policy automation** (`auto_accepted_*`) from **human reviewer identity**; `qa.promotion_review_decisions` **substance** vs schema | **5620/5622** MRQ rows are **`auto_accepted_*`** only; **2** rows are **`confirmed_correct`** without `reviewer` / `reviewed_at` (incomplete identity). **0** rows classify as `true_human_reviewed` in the three-bucket export (after `auto_accepted*` precedence). |

---

## 2. MRQ triage (`scripts/120_review_queue_triage.py --md`)

Evidence: `evidence/triage_summary.md`, `evidence/*.csv`.

### 2.1 Row separation (requested labels)

| Category | Count | Rule (implementation) |
|----------|------:|------------------------|
| **Synthetic automation-only** | **0** | `verification_status` matches [`utils/publication_governance.py`](../../utils/publication_governance.py) synthetic-placeholder predicates (exact + substring markers). |
| **Automation tier (non-synthetic)** | **5620** | `auto_accepted_standard` (3081), `auto_accepted_critical_sample_ok` (1646), `auto_accepted_informational` (893). |
| **True human-reviewed** (conservative) | **0** | Non-pending, not synthetic, not `auto_accepted*`, with non-empty `reviewer` **and** non-null `reviewed_at` (same precedence as `counts_manuscript_quality_tiers.csv` tier D). |
| **Unresolved / malformed** | **0 pending + 2 incomplete** | **Pending:** 0. **Malformed / incomplete governance:** 2 rows — `confirmed_correct`, `reviewer` NULL, `reviewed_at` NULL (`review_row_id` 1966, 1970 in live query snapshot). |

### 2.2 New export: `counts_mrq_three_bucket_signoff.csv`

Roll-up buckets (mutually exclusive, `auto_accepted*` before human-identity):

- `automation_tier_or_incomplete_non_human`: **5622** (all `auto_accepted*` + the 2 incomplete `confirmed_correct`).
- `true_human_reviewed`: **0**.
- `synthetic_automation_only`: **0**.
- `unresolved_pending`: **0**.

---

## 3. `qa.promotion_review_decisions` — publication-grade?

**Schema / release gate (119 structural):** On live prod, **5** rows; **0** rows with NULL/blank `decision_batch_id` → **passes** publication-mode batch-id requirement in [`docs/publication_governance_gate.md`](../../docs/publication_governance_gate.md).

**Substance for manuscript sign-off:** Rows are predominantly **`_tier_batch_summary`** / **policy_batch** / **policy_batch_adjudication** artifacts (tier-policy governance, waiver text pointing at memos), not thousands of row-level clinician adjudications. So:

- **Publication-grade for automation provenance** — yes, for “a batch was recorded with a batch id.”
- **Publication-grade as a human chart-review package** — **no**; that would require-decision imports keyed to entity/row scope and reviewer identity, not only tier summaries.

**Safe import / hydration path (additive, provenance-preserving):**

1. **Append-only decisions:** [`scripts/126_final_master_release.py`](../../scripts/126_final_master_release.py) `--md --decisions-csv … --decision-batch-id …` — extends `qa.promotion_review_decisions` without rewriting history; aligns with orchestrator docstring.
2. **MRQ hydrate:** `126 --hydrate-mrq-from <gate_dir>` or [`scripts/114_qa_schema_setup.py`](../../scripts/114_qa_schema_setup.py) `--md --hydrate-from …` — destructive for the target `run_label` scope; use QA/dev catalog first per sandbox runbook.
3. **Docs:** [`docs/review_queue_triage_export.md`](../../docs/review_queue_triage_export.md), [`docs/publication_governance_gate.md`](../../docs/publication_governance_gate.md).

No new import code was required; path exists and is documented above.

---

## 4. Non-Tg institutional lab wave and script **127**

- **Missing wave:** The historical blocker (“no final institutional extract”) is **closed** — wave label **`final_institutional_20260407`** was ingested via [`scripts/127_analyst_institutional_lab_append.py`](../../scripts/127_analyst_institutional_lab_append.py) (deterministic replace per `--ingestion-wave`). Evidence: [`studies/20260411_final_master_release/EVIDENCE_PACK.md`](../20260411_final_master_release/EVIDENCE_PACK.md), closeout memos under [`studies/20260407_institutional_lab_wave_closeout/`](../20260407_institutional_lab_wave_closeout/).
- **127 is the correct append path** for analyst-delivered institutional chemistry-style CSVs into `main.longitudinal_lab_canonical_v1` with lineage key + wave idempotency.
- **Remaining non-structural lab blockers** are **source-limited** (coverage, dedup rank edge cases, analytes not yet in extract — e.g. free T4/T3, albumin) per README scaffold references — not “run 127 again with no file.”

---

## 5. Context memos reviewed

| Artifact | Role |
|----------|------|
| [`studies/20260407_publication_signoff_live/final_verdict_memo.md`](../20260407_publication_signoff_live/final_verdict_memo.md) | Historical executive language; **banner** supersedes lab-wave blocker; governance caution still directionally right — live MRQ now **synthetic-free** but **automation-heavy**. |
| [`studies/20260409_final_master_release/`](../20260409_final_master_release/) | Superseded evidence pack vs [`studies/20260411_final_master_release/`](../20260411_final_master_release/); prefer **20260411** for row counts. |
| [`scripts/126_final_master_release.py`](../../scripts/126_final_master_release.py) | Final-master orchestration; decisions CSV + MRQ hydrate + optional 127. |
| [`scripts/127_analyst_institutional_lab_append.py`](../../scripts/127_analyst_institutional_lab_append.py) | Institutional lab append / wave replace. |

---

## 6. Executive verdict (April 2026, post structural fixes)

- **Specimen/FHIR structural blockers:** Addressed for promotability at last truth capture; monitor **Check 13** on every `138`/`143` change.
- **Technical MRQ / 119 gate:** **Not blocked** by pending rows or synthetic placeholders on this probe.
- **Manuscript governance:** **Still the primary non-structural gap** — nearly all MRQ closure is **`auto_accepted_*` tier policy**, not named human review; **2** `confirmed_correct` rows lack reviewer provenance; **promotion_review_decisions** records **batch/tier policy**, not row-level clinical review volume.
- **Institutional non-Tg labs:** **Ingested wave present**; residual limits are **analyte coverage / enrichment**, not missing **127** path.

---

## 7. Code / doc changes shipped with this rebaseline

- [`scripts/120_review_queue_triage.py`](../../scripts/120_review_queue_triage.py): added `counts_mrq_three_bucket_signoff.csv` (governance rollup; `auto_accepted*` precedes human-identity).
- [`docs/review_queue_triage_export.md`](../../docs/review_queue_triage_export.md): documents new artifact.
- [`tests/test_120_review_queue_triage.py`](../../tests/test_120_review_queue_triage.py): asserts new CSV.
- [`README.md`](../../README.md): status language separates the three lenses explicitly.
