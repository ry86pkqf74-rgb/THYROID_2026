# Manual review and adjudication playbook — v2 promotion gate

**Run label:** `formalization_20260406_v3`  
**Scope:** Human adjudication for [`manual_review_queue.csv`](manual_review_queue.csv) before final data release.  
**Related:** [`promotion_recommendation.md`](promotion_recommendation.md), [`docs/motherduck_database_contract_v1.md`](../../docs/motherduck_database_contract_v1.md), [`AGENTS.md`](../../AGENTS.md).

---

## Artifact inventory

| File | Purpose |
|------|---------|
| [`manual_review_queue.csv`](manual_review_queue.csv) | Working queue with reviewer-editable columns (5,622 rows). |
| [`manual_review_queue__presignoff_snapshot_20260406.csv`](manual_review_queue__presignoff_snapshot_20260406.csv) | **Immutable** copy of the queue **before** review columns were added; do not edit. |
| [`promotion_scorecard.csv`](promotion_scorecard.csv) | Gate results for this run. |
| [`manifest.json`](manifest.json) | Run metadata; refresh after final gate re-run. |

**Queue composition (this run):** 5,620 `existing_missing_fill_candidate`, 2 `discordant_existing` (both `rad_treatment` vs `medications` / thyroid hormone suppression; `original_value` carries structured RAI metadata while `llm_value` is levothyroxine narrative).

---

## Reviewer SOP (`manual_review_queue`)

### Prerequisites

- **Local** access to clinical notes (e.g. `clinical_notes_long`) keyed by the same `note_row_id` representation as the CSV. **Do not** pull raw note bodies into MotherDuck ([PHI boundary](../../docs/motherduck_database_contract_v1.md)).
- Stable **`reviewer_id`** (string; initials, staff ID, or ORCID — pick one convention for the study).
- Working copy of [`manual_review_queue.csv`](manual_review_queue.csv); treat [`manual_review_queue__presignoff_snapshot_20260406.csv`](manual_review_queue__presignoff_snapshot_20260406.csv) as read-only audit baseline.

### `note_row_id` join note

The contract document describes `note_row_id` as `BIGINT` in some places; **this CSV uses opaque identifiers (e.g. hash-like strings)**. Use the queue value **verbatim** for lookups and QA joins; do not coerce IDs during review.

### Per-row workflow

1. **Triage:** Filter `algorithm_comparison_status`. Complete **`discordant_existing`** rows first, then batch by domain (see [Prioritization](#domain-prioritization)).
2. **Locate evidence:** Open the note with `note_row_id` and `research_id`; flag inconsistencies between them.
3. **Read in context:** Expand around `evidence_span` (minimum: full sentence; preferred: full clinically relevant paragraph). Confirm support for `entity_type` and **`source_domain`** (not keyword proximity alone).
4. **Cross-domain interpretation:** For fill candidates, decide whether `llm_value` is a **valid** fact in `source_domain` that legitimately coexists with a v1 token in `comparison_domain`, versus **mis-assignment** (e.g. levothyroxine paired with RAI structured fields, imaging language stuffed into wrong domain).
5. **Decide:** Set `verification_status` and `promotion_approved` per the [rubric](#decision-rubric).
6. **Audit fields:** Fill [Evidence requirements](#evidence-requirements) on every decided row. If the authoritative span differs from `evidence_span`, put it in `reviewer_evidence_span` (leave machine `evidence_span` unchanged).
7. **Batch QC:** After each batch, independently **spot-audit 5%** of rows marked `confirmed_correct` in that batch for calibration drift.

### Forbidden shortcuts

- Do not approve from `evidence_span` alone when **negation**, **temporal ordering**, or **laterality** matters — read local context.
- Do not treat `original_value` as ground truth when it is **structurally mismatched** to `llm_value` (the two discordant rows are the canonical example). Adjudicate **clinical meaning in the note**.

---

## Domain prioritization

Order by **analytic criticality** (QA tier **critical** on the v2 promotion inventory) × **volume** × **collision risk** (RAI vs thyroid hormone, pathology vs staging/genetics).

### Review first (sequential)

| Order | `source_domain` | Rows (approx.) | Rationale |
|------|-----------------|----------------|-----------|
| 0 | `rad_treatment` | 16 + **2 discordant** | Template for **RAI vs levothyroxine**; defines rubric before scale-up. |
| 1 | `pathology` | 683 | Critical tier; heavy `staging` / `genetics` / `procedures` linkage. |
| 2 | `vascular_invasion` | 370 | Critical tier; `operative_detail` + `staging`. |
| 3 | `rai_detailed` | 564 | Critical tier; large `medications` / `procedures` volume — same failure mode as discordant template. |
| 4 | `recurrence` | 22 | Critical tier; outcome-analytic. |
| 5 | `cervical_ln_detail` | 37 | Pathology-adjacent; mostly `procedures`. |

### Batch-review (shared rubric)

| Batch (`source_domain` → `comparison_domain`) | Rows (approx.) | Notes |
|-----------------------------------------------|----------------|-------|
| `airway_invasion` → `operative_detail` | 1,233 | Dominated by `ete_on_imaging`, `tracheal_deviation`, `mass_effect`, … |
| `airway_invasion` → `staging` | 704 | Staging semantics checklist. |
| `pathology` → `staging` | 500 | Repetitive path-language vs staging token. |
| `imaging` → `operative_detail` / `procedures` | 275 + 212 | Mass effect / negation rules. |
| `survival_followup` → `genetics` / `complications` | 239 + 37 | Longitudinal vs molecular/complication tokens. |
| `functional_outcomes` → `procedures` / `complications` | 191 + 107 | Functional wording vs tokens. |
| `physical_exam` → `operative_detail` / `procedures` | 171 + 50 | Exam vs operative/procedure. |
| `past_surgical_hx` → `procedures` / `medications` | 119 + 45 | History vs lists. |

### Mark `source_limited` explicitly

- **Tail / sparse domains:** `synoptic_pathology_enrichment` (7), `presenting_symptoms` (6), `parathyroid_detail` (2), `frozen_section_detail` (2), `tirads_granular` (1), `dynamic_risk_response` (1).
- **Any row** where `evidence_span` is truncated mid-thought or lacks the predicate for `entity_type`.

### High duplicate-rate domains

Scorecard flags **>5%** duplicate rate for `labs`, `tg_kinetics`, `cervical_ln_detail`, `patient_decision_adherence`. Prefer `not_promotable` or `confirmed_incorrect` when duplicates encode contradictory copies until promotion-time deduplication is applied.

---

## Pilot procedure (before full scale)

1. **Discordant pair:** Adjudicate both `discordant_existing` `rad_treatment` rows with **two reviewers**; document the **RAI vs thyroid hormone** decision rule in `reviewer_comment` / `reason_code`.
2. **RAI / medication slice:** Review a **fixed sample** (e.g. 50 rows) of `rai_detailed` where `comparison_domain = medications`; reconcile procedures with the pilot rule.
3. **Scale check:** Review **100** `airway_invasion` → `operative_detail` rows; measure inter-reviewer agreement or self-consistency; adjust rubric if agreement &lt; preset threshold.
4. **Proceed** to full batches only after steps 1–3 are signed off by the study lead.

---

## Decision rubric

| `verification_status` | When to use | `promotion_approved` |
|------------------------|-------------|----------------------|
| `confirmed_correct` | Context + span **unambiguously** support `llm_value` as `entity_type` in `source_domain`; tension with `comparison_domain` / `original_value` is **explainable overlap**. | `true` |
| `confirmed_incorrect` | Wrong extraction (negation, laterality, hallucination, **wrong drug vs RAI**, wrong organ) or **domain mis-assignment**. | `false` |
| `source_limited` | Ambiguous wording, insufficient span, or note lacks support at clinical standard; do not promote. | `false` |
| `not_promotable` | Duplicate, internal inconsistency with sibling rows, or known pipeline contamination pre-dedup. | `false` |
| `defer` / `needs_second_reviewer` | Expert disagreement; cross-domain conflict unresolved; invalid `comparison_token` mapping suspected. | `false` or empty until second sign-off |

### Second reviewer required

- Every **`discordant_existing`** row.
- Any row in the **`rai_detailed` / `rad_treatment` / `medications`** triangle.
- Any **`pathology` ↔ `genetics`** fill where `llm_value` could imply molecular positivity (**bare gene name ≠ positive test** per [`AGENTS.md`](../../AGENTS.md)).

---

## Evidence requirements

Record on **every** adjudicated row:

| Field | Rule |
|-------|------|
| `note_row_id` | Verbatim from queue. |
| `evidence_span` | Machine span; never silently overwritten in the snapshot. |
| `reviewer_evidence_span` | Required when reviewer relies on a **different** authoritative span. |
| `source_domain` | From queue (join key). |
| `reviewer_id` | Non-null at decision time. |
| `reviewer_decision_at` | UTC ISO-8601 (e.g. `2026-04-07T18:30:00Z`). |
| `verification_status` | Closed vocabulary above. |
| `promotion_approved` | `true` / `false` / empty per rubric. |
| `reviewer_comment` | Required for `defer`, `not_promotable`, `source_limited`, `confirmed_incorrect`. |
| `reason_code` | Optional compact machine tag (`NEGATION_ERROR`, `DOMAIN_MISMATCH`, `RAI_VS_THYROID_HORMONE`, `CONFLICT_WITH_V1_TOKEN`, …). |

**Reproducibility:** A third party can open the same `note_row_id` locally and find the same span supporting the label.

### Optional second review fields

If policy requires, add columns `second_reviewer_id`, `second_reviewed_at` locally (not required by gate scripts); document in commit message or supplementary README if introduced.

---

## Escalation rules

**Cross-domain conflicts:** If `source_domain` **contradicts** canonical or promoted meaning in `comparison_domain` for the **same encounter window**, use `defer`, `reason_code=CONFLICT_WITH_V1_TOKEN`, and escalate — do not `confirmed_correct` without second reviewer + **documented temporal anchor**.

**Multi-source disagreement:** Structured `original_value` (e.g. RAI dose) vs narrative `llm_value` (levothyroxine): **default** `confirmed_incorrect` for the LLM row unless the note clearly supports both and the extraction is not asserting equivalence — then `defer` (ontology) vs `source_limited` (wrong grain).

**Hierarchy:** primary reviewer → second reviewer (clinical senior) → study PI / data governance for cohort-defining facts (pathology, staging, RAI receipt, recurrence).

---

## Hydrating MotherDuck `qa.manual_review_queue`

After updating the CSV, apply DDL and load the reviewed queue (from repo root):

```bash
.venv/bin/python scripts/114_qa_schema_setup.py --md \
  --hydrate-from studies/v2_domain_promotion_gate_formalization_20260406_v3
```

[`114_qa_schema_setup.py`](../../scripts/114_qa_schema_setup.py) maps `llm_value` → `entity_value_norm`, `comparison_domain` → `domain`, `reviewer_id` → `reviewer`, `reviewer_decision_at` → `reviewed_at`, and persists `promotion_approved`, `reviewer_evidence_span`, `reviewer_comment`, `reason_code` when present.

---

## Gate re-run and release readiness

### Implementation note (G7 vs clinical policy)

[`112_v2_domain_promotion_gate.py`](../../scripts/112_v2_domain_promotion_gate.py) **G7** fails only on **same-domain** `discordant_existing` rows; **cross-domain** discordance is waived in the gate detail string. [`promotion_recommendation.md`](promotion_recommendation.md) nonetheless states strict manual sign-off for discordant rows. **For research release:** adjudicate **all** discordant rows (including cross-domain) to `confirmed_correct` or `confirmed_incorrect` with required second review — do not rely on the gate waiver as a substitute for clinical audit.

### Commands after review is complete

From [`promotion_recommendation.md`](promotion_recommendation.md) and [`motherduck_database_contract_v1.md`](../../docs/motherduck_database_contract_v1.md):

```bash
# Re-run gate (writes a new studies/v2_domain_promotion_gate_<label>/ unless --output-dir set)
.venv/bin/python scripts/112_v2_domain_promotion_gate.py \
  --v2-parquets-dir processed/output/v2_parquets \
  --db-path thyroid_master.duckdb \
  --motherduck-check \
  --run-label formalization_post_review_$(date +%Y%m%d_%H%M)

# If preserving edited CSV: merge adjudications into the new run output before hydrating QA
# (today the gate regenerates manual_review_queue from side-by-side; keep this playbook CSV
#  as the sign-off source of truth and copy/reconcile verification_* into the new folder as policy dictates).

# Hydrate QA from the gate directory that holds the **final** reviewed CSV
.venv/bin/python scripts/114_qa_schema_setup.py --md --hydrate-from studies/v2_domain_promotion_gate_<final_label>
```

**Reconcile policy:** Decide explicitly whether the **next** gate run **overwrites** `manual_review_queue.csv` or whether operations **merge** reviewer columns from this study folder into the new output. Until automation exists, treat **[`manual_review_queue.csv`](manual_review_queue.csv) in this folder** as the authoritative human decisions and merge forward manually or via a tracked script in a follow-on task.

### “Good enough” for final release

1. **Discordant:** Zero rows left without `confirmed_correct` or `confirmed_incorrect` (with mandated second review).
2. **Fill candidates:** For each `source_domain` with ≥100 queued rows, either **100%** reviewed or a **pre-specified stratified sample** by `entity_type` (minimum **n ≥ 200** or **≥5%** of domain queue, whichever is larger) with **zero** critical-tier errors in sample; expand if any found.
3. **Residual `defer`:** None for **critical** `source_domain`s; informational domains ≤2 `defer` or PI waiver in release notes.
4. **Artifacts:** Frozen reviewed CSV + calibration memo (top 3 failure modes) + **8/8 PASS** scorecard on the promoting run + `release_manifest` reference ([contract §2.3 / §10](../../docs/motherduck_database_contract_v1.md)).

---

## Column glossary ([`manual_review_queue.csv`](manual_review_queue.csv))

| Column | Description |
|--------|-------------|
| `llm_entity_id` | Stable entity identifier from extraction. |
| `research_id` | Patient key. |
| `note_row_id` | Note join key (opaque in this run). |
| `source_domain` | v2 domain of the LLM extraction. |
| `comparison_domain` | v1 comparison domain for concordance. |
| `comparison_token` | Token driving comparison. |
| `entity_type` | Domain-specific type. |
| `llm_value` | Normalized / primary LLM value string. |
| `original_value` | v1 / structured preview (truncated). |
| `evidence_span` | Length-capped snippet (PHI-safe export). |
| `algorithm_comparison_status` | `discordant_existing` or `existing_missing_fill_candidate`. |
| `original_source_link` | Trace to v1 source when present. |
| `verification_status` | Reviewer decision (rubric). |
| `promotion_approved` | `true` / `false` / empty. |
| `reviewer_evidence_span` | Reviewer’s span if different from `evidence_span`. |
| `reviewer_id` | Reviewer identity. |
| `reviewer_decision_at` | UTC timestamp. |
| `reviewer_comment` | Free text / structured rationale. |
| `reason_code` | Compact categorical tag. |

---

*Playbook version 1.0 — aligned with gate run `formalization_20260406_v3`.*
