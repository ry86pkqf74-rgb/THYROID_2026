# Reviewer SOP — manual review queue (release candidate)

**Applies to:** `qa.manual_review_queue` rows produced by `scripts/112_v2_domain_promotion_gate.py` and hydrated via `scripts/114_qa_schema_setup.py`.  
**Paired policy:** `MANUAL_REVIEW_PLAYBOOK.md`, `docs/domain_mapping_rules.md` § Fill-Candidate Triage Policy.

---

## 0) Principles

1. **No raw note text** in MotherDuck or cloud exports (`review_reason` stays local; use secure note viewer + `note_row_id`).
2. **Discordant rows are never batch-approved.**
3. **Second reviewer** is mandatory where the playbook says so—document identity and timestamp.
4. **Closed vocabulary only** for `verification_status` on publication paths (no synthetic placeholders).

---

## 1) Access & tools

| Need | Location / command |
|------|--------------------|
| Notes | Institutional `clinical_notes_long` (or equivalent) keyed by `note_row_id` / `research_id` |
| Worklists (no PHI) | `scripts/120_review_queue_triage.py --md` → pending-only CSV slices with truncation |
| Batch tier acceptance | `scripts/127_qa_tier_batch_adjudicate.py` (governed; QA write) |
| Hydrate after CSV edit | `scripts/114_qa_schema_setup.py --md --hydrate-from studies/v2_domain_promotion_gate_<label>` |

Use a stable `reviewer_id` convention (initials, employee ID, or ORCID—pick one per study).

---

## 2) Per-row workflow

1. **Sort:** `algorithm_status = discordant_existing` **first**, then critical-tier fill candidates (see playbook domain table).
2. **Open note:** Verify patient and encounter match queue keys; treat `note_row_id` as **opaque**—no coercion.
3. **Read context:** Minimum full sentence; prefer paragraph-level context for negation, time sequence, laterality.
4. **Decide domain truth:** Does `entity_value_norm` reflect a valid fact in **`domain`** (v2 source domain)? Is tension with v1 merely coexistence, or **mis-assignment**?
5. **Set fields:**
   - `verification_status` (rubric below)
   - `promotion_approved` (`true` / `false` / leave empty only when playbook allows)
   - `reviewer_evidence_span` if your span differs from machine `evidence_span`
   - `reason_code` for rejects, deferrals, domain mismatch
   - `reviewer_comment` when required (see playbook)
   - `reviewer_decision_at` (UTC ISO-8601) and `reviewer_id`
6. **Spot audit:** After each batch, independently review **5%** of rows marked `confirmed_correct` in that batch (playbook calibration).

---

## 3) Decision rubric (abbreviated — full table in playbook)

| `verification_status` | `promotion_approved` | When |
|------------------------|---------------------|------|
| `auto_accepted_standard` | true | Standard-tier fill batch under policy |
| `auto_accepted_informational` | true | Informational-tier batch under policy |
| `auto_accepted_critical_sample_ok` | true | Critical tier **after** documented sample gate |
| `confirmed_correct` | true | Note + span support extraction in `domain` |
| `confirmed_incorrect` | false | Negation, wrong drug vs RAI, wrong organ, hallucination |
| `source_limited` | false | Note insufficient at clinical bar |
| `not_promotable` | false | Duplicate / contamination / needs pipeline dedup |
| `defer` / `needs_second_reviewer` | false or empty | Escalation; **no promotion** until closed |

---

## 4) What may be batch-resolved vs always adjudicated

| Situation | Batch / automated? |
|-----------|-------------------|
| `existing_missing_fill_candidate` + standard tier | Yes — `127` + policy |
| `existing_missing_fill_candidate` + informational tier | Yes — `127` + policy |
| `existing_missing_fill_candidate` + critical tier | **Sample first**, then conditional batch |
| `discordant_existing` | **Never** batch-accept |
| RAI vs levothyroxine / medication triangle | Human + **second reviewer** |
| Pathology sentence vs bare gene token | Human + **second reviewer** if molecular positivity implied |
| Truncated / incoherent `evidence_span` | Human → usually `source_limited` |
| Synthetic placeholder status from `126` | **Not acceptable** for manuscript—replace with rubric |

---

## 5) Second reviewer procedure

1. Primary completes row with `needs_second_reviewer` or uses collaborative sheet flag.
2. Second reviewer independently pulls note, confirms or edits decision.
3. Final row must **not** remain in `defer` for **critical** domains for manuscript RC.
4. Record second identity (`second_reviewer_id` column locally if added, or duplicate row in `qa.promotion_review_decisions` with batch id).

---

## 6) Escalation

**Order:** primary reviewer → clinical second → study PI / data governance.

Triggers: cross-domain temporal conflict, suspected bad `comparison_token` linkage, structured v1 field vs narrative v2 field incomparable grain.

---

## 7) After review (operators)

1. Save **reviewed** `manual_review_queue.csv` in the authoritative gate study folder.
2. `114_qa_schema_setup.py --md --hydrate-from <that folder>`.
3. Re-run gate + `119 --release-mode` as required by `docs/motherduck_database_contract_v1.md`.

---

*SOP version 1.0 — clinical research defensibility first.*
