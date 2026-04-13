# Publication governance gate (MRQ + promotion decisions)

This document defines **what must be true for publication / release-signoff** versus **what is allowed for rehearsal**, and where those rules are enforced in code.

## Modes

| Mode | Typical command | `qa.manual_review_queue` | `qa.promotion_review_decisions` |
|------|-----------------|--------------------------|----------------------------------|
| **Publication / release-signoff** | `119_md_formalization_validate.py --md --release-mode` and `126_final_master_release.py` with default `--release-mode` | Every row has non-null, non-blank `verification_status` **and** no synthetic-placeholder status (see below). | If the table has any rows, every row must have non-null, non-blank `decision_batch_id`. |
| **Structural / rehearsal** | `119` without `--release-mode`; `126` with `--no-release-mode`; optional `--synthetic-fill-mrq-verification` on **126** for local rehearsal | Synthetic placeholders may exist on the DB; 119 does not fail on them. Older rows with NULL `verification_status` still show as pending in CHECK 5 but do not fail the run. | Missing `decision_batch_id` on historical rows is not audited in structural mode. |

**126 rehearsal with synthetic fill:** `--synthetic-fill-mrq-verification` is **incompatible** with `--release-mode`. For a rehearsal hydrate, pass `--no-release-mode` so subprocess 119 runs structurally only.

## Blocked synthetic / automation-only `verification_status` values (publication only)

Publication-mode checks reject rows whose `verification_status` matches any of the following (case-insensitive; whitespace trimmed):

1. **Exact:** `SYNTHETIC_AUTOMATION_ONLY_NOT_MANUSCRIPT_SIGNOFF` (canonical placeholder from `scripts/128_mrq_tier_policy_gate_build.py`).
2. **Substring class (equivalent placeholders):** if the normalized value contains any of:
   - `synthetic_automation_only`
   - `not_manuscript_signoff`
   - `automation_only_not_manuscript`

Legitimate reviewer outcomes such as `confirmed_correct`, `confirmed_incorrect`, or tier-policy `auto_accepted_*` values from **128** are **not** in this list.

**Implementation:** `utils/publication_governance.py` (Python helpers + SQL fragments), `scripts/119_md_formalization_validate.py` CHECK **5b** (strict only), `scripts/126_final_master_release.py` CSV preflight when `--release-mode` hydrates MRQ, `scripts/124_md_live_release_audit.py` when `--final-release` is set.

**Historical note:** CHECK 5 previously treated any non-NULL `verification_status` as “reviewed,” so automation placeholders could pass. CHECK 5b closes that gap for `--release-mode`.

## `decision_batch_id` provenance (publication only)

When `qa.promotion_review_decisions` has **at least one row**, release-mode validation **fails** if any row has NULL or blank `decision_batch_id`.

**Implementation:** CHECK 5b in `119_md_formalization_validate.py`; same SQL in `124_md_live_release_audit.py --final-release`. **126** sets `decision_batch_id` when appending from CSV (`append_promotion_decisions`); operators should use `--decision-batch-id` or rely on the default (release date).

## Operator quick reference

- **Publication:** Human-reviewed `manual_review_queue.csv` (no synthetic placeholder column values). Run **128** if the gate still contains `SYNTHETIC_AUTOMATION_ONLY_NOT_MANUSCRIPT_SIGNOFF`. Use **126** with `--release-mode` (default) and real decisions CSV / batch id.
- **Rehearsal:** `126 --synthetic-fill-mrq-verification '<status>' --no-release-mode` or structural-only `119` without `--release-mode`.
- **Dry-run:** `126 --dry-run` does not mutate MotherDuck; the synthetic/release incompatibility is enforced before dry-run completes — fix flags and re-run.

## External governance blockers (repo state)

Publication and manuscript **release-mode** sign-off require a **non-empty `true_human_reviewed`** bucket in `qa.manual_review_queue` exports (see `scripts/120_review_queue_triage.py` → `counts_mrq_three_bucket_signoff.csv`) **or** an **explicit publication-policy waiver** recorded out-of-band. **`auto_accepted_*`** and other **automation-only** `verification_status` values are **not** treated as human manuscript sign-off.

Separately, **open** rows on **`qa.specimen_genomic_link_review_v1`** (see `scripts/151_specimen_genomic_review_queue_export.py` and `qa.v_diag_specimen_review_burden_v1`) still impose **specimen–genomic linkage review burden** until burned down or **documented as waived** under org policy.

Historical reviewed CSVs may exist under `studies/**/manual_review_queue.csv`; **additive hydration** into MotherDuck remains an **operator action** (`114` / `126`) with RW credentials — not implied by files living in git alone.

## Related docs

- [`motherduck_release_runbook_v2.md`](motherduck_release_runbook_v2.md) — validation checklist.
- [`release_runbook.md`](release_runbook.md) — molecular / QA promotion path.
