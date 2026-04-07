# Publication governance blocker — real MRQ adjudication missing

**Supersession (2026-04-07):** Synthetic placeholder MRQ was **resolved** via tier-policy gate build + MotherDuck release **`20260407_tier`**. See [`../20260407_tier_final_master_release/RELEASE_VERDICT.md`](../20260407_tier_final_master_release/RELEASE_VERDICT.md), gate [`../20260407_tier_policy_review_gate/TIER_POLICY_BATCH_MEMO.md`](../20260407_tier_policy_review_gate/TIER_POLICY_BATCH_MEMO.md), and script `scripts/128_mrq_tier_policy_gate_build.py`. The historical analysis below remains for audit context.

---

**Historical status:** STOP — not eligible for publication-grade final master release **while** MRQ used synthetic placeholders.  
**Date (UTC):** 2026-04-07  
**Why (historical):** Manual review queue was structurally “complete” (no NULL `verification_status`) but **5620 / 5622** rows carried **`SYNTHETIC_AUTOMATION_ONLY_NOT_MANUSCRIPT_SIGNOFF`**. The append-only promotion audit table did **not** contain a substantive batch package for manuscript sign-off.

The following checklist applied **before** the tier-policy remediation:

---

## Missing or insufficient artifacts

| Requirement | Finding |
|-------------|---------|
| Reviewed gate directory with `manual_review_queue.csv` | **No checked-in replacement** for the synthetic-dominated snapshot. Closest historical hydrate (`studies/20260409_final_master_release/mrq_hydrate_gate/manual_review_queue.csv`) is still synthetic for essentially all adjudication rows. |
| `promotion_review_decisions.csv` at repo path for `--decisions-csv` | **Absent** — repository-wide search finds **zero** `promotion_review_decisions.csv` files. |
| Live `qa.promotion_review_decisions` as substantive sign-off | **Insufficient:** **2** rows total; **`decision_batch_id` NULL on 2 / 2**; **`evidence_ref` NULL or empty on 2 / 2**. Policy for publication batch requires present `decision_batch_id` and `evidence_ref` where required. |

---

## Live MotherDuck counts (preflight)

Source: direct queries against `Thyroid 2026` after `scripts/120_review_queue_triage.py --md --md-sa` (20260407 bundle under `exports/review_queue_triage_20260407_151758/`, gitignored).

**`qa.manual_review_queue` — `verification_status`**

| verification_status | n_rows |
|---------------------|-------:|
| SYNTHETIC_AUTOMATION_ONLY_NOT_MANUSCRIPT_SIGNOFF | 5,620 |
| confirmed_correct | 2 |

**`qa.promotion_review_decisions`**

| Metric | Value |
|--------|------:|
| Total rows | 2 |
| Rows with `decision_batch_id` IS NULL | 2 |
| Rows with `evidence_ref` NULL or blank | 2 |

**Acceptance failure:** Any release-signoff batch that retains **5,620** synthetic placeholder rows fails the “no synthetic-only signoff” criterion.

---

## Commands already run (this session)

Triage (use `MD_SA_TOKEN` with fallback to `MOTHERDUCK_TOKEN`; set attribution env vars before connect):

```bash
export MOTHERDUCK_CUSTOM_USER_AGENT=THYROID_2026_real_mrq_signoff/1.0
export MOTHERDUCK_SESSION_HINT=real_mrq_signoff_<UTC_YYYYMMDD_HHMM>
.venv/bin/python scripts/120_review_queue_triage.py --md --md-sa
```

---

## Next commands (unblock path)

1. **Produce a human-reviewed gate folder** containing at least `manual_review_queue.csv` (and sibling gate CSVs per `scripts/114_qa_schema_setup.py` / your promotion SOP) with:
   - Non-placeholder `verification_status` for every row that requires adjudication (no `SYNTHETIC_AUTOMATION_ONLY_NOT_MANUSCRIPT_SIGNOFF` for manuscript release).
   - `reviewer` / review timestamps per org policy wherever identity is required.

2. **Author `studies/<YYYYMMDD>_final_master_release/promotion_review_decisions.csv`** (or your chosen path) with columns aligned to `scripts/sql/114_qa_schema_ddl.sql`: every row must carry a **`decision_batch_id`** (script 126 also stamps batch from `--release-date` / `--decision-batch-id`) and **`evidence_ref`** where policy requires traceability.

3. **Re-run preflight** on the CSVs (row counts, no synthetic status, non-null required columns), then execute:

```bash
export MOTHERDUCK_CUSTOM_USER_AGENT=THYROID_2026_real_mrq_signoff/1.0
export MOTHERDUCK_SESSION_HINT=real_mrq_signoff_<UTC_YYYYMMDD_HHMM>
.venv/bin/python scripts/126_final_master_release.py --md --md-sa \
  --release-date 20260407 \
  --hydrate-mrq-from /path/to/reviewed_gate_dir \
  --decisions-csv /path/to/promotion_review_decisions.csv
.venv/bin/python scripts/119_md_formalization_validate.py --md --md-sa --release-mode
```

Replace `20260407` with the actual release tag when the reviewed package exists.

---

## References

- Prior reconciliation: [`studies/20260407_publication_signoff_live/mrq_reconciliation_memo.md`](../20260407_publication_signoff_live/mrq_reconciliation_memo.md)
- Synthetic release evidence (historical): [`studies/20260409_final_master_release/EVIDENCE_PACK.md`](../20260409_final_master_release/EVIDENCE_PACK.md)
