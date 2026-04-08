# Human review readiness (MRQ + promotion) — publication gate

**THYROID_2026 — 2026-04-08 UTC**

## Verdict: **not ready now** (publication-grade *human* sign-off)

Automation and structural gates on live MotherDuck are in good shape (no NULL `verification_status`, **no** synthetic placeholder statuses in the **live** export taken for this audit). That is **not** the same as clinician or operator row-by-row adjudication suitable for manuscript governance when policy requires it.

### Live MotherDuck (`scripts/120_review_queue_triage.py --md`)

**Bundle:** `triage_attempt/review_queue_triage_20260408_075515/`

| Metric | Value |
|--------|------:|
| Total MRQ rows | 5,622 |
| Pending (`verification_status` NULL) | 0 |
| `run_label` | `20260407_tier_policy_review_gate` |

**`verification_status` distribution** (`counts_by_verification_status.csv`):

| verification_status | n_rows |
|---------------------|-------:|
| auto_accepted_standard | 3,081 |
| auto_accepted_critical_sample_ok | 1,646 |
| auto_accepted_informational | 893 |
| confirmed_correct | 2 |

**Manuscript quality tiers** (`counts_manuscript_quality_tiers.csv` — from `scripts/120_review_queue_triage.py`):

| manuscript_quality_tier | n_rows |
|-------------------------|-------:|
| C_automation_tier_policy_only | 5,620 |
| E_reviewed_status_without_reviewer_timestamp | 2 |

**Interpretation:**

- **Tier B** (synthetic placeholder): **0 rows** on live — consistent with `README.md` / rebaseline (Check **5b** posture).
- **Tier C** (`auto_accepted_*`): **5,620 rows** — tier-policy automation; **not** human line review.
- **Tier D** (non-empty `reviewer` + `reviewed_at`): **0 rows** in this export.
- **Tier E**: **2** rows (`confirmed_correct`) without reviewer/timestamp metadata in the tier logic.

**Historical memos** (`studies/20260407_publication_signoff_live/mrq_reconciliation_memo.md`, `final_verdict_memo.md`) describe an earlier snapshot where **`SYNTHETIC_AUTOMATION_ONLY_*` dominated**. **Live triage above supersedes that histogram** for **current** MotherDuck — reconcile any narrative that still cites “5,620 synthetic” against this folder’s CSVs.

### Repo CSV packages

| Artifact | Publication `126` preflight (`assert_mrq_csv_fully_reviewed`) | Human sign-off |
|----------|----------------------------------|----------------|
| `studies/20260407_tier_policy_review_gate/manual_review_queue.csv` | **PASS** (non-blank, no synthetic token) | **No** — almost entirely `auto_accepted_*` |
| `studies/20260409_final_master_release/mrq_hydrate_gate/manual_review_queue.csv` | **FAIL** — 5,620 × `SYNTHETIC_AUTOMATION_ONLY_NOT_MANUSCRIPT_SIGNOFF` | Rehearsal / blocked |

**Hydration gate folder** for `114` / `126` must include reviewed `manual_review_queue.csv` plus companion gate files (`promotion_scorecard.csv`, `schema_validation.csv`, `concordance_summary.csv` as produced by your gate pipeline — see `scripts/126_final_master_release.py` `build_mrq_hydrate_gate_dir`).

### Promotion decisions CSV

**Only** checked-in candidate: `studies/20260407_tier_policy_review_gate/promotion_review_decisions.csv`

- **Rows:** 2 (including batch-summary style record; **not** thousands of entity-level decisions).
- **Required columns** (for `126` / `append_promotion_decisions`): `append_promotion_decisions` fills missing columns with NULL but expects to build: `review_id`, `run_label`, `llm_entity_id`, `research_id`, `domain`, `entity_type`, `algorithm_status`, `verification_status`, `reviewer`, `reviewed_at`, `waiver_reason`, `created_at`, `decision_batch_id`, `source_object_id`, `evidence_ref` (script adds `created_at` / `decision_batch_id`).
- **Substantive org sign-off:** **Insufficient** as a standalone promotion package.

---

## Operator checklist (when preparing real publication inputs)

1. **Export or build** a gate directory whose `manual_review_queue.csv` has:
   - No blank / NULL `verification_status`.
   - No `SYNTHETIC_AUTOMATION_ONLY_*` or other blocked tokens (`docs/publication_governance_gate.md`, `utils/publication_governance.py`).
   - Per org policy: non-empty `reviewer` and `reviewed_at` where human adjudication is required (targets tier **D** posture in triage).
2. **Prepare** `promotion_review_decisions.csv` with **row-level** decisions aligned to your promotion runbook (not only batch summaries), and a defined `decision_batch_id` policy (126 defaults batch id to `--release-date`).
3. **Dry-run** `126` with `--hydrate-mrq-from` and `--decisions-csv` before any live run (see §2 below).
4. **Re-run** `scripts/120_review_queue_triage.py --md` after hydrate so manuscript stakeholders see the post-sign-off histogram.
5. **Run** `scripts/119_md_formalization_validate.py --md --release-mode` per `README.md` / evidence packs.

### Required CSV schemas (MRQ + promotion)

- **MRQ:** Column names must match what `scripts/114_qa_schema_setup.py` expects for hydrate (see `docs/review_queue_triage_export.md`; aliases in `col_map` in 114). Minimum for `126` preflight: **`verification_status`** present; every row non-blank; **no** synthetic placeholder values.
- **Promotion decisions:** See `scripts/126_final_master_release.py` `append_promotion_decisions` `base_cols` list above.

### Exact commands (dry-run vs live)

**Token:** RW MotherDuck token in **`motherduck.local.toml`** (copy from `motherduck.local.toml.example`) or `MOTHERDUCK_TOKEN` / `MD_SA_TOKEN` with `--md-sa` where appropriate — never commit tokens.

**Dry-run (no DB mutations):**

```bash
cd /Users/loganglosser/THYROID_2026
.venv/bin/python scripts/126_final_master_release.py --md --release-date YYYYMMDD \
  --hydrate-mrq-from studies/<your_reviewed_gate_dir> \
  --decisions-csv studies/<your_promotion_review_decisions.csv> \
  --dry-run
```

**Live (only after explicit operator approval — not run in this audit):**

```bash
.venv/bin/python scripts/126_final_master_release.py --md --release-date YYYYMMDD \
  --hydrate-mrq-from studies/<your_reviewed_gate_dir> \
  --decisions-csv studies/<your_promotion_review_decisions.csv>
```

Add lab flags only when a **new** validated institutional CSV is ready (see `LAB_APPEND_READINESS.md`).

**Triage after changes:**

```bash
.venv/bin/python scripts/120_review_queue_triage.py --md
```

---

## What is still missing or insufficient

| Gap | Detail |
|-----|--------|
| Human-adjudicated MRQ at scale | Live queue is overwhelmingly **`auto_accepted_*` (tier C)**; **no** tier **D** rows in this export. |
| Row-level promotion package | Only **2-row** tier-policy CSV in repo; not sufficient as org promotion sign-off. |
| Governance policy | Confirm whether **`auto_accepted_*`** is acceptable for *your* manuscript release bar; if not, replace with reviewed CSV + hydrate. |

**Ready for automation / structural release checks:** yes (pending NULL = 0, synthetic slice = 0 on live).

**Ready for publication-grade *human* MRQ + promotion package:** **no.**
