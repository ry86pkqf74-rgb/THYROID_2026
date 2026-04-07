# MRQ preflight — real human signoff (FAIL CLOSED)

**UTC date:** 2026-04-07  
**Verdict:** **BLOCKED (Branch A)** — do **not** run `scripts/126_final_master_release.py` for “manuscript human signoff” until the gaps below close.

## 1) Candidate packages discovered

| Path | `manual_review_queue.csv` | `promotion_review_decisions.csv` | Notes |
|------|---------------------------|-----------------------------------|--------|
| `studies/20260407_tier_policy_review_gate/` | Yes (5,622 rows) | Yes (**1** data row + header) | Produced by `scripts/128_mrq_tier_policy_gate_build.py` from synthetic-dominated hydrate gate |
| `studies/v2_domain_promotion_gate_formalization_20260406_v3/` | Yes | No | **5,620** rows with **empty** `verification_status` — incomplete |
| Other `studies/*/manual_review_queue.csv` | Many audits/dryruns | **No** other `promotion_review_decisions.csv` in repo (glob) | Mostly **all-null** `verification_status` or not a reviewed adjudication package |

**Latest checked-in “final master” evidence** still documents synthetic MRQ fill: `studies/20260409_final_master_release/EVIDENCE_PACK.md`.

## 2) Preflight vs policy (repo SSOT)

Authoritative fill-candidate + discordance policy: `docs/domain_mapping_rules.md` § *Fill-Candidate Triage Policy*.

### 2.1 Non-null `verification_status`

On `studies/20260407_tier_policy_review_gate/manual_review_queue.csv`:

- **5,622 / 5,622** rows have non-null `verification_status`.
- Breakdown: `auto_accepted_standard` 3,081; `auto_accepted_critical_sample_ok` 1,646; `auto_accepted_informational` 893; `confirmed_correct` **2**.

### 2.2 Discordant rows (must be individually adjudicated)

- `algorithm_comparison_status = discordant_existing`: **2** rows.
- Both carry `verification_status = confirmed_correct` — **acceptable for those two rows**.

### 2.3 Critical-tier fill candidates — **FAIL**

Policy text (abridged): for **critical** QA tier fill candidates, require **sample 10% (min 20 rows)** manual spot-check and **document the sample in `qa.promotion_review_decisions`**.

Facts:

- Critical-tier fill rows in this gate: **1,646** (`auto_accepted_critical_sample_ok`).
- Repo `promotion_review_decisions.csv` contains **one** summary row (`domain = _tier_batch_summary`) plus memo path in `evidence_ref` — **no** per-row or explicit sample manifest for the critical tier.
- **Conclusion:** package is **incomplete** for org-approved “real signoff” under the written policy, even though statuses are non-synthetic.

### 2.4 “Real human-reviewed signoff” vs automation

- **5,620** rows were bulk-rewritten by `scripts/128_mrq_tier_policy_gate_build.py` to `auto_accepted_*`, with `reviewer_id = tier_policy_governance_20260407` on fill-candidate rows.
- That path is **tier automation / governance batching**, not row-by-row clinician or analyst adjudication.
- Task requirement: **do not pass off synthetic-only or automation-only bulk acceptance as manuscript human review.** Keeping `auto_accepted_*` without the **critical sample + promotion_review_decisions documentation** is still **overclaiming** relative to `docs/domain_mapping_rules.md`.

### 2.5 `decision_batch_id` / `evidence_ref`

- CSV omits `decision_batch_id`; `scripts/126_final_master_release.py` stamps it at insert time — OK mechanically.
- `evidence_ref` present on the summary row — **insufficient** vs critical-tier sampling rule above.

## 3) Live MotherDuck (`qa.promotion_review_decisions`)

Executed with RW token resolved via `motherduck_client.get_token(prefer_service_account=True)` (no token values logged); user agent `THYROID_2026_real_mrq_signoff/1.0`.

- **Row count:** 3  
- **Distinct `decision_batch_id`:** 2  
- Latest batches include `20260407_tier_policy` (`policy_batch_adjudication`, reviewer `tier_policy_governance_20260407`) and legacy `legacy_rc_tier_20260407` rows.

So an **append-only decision history exists**, but it is **thin** (counts above) and does **not** evidence critical-tier spot-check rows.

## 4) Branch selection

- **Branch A** — **STOP**; final release **not** executed.
- **Branch B** prerequisites **not** met:** human-complete package** + **critical policy documentation** + **single coherent MRQ snapshot** (see `mrq_reconciliation.md`).

## 5) Next command to run (operator)

1. For **critical** tier: perform the **10% / min 20** spot-check, record outcomes in **`promotion_review_decisions.csv`** (one row per sampled entity or an explicit manifest joined to `evidence_ref`), **or** adjust policy in writing and get sign-off (outside this automation).

2. Reconcile **duplicate** `run_label` rows on live `qa.manual_review_queue` (see reconciliation memo) before any hydrate that claims a single gate truth.

3. When the package is actually complete:

```bash
export MOTHERDUCK_CUSTOM_USER_AGENT=THYROID_2026_real_mrq_signoff/1.0
export MOTHERDUCK_SESSION_HINT=real_mrq_signoff_$(date -u +%Y%m%d_%H%M)
# Prefer MD_SA_TOKEN in env or .streamlit/secrets.toml
.venv/bin/python scripts/126_final_master_release.py --md --md-sa \
  --release-date YYYYMMDD \
  --hydrate-mrq-from studies/<your_reviewed_gate_dir> \
  --decision-batch-id <explicit_batch> \
  --decisions-csv studies/<your_reviewed_gate_dir>/promotion_review_decisions.csv
# Do NOT pass --synthetic-fill-mrq-verification for publication.
```

Then:

```bash
.venv/bin/python scripts/119_md_formalization_validate.py --md --md-sa --release-mode
```
