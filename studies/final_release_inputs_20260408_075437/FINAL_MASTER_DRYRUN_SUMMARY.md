# Final master release — dry-run summary (`scripts/126_final_master_release.py`)

**THYROID_2026 — 2026-04-08 UTC** — **no live MotherDuck mutations** in this audit.

## Commands executed

### A. Minimal dry-run (no hydrate / no lab)

```text
.venv/bin/python scripts/126_final_master_release.py --md --release-date 20260408 --dry-run
```

**Result:** Exit **0**. Preflight OK; notes missing `--hydrate-mrq-from`.

### B. Full argument dry-run (tier-policy *structural* MRQ + decisions + lab paths)

```text
.venv/bin/python scripts/126_final_master_release.py --md --release-date 20260408 \
  --hydrate-mrq-from studies/20260407_tier_policy_review_gate \
  --decisions-csv studies/20260407_tier_policy_review_gate/promotion_review_decisions.csv \
  --lab-csv exports/incoming/final_institutional_chemistry_20260407.csv \
  --ingestion-wave final_institutional_20260407 \
  --dry-run
```

**Result:** Exit **0**.  
**MRQ:** `[preflight] manual_review_queue fully reviewed: 5,622 row(s)` — passes `assert_mrq_csv_fully_reviewed` (non-blank, **no** blocked synthetic tokens).  
**Caveat:** This CSV is **tier-policy automation**, not publication **human** sign-off — see `HUMAN_REVIEW_READINESS.md`.

### C. Synthetic rehearsal gate (expected failure)

```text
.venv/bin/python scripts/126_final_master_release.py --md --release-date 20260408 \
  --hydrate-mrq-from studies/20260409_final_master_release/mrq_hydrate_gate \
  --dry-run
```

**Result:** Exit **1**.  
**Message:** `FATAL: 5,620 manual_review_queue row(s) use publication-blocked synthetic verification_status`

This confirms **`--release-mode` / publication preflight** correctly rejects the rehearsal hydrate folder.

## Related: institutional lab frame validation

```text
.venv/bin/python scripts/127_analyst_institutional_lab_append.py \
  --input exports/incoming/final_institutional_chemistry_20260407.csv \
  --ingestion-wave final_institutional_20260407 \
  --dry-run
```

**Result:** `Prepared 989 lab row(s)` — CSV contract OK.

## Live `120` triage (read-only)

```text
.venv/bin/python scripts/120_review_queue_triage.py --md \
  --output-root studies/final_release_inputs_20260408_075437/triage_attempt
```

**Result:** Bundle `review_queue_triage_20260408_075515/` — MotherDuck connected (`md:Thyroid 2026`); totals in `HUMAN_REVIEW_READINESS.md`.

## When inputs are truly ready

Use **B**’s shape with **your** reviewed gate directory and **substantive** `promotion_review_decisions.csv`, then remove `--dry-run` only under explicit release approval.
