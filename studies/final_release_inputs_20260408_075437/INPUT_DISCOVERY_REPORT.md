# Input discovery report — publication final release

**THYROID_2026 — audit captured 2026-04-08T07:54–07:55 UTC**

## Summary

| Input class | Candidates found | Best current path (this audit) |
|-------------|------------------|--------------------------------|
| `manual_review_queue*.csv` | **27** files under `studies/**` (`find studies -name 'manual_review_queue*.csv'`) | **Structural preflight only:** `studies/20260407_tier_policy_review_gate/manual_review_queue.csv`. **Live SSOT:** MotherDuck `qa.manual_review_queue` + triage bundle in `triage_attempt/review_queue_triage_20260408_075515/`. |
| `promotion_review_decisions.csv` | 1 substantive file (+ 1 count artifact) | `studies/20260407_tier_policy_review_gate/promotion_review_decisions.csv` (**2 rows** — not row-level org sign-off). |
| Institutional lab CSV | 1 under `exports/incoming/` | `exports/incoming/final_institutional_chemistry_20260407.csv` |

## 1. Search methodology

- Glob: `**/manual_review_queue*.csv`, `**/promotion_review_decisions*.csv`, `**/*institutional*lab*.csv`
- Programmatic scan: all `manual_review_queue.csv` paths were scored for `verification_status` blanks, synthetic placeholders (`utils.publication_governance.is_mrq_synthetic_placeholder_verification_status`), and recency (`mtime`).

## 2. All `manual_review_queue.csv` paths (repo)

**Count:** 27 paths matching `manual_review_queue*.csv` (2026-04-08).

Regenerate the exact list:

```bash
cd /Users/loganglosser/THYROID_2026 && find studies -name 'manual_review_queue*.csv' | sort
```

**Note:** Many copies are **pending snapshots** (all `verification_status` null), **synthetic-filled** rehearsal gates (e.g. `studies/20260409_final_master_release/mrq_hydrate_gate/`), or historical dry-run exports — unsuitable for `--release-mode` publication hydrate without replacement.

## 3. `promotion_review_decisions.csv`

| Path | Notes |
|------|--------|
| `studies/20260407_tier_policy_review_gate/promotion_review_decisions.csv` | Only checked-in **decisions-shaped** CSV; **2 rows** (includes tier-policy batch summary style). |
| `studies/live_state_refresh_20260408_074310/promotion_review_decisions_count.csv` | Aggregate counts, not a decisions upload file. |

## 4. Institutional lab CSV

| Path | Rows (127 dry-run) |
|------|---------------------|
| `exports/incoming/final_institutional_chemistry_20260407.csv` | **989** rows prepared (see `FINAL_MASTER_DRYRUN_SUMMARY.md`). |

No other `*institutional*lab*.csv` matched the glob.

## 5. Artifacts saved in this folder

- `triage_attempt/review_queue_triage_20260408_075515/` — live MotherDuck triage from `scripts/120_review_queue_triage.py --md` (token resolved via `motherduck.local.toml` / env per `motherduck_client.py`).
