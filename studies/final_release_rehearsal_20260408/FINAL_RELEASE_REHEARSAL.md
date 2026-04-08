# Final release rehearsal — THYROID_2026

**Study folder:** `studies/final_release_rehearsal_20260408/`  
**TAG:** `20260408`  
**Git SHA:** see `preflight_probe.txt`

## Shared session header (this run)

- `MOTHERDUCK_CUSTOM_USER_AGENT=thyroid_final_release/20260408/<UTC>`  
- `MOTHERDUCK_SESSION_HINT=thyroid_final_release_20260408`  
- `MD_READ_SCALING_SESSION_HINT=thyroid_final_release_ro_20260408`

## Local token fix

`motherduck.local.toml` was **missing**; it was **created** from `.streamlit/secrets.toml` for keys present there (`MOTHERDUCK_TOKEN`, `MD_READ_SCALING_TOKEN`). **`MD_SA_TOKEN` was not present** — RW token resolved as `env:MOTHERDUCK_TOKEN` in the probe. Add `MD_SA_TOKEN` to TOML for automation preference (see `motherduck.local.toml.example`).

## Inputs used

| Input | Path |
|--------|------|
| MRQ hydrate gate | `studies/20260407_tier_policy_review_gate/` |
| Promotion decisions | `studies/20260407_tier_policy_review_gate/promotion_review_decisions.csv` |
| Lab CSV | *none* (no `final_institutional_*` file located) |

## Commands executed

```bash
# 1) QA strict validation
.venv/bin/python scripts/119_md_formalization_validate.py --md --md-env qa --release-mode \
  --output-dir studies/final_release_rehearsal_20260408/qa_release_mode

# 2) Final master dry-run
.venv/bin/python scripts/126_final_master_release.py --md --release-date 20260408 \
  --hydrate-mrq-from studies/20260407_tier_policy_review_gate \
  --decisions-csv studies/20260407_tier_policy_review_gate/promotion_review_decisions.csv \
  --dry-run

# 3) Molecular release workflow (rehearsal — no --execute)
.venv/bin/python scripts/137_md_molecular_release_workflow.py promote --tag 20260408 \
  --output-dir studies/final_release_rehearsal_20260408
# (Long-running subprocess interrupted after duplicate 119; see 137_promote_rehearsal.log)
```

## Results summary

| Question | Answer |
|----------|--------|
| Final-master **dry-run** preflight (126) | **PASS** — MRQ/decisions CSV checks OK |
| QA **119 release-mode** | **FAIL** — 5 checks (blockers) |
| Rollback handle on prod | Zero-copy **PrePromote** DB clone via **130** (SQL printed in 137 log; live requires `--execute`) |
| Read-scaling readers need refresh? | **Yes** after any real prod promote — **136 reader**; use `MD_READ_SCALING_SESSION_HINT` with read-scaling token |
| Blockers remain? | **Yes** — see `blocker_checklist.md` |

## Artifacts

| File | Purpose |
|------|---------|
| `preflight_probe.txt` | Token modes, DB map, git SHA, session env |
| `blocker_checklist.md` | Structured QA failures |
| `119_qa_release_mode.log` | Full 119 stdout |
| `126_final_master_dryrun.log` | 126 dry-run |
| `137_promote_rehearsal.log` | Partial promote rehearsal |
| `qa_release_mode/validation_report.md` | 119 markdown report |

## Strict verdict

**HOLD** — QA release-mode blocked; live path not executed (`PROCEED_PROD_WRITE` absent).

**Next commands:** see `GO_NO_GO.md` § “If you proceed to GO later”.
