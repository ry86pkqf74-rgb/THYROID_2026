# Recommended publish order — 2026-04-08 validation / CI alignment

**Context:** After `git fetch` and `git pull`, **local `main`** and **`origin/main`** both point to **`ad06752`**. There is **nothing to publish** from this clone for the listed 2026-04-08 deliverables — they already exist on **`origin/main`**.

## Order (conceptual; already satisfied on `main`)

1. **CI wiring / offline validation execution (item-2 style)**  
   - **Commit:** `28005b5` — `ci: expand validation execution coverage without new framework`  
   - **Where it lives in CI:** job `validation-contracts-offline` in `.github/workflows/ci.yml` (pytest bundle for script 29 lab SQL + registry + lab canonical + linkage confidence tests).  
   - **Status:** Already merged on `main`; **no follow-up push required** for presence.

2. **Data contract gate (script 145) — optional product emphasis**  
   - **Commit lineage:** e.g. `6892495` (YAML data contract gate + hash-chained audit) and follow-ups.  
   - **Where it lives in CI:** job `data-contract-gate-offline` running `tests/test_data_contract_gate.py`.  
   - **Status:** Already on `main`; **publish “second” only if** you want a **release note** or **tag** to highlight 145 — not required for Git sync.

3. **Validator architecture**  
   - **Recommendation:** **No new validator rewrite** — consistent with the existing split: offline pytest contracts + optional MotherDuck `29_validation_engine.py --md` via manual dispatch.

## If a future clone shows drift

- If `origin/main` **lags** local commits: **push** local `main` (or open a PR if branch protection blocks direct push).  
- If **local** lags: **pull** / **merge** from `origin/main` before adding new work.  
- Do **not** duplicate framework rewrites; extend **offline tests** and **contract YAML** instead.
