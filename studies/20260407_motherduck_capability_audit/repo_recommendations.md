# Repo recommendations — MotherDuck capability audit (2026-04-07)

1. **Makefile vs `secrets.toml`:** `make md-smoke`, `md-v2-gate-md-dryrun`, and `md-live-release-dryrun` require `MOTHERDUCK_TOKEN` or `MD_SA_TOKEN` in the **process environment** (`Makefile` `check_md_rw_token`). Python scripts resolve RW tokens from `.streamlit/secrets.toml`, so operators who only use the secrets file will see **false-negative “no token”** from Make even though `scripts/smoke_test_md_connection.py --md` works. **Recommendation:** document that Make users must `export` RW tokens or `set -a; source .env.motherduck; set +a` where `.env.motherduck` defines them; or extend the Makefile to optionally probe `secrets.toml` (careful: keep secrets off logs).

2. **`md-live-release-dryrun` / 115 tag collision:** `make md-live-release-dryrun` defaults `MD_RELEASE_TAG` to `date -u +%Y%m%d`. If `release_<tag>` already exists on MotherDuck, step 115 fails and **124 aborts**. **Recommendation:** for repeated dry-runs on the same calendar day, set `MD_RELEASE_TAG` to an unused tag (this audit used `20991231` for a full 124 pass-through).

3. **119 structural check — `note_extraction_runs` parity:** Live runs report `local=5` vs `md=3` (**FAIL** inside 119) while the process still exited 0 in this session. **Recommendation:** confirm intended exit semantics for partial FAIL and align local parquet export with MotherDuck if drift is unintentional.

4. **Read-scaling token:** None configured on this machine (`read_scaling_token_mode()` = `none`). **Recommendation:** if analyst dashboards or 136 `reader` production paths are needed, add `MD_READ_SCALING_TOKEN` (or alias) per `docs/motherduck_read_scaling_dashboard.md`; then re-run separation tests and live `connect_read_scaling()`.

5. **Catalog metadata (evidence from `130 inspect`):** Production catalog `Thyroid 2026` is type **`DUCKLAKE`**; dev/qa/pre-promote clones listed as **`DEFAULT`**. Treat snapshot/PITR semantics per contract §8 / runbook — not “generic MotherDuck native” assumptions.

No marketing “Pro/Business plan” labels are asserted from this audit; capabilities are inferred only from **successful connections**, **script output**, and **`MD_INFORMATION_SCHEMA`** rows visible to the current identity.
