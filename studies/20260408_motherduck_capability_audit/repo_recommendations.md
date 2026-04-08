# Repo recommendations (pre–production code changes)

1. **`144` governance signal** — Extend or supplement `scripts/144_md_repo_current_state_summary.py` with a **non-count** MRQ rollup (e.g., distinct `verification_status` / placeholder flags) so NULL-only pending counts cannot mask sign-off blockers described in `final_verdict_memo.md`.

2. **Strict read-only audit profile** — Document or add a Make target that runs **only** fail-closed attach + `PRAGMA database_list` + read-only `SELECT`s, excluding `114_qa_schema_setup.py --md` when the operator requires **zero DDL** on MotherDuck.

3. **RS-only integration test** — Add a controlled test (CI or local) that verifies `ReadScalingTokenForbiddenError` / smoke exit **1** when RW is absent **and** dotenv/secrets do not backfill RW — avoids false confidence from shell `unset` alone.

4. **Read-scaling resilience** — Log or document transient `TransactionException` / DuckLake catalog conflicts on first `connect_read_scaling()`; consider a single retry in dashboard paths if MotherDuck recommends it.

5. **Token documentation** — Keep `.streamlit/secrets.toml` + `.env.motherduck` (gitignored) aligned with `docs/motherduck_database_contract_v1.md` §8; this audit confirmed **both** RW (env) and RS (secrets.toml) work.

6. **Plan wording** — Continue to avoid “Pro plan” labels in automation output; report capability booleans as in `capability_audit.md`.
