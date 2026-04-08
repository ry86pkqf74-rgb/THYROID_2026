## Commands run (2026-04-08 UTC session)

```bash
cd /Users/loganglosser/THYROID_2026

# Token / DB resolution (no secrets printed)
.venv/bin/python -c "from motherduck_client import token_mode, read_scaling_token_mode, resolve_database_for_env; ..."

# Lint / type / tests
.venv/bin/pip install -q ruff
.venv/bin/ruff check scripts utils app llm_extraction motherduck_client.py dashboard.py --select F
.venv/bin/python -m pyflakes scripts utils app llm_extraction motherduck_client.py dashboard.py
.venv/bin/mypy
.venv/bin/pytest -q tests/test_imaging_fna_linkage_mm_v1.py tests/test_multimodal_contract_mm_v1.py tests/test_md_read_scaling_refresh.py tests/test_motherduck_connect_hardening.py tests/test_motherduck_token_modes.py tests/test_smoke_test_md_connection.py tests/test_specimen_fhir_layer.py tests/test_specimen_fhir_qa_diagnostics.py

# MotherDuck RW
.venv/bin/python scripts/smoke_test_md_connection.py --md
.venv/bin/python scripts/130_md_env_bootstrap.py inspect
.venv/bin/python scripts/144_md_repo_current_state_summary.py --md --output studies/20260407_repo_delta_gap_audit/CURRENT_MOTHERDUCK_REPO_STATE.md

# Read-scaling refusal + 136 dry-run
.venv/bin/python -c "from motherduck_client import MotherDuckClient, read_scaling_token_mode; ..."
.venv/bin/python scripts/136_md_read_scaling_snapshot_refresh.py reader --md-env prod --dry-run
.venv/bin/python scripts/136_md_read_scaling_snapshot_refresh.py writer --md-env prod --dry-run

# DuckLake rollback SQL review (no --execute)
.venv/bin/python scripts/130_md_env_bootstrap.py prepromote-backup --label audit_probe_20260407

# Formalization chain (Make — uses get_token → secrets.toml)
make md-v2-gate-md-dryrun

# Release-mode validation
.venv/bin/python scripts/119_md_formalization_validate.py --md --release-mode \
  --output-dir studies/20260407_repo_delta_gap_audit/119_release_mode_rerun

# Live lab waves (RW connection)
.venv/bin/python -c "from pathlib import Path; from utils.md_connect import connect_md_fail_closed; ..."

# Live release audit (initiated; long-running — see validation_results.md)
make md-live-release-dryrun
```
