# THYROID_2026 — MotherDuck operator helpers + local release manifest
# ───────────────────────────────────────────────────────────────────
# MotherDuck read/write tokens (see motherduck_client.get_token):
#   MOTHERDUCK_TOKEN — personal developer (preferred for interactive use)
#   MD_SA_TOKEN      — CI / service account (use with scripts that support --sa)
# Optional: gitignored .env.motherduck — copy from .env.motherduck.example
#
# Legacy release manifest (scripts/96_release_manifest.py) opens local thyroid_master.duckdb only.
# That script still requires LOCAL_DB_PATH to be non-empty internally; Make exports a
# harmless default (.) when unset so the local/legacy target runs without extra setup.
#
# Usage:
#   make md-smoke                         # MOTHERDUCK_TOKEN or MD_SA_TOKEN required; runs scripts/smoke_test_md_connection.py --md (fail-closed)
#   make md-v2-gate-md-dryrun            # formalization path: 116 --md --dry-run → 112 --motherduck-check → 119 --md (all fail-closed on --md)
#   make md-live-release-dryrun          # scripts/124_md_live_release_audit.py --md --dry-run (fail-closed)
#   make md-live-release-final           # scripts/124_md_live_release_audit.py --md --final-release (fail-closed; strict release)
#   make md-v2-gate-local-dryrun         # legacy / local-only: 112 against local DuckDB + parquets (no --md)
#   make md-release-manifest-qa-dryrun   # legacy / local-only: manifest dry-run (script 96, local DB)
#   make md-release-manifest-prod        # legacy / local-only: write release manifest (script 96, local DB)
#   make md-manifest-status              # compare LATEST_MANIFEST.json to HEAD (no DB)
#
# Staging plane: new v2 parquets load into MotherDuck schema v2_stage; canonical tables
# live in main only after promotion. See docs/motherduck_v2_staging_runbook.md

PYTHON := .venv/bin/python

# ── MotherDuck token guard (secrets.toml not visible to Make) ─────────
define check_md_rw_token
	@if [ -z "$$MOTHERDUCK_TOKEN" ] && [ -z "$$MD_SA_TOKEN" ]; then \
		echo "ERROR: Set MOTHERDUCK_TOKEN and/or MD_SA_TOKEN for MotherDuck targets."; \
		echo "  See docs/motherduck_database_contract_v1.md (Connection Reference) and .env.motherduck.example"; \
		exit 1; \
	fi
endef

# ── SA flag helper (scripts/96 still accept --sa for legacy paths) ────
SA_FLAG := $(if $(MD_SA_TOKEN),--sa,)

# 96_release_manifest.py gates on LOCAL_DB_PATH even when opening a local file.
define run96
	@LOCAL_DB_PATH="$${LOCAL_DB_PATH:-.}"; export LOCAL_DB_PATH; \
	$(PYTHON) $(1)
endef

# ── MotherDuck smoke (fail-closed: same gate as connect_md_fail_closed) ─
.PHONY: md-smoke
md-smoke:
	$(check_md_rw_token)
	$(PYTHON) scripts/smoke_test_md_connection.py --md

# ── Formalization path (MotherDuck tokens only; fail-closed --md) ─────
.PHONY: md-v2-gate-md-dryrun
md-v2-gate-md-dryrun:
	$(check_md_rw_token)
	$(PYTHON) scripts/116_md_stage_loader.py --md --dry-run
	$(PYTHON) scripts/112_v2_domain_promotion_gate.py \
		--v2-parquets-dir processed/output/v2_parquets \
		--db-path thyroid_master.duckdb \
		--motherduck-check \
		--run-label make_md_formalization_dryrun
	$(PYTHON) scripts/119_md_formalization_validate.py --md

.PHONY: md-live-release-dryrun
md-live-release-dryrun:
	$(check_md_rw_token)
	$(PYTHON) scripts/124_md_live_release_audit.py --md --dry-run

.PHONY: md-live-release-final
md-live-release-final:
	$(check_md_rw_token)
	$(PYTHON) scripts/124_md_live_release_audit.py --md --final-release

# ── v2 promotion gate dry-run — local / legacy (local DuckDB + parquets only) ─
.PHONY: md-v2-gate-local-dryrun
md-v2-gate-local-dryrun:
	$(PYTHON) scripts/112_v2_domain_promotion_gate.py \
		--v2-parquets-dir processed/output/v2_parquets \
		--db-path thyroid_master.duckdb \
		--run-label make_local_dryrun

# Backward-compatible alias for local-only gate dry-run
.PHONY: md-v2-gate-dryrun
md-v2-gate-dryrun: md-v2-gate-local-dryrun

# ── release manifest — local / legacy (script 96, local thyroid_master.duckdb) ─
.PHONY: md-release-manifest-qa-dryrun
md-release-manifest-qa-dryrun:
	$(call run96,scripts/96_release_manifest.py --env qa --dry-run $(SA_FLAG))

.PHONY: md-release-manifest-prod
md-release-manifest-prod:
	$(call run96,scripts/96_release_manifest.py --env prod $(SA_FLAG))

# ── backward-compatible aliases (script 95 removed from repo) ───────
.PHONY: md-promote-dryrun-dev-qa md-promote-dryrun-qa-prod
md-promote-dryrun-dev-qa:
	@echo "NOTE: scripts/95_environment_promotion.py is not in this repo; running local v2 gate dry-run (legacy)."
	@$(MAKE) md-v2-gate-local-dryrun

md-promote-dryrun-qa-prod:
	@echo "NOTE: scripts/95_environment_promotion.py is not in this repo; running manifest QA dry-run only."
	@$(MAKE) md-release-manifest-qa-dryrun

# ── convenience: local gate dry-run + manifest dry-run (legacy) ─────
.PHONY: md-promote-dryrun-all
md-promote-dryrun-all: md-v2-gate-local-dryrun md-release-manifest-qa-dryrun

# ── manifest status quick-check (no local DuckDB needed) ─
.PHONY: md-manifest-status
md-manifest-status:
	@$(PYTHON) -c "\
	import json, subprocess, sys; \
	from pathlib import Path; \
	p = Path('exports/release_manifests/LATEST_MANIFEST.json'); \
	assert p.exists(), 'No manifest — run: make md-release-manifest-prod'; \
	m = json.loads(p.read_text()); \
	head = subprocess.check_output(['git','rev-parse','--short=7','HEAD'], text=True).strip(); \
	sha_match = m.get('git_sha','')[:7] == head; \
	print(f'Manifest : {m[\"manifest_id\"]}'); \
	print(f'Status   : {m[\"overall_status\"]}'); \
	print(f'SHA      : {m[\"git_sha\"]}  (HEAD={head}, {\"fresh\" if sha_match else \"STALE — re-run manifest\"})'); \
	print(f'Generated: {m[\"generated_at\"]}'); \
	sys.exit(0 if m['overall_status'] == 'RELEASE_READY' and sha_match else 1)"

# ── local hygiene (no data / venv removal) ─────────────────
.PHONY: clean
clean:
	@echo "clean: removing __pycache__ / .pytest_cache under repo ( .venv and .git skipped )"
	@find . \( -path './.git' -o -path './.venv' \) -prune -o -type d -name '__pycache__' -print0 2>/dev/null | xargs -0 rm -rf 2>/dev/null || true
	@rm -rf .pytest_cache 2>/dev/null || true
	@find . \( -path './.git' -o -path './.venv' \) -prune -o -type f -name '*.pyc' -delete 2>/dev/null || true

# ── provenance audit (read-only: --dry-run) ─────────────────
# For full materialization (mutates DuckDB), run without --dry-run:
#   $(PYTHON) scripts/46_provenance_audit.py --md
.PHONY: verify-provenance
verify-provenance:
	$(check_md_rw_token)
	$(PYTHON) scripts/46_provenance_audit.py --md --dry-run
