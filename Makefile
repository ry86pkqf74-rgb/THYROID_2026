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
#   make md-review-queue-triage-md       # scripts/120_review_queue_triage.py --md
#   make md-final-master-dryrun          # 120 triage + 126 --dry-run (needs FINAL_MASTER_* env; see below)
#   make md-final-master-final          # 126 final (mutates MotherDuck; same env as dry-run)
#   make md-analyst-lab-append-dryrun    # 127 --dry-run (needs FINAL_MASTER_LAB_CSV + FINAL_MASTER_INGESTION_WAVE)
#   make md-molecular-promote-rehearsal  # 137 promote (rehearsal: no --execute; 124/136 dry-run)
#   make md-molecular-promote           # 137 promote --execute (prod-safe chain; see docs/release_runbook.md)
#   make md-v2-gate-local-dryrun         # legacy / local-only: 112 against local DuckDB + parquets (no --md)
#   make md-release-manifest-qa-dryrun   # legacy / local-only: manifest dry-run (script 96, local DB)
#   make md-release-manifest-prod        # legacy / local-only: write release manifest (script 96, local DB)
#   make md-manifest-status              # compare LATEST_MANIFEST.json to HEAD (no DB)
#
# Staging plane: new v2 parquets load into MotherDuck schema v2_stage; canonical tables
# live in main only after promotion. See docs/motherduck_v2_staging_runbook.md

PYTHON := .venv/bin/python

# ── MotherDuck token guard (env vars and/or .streamlit/secrets.toml via get_token) ─
define check_md_rw_token
	@$(PYTHON) -c "import sys; from pathlib import Path; sys.path.insert(0, str(Path('.').resolve())); \
from motherduck_client import get_token; \
t = get_token(); \
print('ERROR: No MotherDuck read/write token for Make targets.') if not t else None; \
print('  Set MOTHERDUCK_TOKEN and/or MD_SA_TOKEN in the environment, or add them to .streamlit/secrets.toml .') if not t else None; \
print('  See docs/motherduck_database_contract_v1.md (Connection Reference) and .env.motherduck.example') if not t else None; \
sys.exit(1 if not t else 0)"
endef

# ── SA flag helper (scripts/96 still accept --sa for legacy paths) ────
SA_FLAG := $(if $(MD_SA_TOKEN),--sa,)

# Prefer service account when MD_SA_TOKEN is set (137 matches 119/124 --md-sa pattern).
MD_MOL_SA := $(if $(MD_SA_TOKEN),--md-sa,)

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

# Override when release_${MD_RELEASE_TAG} already exists on MotherDuck (e.g. export MD_RELEASE_TAG=20260410)
MD_RELEASE_TAG ?= $(shell date -u +%Y%m%d)

.PHONY: md-live-release-dryrun
md-live-release-dryrun:
	$(check_md_rw_token)
	$(PYTHON) scripts/124_md_live_release_audit.py --md --dry-run --tag $(MD_RELEASE_TAG)

.PHONY: md-live-release-final
md-live-release-final:
	$(check_md_rw_token)
	$(PYTHON) scripts/124_md_live_release_audit.py --md --final-release --tag $(MD_RELEASE_TAG)

# ── Final-master orchestration (126) + lab append dry-run (127) ────────────
# Legacy path: md-live-release-* uses scripts/124 (tagged release / molecular promote).
# Final-master path: populate env vars then run:
#   FINAL_MASTER_HYDRATE_DIR   — directory with reviewed manual_review_queue.csv (+ gate CSVs)
#   FINAL_MASTER_DECISIONS_CSV — promotion_review_decisions.csv
#   FINAL_MASTER_LAB_CSV / FINAL_MASTER_INGESTION_WAVE — optional; required together for lab wave
#   RELEASE_DATE               — optional YYYYMMDD (default: today's UTC date)
MD_SA_FLAG := $(if $(MD_SA_TOKEN),--md-sa,)

.PHONY: md-review-queue-triage-md
md-review-queue-triage-md:
	$(check_md_rw_token)
	$(PYTHON) scripts/120_review_queue_triage.py --md $(MD_SA_FLAG)

.PHONY: md-final-master-dryrun
md-final-master-dryrun: md-review-queue-triage-md
	$(check_md_rw_token)
	@if [ -z "$$FINAL_MASTER_HYDRATE_DIR" ] || [ ! -d "$$FINAL_MASTER_HYDRATE_DIR" ]; then \
		echo "ERROR: Set FINAL_MASTER_HYDRATE_DIR to a gate directory containing manual_review_queue.csv"; exit 1; \
	fi
	@if [ -z "$$FINAL_MASTER_DECISIONS_CSV" ] || [ ! -f "$$FINAL_MASTER_DECISIONS_CSV" ]; then \
		echo "ERROR: Set FINAL_MASTER_DECISIONS_CSV to promotion_review_decisions.csv"; exit 1; \
	fi
	@REL=$${RELEASE_DATE:-$$(date -u +%Y%m%d)}; \
	EXTRA=""; \
	if [ -n "$$FINAL_MASTER_LAB_CSV" ]; then \
		if [ -z "$$FINAL_MASTER_INGESTION_WAVE" ]; then echo "ERROR: FINAL_MASTER_INGESTION_WAVE required with FINAL_MASTER_LAB_CSV"; exit 1; fi; \
		EXTRA="$$EXTRA --lab-csv $$FINAL_MASTER_LAB_CSV --ingestion-wave $$FINAL_MASTER_INGESTION_WAVE"; \
	fi; \
	$(PYTHON) scripts/126_final_master_release.py --md $(MD_SA_FLAG) --dry-run \
		--release-date $$REL \
		--hydrate-mrq-from "$$FINAL_MASTER_HYDRATE_DIR" \
		--decisions-csv "$$FINAL_MASTER_DECISIONS_CSV" $$EXTRA

.PHONY: md-final-master-final
md-final-master-final:
	$(check_md_rw_token)
	@if [ -z "$$FINAL_MASTER_HYDRATE_DIR" ] || [ ! -d "$$FINAL_MASTER_HYDRATE_DIR" ]; then \
		echo "ERROR: Set FINAL_MASTER_HYDRATE_DIR to a gate directory containing manual_review_queue.csv"; exit 1; \
	fi
	@if [ -z "$$FINAL_MASTER_DECISIONS_CSV" ] || [ ! -f "$$FINAL_MASTER_DECISIONS_CSV" ]; then \
		echo "ERROR: Set FINAL_MASTER_DECISIONS_CSV to promotion_review_decisions.csv"; exit 1; \
	fi
	@REL=$${RELEASE_DATE:-$$(date -u +%Y%m%d)}; \
	EXTRA=""; \
	if [ -n "$$FINAL_MASTER_LAB_CSV" ]; then \
		if [ -z "$$FINAL_MASTER_INGESTION_WAVE" ]; then echo "ERROR: FINAL_MASTER_INGESTION_WAVE required with FINAL_MASTER_LAB_CSV"; exit 1; fi; \
		EXTRA="$$EXTRA --lab-csv $$FINAL_MASTER_LAB_CSV --ingestion-wave $$FINAL_MASTER_INGESTION_WAVE"; \
	fi; \
	$(PYTHON) scripts/126_final_master_release.py --md $(MD_SA_FLAG) \
		--release-date $$REL \
		--hydrate-mrq-from "$$FINAL_MASTER_HYDRATE_DIR" \
		--decisions-csv "$$FINAL_MASTER_DECISIONS_CSV" $$EXTRA

.PHONY: md-analyst-lab-append-dryrun
md-analyst-lab-append-dryrun:
	$(check_md_rw_token)
	@if [ -z "$$FINAL_MASTER_LAB_CSV" ] || [ ! -f "$$FINAL_MASTER_LAB_CSV" ]; then \
		echo "ERROR: Set FINAL_MASTER_LAB_CSV to the analyst lab CSV path"; exit 1; \
	fi
	@if [ -z "$$FINAL_MASTER_INGESTION_WAVE" ]; then \
		echo "ERROR: Set FINAL_MASTER_INGESTION_WAVE (e.g. final_institutional_YYYYMMDD)"; exit 1; \
	fi
	$(PYTHON) scripts/127_analyst_institutional_lab_append.py --md $(MD_SA_FLAG) \
		--input "$$FINAL_MASTER_LAB_CSV" \
		--ingestion-wave "$$FINAL_MASTER_INGESTION_WAVE" \
		--dry-run

.PHONY: md-molecular-promote-rehearsal
md-molecular-promote-rehearsal:
	$(check_md_rw_token)
	$(PYTHON) scripts/137_md_molecular_release_workflow.py promote --tag $$(date -u +%Y%m%d) $(MD_MOL_SA)

.PHONY: md-molecular-promote
md-molecular-promote:
	$(check_md_rw_token)
	$(PYTHON) scripts/137_md_molecular_release_workflow.py promote --tag $$(date -u +%Y%m%d) --execute $(MD_MOL_SA)

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
