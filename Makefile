# THYROID_2026 — local DuckDB promotion & release helpers
# ──────────────────────────────────────────────────────
# Token resolution (in priority order):
#   1. LOCAL_DB_PATH  – service-account token for CI / shared environments
#   2. LOCAL_DB_PATH – personal token (fallback to .streamlit/secrets.toml inside scripts)
#
# Usage:
#   make md-promote-dryrun-dev-qa       # dry-run DEV → QA gate
#   make md-promote-dryrun-qa-prod      # dry-run QA → PROD gate
#   make md-release-manifest-prod       # generate release manifest against PROD

PYTHON := .venv/bin/python

# ── token guard ────────────────────────────────────────
define check_token
	@if [ -z "$$LOCAL_DB_PATH" ] && [ -z "$$LOCAL_DB_PATH" ]; then \
		echo "ERROR: Neither LOCAL_DB_PATH nor LOCAL_DB_PATH is set."; \
		echo "  export LOCAL_DB_PATH=<token>   (personal)"; \
		echo "  export LOCAL_DB_PATH=<token>         (service account)"; \
		exit 1; \
	fi
endef

# ── SA flag helper ─────────────────────────────────────
# Appends --sa when LOCAL_DB_PATH is set so scripts use the SA path.
SA_FLAG := $(if $(LOCAL_DB_PATH),--sa,)

# ── promotion dry-runs ─────────────────────────────────
.PHONY: md-promote-dryrun-dev-qa
md-promote-dryrun-dev-qa:
	$(check_token)
	$(PYTHON) scripts/95_environment_promotion.py --from dev --to qa --dry-run $(SA_FLAG)

.PHONY: md-promote-dryrun-qa-prod
md-promote-dryrun-qa-prod:
	$(check_token)
	$(PYTHON) scripts/96_release_manifest.py --env qa --dry-run $(SA_FLAG)
	$(PYTHON) scripts/95_environment_promotion.py --from qa --to prod --dry-run $(SA_FLAG)

# ── release manifest ───────────────────────────────────
.PHONY: md-release-manifest-prod
md-release-manifest-prod:
	$(check_token)
	$(PYTHON) scripts/96_release_manifest.py --env prod $(SA_FLAG)

# ── convenience: full dry-run sweep ────────────────────
.PHONY: md-promote-dryrun-all
md-promote-dryrun-all: md-promote-dryrun-dev-qa md-promote-dryrun-qa-prod

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
	$(check_token)
	$(PYTHON) scripts/46_provenance_audit.py --md --dry-run
