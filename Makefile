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
