# THYROID_2026 — MotherDuck promotion & release helpers
# ──────────────────────────────────────────────────────
# Token resolution (in priority order):
#   1. MD_SA_TOKEN  – service-account token for CI / shared environments
#   2. MOTHERDUCK_TOKEN – personal token (fallback to .streamlit/secrets.toml inside scripts)
#
# Usage:
#   make md-promote-dryrun-dev-qa       # dry-run DEV → QA gate
#   make md-promote-dryrun-qa-prod      # dry-run QA → PROD gate
#   make md-release-manifest-prod       # generate release manifest against PROD

PYTHON := .venv/bin/python

# ── token guard ────────────────────────────────────────
define check_token
	@if [ -z "$$MOTHERDUCK_TOKEN" ] && [ -z "$$MD_SA_TOKEN" ]; then \
		echo "ERROR: Neither MOTHERDUCK_TOKEN nor MD_SA_TOKEN is set."; \
		echo "  export MOTHERDUCK_TOKEN=<token>   (personal)"; \
		echo "  export MD_SA_TOKEN=<token>         (service account)"; \
		exit 1; \
	fi
endef

# ── SA flag helper ─────────────────────────────────────
# Appends --sa when MD_SA_TOKEN is set so scripts use the SA path.
SA_FLAG := $(if $(MD_SA_TOKEN),--sa,)

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

# ── manifest status quick-check (no MotherDuck needed) ─
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
