#!/usr/bin/env bash
# Finish hygiene + push — 2026-05-05
# Run this in YOUR terminal where GitHub auth works (sandbox can't push).
#
# What this does:
#   1. Pushes the already-committed hygiene commit (2266cc2) to origin/main
#   2. Adds + commits the 3 new infra docs (Cortex walkthrough, mig_309, mig_310)
#   3. Pushes the second commit
#
# Run from repo root.

set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

# Clear any stale locks
[ -f .git/index.lock ] && rm -f .git/index.lock && echo "Cleared stale .git/index.lock"

echo "==> Push existing hygiene commit"
git push origin main

echo "==> Stage + commit infra docs"
git add CORTEX_ANALYST_BIND_WALKTHROUGH_20260505.md
git add cursor_prompts/CURSOR_PROMPT_MIG_309_SF_SP_V3_FIX_20260505.md
git add cursor_prompts/CURSOR_PROMPT_MIG_310_FNA_NLP_SIZE_EXTRACTION_20260505.md
git add scripts/finish_commit_and_push_20260505.sh

git commit -m "feat(infra): mig_309 SP-v3-fix + mig_310 FNA-NLP-size + Cortex Analyst bind walkthrough

mig_309 (cursor prompt): Snowflake VALIDATE_ALL_COHORTS_V3 SP fix with 3 strategies (Option A pre-materialize, B SHOW TABLES, C hardcode). Closes CF-mig_305-SP-V3-HANG once cursor lands.

mig_310 (cursor prompt): FNA NLP size_cm + laterality extraction via Cortex AI_EXTRACT. Mirrors mig_298 (Hashimoto/Graves) Option-2 pattern: SF NLP_FNA_SIZE_FULL_RESULTS_v1 -> MD manuscript_workspace.nlp_fna_size_rollup_v1 -> imaging_fna_linkage_v4 with size_score_v4 weighted prior. Closes CF-FNA-SIZE-CM-NULL.

Cortex Analyst bind walkthrough: 5-step Snowsight UI flow for binding m025_nodule_level_semantic_model.yaml. Logan-action; cursor cannot bind. Once smoke-tested, mig_312 batch can scaffold M032/M037/M038/M044 patient-grain semantic models.

mig_308 (executed live, not committed): backfilled signoffs for mig_264/274/276/278/279/301/301b/304. Deferred mig_266/270 (need cursor probe), mig_305 (superseded by mig_309)."

git push origin main

echo
echo "DONE — pushed."
