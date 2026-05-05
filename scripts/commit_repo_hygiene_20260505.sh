#!/usr/bin/env bash
# Repo hygiene commit — 2026-05-05
# Runs from repo root. Idempotent (skips already-staged paths).
#
# What this commits:
#   - M044_FINAL_PACKAGE/ (v4 + v5 manuscript, tables, figures, all_stats.xlsx)
#   - cursor_prompts/ from 4/22-5/4 (provenance for analyses that already landed)
#   - .cursor/skills/check-secrets/ (cursor skill)
#   - archive/m044_ete_predecessors_20260505/ (retired v1-v3 drafts + reconciliation README)
#   - .gitignore additions for backups/, runs/, exports/, processed/remaining/, etc.
#
# What this DOES NOT touch:
#   - M025_FINAL_PACKAGE/ (your other chat owns these — modified files there)
#
# Usage: bash scripts/commit_repo_hygiene_20260505.sh

set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

# Check for stale lock — if present, abort
if [ -f .git/index.lock ]; then
  echo "ERROR: .git/index.lock present. Wait for other git op to finish, or rm it manually."
  exit 1
fi

# Check the M025 state isn't being clobbered
git diff --cached --name-only | grep -q "^M025_FINAL_PACKAGE" && {
  echo "WARN: M025_FINAL_PACKAGE staged in index — your other chat may be mid-commit."
  echo "Aborting to avoid collision. Re-run after that chat finishes."
  exit 2
}

echo "==> Staging hygiene paths"
git add .gitignore
git add .cursor/skills/check-secrets/SKILL.md
git add M044_FINAL_PACKAGE/
git add archive/m044_ete_predecessors_20260505/
git add cursor_prompts/CURSOR_PROMPT_ATA_RSS_2025_IMPLEMENTATION_20260504.md
git add cursor_prompts/CURSOR_PROMPT_ATA_RSS_2025_v2_MARGIN_FIX_20260504.md
git add cursor_prompts/CURSOR_PROMPT_DTC_NULL_T_STAGE_GROUP_FILL_20260422_SCRIPT_394.md
git add cursor_prompts/CURSOR_PROMPT_DTC_STAGE_GROUP_ORPHAN_FILL_20260422_SCRIPT_393.md
git add cursor_prompts/CURSOR_PROMPT_DTC_T_SYNC_STAGE_GROUP_FILL_20260423_SCRIPT_395.md
git add cursor_prompts/CURSOR_PROMPT_ETE_BOOLEAN_STRING_NORMALIZATION_20260422_SCRIPT_392.md
git add cursor_prompts/CURSOR_PROMPT_M019_RAI_OUTCOMES_20260504.md
git add cursor_prompts/CURSOR_PROMPT_M025_TIRADS_PERFORMANCE_20260504.md
git add cursor_prompts/CURSOR_PROMPT_M029_FNA_CONCORDANCE_20260504.md
git add cursor_prompts/CURSOR_PROMPT_M033_AFIRMA_VS_THYROSEQ_20260504.md
git add cursor_prompts/CURSOR_PROMPT_M043_LN_PREDICTORS_20260504.md
git add cursor_prompts/CURSOR_PROMPT_M044_AJCC_ETE_PSM_20260504.md
git add cursor_prompts/CURSOR_PROMPT_M047_FROZEN_SECTION_20260504.md
git add cursor_prompts/CURSOR_PROMPT_M083_BRAF_DUAL_PLATFORM_DISCORDANCE_20260504.md
git add cursor_prompts/CURSOR_PROMPT_MIG_63_ETE_QUEUE_CLOSEOUT_20260427.md
git add cursor_prompts/CURSOR_PROMPT_MIG_64_ETE_DISAGREEMENT_ADJUDICATION_20260427.md
git add cursor_prompts/CURSOR_PROMPT_TABLE1_TIER1_MANUSCRIPTS_20260504.md
git add cursor_prompts/CURSOR_PROMPT_TUMOR_T_STAGE_GAPS_AUDIT_20260423_SCRIPT_400.md
git add scripts/commit_repo_hygiene_20260505.sh

# Stage any cursor_prompts files dated 2026-04-22 onward that aren't tracked yet
git add cursor_prompts/*20260422* cursor_prompts/*20260423* cursor_prompts/*20260424* cursor_prompts/*20260427* cursor_prompts/*20260428* cursor_prompts/*20260429* cursor_prompts/*20260430* cursor_prompts/*20260501* cursor_prompts/*20260502* cursor_prompts/*20260503* cursor_prompts/*20260504* 2>/dev/null || true

echo "==> Staged paths:"
git diff --cached --name-only | head -60

echo
echo "==> Commit"
git commit -m "chore(repo-hygiene): land M044 FINAL_PACKAGE v5 + 5/4 cursor prompts; retire M044 v1-v3 drafts; broaden .gitignore

- M044_FINAL_PACKAGE/ contains v5 manuscript (Item-1 reconciled to FU IQR 5.91 yr per locked Excel; v3 LaTeX 5.89 was rounding delta)
- All 4/22-5/4 cursor prompts committed for provenance (ATA RSS v1+v2, DTC stage_group fills, M019/M025/M029/M033/M043/M044/M047/M083, ETE adjudication, T-stage gap audit)
- Retired M044 v1-v3 root-level drafts to archive/m044_ete_predecessors_20260505/ with reconciliation README
- .gitignore broadened: backups/, runs/, hpc/, runbook/, processed/remaining|output, exports/canonical_patient_master_pre*, exports/mig*_postapply_*, dive_audit_20260418/, extraction.sbatch
- Stale Apr 2026 handoff docs (COMPREHENSIVE_REVIEW, COWORK_CONTINUATION, HANDOFF, HYPERCLUSTER_*) gitignored — superseded by COWORK_HANDOFF_M025_V2_PIVOT_20260504.md
- Stale dryrun audit prompts (PROMPT_13, PROMPT_18, PROMPT_8_9, dryrun_audit_prompts_10_11_12) gitignored — superseded by 5/4 cursor_prompts/

Refs: pub_v1_1_20260504, mig_307b"

echo
echo "==> Push"
git push origin main

echo
echo "DONE."
