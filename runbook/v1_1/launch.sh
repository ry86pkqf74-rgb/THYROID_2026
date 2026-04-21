#!/usr/bin/env bash
# Mac-side launcher — run this from your laptop. It scps the runbook, the
# input parquet, and the fresh extractor source to the cluster, then submits
# the sbatch. Set CLUSTER_HOST below (or export it in your shell) to match
# your ~/.ssh/config.
#
# Usage:
#   ./launch.sh                       # ship + submit
#   DRY_RUN=1 ./launch.sh             # show commands, don't run
#   SKIP_SCP=1 ./launch.sh            # just submit sbatch (files already up)
#   SKIP_SUBMIT=1 ./launch.sh         # only scp, don't submit

set -euo pipefail

CLUSTER_USER="${CLUSTER_USER:-lglosse}"
CLUSTER_HOST="${CLUSTER_HOST:-hypercluster}"   # edit to match your ssh alias
MAC_REPO="${MAC_REPO:-$HOME/THYROID_2026}"

CL_USERS="/users/${CLUSTER_USER}/THYROID_2026"
CL_SCRATCH="/scratch/${CLUSTER_USER}/thyroid_repo"

run() {
    echo "[+] $*"
    if [ "${DRY_RUN:-0}" = "1" ]; then return 0; fi
    "$@"
}

echo "=== TIRADS v1_1 launcher ==="
echo "    cluster: ${CLUSTER_USER}@${CLUSTER_HOST}"
echo "    mac repo: ${MAC_REPO}"
echo

if [ "${SKIP_SCP:-0}" != "1" ]; then
    echo "=== 1. ensure dest dirs on cluster ==="
    run ssh "${CLUSTER_USER}@${CLUSTER_HOST}" \
        "mkdir -p ${CL_USERS}/runbook ${CL_USERS}/runbook/logs ${CL_USERS}/extractions/tirads_v1_1 ${CL_SCRATCH}/processed/remaining ${CL_SCRATCH}/scripts/vastai ${CL_SCRATCH}/llm_extraction/prompts ${CL_SCRATCH}/llm_extraction"

    echo
    echo "=== 2. ship runbook folder ==="
    run scp -r "${MAC_REPO}/runbook/v1_1" \
        "${CLUSTER_USER}@${CLUSTER_HOST}:${CL_USERS}/runbook/"

    echo
    echo "=== 3. ship clinical_notes_long.parquet ==="
    run scp "${MAC_REPO}/scripts/output/parquet/main/clinical_notes_long.parquet" \
        "${CLUSTER_USER}@${CLUSTER_HOST}:${CL_SCRATCH}/processed/remaining/"

    echo
    echo "=== 4. ship fresh extractor source (may overwrite stale copies) ==="
    run scp "${MAC_REPO}/scripts/vastai/run_extraction_concurrent.py" \
        "${CLUSTER_USER}@${CLUSTER_HOST}:${CL_SCRATCH}/scripts/vastai/"
    run scp "${MAC_REPO}/llm_extraction/prompts/tirads_granular_extraction_v1.txt" \
        "${CLUSTER_USER}@${CLUSTER_HOST}:${CL_SCRATCH}/llm_extraction/prompts/"
    # Supporting module: run_extraction_concurrent.py doesn't actually import from base.py,
    # but ship it in case downstream scripts do.
    if [ -f "${MAC_REPO}/llm_extraction/base.py" ]; then
        run scp "${MAC_REPO}/llm_extraction/base.py" \
            "${CLUSTER_USER}@${CLUSTER_HOST}:${CL_SCRATCH}/llm_extraction/"
    fi
    # Ship run_telemetry.py if present (imported by extract_llm.py; harmless here)
    if [ -f "${MAC_REPO}/llm_extraction/run_telemetry.py" ]; then
        run scp "${MAC_REPO}/llm_extraction/run_telemetry.py" \
            "${CLUSTER_USER}@${CLUSTER_HOST}:${CL_SCRATCH}/llm_extraction/"
    fi
fi

if [ "${SKIP_SUBMIT:-0}" != "1" ]; then
    echo
    echo "=== 5. submit sbatch ==="
    run ssh "${CLUSTER_USER}@${CLUSTER_HOST}" \
        "sbatch ${CL_USERS}/runbook/v1_1/tirads_v1_1.sbatch"

    echo
    echo "=== done. Watch progress with: ==="
    echo "    ssh ${CLUSTER_USER}@${CLUSTER_HOST} \"squeue -u ${CLUSTER_USER}\""
    echo "    ssh ${CLUSTER_USER}@${CLUSTER_HOST} \"tail -f ${CL_USERS}/runbook/logs/tirads_v11_<JOBID>.out\""
fi
