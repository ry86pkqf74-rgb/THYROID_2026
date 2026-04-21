#!/usr/bin/env bash
# Chain driver — waits for Job 1a (pathology) extraction process to exit,
# then runs 1b cervical_ln_detail → 1c tirads_granular → 3 esophageal_invasion
# sequentially on the same vLLM endpoint (RunPod pod pmza5juk7ru2xl).
#
# Launch:
#   cd "/Users/ros/THyroid 2026"
#   nohup bash runs/round2_20260421/run_chain_post_pathology.sh \
#       > runs/round2_20260421/chain.log 2>&1 &
#   disown
#
# The 1a process is identified by its unique argv signature; we don't hard-code
# its PID so the script is safe to re-launch if it dies early.

set -uo pipefail  # no -e so a single job failure doesn't skip the rest
cd "$(dirname "$0")/../.."
REPO_ROOT="$(pwd)"

log() { printf '%s [chain] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"; }

SIGNATURE='run_extraction_concurrent.py.*--domains pathology'

log "waiting for 1a pathology extraction to finish (signature: ${SIGNATURE})"
while true; do
    # pgrep -f matches against full command line; exit 0 if anything matches
    if ! pgrep -f "${SIGNATURE}" > /dev/null 2>&1; then
        break
    fi
    sleep 60
done
log "1a process has exited"

# Give filesystem a moment to flush the final parquet.
sleep 10

PATH_PQ="runs/round2_20260421/pathology/output/note_entities_llm_pathology.parquet"
if [ -f "${PATH_PQ}" ]; then
    rows=$(.venv/bin/python - <<PY
import pyarrow.parquet as pq
print(pq.ParquetFile('${PATH_PQ}').metadata.num_rows)
PY
)
    log "1a parquet present: ${rows} rows"
else
    log "WARNING: 1a parquet not found at ${PATH_PQ} — checkpoint may be incomplete"
    wc -l runs/round2_20260421/pathology/output/note_entities_llm_pathology.ckpt.jsonl 2>/dev/null || true
fi

run_job() {
    local tag="$1"
    local wrapper="$2"
    local domain="$3"
    local logdir="runs/round2_20260421/${domain}/output"
    mkdir -p "${logdir}"
    log "=== launching ${tag} (${domain}) ==="
    bash "runs/round2_20260421/${wrapper}" 2>&1 | tee -a "${logdir}/run.log"
    local rc=${PIPESTATUS[0]}
    log "=== ${tag} exited rc=${rc} ==="
    return ${rc}
}

run_job "1b cervical_ln_detail" "run_1b_cervical_ln.sh"       "cervical_ln_detail" || log "1b failed — continuing to 1c"
run_job "1c tirads_granular"    "run_1c_tirads_granular.sh"   "tirads_granular"    || log "1c failed — continuing to job 3"
run_job "3  esophageal_invasion" "run_3_esophageal.sh"        "esophageal_invasion" || log "job 3 failed"

log "ALL JOBS IN CHAIN COMPLETE"
