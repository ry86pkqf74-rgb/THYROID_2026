#!/bin/bash
# =====================================================================
# extraction.sbatch — tg_kinetics domain extraction via Qwen2.5-72B-AWQ
# =====================================================================
# Submit with:  sbatch ~/THYROID_2026/runbook/extraction.sbatch
#
# Batch job (NOT interactive salloc) — survives your SSH disconnect by
# construction. Monitor via NFS logs in ~/THYROID_2026/runbook/logs/.
#
# Key design decisions (from the 2026-04-17 session hard-won lessons):
#   1. sbatch not salloc — allocation independent of shell state
#   2. HF cache stays on NFS (~/.cache/huggingface/) — persists across
#      allocations, ~40 min saved on re-runs
#   3. Repo staged to /tmp for faster IO, but patches applied every run
#      via apply_patches.py so working tree on NFS stays clean
#   4. /dev/shm cleanup before vLLM start — prevents IPC leak from
#      previous crashed run
#   5. Resumable chunk loop with .completed_chunks/ markers on NFS
#      — picks up where we left off if re-run after timeout
# =====================================================================

# --- SBATCH directives -----------------------------------------------
#SBATCH --job-name=tg-kinetics
#SBATCH --partition=l40s-8-gm384-c192-m1536     # confirmed 2026-04-17
#SBATCH --gpus=8                                 # untyped required by l40s plugin (see §0 G6)
#SBATCH --cpus-per-task=24
#SBATCH --mem=256G
#SBATCH --time=16:00:00                          # ~2h rebuild + ~13h extraction
#SBATCH --output=/users/lglosse/THYROID_2026/runbook/logs/sbatch_%j.out
#SBATCH --error=/users/lglosse/THYROID_2026/runbook/logs/sbatch_%j.err

set -eo pipefail

# --- Paths ------------------------------------------------------------
# REPO_TMP is deferred — phase 1 selects a writable+exec parent dir at
# runtime, and we stage the repo next to the conda env there. Job 483951
# proved /tmp on compute nodes rejects modify-after-create (cp works,
# but pathlib.write_text gets EACCES even though POSIX perms allow it
# — likely SELinux reclass on file reuse, or hardened /tmp policy).
REPO_NFS=${HOME}/scratch_repo
REPO_TMP=""   # assigned after probe in phase 1
NFS_ROOT=${HOME}/THYROID_2026
RUNBOOK=${NFS_ROOT}/runbook
LOG_DIR=${RUNBOOK}/logs/${SLURM_JOB_ID}
DONE_DIR=${NFS_ROOT}/extracted/.completed_chunks
OUT_DIR=${NFS_ROOT}/extracted/outputs
mkdir -p "${LOG_DIR}" "${DONE_DIR}" "${OUT_DIR}"

echo "[$(date -Iseconds)] SLURM_JOB_ID=${SLURM_JOB_ID} on $(hostname)"
echo "REPO_NFS=${REPO_NFS} REPO_TMP=<deferred, phase 1>"

# --- Cleanup hook — runs on any exit ---------------------------------
cleanup() {
    echo "[$(date -Iseconds)] cleanup: killing vLLM (PID=${VLLM_PID:-none})"
    [ -n "${VLLM_PID:-}" ] && kill ${VLLM_PID} 2>/dev/null || true
    # Copy outputs back to NFS (idempotent — runs even on partial completion)
    [ -f "${REPO_TMP}/processed/note_entities_llm_tg_kinetics.parquet" ] && \
        cp -v "${REPO_TMP}/processed/note_entities_llm_tg_kinetics.parquet" \
              "${OUT_DIR}/note_entities_llm_tg_kinetics_${SLURM_JOB_ID}.parquet" || true
    [ -f "${REPO_TMP}/scripts/output/parquet_backup/note_entities_llm_tg_kinetics.parquet" ] && \
        cp -v "${REPO_TMP}/scripts/output/parquet_backup/note_entities_llm_tg_kinetics.parquet" \
              "${OUT_DIR}/note_entities_llm_tg_kinetics_backup_${SLURM_JOB_ID}.parquet" || true
    echo "[$(date -Iseconds)] cleanup complete"
}
trap cleanup EXIT INT TERM

# --- [1/7] Conda env — runtime fallback chain for writable+exec path -
# Context (hard-won 2026-04-17):
#   * NFS $HOME has a ~50G quota; a full vllm env (~10G) pushes it over
#     during pip install → EDQUOT 122. Confirmed on job 483936.
#   * /tmp on compute nodes is mounted NOEXEC (job 483938 hit exit 126
#     `bad interpreter: Permission denied` on the python shim). Even
#     though /tmp on ondemand nodes is execable, compute nodes harden it.
#   * /scratch is per-node instance-store, but owned root:HyPER-01-PROD-
#     Common — needs group membership to write into /scratch/$USER.
#   * /dev/shm is tmpfs, always writable, always exec, but compute nodes
#     with 1.5T RAM typically size /dev/shm at ~half of that.
#
# Strategy: probe each candidate at runtime with a write+exec test, pick
# the first that works. Keeps the sbatch portable across node types.
echo "=== [1/7] Setting up Python env (runtime path selection) ==="

probe_exec() {
    # $1 = candidate parent dir; returns 0 if we can write AND exec there
    local P="$1"
    [ -z "${P}" ] && return 1
    mkdir -p "${P}" 2>/dev/null || return 1
    [ -w "${P}" ] || return 1
    local T="${P}/.exec_probe_$$"
    cat > "${T}" <<'PROBE'
#!/bin/bash
echo ok
PROBE
    chmod +x "${T}" 2>/dev/null || { rm -f "${T}"; return 1; }
    local OUT
    OUT=$("${T}" 2>/dev/null) || { rm -f "${T}"; return 1; }
    rm -f "${T}"
    [ "${OUT}" = "ok" ] || return 1
    return 0
}

ENV_PATH=""
for CAND in "/scratch/${USER}" "/dev/shm/${USER}" "/tmp/${USER}" "${HOME}/envs"; do
    if probe_exec "${CAND}"; then
        ENV_PATH="${CAND}/thyroid_vllm_env"
        echo "  [path] selected ${CAND} for env (writable+exec OK)"
        break
    else
        echo "  [path] rejected ${CAND} (not writable or noexec)"
    fi
done
if [ -z "${ENV_PATH}" ]; then
    echo "ERROR: no writable+exec parent dir found on this node"
    exit 1
fi

# Pip cache co-located with env so we don't double-spend quota on NFS
PIP_PARENT=$(dirname "${ENV_PATH}")
export PIP_CACHE_DIR="${PIP_PARENT}/pip_cache"
REPO_TMP="${PIP_PARENT}/thyroid_repo"   # co-locate with env on writable+exec fs
mkdir -p "${PIP_CACHE_DIR}"
echo "  ENV_PATH=${ENV_PATH}"
echo "  PIP_CACHE_DIR=${PIP_CACHE_DIR}"
echo "  REPO_TMP=${REPO_TMP}"

source /opt/anaconda/etc/profile.d/conda.sh
if [ ! -x "${ENV_PATH}/bin/python" ]; then
    conda create -y -p "${ENV_PATH}" python=3.11 pip 2>&1 | tail -5
fi
conda activate "${ENV_PATH}"
pip install --no-input vllm==0.6.6 openai pyarrow pandas pyyaml 2>&1 | tail -5

# Prepend the env's bundled nvidia/*/lib dirs to LD_LIBRARY_PATH so the
# wheel-shipped libcusparse / libnvJitLink / libcublas / etc win over any
# older system-wide CUDA the HPC modulefiles expose. Without this, job
# 483940 died with:
#   libcusparse.so.12: undefined symbol: __nvJitLinkComplete_12_4
# because an older system libnvJitLink was resolved first.
SITE_PKGS=$("${ENV_PATH}/bin/python" -c "import sysconfig; print(sysconfig.get_paths()['purelib'])")
NVIDIA_LIB_DIRS=$(find "${SITE_PKGS}/nvidia" -maxdepth 2 -type d -name lib 2>/dev/null | sort -u | paste -sd: -)
export LD_LIBRARY_PATH="${NVIDIA_LIB_DIRS}:${LD_LIBRARY_PATH:-}"
echo "  LD_LIBRARY_PATH (head): ${LD_LIBRARY_PATH:0:400}..."

python -c "import vllm, torch; print(f'vllm={vllm.__version__} torch={torch.__version__} gpus={torch.cuda.device_count()}')"

# --- [2/7] Stage repo to a writable location -------------------------
# Context (hard-won 2026-04-17, jobs 483949+483951):
#   The NFS source file is -rw-r--r-- owned by lglosse, yet writing to
#   the /tmp copy hits PermissionError. cp and chmod both succeed but
#   path.write_text('w') still returns EACCES. Something on the compute
#   node's /tmp (ACL? bind-mount? immutable?) rejects rewrites of
#   existing files.
#
# Fix: co-locate REPO_TMP next to the conda env on /scratch, which we
# already proved writable+execable in phase 1. /tmp is only used for
# transient scratch that never gets rewritten.
PARENT_DIR=$(dirname "${ENV_PATH}")
REPO_TMP="${PARENT_DIR}/thyroid_repo"
echo "=== [2/7] Staging repo to ${REPO_TMP} ==="
rm -rf "${REPO_TMP}"
cp -r "${REPO_NFS}" "${REPO_TMP}"
chmod -R u+w "${REPO_TMP}"
# Clean up dev detritus we don't need on compute node
rm -rf "${REPO_TMP}/.git" "${REPO_TMP}/.cursor" "${REPO_TMP}/logs" 2>/dev/null || true
# Diagnostics — prove the target file is writable before phase 3
DBG="${REPO_TMP}/llm_extraction/extract_llm.py"
echo "  [dbg] target:       ${DBG}"
echo "  [dbg] ls -la:       $(ls -la ${DBG} 2>&1)"
echo "  [dbg] stat:         $(stat -c '%a %U:%G %s' ${DBG} 2>&1)"
echo "  [dbg] lsattr:       $(lsattr ${DBG} 2>&1 | head -1)"
echo "  [dbg] write probe:  $(printf '' >> ${DBG} && echo OK || echo FAIL)"
echo "  [dbg] /tmp mount:   $(awk '$2=="/tmp"{print $3, $4; exit}' /proc/mounts)"
echo "  [dbg] scratch mnt:  $(awk '$2=="/scratch"{print $3, $4; exit}' /proc/mounts)"

# --- [3/7] Apply Bug 9 + Bug 10 patches (idempotent) -----------------
echo "=== [3/7] Applying patches ==="
python "${RUNBOOK}/apply_patches.py" --repo "${REPO_TMP}"

# --- [4/7] Stage parquet data ----------------------------------------
echo "=== [4/7] Staging clinical notes ==="
mkdir -p "${REPO_TMP}/processed"
if [ -f "${REPO_NFS}/processed/clinical_notes_long.parquet" ]; then
    cp -v "${REPO_NFS}/processed/clinical_notes_long.parquet" "${REPO_TMP}/processed/"
else
    SRC=/users/lglosse/thyroid_extract/THYROID_2026/processed/clinical_notes_long.parquet
    [ -f "${SRC}" ] && cp -v "${SRC}" "${REPO_TMP}/processed/" || {
        echo "ERROR: cannot locate clinical_notes_long.parquet"; exit 1
    }
fi
python -c "
import pandas as pd
df = pd.read_parquet('${REPO_TMP}/processed/clinical_notes_long.parquet')
print(f'  rows={len(df)} unique_rids={df.research_id.nunique()}')
assert len(df) >= 11000, 'unexpected row count'
"

# --- [5/7] Launch vLLM -----------------------------------------------
echo "=== [5/7] Launching vLLM ==="
rm -f /dev/shm/psm_* /dev/shm/sem.loky-* /dev/shm/__KMP_REGISTERED_LIB_* 2>/dev/null || true
unset HF_HUB_OFFLINE
export VLLM_WORKER_MULTIPROC_METHOD=spawn
export NCCL_DEBUG=WARN

cd "${REPO_TMP}"
nohup python -m vllm.entrypoints.openai.api_server \
    --model Qwen/Qwen2.5-72B-Instruct-AWQ \
    --quantization awq_marlin \
    --tensor-parallel-size 8 \
    --gpu-memory-utilization 0.90 \
    --max-model-len 8192 \
    --max-num-seqs 64 \
    --enable-chunked-prefill \
    --disable-log-requests \
    --host 127.0.0.1 \
    --port 8000 \
    --served-model-name qwen2.5-72b \
    > "${LOG_DIR}/vllm_server.log" 2>&1 &
VLLM_PID=$!
echo "  vLLM PID=${VLLM_PID} — waiting for /v1/models..."

# First-run HF download can take ~45 min; subsequent runs ~3 min
for i in $(seq 1 360); do
    if curl -s -f --max-time 5 http://127.0.0.1:8000/v1/models >/dev/null 2>&1; then
        echo "  vLLM ready at ~${i}0s"
        curl -s http://127.0.0.1:8000/v1/models | python -m json.tool
        break
    fi
    kill -0 ${VLLM_PID} 2>/dev/null || {
        echo "  vLLM died:"; tail -60 "${LOG_DIR}/vllm_server.log"; exit 1
    }
    [ $((i % 6)) -eq 0 ] && echo "  ...still loading (${i}0s)"
    sleep 10
done

# --- [6/7] Run extraction with resumable chunking --------------------
echo "=== [6/7] Running extraction ==="
export VLLM_BASE_URL=http://127.0.0.1:8000/v1
export VLLM_MODEL_ID=qwen2.5-72b
export VLLM_API_KEY=EMPTY
export EXTRACT_MAX_CHUNK_CHARS=32000
export EXTRACT_OP_CHUNK_CHARS=48000

# Regenerate chunk files (23 × 250 rids, last = 93)
CHUNK_DIR=${REPO_TMP}/chunks
mkdir -p "${CHUNK_DIR}"
python - <<'PY'
import pandas as pd, os
rids = sorted(pd.read_parquet(
    os.path.expandvars("${REPO_TMP}/processed/clinical_notes_long.parquet")
).research_id.unique())
for i in range(0, len(rids), 250):
    n = i // 250 + 1
    with open(os.path.expandvars(f"${CHUNK_DIR}/chunk_{n:02d}.txt"), "w") as f:
        f.write("\n".join(map(str, rids[i:i+250])) + "\n")
print(f"  Wrote {n} chunks, last size={len(rids[i:i+250])}")
PY

# Dynamic halt — stop 15 min before Slurm kills us
END_EPOCH=$(date -d "$(scontrol show job ${SLURM_JOB_ID} | awk -F= '/EndTime/{print $2; exit}' | awk '{print $1}')" +%s)
HALT_EPOCH=$((END_EPOCH - 900))
echo "  Slurm EndTime=$(date -d @${END_EPOCH} -Iseconds)  halt-at=$(date -d @${HALT_EPOCH} -Iseconds)"

for i in $(seq -f "%02g" 1 23); do
    if [ -f "${DONE_DIR}/chunk_${i}.done" ]; then
        echo "  SKIP chunk_${i} (already done)"
        continue
    fi
    NOW=$(date +%s)
    if [ "${NOW}" -ge "${HALT_EPOCH}" ]; then
        echo "  HALT: within 15 min of Slurm EndTime, stopping"
        break
    fi
    CODE=$(curl -s -o /dev/null -w "%{http_code}" --max-time 5 http://127.0.0.1:8000/v1/models || echo 000)
    if [ "${CODE}" != "200" ]; then
        echo "  vLLM DOWN (HTTP ${CODE}), skipping chunk_${i}"
        continue
    fi
    echo "  === chunk_${i} === $(date -Iseconds)"
    python llm_extraction/run_extraction.py \
        --target tg_kinetics \
        --research-ids "${CHUNK_DIR}/chunk_${i}.txt" \
        --workers 2 \
        > "${LOG_DIR}/chunk_${i}.log" 2>&1
    rc=$?
    if [ ${rc} -eq 0 ]; then
        touch "${DONE_DIR}/chunk_${i}.done"
        echo "  done chunk_${i}"
    else
        echo "  FAIL chunk_${i} rc=${rc} (continuing)"
    fi
done

# --- [7/7] Final summary ---------------------------------------------
echo "=== [7/7] Summary ==="
echo "  Completed chunks: $(ls ${DONE_DIR} 2>/dev/null | wc -l) / 23"
if [ -f "${REPO_TMP}/processed/note_entities_llm_tg_kinetics.parquet" ]; then
    python -c "
import pandas as pd, json
df = pd.read_parquet('${REPO_TMP}/processed/note_entities_llm_tg_kinetics.parquet')
print(f'  Output rows: {len(df)}')
if 'result_json' in df.columns:
    ents = df.result_json.map(lambda s: len(json.loads(s).get('entities',[])) if isinstance(s,str) else 0)
    print(f'  Notes with entities: {int((ents>0).sum())} / {len(df)}')
    print(f'  Total entities: {int(ents.sum())}')
"
fi
echo "[$(date -Iseconds)] extraction.sbatch complete"
# cleanup() runs on trap EXIT
