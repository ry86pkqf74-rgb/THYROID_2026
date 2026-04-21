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

# Bug 11 fix (job 483958): vLLM 0.6.6 pulls transformers 4.45.x which has
# a slow Qwen2Tokenizer missing `all_special_tokens_extended`, crashing
# vllm/transformers_utils/tokenizer.py:35 with AttributeError.
# Upgrade transformers + tokenizers to a version where the property is
# properly exposed on the slow tokenizer.
pip install --upgrade --no-input "transformers>=4.48,<4.50" "tokenizers>=0.20,<0.22" 2>&1 | tail -3

# Prepend the env's bundled nvidia/*/lib dirs to LD_LIBRARY_PATH so the
# wheel-shipped libcusparse / libnvJitLink / libcublas / etc win over any
# older system-wide CUDA the HPC modulefiles expose. Without this, job
# 483940 died with:
#   libcusparse.so.12: undefined symbol: __nvJitLinkComplete_12_4
# because an older system libnvJitLink was resolved first.
# IMPORTANT: LD_LIBRARY_PATH must be set BEFORE any python import of torch
# or transformers (transformers imports torch). Job 483960 hit this bug
# when the Bug-11 assertion ran before this export.
SITE_PKGS=$("${ENV_PATH}/bin/python" -c "import sysconfig; print(sysconfig.get_paths()['purelib'])")
NVIDIA_LIB_DIRS=$(find "${SITE_PKGS}/nvidia" -maxdepth 2 -type d -name lib 2>/dev/null | sort -u | paste -sd: -)
export LD_LIBRARY_PATH="${NVIDIA_LIB_DIRS}:${LD_LIBRARY_PATH:-}"
echo "  LD_LIBRARY_PATH (head): ${LD_LIBRARY_PATH:0:400}..."

# Bug 11 assertion (must run AFTER LD_LIBRARY_PATH export because it
# imports transformers, which imports torch, which loads libcusparse).
python -c "from transformers.tokenization_utils_base import SpecialTokensMixin; assert hasattr(SpecialTokensMixin, 'all_special_tokens_extended'), 'Bug 11 fix did not stick'; print('  Bug11 fix verified: transformers has all_special_tokens_extended property')"

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
export REPO_TMP   # phase 6 python heredoc reads this via os.environ
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

# Bug 13 fix (job 483962): NFS $HOME has ~50GB quota; Qwen2.5-72B-AWQ
# weights are ~40GB + partial download state pushes it over, giving
#   OSError: [Errno 122] Disk quota exceeded
# during _create_symlink. Pin the HF cache to /scratch (~5.8TB available)
# and sweep any NFS leftovers from prior failed runs to free quota.
export HF_HOME=/scratch/lglosse/.cache/huggingface
export HUGGINGFACE_HUB_CACHE=${HF_HOME}/hub
export TRANSFORMERS_CACHE=${HF_HOME}/hub   # legacy var some libs still read
mkdir -p "${HF_HOME}/hub"

echo "  Disk usage before P5:"
df -h /users/lglosse /scratch/lglosse 2>/dev/null | sed 's/^/    /'
du -sh "$HOME/.cache/huggingface" 2>/dev/null | sed 's/^/    NFS HF cache: /' || true

# Free NFS quota: any partial HF cache in $HOME from prior failed runs
# is evicted. The real cache now lives on /scratch so nothing useful is
# lost. Only runs if $HOME/.cache/huggingface differs from HF_HOME.
if [ -d "$HOME/.cache/huggingface" ] && [ "$HOME/.cache/huggingface" != "${HF_HOME}" ]; then
    echo "  Evicting $HOME/.cache/huggingface to free NFS quota..."
    rm -rf "$HOME/.cache/huggingface" 2>&1 | tail -3 || true
fi

# Bug 12 fix (job 483961): huggingface_hub 0.36.2 xet download protocol
# crashed mid-download with:
#   RuntimeError: Data processing error: File reconstruction error:
#   Internal Writer Error: Background writer channel closed
# Disable xet entirely and use classic HTTP download instead.
export HF_HUB_DISABLE_XET=1
export HF_HUB_ENABLE_HF_TRANSFER=0

# Clear any partial/corrupt download state in the scratch HF cache.
find "${HF_HOME}" -name '*.incomplete' -delete 2>/dev/null || true
find "${HF_HOME}" -name '*.lock' -delete 2>/dev/null || true
rm -rf "${HF_HOME}/xet" 2>/dev/null || true

echo "  HF_HOME=${HF_HOME}"
echo "  HF_HUB_DISABLE_XET=1, partial cache swept"

# Bug 16 fix (job 483969): curl POST to 127.0.0.1:8000 works, but httpcore returns
# EACCES (Errno 13) on POST. Hypothesis: httpx resolves "127.0.0.1" via getaddrinfo
# and the node's netfilter policy returns EACCES on one of the returned families
# (likely IPv6 loopback). Curl prefers IPv4 for literal 127.0.0.1 so never hits it.
# Fix: bind vLLM to 0.0.0.0 and address the client at the node's primary IPv4 — this
# bypasses whatever loopback weirdness is happening for the python TCP path.
VLLM_HOST_IP=$(hostname -I 2>/dev/null | awk '{print $1}')
[ -z "${VLLM_HOST_IP}" ] && VLLM_HOST_IP=127.0.0.1
export VLLM_HOST_IP
echo "  VLLM_HOST_IP=${VLLM_HOST_IP} (bind 0.0.0.0; clients use this IP)"

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
    --host 0.0.0.0 \
    --port 8000 \
    --served-model-name qwen2.5-72b \
    > "${LOG_DIR}/vllm_server.log" 2>&1 &
VLLM_PID=$!
echo "  vLLM PID=${VLLM_PID} — waiting for /v1/models..."

# First-run HF download can take ~45 min; subsequent runs ~3 min
for i in $(seq 1 360); do
    if curl -s -f --max-time 5 http://${VLLM_HOST_IP}:8000/v1/models >/dev/null 2>&1; then
        echo "  vLLM ready at ~${i}0s (host=${VLLM_HOST_IP})"
        curl -s http://${VLLM_HOST_IP}:8000/v1/models | python -m json.tool
        break
    fi
    kill -0 ${VLLM_PID} 2>/dev/null || {
        echo "  vLLM died:"; tail -60 "${LOG_DIR}/vllm_server.log"; exit 1
    }
    [ $((i % 6)) -eq 0 ] && echo "  ...still loading (${i}0s)"
    sleep 10
done

# --- [5.5/7] Bug 16 diagnostic — confirm POST /v1/chat/completions works -
# Bug 16 (job 483967): GET /v1/models returns 200 but run_extraction.py reports
# "Connection error" on every POST /v1/chat/completions. No proxy env vars were set.
# This block runs after vLLM confirms ready and BEFORE the chunk loop, so we capture:
#   1. any inherited proxy state (defensive reset)
#   2. openai / httpx package versions
#   3. raw curl POST to /v1/chat/completions (isolates python client from network)
#   4. openai python client chat.completions.create (matches run_extraction.py path)
# Failure in step 3 = server/network; failure in step 4 only = python client config.
echo "=== [5.5/7] POST smoke test (Bug 16 diagnostic) ==="
unset HTTP_PROXY HTTPS_PROXY http_proxy https_proxy ALL_PROXY all_proxy
export NO_PROXY="127.0.0.1,localhost,0.0.0.0,::1"
export no_proxy="${NO_PROXY}"
echo "  Proxy env after reset: $(env | grep -iE proxy | tr '\n' ' ' || echo 'none')"

python - <<'PYVER' 2>&1 | sed 's/^/    /'
import openai, httpx
print(f"openai=={openai.__version__}")
print(f"httpx=={httpx.__version__}")
PYVER

echo "  Raw-socket reachability test (isolates IPv4 vs IPv6 EACCES):"
VLLM_HOST_IP="${VLLM_HOST_IP}" python - <<'PYSOCK' 2>&1 | sed 's/^/    /'
import os, socket, errno
host = os.environ["VLLM_HOST_IP"]
for label, addr, family in [
    (f"IPv4 {host}:8000",  (host, 8000),          socket.AF_INET),
    ("IPv4 127.0.0.1:8000", ("127.0.0.1", 8000),  socket.AF_INET),
    ("IPv6 ::1:8000",       ("::1", 8000, 0, 0),  socket.AF_INET6),
]:
    try:
        s = socket.socket(family, socket.SOCK_STREAM)
        s.settimeout(3)
        s.connect(addr)
        s.close()
        print(f"  {label}: OK (connect succeeded)")
    except OSError as e:
        code = errno.errorcode.get(e.errno, e.errno)
        print(f"  {label}: FAIL {code} ({e.strerror})")
PYSOCK

# Bug 16 deep diagnostics (v14): login-node Python works fine, so the EACCES
# is specific to the compute-node cgroup. Dump binary info, cgroup, LSM
# status, a control connect to sshd:22, and a control test against a
# Python http.server on port 9000 (same port-range as user code).
echo "  --- Deep diagnostics ---"
echo "  which python / binary:"
ls -la "$(which python)" "$(which python3)" 2>&1 | sed 's/^/    /'
file "$(readlink -f "$(which python)")" 2>&1 | sed 's/^/    /'
getcap "$(readlink -f "$(which python)")" 2>&1 | sed 's/^/    /'
echo "  cgroup:"
cat /proc/self/cgroup 2>&1 | sed 's/^/    /'
echo "  relevant env:"
env | grep -iE 'ld_preload|ld_library|pythonpath|pythonhome|conda|virtual_env|proxy|slurm_job' 2>&1 | sed 's/^/    /' | head -30
echo "  LSM / seccomp:"
(sestatus 2>/dev/null || echo "sestatus n/a") | sed 's/^/    /'
grep -E 'Seccomp|CapEff|CapPrm|NoNewPrivs' /proc/self/status 2>&1 | sed 's/^/    /'
echo "  control: python -> sshd:22 (must succeed):"
python - <<'PYCTL' 2>&1 | sed 's/^/    /'
import socket
try:
    s = socket.socket(); s.settimeout(3); s.connect(("127.0.0.1", 22))
    banner = s.recv(32); s.close()
    print(f"  127.0.0.1:22 OK banner={banner!r}")
except OSError as e:
    print(f"  127.0.0.1:22 FAIL errno={e.errno} {e.strerror}")
PYCTL
echo "  control: python http.server on 9000 + python + curl connects:"
python3 -m http.server 9000 >/dev/null 2>&1 &
HTTPPID=$!
sleep 1
python - <<'PYHTTP' 2>&1 | sed 's/^/    /'
import socket
try:
    s = socket.socket(); s.settimeout(3); s.connect(("127.0.0.1", 9000))
    s.close(); print("  py->127.0.0.1:9000 OK")
except OSError as e:
    print(f"  py->127.0.0.1:9000 FAIL errno={e.errno} {e.strerror}")
PYHTTP
curl -sS -o /dev/null -w "    curl->127.0.0.1:9000 HTTP %{http_code}\n" --max-time 3 http://127.0.0.1:9000/ 2>&1
kill $HTTPPID 2>/dev/null
wait $HTTPPID 2>/dev/null
echo "  strace connect() to vLLM port 8000:"
( strace -f -e trace=connect python - <<'PYSTR'
import socket
try:
    s = socket.socket(); s.connect(("127.0.0.1", 8000)); print("OK")
except OSError as e:
    print(f"FAIL errno={e.errno} {e.strerror}")
PYSTR
) 2>&1 | tail -15 | sed 's/^/    /'
echo "  --- end deep diagnostics ---"

echo "  curl POST /v1/chat/completions (via node IP):"
curl -sS -X POST http://${VLLM_HOST_IP}:8000/v1/chat/completions \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer EMPTY" \
    -d '{"model":"qwen2.5-72b","messages":[{"role":"user","content":"say ok"}],"max_tokens":5}' \
    --max-time 30 2>&1 | head -20 | sed 's/^/    /' || echo "    curl POST FAILED"

echo "  openai python client smoke test (via node IP):"
VLLM_BASE_URL_SMOKE="http://${VLLM_HOST_IP}:8000/v1" \
python - <<'PYCLIENT' 2>&1 | sed 's/^/    /'
import os, traceback
import openai
client = openai.OpenAI(
    base_url=os.environ["VLLM_BASE_URL_SMOKE"],
    api_key=os.environ.get("VLLM_API_KEY", "EMPTY"),
)
try:
    models = client.models.list()
    print(f"models.list() OK: {[m.id for m in models.data]}")
except Exception as e:
    print(f"models.list() FAILED: {type(e).__name__}: {e}")
    if getattr(e, '__cause__', None):
        print(f"  caused by: {type(e.__cause__).__name__}: {e.__cause__}")
    traceback.print_exc()

try:
    resp = client.chat.completions.create(
        model=os.environ.get("VLLM_MODEL_ID", "qwen2.5-72b"),
        messages=[{"role": "user", "content": "say ok"}],
        max_tokens=5,
    )
    print(f"chat.completions OK: {resp.choices[0].message.content!r}")
except Exception as e:
    print(f"chat.completions FAILED: {type(e).__name__}: {e}")
    if getattr(e, '__cause__', None):
        print(f"  caused by: {type(e.__cause__).__name__}: {e.__cause__}")
    traceback.print_exc()
PYCLIENT
echo "=== end [5.5/7] ==="

# --- [6/7] Run extraction with resumable chunking --------------------
echo "=== [6/7] Running extraction ==="
export VLLM_BASE_URL=http://${VLLM_HOST_IP}:8000/v1
export VLLM_MODEL_ID=qwen2.5-72b
export VLLM_API_KEY=EMPTY

# Bug 15/16 fix: stale done-markers from a prior failed run (where every chunk hit
# Connection error but run_extraction.py still exit-0'd) make the loop short-circuit.
# Wipe markers so this run actually re-processes any chunk whose underlying data
# isn't yet in the (now-moved-aside) scratch parquet.
if ls "${DONE_DIR}"/chunk_*.done >/dev/null 2>&1; then
    echo "  Bug15/16: wiping $(ls ${DONE_DIR}/chunk_*.done 2>/dev/null | wc -l) stale done-markers"
    rm -f "${DONE_DIR}"/chunk_*.done
fi
export EXTRACT_MAX_CHUNK_CHARS=32000
export EXTRACT_OP_CHUNK_CHARS=48000

# Bug 15 fix (job 483967): run_extraction.py resumes from an existing output parquet
# and exits 0 when every research_id in a chunk is already present. A stale Ollama-era
# parquet on scratch made every chunk complete in 2-3s without ever calling vLLM.
# Move any pre-existing output parquets aside so this run is a clean slate. The P7
# cleanup hook will archive whatever we produce back to NFS under _${SLURM_JOB_ID}.
echo "  Bug15: clearing any stale output parquets so extraction cannot short-circuit"
for _p in "${REPO_TMP}/processed/note_entities_llm_tg_kinetics.parquet" \
          "${REPO_TMP}/scripts/output/parquet_backup/note_entities_llm_tg_kinetics.parquet"; do
    if [ -f "${_p}" ]; then
        _ts=$(date +%Y%m%d_%H%M%S)
        mv -v "${_p}" "${_p}.pre_awq_${_ts}" || true
    fi
done

# Regenerate chunk files (23 × 250 rids, last = 93)
CHUNK_DIR=${REPO_TMP}/chunks
export CHUNK_DIR   # heredoc below reads via os.environ
mkdir -p "${CHUNK_DIR}"
python - <<'PY'
import pandas as pd, os
repo_tmp = os.environ["REPO_TMP"]
chunk_dir = os.environ["CHUNK_DIR"]
rids = sorted(pd.read_parquet(f"{repo_tmp}/processed/clinical_notes_long.parquet").research_id.unique())
n = 0
for i in range(0, len(rids), 250):
    n = i // 250 + 1
    with open(f"{chunk_dir}/chunk_{n:02d}.txt", "w") as f:
        f.write("\n".join(map(str, rids[i:i+250])) + "\n")
print(f"  Wrote {n} chunks, last size={len(rids[i:i+250])}")
PY

# Dynamic halt — stop 15 min before Slurm kills us
# Bug 14 fix (job 483963): scontrol prints StartTime/EndTime/Deadline on the same line;
# the old `-F= '{print $2}'` picked up the StartTime value. Use grep -oP lookbehind so
# only the EndTime timestamp is captured.
END_TS=$(scontrol show job ${SLURM_JOB_ID} | grep -oP 'EndTime=\K\S+')
START_TS=$(scontrol show job ${SLURM_JOB_ID} | grep -oP 'StartTime=\K\S+')
END_EPOCH=$(date -d "${END_TS}" +%s 2>/dev/null || echo 0)
START_EPOCH=$(date -d "${START_TS}" +%s 2>/dev/null || echo 0)
NOW_EPOCH=$(date +%s)
# Sanity fallback: if EndTime parse failed or came back <= now, assume StartTime + 16h
if [ "${END_EPOCH}" -le "${NOW_EPOCH}" ]; then
    echo "  WARN: parsed EndTime (${END_TS}) <= now; falling back to StartTime+16h"
    END_EPOCH=$((START_EPOCH + 16*3600))
fi
HALT_EPOCH=$((END_EPOCH - 900))
echo "  Slurm StartTime=$(date -d @${START_EPOCH} -Iseconds)"
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
    CODE=$(curl -s -o /dev/null -w "%{http_code}" --max-time 5 http://${VLLM_HOST_IP}:8000/v1/models || echo 000)
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
