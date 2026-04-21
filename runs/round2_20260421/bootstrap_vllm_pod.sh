#!/usr/bin/env bash
# Bootstrap vLLM on a freshly-provisioned RunPod pytorch pod.
# Runs ON the pod (over SSH), not locally. Idempotent — safe to re-run.
#
# After this finishes you should see:
#   curl -s http://localhost:8000/v1/models -H "Authorization: Bearer thyroid2026"
# returning {"object":"list","data":[{"id":"qwen2.5-32b",...}]}
#
# Usage (from Mac):
#   scp -i $KEY -P $PORT bootstrap_vllm_pod.sh root@$HOST:/root/
#   ssh ... 'bash /root/bootstrap_vllm_pod.sh'
set -euo pipefail

MODEL_REPO="Qwen/Qwen2.5-32B-Instruct"
SERVED_NAME="qwen2.5-32b"
API_KEY="thyroid2026"
PORT=8000

# Redirect HF cache to the 150 GB network volume.
export HF_HOME=/workspace/hf_cache
export HUGGINGFACE_HUB_CACHE=/workspace/hf_cache
mkdir -p "$HF_HOME"

echo "[bootstrap] vllm check"
if ! python3 -c 'import vllm' 2>/dev/null; then
    echo "[bootstrap] installing vllm"
    pip install --no-cache-dir --break-system-packages 'vllm>=0.6.3' 2>&1 | tail -20
fi
python3 -c 'import vllm; print("vllm", vllm.__version__)'

echo "[bootstrap] checking existing model download"
MODEL_DIR="$HF_HOME/hub/models--Qwen--Qwen2.5-32B-Instruct"
if [ ! -d "$MODEL_DIR/snapshots" ] || [ -z "$(ls -A "$MODEL_DIR/snapshots" 2>/dev/null)" ]; then
    echo "[bootstrap] downloading $MODEL_REPO (~65 GB) to $HF_HOME"
    python3 -m huggingface_hub download "$MODEL_REPO" --cache-dir "$HF_HOME" 2>&1 | tail -10 || {
        echo "[bootstrap] hf_hub download CLI failed, falling back to snapshot_download"
        python3 - <<PY
from huggingface_hub import snapshot_download
snapshot_download("$MODEL_REPO", cache_dir="$HF_HOME", max_workers=8)
PY
    }
else
    echo "[bootstrap] model already cached at $MODEL_DIR"
fi

echo "[bootstrap] existing vllm process?"
if pgrep -f 'vllm.entrypoints.openai.api_server' > /dev/null; then
    echo "[bootstrap] vllm already running"
    pgrep -fa vllm.entrypoints.openai.api_server || true
else
    echo "[bootstrap] launching vllm in background"
    mkdir -p /workspace/logs
    nohup python3 -m vllm.entrypoints.openai.api_server \
        --model "$MODEL_REPO" \
        --served-model-name "$SERVED_NAME" \
        --host 0.0.0.0 --port "$PORT" \
        --api-key "$API_KEY" \
        --dtype auto \
        --max-model-len 16384 \
        --gpu-memory-utilization 0.92 \
        > /workspace/logs/vllm.log 2>&1 &
    disown
    echo "[bootstrap] vllm launched, pid=$!"
fi

echo "[bootstrap] waiting for /v1/models to respond (up to 10 min while weights load)"
for i in $(seq 1 120); do
    code="$(curl -s -o /tmp/probe.json -w '%{http_code}' \
        --max-time 5 -H "Authorization: Bearer $API_KEY" \
        "http://localhost:$PORT/v1/models" 2>/dev/null || true)"
    if [ "$code" = "200" ]; then
        echo "[bootstrap] vllm READY after ${i}x5s"
        cat /tmp/probe.json
        exit 0
    fi
    sleep 5
done

echo "[bootstrap] vllm NOT READY after 10 min — tailing log"
tail -80 /workspace/logs/vllm.log
exit 2
