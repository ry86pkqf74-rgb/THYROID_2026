#!/usr/bin/env bash
# Launch vLLM serving Qwen2.5-32B on the RunPod pod.
# Staged to /root/launch_vllm.sh, invoked by
#   ssh pod "nohup bash /root/launch_vllm.sh >/dev/null 2>&1 &"
set -euo pipefail
export HF_HOME=/workspace/hf_cache
mkdir -p /workspace/logs
cd /workspace
exec python3 -m vllm.entrypoints.openai.api_server \
    --model Qwen/Qwen2.5-32B-Instruct \
    --served-model-name qwen2.5-32b \
    --host 0.0.0.0 --port 8000 \
    --api-key thyroid2026 \
    --dtype auto \
    --max-model-len 32768 \
    --gpu-memory-utilization 0.92 \
    > /workspace/logs/vllm.log 2>&1
