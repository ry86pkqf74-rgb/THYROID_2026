# H200 NVL — 9domain_v4 rerun (2026-04-19)
# Offer 19464089 → contract 35234131, France FR, $1.94/hr (cheaper than prior $2.28)
# NOTE: this offer does NOT expose vast SSH proxy. Driver runs on Mac against remote HTTP.
export VAST_INSTANCE_ID=35234131
export VAST_PUBLIC_IP=213.5.130.43
export VAST_VLLM_EXT_PORT=20049
export VLLM_URL=http://213.5.130.43:20049/v1
export VLLM_MODEL=qwen2.5-32b
export VLLM_MAX_MODEL_LEN=32768
