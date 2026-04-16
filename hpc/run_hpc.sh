#!/bin/bash
set -euo pipefail

echo "[1/8] Setting up Python 3.11 ..."
module load anaconda3 2>/dev/null || module load miniconda3 2>/dev/null || module load conda 2>/dev/null || true
if ! command -v conda &> /dev/null; then
  wget -q https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O /tmp/miniconda.sh
  bash /tmp/miniconda.sh -b -p "${HOME}/miniconda3"
  eval "$(${HOME}/miniconda3/bin/conda shell.bash hook)"
fi
if ! conda env list 2>/dev/null | grep -q "thyroid_vllm"; then
  conda create -y -n thyroid_vllm python=3.11
fi
conda activate thyroid_vllm
echo "  Python: $(python3 --version)"

echo "[2/8] Installing vLLM and deps ..."
pip install --quiet vllm openai pyarrow pandas pyyaml 2>&1 | tail -3
python3 -c "import torch; n=torch.cuda.device_count(); print(f'  CUDA: {torch.cuda.is_available()}, GPUs: {n}'); [print(f'    {i}: {torch.cuda.get_device_name(i)}') for i in range(n)]"

echo "[3/8] Setting up repo ..."
WORK_DIR="${HOME}/thyroid_extract"
REPO_DIR="${WORK_DIR}/THYROID_2026"
mkdir -p "${WORK_DIR}"
if [ -d "${REPO_DIR}/.git" ]; then
  cd "${REPO_DIR}" && git pull --ff-only
else
  cd "${WORK_DIR}" && git clone https://github.com/ry86pkqf74-rgb/THYROID_2026.git
fi
cd "${REPO_DIR}"

echo "[4/8] Checking data ..."
NOTES="${REPO_DIR}/processed/clinical_notes_long.parquet"
if [ ! -f "${NOTES}" ]; then
  echo "  MISSING: ${NOTES}"
  echo "  Upload via OnDemand file manager, then re-run."
  exit 1
fi
echo "  Notes: $(python3 -c "import pyarrow.parquet as pq; print(pq.read_metadata('${NOTES}').num_rows)") rows"

echo "[5/8] Launching vLLM (2-5 min to load 72B model) ..."
VLLM_MODEL="Qwen/Qwen2.5-72B-Instruct-AWQ"
GPU_COUNT=$(python3 -c "import torch; print(torch.cuda.device_count())")
if [ "${GPU_COUNT}" -ge 8 ]; then GPUS="4,5,6,7"; else GPUS="0,1,2,3"; fi
echo "  Using GPUs: ${GPUS}"
pkill -f "vllm.entrypoints" 2>/dev/null || true
sleep 2
CUDA_VISIBLE_DEVICES=${GPUS} python3 -m vllm.entrypoints.openai.api_server \
  --model "${VLLM_MODEL}" --quantization awq --tensor-parallel-size 4 \
  --max-model-len 32768 --gpu-memory-utilization 0.90 \
  --host 0.0.0.0 --port 8000 --dtype float16 \
  --trust-remote-code --disable-log-requests \
  > "${WORK_DIR}/vllm.log" 2>&1 &
VLLM_PID=$!
echo "  PID: ${VLLM_PID}, waiting ..."

for i in $(seq 1 120); do
  if curl -s --max-time 5 http://localhost:8000/v1/models | python3 -c "import sys,json; d=json.load(sys.stdin); sys.exit(0 if d.get('data') else 1)" 2>/dev/null; then
    echo "  vLLM ready! (~${i}0s)"; break
  fi
  kill -0 ${VLLM_PID} 2>/dev/null || { echo "  DIED:"; tail -20 "${WORK_DIR}/vllm.log"; exit 1; }
  [ $((i%6)) -eq 0 ] && echo "  ...loading (${i}0s)"
  sleep 10
done

echo "[6/8] Smoke test ..."
python3 -c "
import openai,json
c=openai.OpenAI(api_key='dummy',base_url='http://localhost:8000/v1')
r=c.chat.completions.create(model='Qwen/Qwen2.5-72B-Instruct-AWQ',messages=[{'role':'system','content':'Extract as JSON:{\"entities\":[...]}'},{'role':'user','content':'Tg 2.3 ng/mL on 2024-01-15, BRAF V600E.'}],temperature=0,response_format={'type':'json_object'},max_tokens=500)
d=json.loads(r.choices[0].message.content)
print(f'  OK: {len(d.get(\"entities\",[]))} entities')
"

echo "[7/8] Extracting 5 domains ..."
RESULTS_DIR="${WORK_DIR}/results_$(date +%Y%m%d_%H%M%S)"
mkdir -p "${RESULTS_DIR}"
export VLLM_BASE_URL="http://localhost:8000/v1"
export VLLM_MODEL_ID="Qwen/Qwen2.5-72B-Instruct-AWQ"
export VLLM_API_KEY="dummy"
export EXTRACT_MAX_CHUNK_CHARS=32000
export EXTRACT_OP_CHUNK_CHARS=48000
cd "${REPO_DIR}"

for D in synoptic_pathology_enrichment tg_kinetics dynamic_risk_response us_nodule_dynamics presenting_symptoms; do
  echo "=== ${D} === $(date +%H:%M:%S)"
  python3 llm_extraction/run_extraction.py --target "${D}" --workers 2 2>&1 | tee "${RESULTS_DIR}/${D}.log"
  echo ""
done

echo "[8/8] Packaging ..."
for D in synoptic_pathology_enrichment tg_kinetics dynamic_risk_response us_nodule_dynamics presenting_symptoms; do
  PQ="${REPO_DIR}/processed/note_entities_llm_${D}.parquet"
  [ -f "${PQ}" ] && cp "${PQ}" "${RESULTS_DIR}/" && echo "  ${D}: $(python3 -c "import pyarrow.parquet as pq;print(pq.read_metadata('${PQ}').num_rows)") rows"
done
tar czf "${WORK_DIR}/thyroid_results.tar.gz" -C "${RESULTS_DIR}" .
echo "DONE! Results: ${RESULTS_DIR}/"
echo "Tarball: ${WORK_DIR}/thyroid_results.tar.gz"
kill ${VLLM_PID} 2>/dev/null || true
