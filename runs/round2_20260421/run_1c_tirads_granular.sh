#!/usr/bin/env bash
# Job 1c — re-extract note_entities_llm_tirads_granular on qwen2.5-32b.
set -euo pipefail
cd "$(dirname "$0")/../.."
export DOMAIN=tirads_granular
export INPUT_PARQUET="$(pwd)/processed/remaining/round2_20260421/input_clinical_notes_long.parquet"
exec runs/round2_20260421/run_extraction.sh
