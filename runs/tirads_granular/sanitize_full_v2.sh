#!/usr/bin/env bash
# One-shot: sanitize the latest v2 ckpt into clean parquet pairs.
# Safe to run while the driver is still writing — reads the ckpt as a
# snapshot and emits into a fresh pair of files each time.
set -euo pipefail

cd "$(dirname "$0")/../.."

OUT_DIR="runs/tirads_granular/full_v2_output"
CKPT="$OUT_DIR/note_entities_llm_tirads_granular.ckpt.jsonl"
NOD_OUT="$OUT_DIR/nodules_clean.parquet"
REP_OUT="$OUT_DIR/reports_clean.parquet"

if [[ ! -f "$CKPT" ]]; then
    echo "ckpt not found: $CKPT" >&2
    exit 2
fi

.venv/bin/python -m llm_extraction.sanitize_tirads_v2 \
    --input       "$CKPT" \
    --out-nodules "$NOD_OUT" \
    --out-reports "$REP_OUT"
