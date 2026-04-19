#!/usr/bin/env bash
# ship_v2.sh — run this ONCE the driver has written 8,810 rows.
#
# What it does, in order:
#   1. Verify ckpt has 8,810 rows (no silent partial ship).
#   2. Run sanitize_full_v2.sh → emits definitive nodules_clean.parquet
#      and reports_clean.parquet into runs/tirads_granular/full_v2_output/.
#   3. Stage exactly the v2-relevant files (sanitizer, prompt, wrapper,
#      flattener, registry bump, driver bump, final parquets) and commit.
#   4. Print next-step instructions (push + Vast.ai teardown).
#
# Does NOT push; does NOT destroy Vast.ai; does NOT write MotherDuck.
# Those are explicit one-liners printed at the end so you can review the
# commit before it leaves the Mac.

set -euo pipefail

cd "$(dirname "$0")/../.."
ROOT="$(pwd)"

CKPT="runs/tirads_granular/full_v2_output/note_entities_llm_tirads_granular.ckpt.jsonl"
NOD_OUT="runs/tirads_granular/full_v2_output/nodules_clean.parquet"
REP_OUT="runs/tirads_granular/full_v2_output/reports_clean.parquet"
TARGET_ROWS=8810

log() { printf '\033[1;34m[ship_v2]\033[0m %s\n' "$*"; }
die() { printf '\033[1;31m[ship_v2]\033[0m %s\n' "$*" >&2; exit 1; }

# ─── step 1: guard ──────────────────────────────────────────────────
[[ -f "$CKPT" ]] || die "ckpt missing: $CKPT"
ROWS=$(wc -l < "$CKPT" | tr -d ' ')
log "ckpt rows: $ROWS / $TARGET_ROWS"
if [[ "$ROWS" -lt "$TARGET_ROWS" ]]; then
    die "ckpt only has $ROWS rows (<$TARGET_ROWS). Driver not finished — refusing to ship partial."
fi
if [[ "$ROWS" -gt "$TARGET_ROWS" ]]; then
    log "warn: ckpt has $ROWS rows (>$TARGET_ROWS). Proceeding — sanitizer will dedup by note_row_id."
fi

# ─── step 2: sanitize ───────────────────────────────────────────────
log "running sanitize_full_v2.sh ..."
bash runs/tirads_granular/sanitize_full_v2.sh
[[ -f "$NOD_OUT" ]] || die "sanitizer did not produce $NOD_OUT"
[[ -f "$REP_OUT" ]] || die "sanitizer did not produce $REP_OUT"
log "parquet sizes:"
ls -lh "$NOD_OUT" "$REP_OUT"

# ─── step 3: stage + commit ─────────────────────────────────────────
log "git status before staging:"
git status --short | head -20

log "staging v2 pipeline files ..."
# new files
git add llm_extraction/sanitize_tirads_v2.py
git add llm_extraction/prompts/tirads_granular_extraction_v2.txt
git add runs/tirads_granular/sanitize_full_v2.sh
git add runs/tirads_granular/ship_v2.sh
git add scripts/flatten_tirads_us.py
# final artifacts
git add -f "$NOD_OUT" "$REP_OUT"
# tracked modifications
git add config/extraction_domain_registry.yaml
git add scripts/vastai/run_extraction_concurrent.py

log "files staged for commit:"
git diff --cached --stat

COMMIT_MSG_FILE=$(mktemp)
cat > "$COMMIT_MSG_FILE" <<'MSG'
feat(tirads v2): ACR+ATA+Kwak+EU granular extraction + sanitizer + 8,810-note run

Pipeline:
  - prompts/tirads_granular_extraction_v2.txt: schema covering ACR TI-RADS 2017,
    ATA 2015 sonographic pattern, Kwak-TIRADS, EU-TIRADS, plus longitudinal
    nodule tracking (pole/position anchors, comparison_statement, etc.).
  - scripts/flatten_tirads_us.py: flattens us_nodules_tirads.parquet (wide,
    us_1..us_14) → one row per (research_id, us_N) exam for the driver.
  - scripts/vastai/run_extraction_concurrent.py: expand note_text cap 6k→12k
    and max_tokens 1.5k→12k to fit the v2 per-nodule schema output.
  - config/extraction_domain_registry.yaml: point tirads_granular at v2 prompt.
  - llm_extraction/sanitize_tirads_v2.py: post-processor — enum coercion,
    LN-leak filter, post-thyroidectomy fossa filter, unit-bug rescaler
    (×10/×100/×1000), emits nodules_clean.parquet + reports_clean.parquet.
  - runs/tirads_granular/sanitize_full_v2.sh: one-shot wrapper.
  - runs/tirads_granular/ship_v2.sh: end-to-end ship guard + sanitize + stage.

Run:
  - Qwen/Qwen2.5-32B-Instruct-AWQ on Vast.ai H200 NVL via vLLM (prefix caching,
    chunked-prefill, max-num-seqs=64, client concurrency=48, gpu_mem_util=0.92).
  - 8,810 ultrasound reports processed; final outputs committed.
MSG

log "committing ..."
git commit -F "$COMMIT_MSG_FILE"
rm -f "$COMMIT_MSG_FILE"

log "HEAD after commit:"
git log --oneline -1

# ─── step 4: next steps ─────────────────────────────────────────────
cat <<'NEXT'

────────────────────────────────────────────────────────────────────
ship_v2.sh done. Next steps (review before running):

  1. Push to origin:
         git push origin main

  2. Destroy Vast.ai instance:
         vastai destroy instance 35183957

MotherDuck: NOT touched by this script. If you want these parquets in
MotherDuck, say so explicitly — the "no MotherDuck writes" rule from
the start of the project still applies until you lift it.
────────────────────────────────────────────────────────────────────
NEXT
