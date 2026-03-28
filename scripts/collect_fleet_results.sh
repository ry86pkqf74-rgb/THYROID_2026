#!/usr/bin/env bash
# collect_fleet_results.sh -- Pull LLM extraction results from ResearchFlow servers
# back to the local THYROID_2026 repo.
#
# Results go to: processed/remaining/note_entities_llm_<domain>.parquet
# Checkpoints:   processed/remaining/note_entities_llm_<domain>.ckpt.jsonl
#
# Usage:
#   bash scripts/collect_fleet_results.sh          # collect all
#   bash scripts/collect_fleet_results.sh --status  # check progress only

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
LOCAL_OUTPUT="$REPO_ROOT/processed/remaining"

SSH_OPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=15"

# Server -> domain mapping (must match deploy_fleet_extraction.sh)
# Format: NAME:IP:PASSWORD:DOMAINS
SERVERS=(
  "S6:38.242.238.209:Testros13:complications recurrence"
  "S2:195.26.254.65:Testros13:genetics"
  "S3:195.26.251.218:Testros13:medications"
  "S1:217.77.2.114:Testros13:staging"
  "S7:209.126.13.57:Testros13:procedures"
  "S8:207.244.235.10:sLwYPHhrrsr8q6ZRWKcA:problem_list"
)

REMOTE_OUTPUT="/opt/thyroid_extraction/output"

log() { echo "[$(date +%H:%M:%S)] $*"; }

check_status() {
  log "=== Fleet Extraction Status ==="
  local total_done=0
  local total_notes=11037
  for entry in "${SERVERS[@]}"; do
    IFS=: read -r name ip pass domains <<< "$entry"
    log "--- $name ($ip) | domains: $domains ---"
    result=$(sshpass -p "$pass" ssh $SSH_OPTS "root@$ip" "
      for f in $REMOTE_OUTPUT/note_entities_llm_*.ckpt.jsonl; do
        if [ -f \"\$f\" ]; then
          domain=\$(basename \"\$f\" | sed 's/note_entities_llm_//;s/.ckpt.jsonl//')
          count=\$(wc -l < \"\$f\")
          echo \"  \$domain: \$count / $total_notes notes\"
        fi
      done
      if pgrep -f run_extraction_split > /dev/null 2>&1; then
        echo '  Process: RUNNING'
      else
        echo '  Process: STOPPED'
      fi
    " 2>&1) || result="  UNREACHABLE"
    echo "$result"
    echo ""
  done
}

collect_results() {
  log "=== Collecting extraction results to $LOCAL_OUTPUT ==="
  mkdir -p "$LOCAL_OUTPUT"

  for entry in "${SERVERS[@]}"; do
    IFS=: read -r name ip pass domains <<< "$entry"
    log "--- $name ($ip) ---"

    # Copy parquet files
    for domain in $domains; do
      parquet="note_entities_llm_${domain}.parquet"
      ckpt="note_entities_llm_${domain}.ckpt.jsonl"

      # Check if parquet exists on remote
      exists=$(sshpass -p "$pass" ssh $SSH_OPTS "root@$ip" "test -f $REMOTE_OUTPUT/$parquet && echo yes || echo no" 2>&1)
      if [ "$exists" = "yes" ]; then
        log "  Copying $parquet ..."
        sshpass -p "$pass" scp $SSH_OPTS "root@$ip:$REMOTE_OUTPUT/$parquet" "$LOCAL_OUTPUT/$parquet" 2>&1
        log "  OK: $parquet -> $LOCAL_OUTPUT/"
      else
        log "  SKIP: $parquet not ready yet"
      fi

      # Always copy checkpoint (for progress tracking)
      ckpt_exists=$(sshpass -p "$pass" ssh $SSH_OPTS "root@$ip" "test -f $REMOTE_OUTPUT/$ckpt && echo yes || echo no" 2>&1)
      if [ "$ckpt_exists" = "yes" ]; then
        sshpass -p "$pass" scp $SSH_OPTS "root@$ip:$REMOTE_OUTPUT/$ckpt" "$LOCAL_OUTPUT/$ckpt" 2>&1
        lines=$(wc -l < "$LOCAL_OUTPUT/$ckpt" 2>/dev/null || echo 0)
        log "  Checkpoint: $ckpt ($lines notes)"
      fi
    done
    echo ""
  done

  # Merge all domain parquets into combined if enough exist
  local count=$(ls "$LOCAL_OUTPUT"/note_entities_llm_*.parquet 2>/dev/null | grep -v combined | grep -v tmp | wc -l)
  log "=== $count domain parquet files collected ==="
  if [ "$count" -ge 2 ]; then
    log "Run the extraction script with --merge-only to combine (or wait for all 7 domains)"
  fi
}

# Main
case "${1:-}" in
  --status) check_status ;;
  *)        collect_results ;;
esac
