#!/usr/bin/env bash
# deploy_fleet_extraction.sh -- Deploy LLM extraction across ResearchFlow servers.
#
# Sends the extraction script, input data, and prompts to each server,
# then launches extraction with assigned domains in the background (nohup).
#
# Domain assignment (7 domains across 8 servers, balancing by server power):
#   S6 (48C/125G) -> complications, recurrence  (2 domains, biggest server)
#   S1 (18C/94G)  -> staging
#   S2 (18C/94G)  -> genetics
#   S3 (18C/94G)  -> medications
#   S7 (24C/61G)  -> procedures
#   S8 (24C/61G)  -> problem_list
#   S4, S5 -> standby (available for re-runs or catch-up)
#
# Usage:
#   bash scripts/deploy_fleet_extraction.sh          # deploy + launch all
#   bash scripts/deploy_fleet_extraction.sh --check  # check status on all servers

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"

REMOTE_DIR="/opt/thyroid_extraction"
INPUT_PARQUET="$REPO_ROOT/processed/remaining/clinical_notes_long.parquet"
EXTRACTION_SCRIPT="$REPO_ROOT/scripts/run_extraction_split.py"
PROMPTS_DIR="$REPO_ROOT/llm_extraction/prompts"

# Server definitions: NAME:IP:PASSWORD:DOMAINS
SERVERS=(
  "S6:38.242.238.209:Testros13:complications recurrence"
  "S1:217.77.2.114:Testros13:staging"
  "S2:195.26.254.65:Testros13:genetics"
  "S3:195.26.251.218:Testros13:medications"
  "S7:209.126.13.57:Testros13:procedures"
  "S8:207.244.235.10:sLwYPHhrrsr8q6ZRWKcA:problem_list"
)

SSH_OPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=15"

log() { echo "[$(date +%H:%M:%S)] $*"; }

ssh_cmd() {
  local ip="$1" pass="$2" cmd="$3"
  sshpass -p "$pass" ssh $SSH_OPTS "root@$ip" "$cmd" 2>&1
}

scp_cmd() {
  local ip="$1" pass="$2" src="$3" dst="$4"
  sshpass -p "$pass" scp $SSH_OPTS -r "$src" "root@$ip:$dst" 2>&1
}

check_status() {
  log "=== Checking extraction status across fleet ==="
  for entry in "${SERVERS[@]}"; do
    IFS=: read -r name ip pass domains <<< "$entry"
    log "--- $name ($ip) ---"
    ssh_cmd "$ip" "$pass" "
      if [ -d $REMOTE_DIR ]; then
        echo 'Extraction dir: exists'
        for f in $REMOTE_DIR/processed/output/note_entities_llm_*.ckpt.jsonl; do
          if [ -f \"\$f\" ]; then
            domain=\$(basename \"\$f\" | sed 's/note_entities_llm_//;s/.ckpt.jsonl//')
            count=\$(wc -l < \"\$f\")
            echo \"  \$domain: \$count notes done\"
          fi
        done
        # Check if extraction process is running
        if pgrep -f 'run_extraction_split' > /dev/null 2>&1; then
          echo 'Process: RUNNING'
          ps aux | grep run_extraction_split | grep -v grep | awk '{print \"  PID:\", \$2, \"CPU:\", \$3\"%\", \"MEM:\", \$4\"%\", \"TIME:\", \$10}'
        else
          echo 'Process: NOT RUNNING'
        fi
      else
        echo 'Extraction dir: not deployed yet'
      fi
    " || log "  $name: unreachable"
    echo ""
  done
}

deploy_and_run() {
  for entry in "${SERVERS[@]}"; do
    IFS=: read -r name ip pass domains <<< "$entry"
    log "=== Deploying to $name ($ip) -- domains: $domains ==="

    # Create remote directory structure
    ssh_cmd "$ip" "$pass" "mkdir -p $REMOTE_DIR/prompts $REMOTE_DIR/processed/output $REMOTE_DIR/processed/remaining" || {
      log "  FAILED to create dirs on $name"; continue
    }

    # Copy extraction script
    scp_cmd "$ip" "$pass" "$EXTRACTION_SCRIPT" "$REMOTE_DIR/run_extraction_split.py" || {
      log "  FAILED to copy script to $name"; continue
    }

    # Copy input parquet
    log "  Copying input data (24 MB) ..."
    scp_cmd "$ip" "$pass" "$INPUT_PARQUET" "$REMOTE_DIR/clinical_notes_long.parquet" || {
      log "  FAILED to copy data to $name"; continue
    }

    # Copy prompt files
    scp_cmd "$ip" "$pass" "$PROMPTS_DIR/" "$REMOTE_DIR/prompts/" || {
      log "  FAILED to copy prompts to $name"; continue
    }

    # Install Python dependencies if needed (try --break-system-packages, fall back without)
    ssh_cmd "$ip" "$pass" "pip install pandas pyarrow openai tenacity python-dotenv --break-system-packages -q 2>&1 || pip install pandas pyarrow openai tenacity python-dotenv -q 2>&1 || pip3 install pandas pyarrow openai tenacity python-dotenv -q 2>&1 || true" || true

    # Kill any existing extraction process
    ssh_cmd "$ip" "$pass" "pkill -f 'run_extraction_split' 2>/dev/null || true"

    # Create a minimal wrapper that sets up paths correctly
    ssh_cmd "$ip" "$pass" "cat > $REMOTE_DIR/run.sh << 'WRAPPER'
#!/bin/bash
cd $REMOTE_DIR
# The script expects repo structure; create symlinks
mkdir -p processed/remaining llm_extraction
ln -sf $REMOTE_DIR/clinical_notes_long.parquet processed/remaining/clinical_notes_long.parquet
ln -sf $REMOTE_DIR/prompts llm_extraction/prompts

python3 run_extraction_split.py \\
  --url http://localhost:11434/v1 \\
  --model qwen3:14b \\
  --domains \$@ \\
  --output-dir $REMOTE_DIR/processed/output \\
  --input-parquet $REMOTE_DIR/processed/remaining/clinical_notes_long.parquet

echo \"Extraction finished at \$(date)\" >> $REMOTE_DIR/extraction.log
WRAPPER
chmod +x $REMOTE_DIR/run.sh"

    # Launch extraction in background
    log "  Launching extraction: domains=[$domains] ..."
    ssh_cmd "$ip" "$pass" "nohup bash $REMOTE_DIR/run.sh $domains > $REMOTE_DIR/extraction.log 2>&1 &"

    log "  $name deployed and running!"
    echo ""
  done

  log "=== All servers deployed! ==="
  log "Check status with: bash scripts/deploy_fleet_extraction.sh --check"
}

# Main
if [[ "${1:-}" == "--check" ]]; then
  check_status
else
  deploy_and_run
fi
