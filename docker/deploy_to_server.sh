#!/bin/bash
# Deploy Docker LangChain extraction pipeline to a remote server
# Usage: ./deploy_to_server.sh <server_ip> <ssh_password> [ssh_port] [domains...]
#
# Example:
#   ./deploy_to_server.sh 77.42.85.109 Testros13 22 vascular_invasion rai_detailed
#   ./deploy_to_server.sh 107.206.71.138 "" 43384  # V2, key-based auth

set -e

SERVER_IP="${1:?Usage: $0 <ip> <password> [port] [domains...]}"
SSH_PASS="${2:-}"
SSH_PORT="${3:-22}"
shift 3 2>/dev/null || true
DOMAINS="${*:-imaging pathology labs}"

# SSH command helper
if [ -n "$SSH_PASS" ]; then
    SSH_CMD="sshpass -p '$SSH_PASS' ssh -o StrictHostKeyChecking=no -p $SSH_PORT root@$SERVER_IP"
    SCP_CMD="sshpass -p '$SSH_PASS' scp -o StrictHostKeyChecking=no -P $SSH_PORT"
else
    SSH_CMD="ssh -o StrictHostKeyChecking=no -p $SSH_PORT root@$SERVER_IP"
    SCP_CMD="scp -o StrictHostKeyChecking=no -P $SSH_PORT"
fi

REMOTE_DIR="/opt/thyroid_extraction"

echo "=== Deploying LangChain Docker pipeline to $SERVER_IP:$SSH_PORT ==="
echo "    Domains: $DOMAINS"

# 1. Create remote directory structure
echo "--- Creating directories ---"
eval $SSH_CMD "'mkdir -p $REMOTE_DIR/{docker,scripts,notes_extraction_new/prompts,processed/remaining,output}'"

# 2. Copy files
echo "--- Copying Docker files ---"
SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
eval $SCP_CMD "$SCRIPT_DIR/docker/Dockerfile" "root@$SERVER_IP:$REMOTE_DIR/docker/"
eval $SCP_CMD "$SCRIPT_DIR/docker/requirements.txt" "root@$SERVER_IP:$REMOTE_DIR/docker/"
eval $SCP_CMD "$SCRIPT_DIR/docker/docker-compose.yml" "root@$SERVER_IP:$REMOTE_DIR/docker/"
eval $SCP_CMD "$SCRIPT_DIR/scripts/langchain_extraction.py" "root@$SERVER_IP:$REMOTE_DIR/scripts/"
eval $SCP_CMD "$SCRIPT_DIR/scripts/run_extraction_split.py" "root@$SERVER_IP:$REMOTE_DIR/scripts/"

echo "--- Copying prompt files ---"
eval $SCP_CMD -r "$SCRIPT_DIR/notes_extraction_new/prompts/" "root@$SERVER_IP:$REMOTE_DIR/notes_extraction_new/prompts/"

# 3. Build Docker image
echo "--- Building Docker image ---"
eval $SSH_CMD "'cd $REMOTE_DIR && docker build -f docker/Dockerfile -t thyroid-extractor:latest .'"

# 4. Run extraction
echo "--- Starting extraction: $DOMAINS ---"
DOMAIN_ARGS=""
for d in $DOMAINS; do DOMAIN_ARGS="$DOMAIN_ARGS $d"; done

eval $SSH_CMD "'cd $REMOTE_DIR && docker run -d \
    --name thyroid-langchain-\$(date +%s) \
    -v $REMOTE_DIR/processed/remaining:/app/processed/remaining:ro \
    -v $REMOTE_DIR/output:/app/output \
    -v $REMOTE_DIR/notes_extraction_new/prompts:/app/notes_extraction_new/prompts:ro \
    --add-host host.docker.internal:host-gateway \
    -e OLLAMA_BASE_URL=http://host.docker.internal:11434/v1 \
    -e OLLAMA_MODEL=qwen3:14b \
    thyroid-extractor:latest \
    --domains $DOMAIN_ARGS \
    --output-dir /app/output \
    --input-parquet /app/processed/remaining/clinical_notes_long.parquet'"

echo "=== Deployment complete ==="
echo "    Monitor: ssh root@$SERVER_IP -p $SSH_PORT 'docker logs -f thyroid-langchain-*'"
