#!/usr/bin/env bash
# Deploy 6-host Vast.ai fleet for targeted TI-RADS/LN/FNA extraction.
# Each host gets one shard (00..05) of the combined notes parquet.
#
# Usage (from Mac in repo root):
#   bash scripts/vastai/deploy_fleet.sh
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

# Host → (user@host, port, shard-index) mapping.
# Fill in after `vastai ssh-url <id>` once all instances are "running".
# NOTE: update this table after each provisioning run.
HOSTS=(
  "root@192.222.53.66 27154 00 34897258"
  "root@ssh2.vast.ai  18808 01 34898808"
  "root@ssh3.vast.ai  18810 02 34898810"
  "root@ssh9.vast.ai  18810 03 34898811"
  "root@ssh7.vast.ai  18814 04 34898814"
  "root@ssh1.vast.ai  18814 05 34898815"
)

deploy_one() {
    local target="$1" port="$2" shard="$3" vid="$4"
    local logfile="/tmp/thyroid_fleet_deploy_${vid}.log"
    echo "[${vid} shard=${shard}] deploying → ${target}:${port}  (log: ${logfile})"
    {
        # 1) upload bootstrap + shard + scripts
        scp -P "$port" -o StrictHostKeyChecking=no \
            "scripts/vastai/bootstrap_rerun.sh" \
            "scripts/vastai/run_shard.sh" \
            "$target:/root/"

        scp -P "$port" -o StrictHostKeyChecking=no \
            "processed/remaining/shards/clinical_notes_shard_${shard}of06.parquet" \
            "$target:/root/clinical_notes_shard.parquet"

        # 2) run bootstrap (clone repo, install ollama, pull model)
        ssh -p "$port" -o StrictHostKeyChecking=no "$target" "bash /root/bootstrap_rerun.sh" || true

        # 3) move shard into position + launch
        ssh -p "$port" -o StrictHostKeyChecking=no "$target" \
            "mkdir -p /root/THYROID_2026/processed/remaining && \
             mv /root/clinical_notes_shard.parquet \
                /root/THYROID_2026/processed/remaining/clinical_notes_shard.parquet && \
             cp /root/run_shard.sh /root/THYROID_2026/scripts/vastai/run_shard.sh && \
             chmod +x /root/THYROID_2026/scripts/vastai/run_shard.sh && \
             nohup bash /root/THYROID_2026/scripts/vastai/run_shard.sh > /var/log/run_shard.log 2>&1 < /dev/null &
             disown ; sleep 3 ; ps -ef | grep run_extraction | grep -v grep"
    } > "$logfile" 2>&1 && echo "[${vid} shard=${shard}] LAUNCHED" || echo "[${vid} shard=${shard}] FAILED (see ${logfile})"
}

# Deploy all in parallel
for line in "${HOSTS[@]}"; do
    set -- $line
    deploy_one "$1" "$2" "$3" "$4" &
done
wait
echo "==> all deploys dispatched"
