#!/usr/bin/env bash
# Job 3 — esophageal_invasion extraction against OPNOTEs only.
set -euo pipefail
cd "$(dirname "$0")/../.."
export DOMAIN=esophageal_invasion
export INPUT_PARQUET="$(pwd)/processed/remaining/round2_20260421/input_opnotes_only.parquet"
exec runs/round2_20260421/run_extraction.sh
