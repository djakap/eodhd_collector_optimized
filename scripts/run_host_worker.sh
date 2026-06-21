#!/usr/bin/env bash
# Start Prefect host worker for flows that need Docker/host filesystem access.
# Runs alongside the Docker-based worker — handles deployments targeting 'host-pool'.
#
# Usage:
#   ./scripts/run_host_worker.sh          # run in foreground
#   ./scripts/run_host_worker.sh &        # run in background

set -e

cd "$(dirname "$0")/.."

export PREFECT_API_URL="http://localhost:4200/api"

echo "Creating host-pool (ignore error if already exists)..."
prefect work-pool create host-pool --type process 2>/dev/null || true

echo "Deploying backup-copy flow to host-pool..."
prefect deploy flows/backup_copy_flow.py:backup_copy_flow \
  --name backup-copy \
  --pool host-pool \
  --param latest_only=true 2>/dev/null || true

echo ""
echo "Starting host worker (host-pool)..."
echo "Keep this terminal open, or run with: nohup ./scripts/run_host_worker.sh > /tmp/host_worker.log 2>&1 &"
echo ""
prefect worker start --pool host-pool
