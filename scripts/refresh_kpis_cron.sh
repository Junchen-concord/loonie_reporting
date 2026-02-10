#!/bin/bash

# Wrapper script for `python -m loonie_reporting.refresh_kpis` to be run via cron.
# - Ensures correct working directory
# - Writes logs to ./logs
# - Activates venv if present

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
REPO_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"

cd "$REPO_ROOT" || exit 1

LOG_DIR="$REPO_ROOT/logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/refresh_kpis_$(date +%Y%m%d_%H%M%S).log"

if [ -f "$REPO_ROOT/.venv/bin/activate" ]; then
  source "$REPO_ROOT/.venv/bin/activate"
fi

echo "Starting KPI refresh at $(date)" >> "$LOG_FILE"
python "$REPO_ROOT/python/refresh_kpis.py" >> "$LOG_FILE" 2>&1
EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
  echo "KPI refresh completed successfully at $(date)" >> "$LOG_FILE"
else
  echo "KPI refresh failed with exit code $EXIT_CODE at $(date)" >> "$LOG_FILE"
fi

exit $EXIT_CODE


