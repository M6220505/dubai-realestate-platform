#!/usr/bin/env bash
# Installs a cron job to run the weekly pipeline every Sunday at 03:00 AM.
# Run this script once on your server to register the schedule.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON="$(which python3)"
LOG_FILE="$SCRIPT_DIR/logs/weekly_pipeline.log"

mkdir -p "$SCRIPT_DIR/logs"

CRON_JOB="0 3 * * 0 cd \"$SCRIPT_DIR\" && $PYTHON -m pipeline.weekly_pipeline >> \"$LOG_FILE\" 2>&1"

# Add to crontab if not already present
(crontab -l 2>/dev/null | grep -v "weekly_pipeline"; echo "$CRON_JOB") | crontab -

echo "Cron job registered:"
echo "  $CRON_JOB"
echo ""
echo "View current crontab with: crontab -l"
echo "View logs at: $LOG_FILE"
