#!/bin/bash
# File location: scripts/setup_archive_cron.sh
# Setup script to install the monthly archive cron job
# Run this script once on the host machine to configure the cron job

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ARCHIVE_SCRIPT="${SCRIPT_DIR}/run_monthly_archive.sh"

# Default schedule: 3:00 AM on the 1st of each month
CRON_SCHEDULE="0 3 1 * *"

echo "=========================================="
echo "Archive Cron Job Setup"
echo "=========================================="
echo ""
echo "This will configure a cron job to run the archive script:"
echo "  Script: $ARCHIVE_SCRIPT"
echo "  Schedule: $CRON_SCHEDULE (3:00 AM on 1st of each month)"
echo ""

# Check if archive script exists and is executable
if [ ! -f "$ARCHIVE_SCRIPT" ]; then
    echo "ERROR: Archive script not found at $ARCHIVE_SCRIPT"
    exit 1
fi

# Make sure the archive script is executable
chmod +x "$ARCHIVE_SCRIPT"
echo "Made $ARCHIVE_SCRIPT executable"

# Create the cron entry
CRON_ENTRY="$CRON_SCHEDULE $ARCHIVE_SCRIPT"

# Check if cron job already exists
if crontab -l 2>/dev/null | grep -q "run_monthly_archive.sh"; then
    echo ""
    echo "WARNING: An archive cron job already exists:"
    crontab -l | grep "run_monthly_archive.sh"
    echo ""
    read -p "Do you want to replace it? (y/n): " -n 1 -r
    echo ""
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Cancelled. Existing cron job unchanged."
        exit 0
    fi
    # Remove existing entry
    crontab -l 2>/dev/null | grep -v "run_monthly_archive.sh" | crontab -
    echo "Removed existing cron job"
fi

# Add new cron job
(crontab -l 2>/dev/null; echo "$CRON_ENTRY") | crontab -

echo ""
echo "=========================================="
echo "Cron job installed successfully!"
echo "=========================================="
echo ""
echo "Current cron jobs:"
crontab -l | grep -v "^#" | grep -v "^$" || echo "(none)"
echo ""
echo "The archive job will run at 3:00 AM on the 1st of each month."
echo ""
echo "To test the archive job manually (dry run):"
echo "  docker exec pull-resta-order-details-order_sync-1 python scripts/run_archive_job.py --dry-run"
echo ""
echo "To run the archive job manually (actual run):"
echo "  $ARCHIVE_SCRIPT"
echo ""
echo "To view archive logs:"
echo "  tail -f ${SCRIPT_DIR}/../logs/archive_job.log"
echo ""
echo "To remove the cron job:"
echo "  crontab -l | grep -v 'run_monthly_archive.sh' | crontab -"
echo ""
