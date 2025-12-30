#!/bin/bash
# File location: scripts/run_monthly_archive.sh
# Monthly archive job runner script
# This script is designed to be called by cron on the host machine

set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
CONTAINER_NAME="pull-resta-order-details-order_sync-1"
LOG_DIR="${PROJECT_DIR}/logs"
LOG_FILE="${LOG_DIR}/archive_job.log"
RETENTION_MONTHS=24

# Ensure log directory exists
mkdir -p "$LOG_DIR"

# Logging function
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

log "=========================================="
log "Starting monthly archive job"
log "=========================================="
log "Project directory: $PROJECT_DIR"
log "Container: $CONTAINER_NAME"
log "Retention period: $RETENTION_MONTHS months"

# Check if container is running
if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    log "ERROR: Container $CONTAINER_NAME is not running"
    log "Attempting to find running container..."

    # Try to find any container with order_sync in the name
    RUNNING_CONTAINER=$(docker ps --format '{{.Names}}' | grep "order_sync" | head -1)

    if [ -n "$RUNNING_CONTAINER" ]; then
        log "Found running container: $RUNNING_CONTAINER"
        CONTAINER_NAME="$RUNNING_CONTAINER"
    else
        log "ERROR: No order_sync container found. Exiting."
        exit 1
    fi
fi

log "Using container: $CONTAINER_NAME"

# Run the archive job
log "Executing archive job..."
if docker exec "$CONTAINER_NAME" python scripts/run_archive_job.py --retention-months "$RETENTION_MONTHS" 2>&1 | tee -a "$LOG_FILE"; then
    log "Archive job completed successfully"
else
    EXIT_CODE=$?
    log "ERROR: Archive job failed with exit code $EXIT_CODE"
    exit $EXIT_CODE
fi

log "=========================================="
log "Archive job finished"
log "=========================================="
