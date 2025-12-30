# Data Archive System

This document describes the data archiving system for moving old order data from the main database (`RestaOrders`) to the historical database (`RestaOrders_Historical`).

## Overview

The archive system moves data older than 24 months from the main database to a historical database. This keeps the main database performant while preserving historical data for analysis.

### What Gets Archived

| Main Database Table | Archive Table | Date Filter |
|---------------------|---------------|-------------|
| `fact_payments` | `fact_payments_archive` | Via `dim_datetime` join |
| `fact_customer_metrics` | `fact_customer_metrics_archive` | Via `dim_datetime` join |
| `fact_orders` | `fact_orders_archive` | Via `dim_datetime` join |
| `payments` | `payments_archive` | Via `orders.creation_date` |
| `orders` | `orders_archive` | `creation_date` |

### Archive Order (FK-safe)

Tables are archived in this order to respect foreign key constraints:
1. `fact_payments` (child)
2. `fact_customer_metrics` (child)
3. `fact_orders` (parent of above)
4. `payments` (child)
5. `orders` (parent of payments)

## Manual Execution

### Using the Shell Script (Recommended)

Run the archive job from the host machine:

```bash
# From the project root directory
./scripts/run_monthly_archive.sh
```

This script:
- Automatically finds the running Docker container
- Executes the Python archive job
- Logs output to `logs/archive_job.log`

### Using Docker Exec Directly

```bash
# Dry run (shows what would be archived without making changes)
docker exec pull-resta-order-details-order_sync-1 python scripts/run_archive_job.py --dry-run

# Actual archive run
docker exec pull-resta-order-details-order_sync-1 python scripts/run_archive_job.py

# Custom retention period (e.g., 12 months instead of 24)
docker exec pull-resta-order-details-order_sync-1 python scripts/run_archive_job.py --retention-months 12

# View archive summary (no archiving, just shows current state)
docker exec pull-resta-order-details-order_sync-1 python scripts/run_archive_job.py --summary
```

### Command Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `--dry-run` | Show what would be done without making changes | Off |
| `--retention-months N` | Number of months to retain in main database | 24 |
| `--summary` | Show archive summary without running archive | Off |

## Automatic Execution (Cron)

The archive job is configured to run automatically via cron on the host machine.

### Current Schedule

```
0 3 1 * * /home/quarxadmin/repos/pull-resta-order-details/scripts/run_monthly_archive.sh
```

This runs at **3:00 AM on the 1st of each month**.

### Viewing the Cron Job

```bash
crontab -l
```

### Setting Up the Cron Job

If the cron job needs to be reinstalled:

```bash
# Option 1: Use the setup script
./scripts/setup_archive_cron.sh

# Option 2: Manual installation
(crontab -l 2>/dev/null; echo "0 3 1 * * /home/quarxadmin/repos/pull-resta-order-details/scripts/run_monthly_archive.sh") | crontab -
```

### Removing the Cron Job

```bash
crontab -l | grep -v 'run_monthly_archive.sh' | crontab -
```

### Changing the Schedule

Common cron schedule examples:

```bash
# Every Sunday at 2:00 AM
0 2 * * 0 /path/to/run_monthly_archive.sh

# 1st and 15th of each month at 3:00 AM
0 3 1,15 * * /path/to/run_monthly_archive.sh

# Every day at midnight (not recommended for archive)
0 0 * * * /path/to/run_monthly_archive.sh
```

To change the schedule:
```bash
crontab -e
```

## Logs

### Archive Job Log

```bash
# View recent logs
tail -50 logs/archive_job.log

# Follow logs in real-time
tail -f logs/archive_job.log

# View full log
cat logs/archive_job.log
```

### Log Location

- Shell script logs: `logs/archive_job.log`
- Python job output: Included in the same log file

## Troubleshooting

### Container Not Found

If you see "Container not running" error:

```bash
# Check if container is running
docker ps | grep order_sync

# Start the container if needed
docker-compose up -d
```

### Permission Denied on Logs

```bash
# Fix log directory permissions
sudo chown -R quarxadmin:quarxadmin /home/quarxadmin/repos/pull-resta-order-details/logs/
```

### Database Connection Issues

Check that:
1. The container is running
2. Database server is accessible
3. Credentials in `.env` file are correct

### Verification Failed Error

If archive verification fails, the job will not delete records from the main database. Check:
1. Historical database connection
2. Archive table schema matches source table
3. Disk space on historical database server

## Files

| File | Description |
|------|-------------|
| `scripts/run_monthly_archive.sh` | Shell script for running archive (called by cron) |
| `scripts/setup_archive_cron.sh` | One-time setup script for cron job |
| `scripts/run_archive_job.py` | Python archive job implementation |
| `src/services/archive_service.py` | Core archive service class |
| `logs/archive_job.log` | Archive job log file |

## Safety Features

1. **Verification**: Records are verified in the archive before deletion from main database
2. **FK-Safe Order**: Tables are processed in order that respects foreign key constraints
3. **Dry Run**: Test what would happen without making changes
4. **Logging**: All operations are logged for audit purposes
5. **Error Handling**: Failed operations are logged and don't affect other tables
