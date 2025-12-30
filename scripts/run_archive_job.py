# File location: scripts/run_archive_job.py
"""
Script to run the monthly data archive job.

This script moves data older than the retention period (default: 24 months)
from the main database to the historical database.

Usage:
    python scripts/run_archive_job.py
    python scripts/run_archive_job.py --dry-run
    python scripts/run_archive_job.py --retention-months 12
    python scripts/run_archive_job.py --summary

Options:
    --dry-run           Show what would be done without making changes
    --retention-months  Number of months to retain (default: 24)
    --summary           Show archive summary without running archive
"""
import argparse
import asyncio
import logging
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config.settings import get_config
from src.services.archive_service import ArchiveService

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def run_archive(retention_months: int, dry_run: bool = False):
    """Run the archive job."""
    try:
        config = get_config()

        logger.info("=" * 60)
        logger.info("DATA ARCHIVE JOB")
        logger.info("=" * 60)
        logger.info(f"Main database: {config.database.database}")
        logger.info(f"Historical database: {config.database.historical_database}")
        logger.info(f"Retention period: {retention_months} months")
        logger.info("=" * 60)

        # Initialize archive service
        archive_service = ArchiveService(
            main_connection_string=config.database.connection_string,
            historical_connection_string=config.database.historical_connection_string,
            retention_months=retention_months
        )

        cutoff_date = archive_service.get_cutoff_date()
        logger.info(f"Cutoff date: {cutoff_date}")
        logger.info(f"Records with dates before {cutoff_date} will be archived")
        logger.info("")

        if dry_run:
            logger.info("*** DRY RUN MODE - No changes will be made ***")
            logger.info("")

        # Run the archive job
        stats = await archive_service.run_archive_job(dry_run=dry_run)

        # Print summary
        print("\n" + "=" * 60)
        print("ARCHIVE JOB SUMMARY")
        print("=" * 60)
        print(f"Tables processed:   {stats['tables_processed']}")
        print(f"Records archived:   {stats['records_archived']}")
        print(f"Records deleted:    {stats['records_deleted']}")
        print(f"Errors:             {stats['errors']}")
        print("=" * 60)

        if dry_run:
            print("\n*** DRY RUN - No changes were made ***")
            print("Run without --dry-run to perform the actual archive.")

        return stats

    except Exception as e:
        logger.error(f"Archive job failed: {str(e)}")
        raise


def show_summary():
    """Show archive summary from historical database."""
    try:
        config = get_config()

        archive_service = ArchiveService(
            main_connection_string=config.database.connection_string,
            historical_connection_string=config.database.historical_connection_string
        )

        summary = archive_service.get_archive_summary()

        print("\n" + "=" * 70)
        print("ARCHIVE SUMMARY")
        print("=" * 70)
        print(f"{'Table':<35} {'Records':<12} {'Earliest':<12} {'Latest':<12}")
        print("-" * 70)

        for table, info in summary.items():
            if 'error' in info:
                print(f"{table:<35} Error: {info['error']}")
            else:
                earliest = info['earliest_archive'].strftime('%Y-%m-%d') if info['earliest_archive'] else 'N/A'
                latest = info['latest_archive'].strftime('%Y-%m-%d') if info['latest_archive'] else 'N/A'
                print(f"{table:<35} {info['record_count']:<12} {earliest:<12} {latest:<12}")

        print("=" * 70)

    except Exception as e:
        logger.error(f"Failed to get archive summary: {str(e)}")
        raise


async def main():
    parser = argparse.ArgumentParser(
        description="Run the monthly data archive job"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without making changes"
    )
    parser.add_argument(
        "--retention-months",
        type=int,
        default=24,
        help="Number of months to retain in main database (default: 24)"
    )
    parser.add_argument(
        "--summary",
        action="store_true",
        help="Show archive summary without running archive"
    )

    args = parser.parse_args()

    if args.summary:
        show_summary()
    else:
        await run_archive(
            retention_months=args.retention_months,
            dry_run=args.dry_run
        )


if __name__ == "__main__":
    asyncio.run(main())
