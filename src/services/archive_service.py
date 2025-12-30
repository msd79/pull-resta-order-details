# File location: src/services/archive_service.py
"""
Archive Service for moving data older than retention period to historical database.

This service handles:
1. Identifying records older than the retention period (default: 24 months)
2. Copying records to the historical database
3. Deleting records from the main database (in FK-safe order)
4. Logging all archive operations for audit

The deletion order respects foreign key constraints:
- Dimensional: FactPayments -> FactCustomerMetrics -> FactOrders
- OLTP: payments -> orders
"""
from datetime import datetime, date as date_type
from dateutil.relativedelta import relativedelta
from typing import Dict, List, Optional, Tuple
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
import logging
import time


class ArchiveService:
    """
    Handles moving data older than retention period to historical database.
    """

    def __init__(
        self,
        main_connection_string: str,
        historical_connection_string: str,
        retention_months: int = 24
    ):
        """
        Initialize the archive service.

        Args:
            main_connection_string: Connection string for main database
            historical_connection_string: Connection string for historical database
            retention_months: Number of months to retain in main database (default: 24)
        """
        self.main_engine = create_engine(main_connection_string)
        self.historical_engine = create_engine(historical_connection_string)
        self.retention_months = retention_months
        self.logger = logging.getLogger(__name__)

        # Track statistics
        self.stats = {
            'tables_processed': 0,
            'records_archived': 0,
            'records_deleted': 0,
            'errors': 0
        }

    def get_cutoff_date(self) -> date_type:
        """Calculate the cutoff date based on retention period."""
        return (datetime.now() - relativedelta(months=self.retention_months)).date()

    async def run_archive_job(self, dry_run: bool = False) -> Dict:
        """
        Main archive job - run monthly.

        Args:
            dry_run: If True, show what would be done without making changes

        Returns:
            Dictionary of statistics about the archive operation
        """
        cutoff_date = self.get_cutoff_date()
        self.logger.info(f"Starting archive job. Cutoff date: {cutoff_date}")
        self.logger.info(f"Archiving data older than {self.retention_months} months")

        if dry_run:
            self.logger.info("DRY RUN - No changes will be made")

        try:
            # Archive in FK-safe order
            # 1. Archive fact tables (children first)
            await self._archive_table(
                source_table='fact_payments',
                target_table='fact_payments_archive',
                date_column='datetime_key',
                cutoff_date=cutoff_date,
                dry_run=dry_run,
                use_datetime_dim=True
            )

            await self._archive_table(
                source_table='fact_customer_metrics',
                target_table='fact_customer_metrics_archive',
                date_column='datetime_key',
                cutoff_date=cutoff_date,
                dry_run=dry_run,
                use_datetime_dim=True
            )

            await self._archive_table(
                source_table='fact_orders',
                target_table='fact_orders_archive',
                date_column='datetime_key',
                cutoff_date=cutoff_date,
                dry_run=dry_run,
                use_datetime_dim=True,
                add_creation_date=True
            )

            # 2. Archive OLTP tables (children first)
            await self._archive_table(
                source_table='payments',
                target_table='payments_archive',
                date_column='order_id',
                cutoff_date=cutoff_date,
                dry_run=dry_run,
                use_order_date=True
            )

            await self._archive_table(
                source_table='orders',
                target_table='orders_archive',
                date_column='creation_date',
                cutoff_date=cutoff_date,
                dry_run=dry_run
            )

            self.logger.info("Archive job completed successfully")

        except Exception as e:
            self.logger.error(f"Archive job failed: {str(e)}")
            self.stats['errors'] += 1
            raise

        return self.stats

    async def _archive_table(
        self,
        source_table: str,
        target_table: str,
        date_column: str,
        cutoff_date: date_type,
        dry_run: bool = False,
        use_datetime_dim: bool = False,
        use_order_date: bool = False,
        add_creation_date: bool = False
    ) -> Tuple[int, int]:
        """
        Archive a single table.

        Args:
            source_table: Name of source table in main database
            target_table: Name of target table in historical database
            date_column: Column to filter by date
            cutoff_date: Date before which records should be archived
            dry_run: If True, don't make changes
            use_datetime_dim: If True, join with dim_datetime to filter by date
            use_order_date: If True, join with orders table to filter by date
            add_creation_date: If True, add original_creation_date column

        Returns:
            Tuple of (records_archived, records_deleted)
        """
        start_time = time.time()
        self.logger.info(f"Archiving {source_table} -> {target_table}")

        try:
            with self.main_engine.connect() as main_conn:
                with self.historical_engine.connect() as hist_conn:
                    # Build the WHERE clause based on the filtering method
                    if use_datetime_dim:
                        # Join with dim_datetime to filter by date
                        where_clause = f"""
                            {date_column} IN (
                                SELECT datetime_key FROM dim_datetime
                                WHERE date < :cutoff_date
                            )
                        """
                    elif use_order_date:
                        # Join with orders table to filter by date (for payments)
                        where_clause = f"""
                            order_id IN (
                                SELECT id FROM orders
                                WHERE creation_date < :cutoff_date
                            )
                        """
                    else:
                        # Direct date comparison
                        where_clause = f"{date_column} < :cutoff_date"

                    # Count records to archive
                    count_sql = f"SELECT COUNT(*) FROM {source_table} WHERE {where_clause}"
                    result = main_conn.execute(
                        text(count_sql),
                        {'cutoff_date': cutoff_date}
                    )
                    record_count = result.scalar()

                    self.logger.info(f"  Found {record_count} records to archive")

                    if record_count == 0:
                        return 0, 0

                    if dry_run:
                        self.logger.info(f"  DRY RUN: Would archive {record_count} records")
                        return record_count, 0

                    # Get column names from source table
                    columns_sql = f"""
                        SELECT COLUMN_NAME
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_NAME = :table_name
                        ORDER BY ORDINAL_POSITION
                    """
                    columns_result = main_conn.execute(
                        text(columns_sql),
                        {'table_name': source_table}
                    )
                    columns = [row[0] for row in columns_result]

                    # Build INSERT statement
                    columns_str = ', '.join(columns)
                    if add_creation_date:
                        # For fact_orders, also capture original creation date
                        # Prefix columns with 's.' to avoid ambiguity with joined table
                        prefixed_columns = ', '.join([f's.{col}' for col in columns])
                        # Also prefix the where clause datetime_key
                        prefixed_where = where_clause.replace('datetime_key', 's.datetime_key')
                        select_sql = f"""
                            SELECT {prefixed_columns}, dd.datetime as original_creation_date, GETDATE() as archived_date
                            FROM {source_table} s
                            JOIN dim_datetime dd ON s.datetime_key = dd.datetime_key
                            WHERE {prefixed_where}
                        """
                        insert_columns = f"{columns_str}, original_creation_date, archived_date"
                    else:
                        select_sql = f"""
                            SELECT {columns_str}, GETDATE() as archived_date
                            FROM {source_table}
                            WHERE {where_clause}
                        """
                        insert_columns = f"{columns_str}, archived_date"

                    # Fetch data from main database
                    self.logger.info(f"  Fetching records from {source_table}...")
                    data_result = main_conn.execute(
                        text(select_sql),
                        {'cutoff_date': cutoff_date}
                    )
                    rows = data_result.fetchall()

                    if rows:
                        # Insert into historical database
                        self.logger.info(f"  Inserting {len(rows)} records into {target_table}...")

                        # Build parameterized insert
                        placeholders = ', '.join([f':col{i}' for i in range(len(rows[0]))])
                        insert_sql = f"INSERT INTO {target_table} VALUES ({placeholders})"

                        # Insert in batches
                        batch_size = 1000
                        for i in range(0, len(rows), batch_size):
                            batch = rows[i:i + batch_size]
                            for row in batch:
                                params = {f'col{j}': val for j, val in enumerate(row)}
                                hist_conn.execute(text(insert_sql), params)
                            hist_conn.commit()
                            self.logger.debug(f"    Inserted batch {i // batch_size + 1}")

                        # Verify insertion (use 60 minute window to account for large inserts)
                        verify_sql = f"SELECT COUNT(*) FROM {target_table} WHERE archived_date >= DATEADD(minute, -60, GETDATE())"
                        verify_result = hist_conn.execute(text(verify_sql))
                        inserted_count = verify_result.scalar()

                        if inserted_count < len(rows):
                            raise Exception(f"Verification failed: expected {len(rows)}, found {inserted_count}")

                        self.logger.info(f"  Verified {inserted_count} records in {target_table}")

                        # Delete from main database
                        self.logger.info(f"  Deleting {len(rows)} records from {source_table}...")
                        delete_sql = f"DELETE FROM {source_table} WHERE {where_clause}"
                        main_conn.execute(
                            text(delete_sql),
                            {'cutoff_date': cutoff_date}
                        )
                        main_conn.commit()

                        # Log the operation
                        self._log_archive_operation(
                            hist_conn=hist_conn,
                            cutoff_date=cutoff_date,
                            table_name=source_table,
                            records_archived=len(rows),
                            records_deleted=len(rows),
                            status='SUCCESS',
                            duration_seconds=time.time() - start_time
                        )

                        self.stats['tables_processed'] += 1
                        self.stats['records_archived'] += len(rows)
                        self.stats['records_deleted'] += len(rows)

                        return len(rows), len(rows)

                    return 0, 0

        except Exception as e:
            self.logger.error(f"  Error archiving {source_table}: {str(e)}")

            # Log the failure
            try:
                with self.historical_engine.connect() as hist_conn:
                    self._log_archive_operation(
                        hist_conn=hist_conn,
                        cutoff_date=cutoff_date,
                        table_name=source_table,
                        records_archived=0,
                        records_deleted=0,
                        status='FAILED',
                        error_message=str(e),
                        duration_seconds=time.time() - start_time
                    )
            except:
                pass

            raise

    def _log_archive_operation(
        self,
        hist_conn,
        cutoff_date: date_type,
        table_name: str,
        records_archived: int,
        records_deleted: int,
        status: str,
        error_message: str = None,
        duration_seconds: float = None
    ):
        """Log an archive operation to the archive_log table."""
        try:
            log_sql = """
                INSERT INTO archive_log
                (archive_date, cutoff_date, table_name, records_archived, records_deleted, status, error_message, duration_seconds)
                VALUES
                (GETDATE(), :cutoff_date, :table_name, :records_archived, :records_deleted, :status, :error_message, :duration_seconds)
            """
            hist_conn.execute(text(log_sql), {
                'cutoff_date': cutoff_date,
                'table_name': table_name,
                'records_archived': records_archived,
                'records_deleted': records_deleted,
                'status': status,
                'error_message': error_message,
                'duration_seconds': duration_seconds
            })
            hist_conn.commit()
        except Exception as e:
            self.logger.warning(f"Failed to log archive operation: {str(e)}")

    def get_archive_summary(self) -> Dict:
        """Get a summary of archived data from the historical database."""
        summary = {}
        try:
            with self.historical_engine.connect() as conn:
                tables = [
                    'fact_orders_archive',
                    'fact_payments_archive',
                    'fact_customer_metrics_archive',
                    'orders_archive',
                    'payments_archive'
                ]

                for table in tables:
                    try:
                        count_result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                        count = count_result.scalar()

                        date_result = conn.execute(text(f"""
                            SELECT MIN(archived_date), MAX(archived_date)
                            FROM {table}
                        """))
                        date_row = date_result.fetchone()

                        summary[table] = {
                            'record_count': count,
                            'earliest_archive': date_row[0],
                            'latest_archive': date_row[1]
                        }
                    except Exception as e:
                        summary[table] = {'error': str(e)}

        except Exception as e:
            self.logger.error(f"Error getting archive summary: {str(e)}")

        return summary
