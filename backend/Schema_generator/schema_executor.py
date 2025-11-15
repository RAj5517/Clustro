"""
Program B: Schema Executor

This program:
- Reads SQL statements from schema_jobs table (status='pending')
- Executes them safely inside transactions
- Updates status (completed/error)
- Logs all results
"""

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from typing import Dict, List, Any, Optional
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('schema_executor.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


class SchemaExecutor:
    """Executes SQL statements from schema_jobs table safely."""
    
    def __init__(self, db_config: Dict[str, Any]):
        """
        Initialize Schema Executor.
        
        Args:
            db_config: PostgreSQL connection configuration
        """
        self.db_config = db_config
        self.conn = None
        self.cursor = None
    
    def connect(self):
        """Connect to PostgreSQL."""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            # Use autocommit for transaction control
            self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            self.cursor = self.conn.cursor()
            logger.info("Connected to PostgreSQL database")
        except psycopg2.Error as e:
            logger.error(f"Failed to connect to database: {e}")
            raise ConnectionError(f"Failed to connect to database: {e}")
    
    def close(self):
        """Close database connection."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("Database connection closed")
    
    def fetch_pending_jobs(self) -> List[Dict[str, Any]]:
        """
        Fetch all pending SQL statements from schema_jobs table.
        
        Returns:
            List of job dictionaries:
            [{
                'id': int,
                'table_name': str,
                'sql_text': str,
                'status': str,
                'created_at': datetime,
                ...
            }, ...]
        """
        query = sql.SQL("""
            SELECT id, table_name, sql_text, status, created_at, updated_at
            FROM schema_jobs
            WHERE status = 'pending'
            ORDER BY created_at ASC
        """)
        
        self.cursor.execute(query)
        
        jobs = []
        for row in self.cursor.fetchall():
            jobs.append({
                'id': row[0],
                'table_name': row[1],
                'sql_text': row[2],
                'status': row[3],
                'created_at': row[4],
                'updated_at': row[5]
            })
        
        logger.info(f"Fetched {len(jobs)} pending jobs")
        return jobs
    
    def execute_job(self, job: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute a single SQL job in a transaction.
        
        Args:
            job: Job dictionary with id, table_name, sql_text, etc.
            
        Returns:
            Result dictionary:
            {
                'success': bool,
                'job_id': int,
                'error_message': Optional[str]
            }
        """
        job_id = job['id']
        table_name = job['table_name']
        sql_text = job['sql_text']
        
        logger.info(f"Executing job {job_id} for table: {table_name}")
        
        try:
            # Begin transaction (by disabling autocommit temporarily)
            self.conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)
            
            try:
                # Execute SQL statement
                self.cursor.execute("BEGIN;")
                self.cursor.execute(sql_text)
                self.cursor.execute("COMMIT;")
                
                # Update job status to completed
                self._update_job_status(job_id, 'completed', None)
                
                logger.info(f"[OK] Job {job_id} completed successfully for table: {table_name}")
                
                return {
                    'success': True,
                    'job_id': job_id,
                    'error_message': None
                }
                
            except psycopg2.Error as e:
                # Rollback transaction
                self.cursor.execute("ROLLBACK;")
                
                error_message = str(e)
                logger.error(f"[ERROR] Job {job_id} failed: {error_message}")
                
                # Update job status to error
                self._update_job_status(job_id, 'error', error_message)
                
                return {
                    'success': False,
                    'job_id': job_id,
                    'error_message': error_message
                }
            
            finally:
                # Restore autocommit
                self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                
        except Exception as e:
            error_message = f"Unexpected error executing job {job_id}: {str(e)}"
            logger.error(error_message)
            
            # Try to update status even if execution failed
            try:
                self._update_job_status(job_id, 'error', error_message)
            except:
                pass  # Best effort
            
            return {
                'success': False,
                'job_id': job_id,
                'error_message': error_message
            }
    
    def _update_job_status(self, job_id: int, status: str, error_message: Optional[str]):
        """
        Update job status in schema_jobs table.
        
        Args:
            job_id: Job ID
            status: New status ('completed' or 'error')
            error_message: Error message if status is 'error'
        """
        if error_message:
            query = sql.SQL("""
                UPDATE schema_jobs
                SET status = %s,
                    error_message = %s,
                    updated_at = NOW()
                WHERE id = %s
            """)
            self.cursor.execute(query, (status, error_message, job_id))
        else:
            query = sql.SQL("""
                UPDATE schema_jobs
                SET status = %s,
                    updated_at = NOW()
                WHERE id = %s
            """)
            self.cursor.execute(query, (status, job_id))
        
        self.conn.commit()
    
    def execute_all_pending(self, stop_on_error: bool = False) -> Dict[str, Any]:
        """
        Execute all pending jobs.
        
        Args:
            stop_on_error: If True, stop execution on first error
            
        Returns:
            Summary dictionary:
            {
                'total_jobs': int,
                'completed': int,
                'failed': int,
                'results': List[Dict]
            }
        """
        try:
            self.connect()
        except Exception as e:
            logger.error(f"Failed to connect: {e}")
            return {
                'total_jobs': 0,
                'completed': 0,
                'failed': 0,
                'results': [],
                'error': str(e)
            }
        
        try:
            # Fetch pending jobs
            jobs = self.fetch_pending_jobs()
            
            if not jobs:
                logger.info("No pending jobs found")
                return {
                    'total_jobs': 0,
                    'completed': 0,
                    'failed': 0,
                    'results': []
                }
            
            logger.info(f"Processing {len(jobs)} pending jobs...")
            
            results = []
            completed = 0
            failed = 0
            
            for job in jobs:
                result = self.execute_job(job)
                results.append(result)
                
                if result['success']:
                    completed += 1
                else:
                    failed += 1
                    if stop_on_error:
                        logger.warning("Stopping execution due to error (stop_on_error=True)")
                        break
            
            summary = {
                'total_jobs': len(jobs),
                'completed': completed,
                'failed': failed,
                'results': results
            }
            
            logger.info(f"Execution complete: {completed} completed, {failed} failed")
            return summary
            
        finally:
            self.close()
    
    def retry_failed_jobs(self) -> Dict[str, Any]:
        """
        Retry jobs that previously failed (status='error').
        
        Returns:
            Summary dictionary
        """
        try:
            self.connect()
        except Exception as e:
            logger.error(f"Failed to connect: {e}")
            return {
                'total_jobs': 0,
                'completed': 0,
                'failed': 0,
                'results': [],
                'error': str(e)
            }
        
        try:
            # Fetch failed jobs
            query = sql.SQL("""
                SELECT id, table_name, sql_text, status, created_at, updated_at
                FROM schema_jobs
                WHERE status = 'error'
                ORDER BY created_at ASC
            """)
            
            self.cursor.execute(query)
            jobs = []
            for row in self.cursor.fetchall():
                jobs.append({
                    'id': row[0],
                    'table_name': row[1],
                    'sql_text': row[2],
                    'status': row[3],
                    'created_at': row[4],
                    'updated_at': row[5]
                })
            
            if not jobs:
                logger.info("No failed jobs found")
                return {
                    'total_jobs': 0,
                    'completed': 0,
                    'failed': 0,
                    'results': []
                }
            
            # Reset status to pending
            for job in jobs:
                query = sql.SQL("""
                    UPDATE schema_jobs
                    SET status = 'pending',
                        error_message = NULL,
                        updated_at = NOW()
                    WHERE id = %s
                """)
                self.cursor.execute(query, (job['id'],))
            
            self.conn.commit()
            logger.info(f"Reset {len(jobs)} failed jobs to pending")
            
            # Execute all pending (which now includes the retried jobs)
            return self.execute_all_pending()
            
        finally:
            self.close()
    
    def get_job_status(self) -> Dict[str, int]:
        """Get status summary of all jobs."""
        try:
            self.connect()
        except Exception as e:
            logger.error(f"Failed to connect: {e}")
            return {}
        
        try:
            query = sql.SQL("""
                SELECT status, COUNT(*) as count
                FROM schema_jobs
                GROUP BY status
            """)
            
            self.cursor.execute(query)
            
            status_summary = {}
            for row in self.cursor.fetchall():
                status_summary[row[0]] = row[1]
            
            return status_summary
            
        finally:
            self.close()


def execute_pending_jobs(db_config: Dict[str, Any], stop_on_error: bool = False) -> Dict[str, Any]:
    """
    Convenience function to execute all pending jobs.
    
    Args:
        db_config: Database configuration dictionary
        stop_on_error: If True, stop on first error
        
    Returns:
        Summary dictionary
    """
    executor = SchemaExecutor(db_config)
    return executor.execute_all_pending(stop_on_error=stop_on_error)


if __name__ == "__main__":
    import sys
    import argparse
    import os
    
    parser = argparse.ArgumentParser(description='Schema Executor - Program B')
    parser.add_argument('--retry', action='store_true', help='Retry failed jobs')
    parser.add_argument('--stop-on-error', action='store_true', help='Stop on first error')
    parser.add_argument('--status', action='store_true', help='Show job status summary')
    parser.add_argument('--provider', type=str, help='Cloud provider (aws, gcp, azure, supabase, neon, heroku)')
    parser.add_argument('--config', type=str, help='Use custom config module')
    
    args = parser.parse_args()
    
    # Try to import config module
    try:
        if args.config:
            import importlib
            config_module = importlib.import_module(args.config)
            get_db_config = config_module.get_db_config
            get_cloud_db_config = config_module.get_cloud_db_config
        else:
            from config import get_db_config, get_cloud_db_config, validate_db_config
        
        # Use config from environment or cloud provider (defaults to local)
        if args.provider:
            db_config = get_cloud_db_config(args.provider)
        else:
            print("Using local PostgreSQL (default)")
            db_config = get_db_config()
        
        if not validate_db_config(db_config):
            print("[ERROR] Invalid database configuration!")
            print()
            print("Setup Instructions:")
            print("   1. Install PostgreSQL: https://www.postgresql.org/download/")
            print("   2. Create database: createdb clustro")
            print("   3. Set DB_PASSWORD environment variable")
            print("   4. Or use --provider flag for cloud databases")
            print()
            sys.exit(1)
    except ImportError:
        print("[ERROR] Config module not found!")
        print("   Please make sure config.py is in the same directory.")
        sys.exit(1)
    
    executor = SchemaExecutor(db_config)
    
    if args.status:
        print("=" * 60)
        print("Job Status Summary")
        print("=" * 60)
        
        status_summary = executor.get_job_status()
        
        if status_summary:
            for status, count in status_summary.items():
                print(f"{status.capitalize()}: {count}")
        else:
            print("No jobs found")
        
    elif args.retry:
        print("=" * 60)
        print("Schema Executor - Program B (Retry Mode)")
        print("=" * 60)
        print("Retrying failed jobs...")
        print()
        
        summary = executor.retry_failed_jobs()
        
        print(f"\nTotal jobs: {summary['total_jobs']}")
        print(f"[OK] Completed: {summary['completed']}")
        print(f"[ERROR] Failed: {summary['failed']}")
        
        if summary.get('error'):
            print(f"\nError: {summary['error']}")
    
    else:
        print("=" * 60)
        print("Schema Executor - Program B")
        print("=" * 60)
        print("Executing pending jobs...")
        print()
        
        summary = executor.execute_all_pending(stop_on_error=args.stop_on_error)
        
        print(f"\nTotal jobs: {summary['total_jobs']}")
        print(f"[OK] Completed: {summary['completed']}")
        print(f"[ERROR] Failed: {summary['failed']}")
        
        if summary.get('error'):
            print(f"\nError: {summary['error']}")
        
        if summary['results']:
            print("\nDetailed Results:")
            for result in summary['results']:
                status = "[OK]" if result['success'] else "[ERROR]"
                print(f"  {status} Job {result['job_id']}: {result.get('error_message', 'Success')}")

