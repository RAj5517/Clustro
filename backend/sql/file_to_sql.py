"""
File to SQL Converter

Main orchestrator that:
1. Converts files to structured data
2. Extracts attributes
3. Matches attributes semantically
4. Makes schema evolution decisions
5. Executes SQL operations
"""

from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
import sys
import os

# Add parent directory to path for logger import
sys.path.insert(0, str(Path(__file__).parent.parent))

from logger_config import get_logger
logger = get_logger('auraverse.file_to_sql')

from file_to_rows import file_to_rows
from semantic_attribute_matcher import extract_attributes, normalize_attribute, is_id_attribute
from schema_evolution_engine import SchemaEvolutionEngine
from sql_executor import SQLExecutor
from config import get_db_config


class FileToSQLConverter:
    """Main converter that orchestrates file-to-SQL pipeline."""
    
    def __init__(self, db_config: Optional[Dict[str, Any]] = None):
        """
        Initialize converter.
        
        Args:
            db_config: Database configuration (uses get_db_config() if None)
        """
        if db_config is None:
            db_config = get_db_config()
        
        self.db_config = db_config
        self.evolution_engine = SchemaEvolutionEngine(db_config)
        self.sql_executor = SQLExecutor(db_config)
        self.logs = []
        logger.info("FileToSQLConverter initialized")
    
    def log(self, message: str, level: str = "INFO"):
        """Add log message to both logger and internal logs list."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] [{level}] {message}"
        self.logs.append(log_entry)
        
        # Also log to proper logger
        log_level = level.upper()
        if log_level == "DEBUG":
            logger.debug(message)
        elif log_level == "INFO":
            logger.info(message)
        elif log_level == "WARNING":
            logger.warning(message)
        elif log_level == "ERROR":
            logger.error(message)
        else:
            logger.info(message)
    
    def convert_file(
        self,
        file_path: Path,
        table_name: Optional[str] = None,
        primary_key: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Convert file to SQL-ready data and execute database operations.
        
        Args:
            file_path: Path to file
            table_name: Optional custom table name (auto-generated if None)
            primary_key: Optional primary key column name
        
        Returns:
            Dictionary with conversion results:
            {
                'success': bool,
                'table_name': str,
                'rows_inserted': int,
                'decision': str,
                'mapping': Dict[str, str],
                'new_fields': List[str],
                'match_ratio': float,
                'logs': List[str],
                'error': Optional[str]
            }
        """
        self.logs = []
        logger.info("=" * 60)
        logger.info(f"Starting file conversion: {file_path.name}")
        logger.info("=" * 60)
        self.log(f"Starting conversion of file: {file_path}")
        
        try:
            # Step 1: Convert file to rows
            self.log("Step 1: Converting file to structured rows...")
            rows, file_type, error = file_to_rows(file_path)
            
            if error:
                return {
                    'success': False,
                    'table_name': None,
                    'rows_inserted': 0,
                    'decision': None,
                    'mapping': {},
                    'new_fields': [],
                    'match_ratio': 0.0,
                    'logs': self.logs,
                    'error': error
                }
            
            if not rows:
                return {
                    'success': False,
                    'table_name': None,
                    'rows_inserted': 0,
                    'decision': None,
                    'mapping': {},
                    'new_fields': [],
                    'match_ratio': 0.0,
                    'logs': self.logs,
                    'error': 'No structured data extracted from file'
                }
            
            self.log(f"Extracted {len(rows)} rows from {file_type} file")
            
            # Step 2: Extract attributes
            self.log("Step 2: Extracting attributes...")
            attributes = extract_attributes(rows)
            normalized_attributes = [normalize_attribute(attr) for attr in attributes]
            
            # Detect ID attributes
            id_attributes = [attr for attr in attributes if is_id_attribute(attr)]
            regular_attributes = [attr for attr in attributes if not is_id_attribute(attr)]
            
            if id_attributes:
                self.log(f"Detected ID attribute(s): {id_attributes} (will be set as primary key)")
            self.log(f"Found {len(attributes)} attributes: {attributes}")
            self.log(f"  - Regular attributes: {len(regular_attributes)}")
            self.log(f"  - ID attributes: {len(id_attributes)} (excluded from matching calculations)")
            
            # Step 3: Make schema evolution decision
            self.log("Step 3: Analyzing schema and making decision...")
            
            # Refresh metadata before making decision to ensure we have latest tables
            logger.debug("Refreshing metadata before schema decision...")
            self.evolution_engine.refresh_metadata()
            logger.info(f"Metadata refreshed: {len(self.evolution_engine.table_metadata)} tables available")
            
            decision_result = self.evolution_engine.make_decision(attributes, rows)
            
            decision = decision_result['decision']
            target_table = decision_result['table_name']
            mapping = decision_result['mapping']
            new_fields = decision_result['new_fields']
            match_ratio = decision_result['match_ratio']
            reason = decision_result['reason']
            
            self.log(f"Decision: {decision} - {reason}")
            if mapping:
                self.log(f"Attribute mapping: {mapping}")
            if new_fields:
                self.log(f"New fields: {new_fields}")
            
            # Step 4: Execute SQL operations
            self.log("Step 4: Executing database operations...")
            rows_inserted = 0
            final_table_name = table_name or target_table
            
            if decision == 'same_table':
                # Insert into existing table
                self.log(f"Inserting {len(rows)} rows into existing table: {target_table}")
                rows_inserted, error = self.sql_executor.insert_rows(
                    target_table,
                    rows,
                    attribute_mapping=mapping
                )
                final_table_name = target_table
                
            elif decision == 'evolved_table':
                # Add new columns and insert
                self.log(f"Evolving table: {target_table}")
                self.log(f"Adding {len(new_fields)} new columns: {new_fields}")
                
                # Add new columns
                success = self.sql_executor.alter_table_add_columns(
                    target_table,
                    new_fields,
                    rows
                )
                
                if success:
                    # Refresh metadata
                    self.evolution_engine.refresh_metadata()
                    
                    # Insert rows - include new_fields as regular columns (not JSONB)
                    rows_inserted, error = self.sql_executor.insert_rows(
                        target_table,
                        rows,
                        attribute_mapping=mapping,
                        new_fields=new_fields  # Pass new_fields to include their values
                    )
                    final_table_name = target_table
                else:
                    error = "Failed to add new columns"
                
            elif decision == 'evolved_table_jsonb':
                # Add JSONB column and insert
                self.log(f"Evolving table with JSONB: {target_table}")
                self.log(f"Storing {len(new_fields)} fields in JSONB: {new_fields}")
                
                # Ensure JSONB column exists
                success = self.sql_executor.ensure_jsonb_column(target_table)
                
                if success:
                    # Refresh metadata
                    self.evolution_engine.refresh_metadata()
                    
                    # Insert rows with extra fields in JSONB
                    rows_inserted, error = self.sql_executor.insert_rows(
                        target_table,
                        rows,
                        attribute_mapping=mapping,
                        extra_fields=new_fields
                    )
                    final_table_name = target_table
                else:
                    error = "Failed to ensure JSONB column"
                
            elif decision == 'new_table':
                # Create new table
                if not final_table_name:
                    final_table_name = self.sql_executor.generate_table_name(attributes)
                
                self.log(f"Creating new table: {final_table_name}")
                
                # Auto-detect primary key from ID attributes if not provided
                detected_pk = primary_key
                if not detected_pk and id_attributes:
                    detected_pk = id_attributes[0]  # Use first ID attribute
                    self.log(f"Auto-detected primary key: {detected_pk}")
                
                # Create table
                success = self.sql_executor.create_table(
                    final_table_name,
                    attributes,
                    rows,
                    primary_key=detected_pk
                )
                
                if success:
                    # Refresh metadata
                    self.evolution_engine.refresh_metadata()
                    
                    # Insert rows
                    rows_inserted, error = self.sql_executor.insert_rows(
                        final_table_name,
                        rows,
                        attribute_mapping=None,
                        extra_fields=None
                    )
                else:
                    error = "Failed to create table"
            
            # Check for errors
            if error:
                self.log(f"Error: {error}", level="ERROR")
                return {
                    'success': False,
                    'table_name': final_table_name,
                    'rows_inserted': rows_inserted,
                    'decision': decision,
                    'mapping': mapping,
                    'new_fields': new_fields,
                    'match_ratio': match_ratio,
                    'logs': self.logs,
                    'error': error
                }
            
            self.log(f"Successfully inserted {rows_inserted} rows into {final_table_name}")
            
            return {
                'success': True,
                'table_name': final_table_name,
                'rows_inserted': rows_inserted,
                'decision': decision,
                'mapping': mapping,
                'new_fields': new_fields,
                'match_ratio': match_ratio,
                'logs': self.logs,
                'error': None
            }
            
        except Exception as e:
            error_msg = f"Unexpected error: {str(e)}"
            logger.error(f"Unexpected error in convert_file: {e}", exc_info=True)
            self.log(error_msg, level="ERROR")
            return {
                'success': False,
                'table_name': None,
                'rows_inserted': 0,
                'decision': None,
                'mapping': {},
                'new_fields': [],
                'match_ratio': 0.0,
                'logs': self.logs,
                'error': error_msg
            }
    
    def get_logs(self) -> List[str]:
        """Get all log messages."""
        return self.logs


def convert_file_to_sql(
    file_path: str,
    table_name: Optional[str] = None,
    primary_key: Optional[str] = None,
    db_config: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Convenience function to convert a file to SQL.
    
    Args:
        file_path: Path to file
        table_name: Optional custom table name
        primary_key: Optional primary key column name
        db_config: Optional database configuration
    
    Returns:
        Dictionary with conversion results
    """
    converter = FileToSQLConverter(db_config)
    return converter.convert_file(Path(file_path), table_name, primary_key)


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python file_to_sql.py <file_path> [table_name] [primary_key]")
        sys.exit(1)
    
    file_path = sys.argv[1]
    table_name = sys.argv[2] if len(sys.argv) > 2 else None
    primary_key = sys.argv[3] if len(sys.argv) > 3 else None
    
    print("=" * 60)
    print("File to SQL Converter")
    print("=" * 60)
    print(f"File: {file_path}")
    if table_name:
        print(f"Table name: {table_name}")
    if primary_key:
        print(f"Primary key: {primary_key}")
    print()
    
    result = convert_file_to_sql(file_path, table_name, primary_key)
    
    print("\n" + "=" * 60)
    print("CONVERSION RESULT")
    print("=" * 60)
    print(f"Success: {result['success']}")
    print(f"Table: {result['table_name']}")
    print(f"Rows inserted: {result['rows_inserted']}")
    print(f"Decision: {result['decision']}")
    print(f"Match ratio: {result['match_ratio']:.0%}")
    
    if result['mapping']:
        print(f"\nAttribute Mapping:")
        for new_attr, existing_attr in result['mapping'].items():
            print(f"  {new_attr} -> {existing_attr}")
    
    if result['new_fields']:
        print(f"\nNew Fields: {result['new_fields']}")
    
    if result['error']:
        print(f"\nError: {result['error']}")
    
    print("\n" + "=" * 60)
    print("LOGS")
    print("=" * 60)
    for log in result['logs']:
        print(log)

