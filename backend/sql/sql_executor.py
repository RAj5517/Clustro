"""
SQL Executor

Handles database operations:
- INSERT rows
- ALTER TABLE (add columns)
- CREATE TABLE
- JSONB column handling
"""

import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values, Json
from typing import Dict, List, Any, Optional, Tuple
import sys
from pathlib import Path

# Add parent directory to path for logger import
sys.path.insert(0, str(Path(__file__).parent.parent))

from logger_config import get_logger
logger = get_logger('auraverse.sql_executor')

from semantic_attribute_matcher import infer_type, normalize_attribute


class SQLExecutor:
    """Executes SQL operations for schema evolution."""
    
    def __init__(self, db_config: Dict[str, Any]):
        """
        Initialize SQL executor.
        
        Args:
            db_config: Database connection configuration
        """
        self.db_config = db_config
        logger.debug("SQLExecutor initialized")
    
    def _get_connection(self):
        """Get database connection."""
        logger.debug(f"Connecting to database: {self.db_config['database']}@{self.db_config['host']}")
        conn = psycopg2.connect(**self.db_config)
        logger.debug("Database connection established")
        return conn
    
    def infer_column_type(self, values: List[Any], column_name: str) -> str:
        """
        Infer PostgreSQL column type from sample values.
        
        Args:
            values: List of sample values
            column_name: Column name (for hints)
        
        Returns:
            PostgreSQL type string
        """
        if not values:
            return 'TEXT'
        
        # Filter out None values
        non_null_values = [v for v in values if v is not None]
        if not non_null_values:
            return 'TEXT'
        
        # Check all values
        types_found = set()
        for value in non_null_values[:100]:  # Sample first 100
            types_found.add(infer_type(value))
        
        # If all same type, use it
        if len(types_found) == 1:
            pg_type = list(types_found)[0]
        else:
            # Mixed types - use TEXT
            pg_type = 'TEXT'
        
        # Adjust VARCHAR length
        if pg_type == 'VARCHAR':
            max_length = max((len(str(v)) for v in non_null_values[:100]), default=0)
            if max_length > 255:
                pg_type = 'TEXT'
            elif max_length > 0:
                # Round up to nearest 50
                length = ((max_length // 50) + 1) * 50
                pg_type = f'VARCHAR({min(length, 1000)})'
        
        return pg_type
    
    def create_table(
        self,
        table_name: str,
        attributes: List[str],
        data: List[Dict[str, Any]],
        primary_key: Optional[str] = None
    ) -> bool:
        """
        Create a new table with specified attributes.
        
        Automatically detects and sets ID attributes as primary keys.
        
        Args:
            table_name: Name of table to create
            attributes: List of attribute names
            data: Sample data for type inference
            primary_key: Optional primary key column name (if not provided, auto-detects ID)
        
        Returns:
            True if successful, False otherwise
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Auto-detect ID attribute if primary_key not specified
            from semantic_attribute_matcher import is_id_attribute
            if not primary_key:
                # Find first ID attribute in the list
                for attr in attributes:
                    if is_id_attribute(attr):
                        primary_key = attr
                        break
            
            # Infer types for each column
            column_defs = []
            pk_found = False
            
            for attr in attributes:
                # Get sample values
                sample_values = [
                    row.get(attr) for row in data[:100]
                    if attr in row
                ]
                
                pg_type = self.infer_column_type(sample_values, attr)
                normalized_attr = normalize_attribute(attr)
                
                # Build column definition
                col_def = f'"{normalized_attr}" {pg_type}'
                
                # Check if this is the primary key
                is_pk = False
                if primary_key:
                    normalized_pk = normalize_attribute(primary_key)
                    if normalized_attr == normalized_pk:
                        is_pk = True
                        pk_found = True
                elif is_id_attribute(attr):
                    # Auto-detect: if no PK specified but this is an ID, use it
                    is_pk = True
                    pk_found = True
                
                if is_pk:
                    col_def = col_def + ' PRIMARY KEY'
                
                column_defs.append(col_def)
            
            # Create table SQL
            create_sql = f"""
                CREATE TABLE IF NOT EXISTS "{table_name}" (
                    {', '.join(column_defs)}
                );
            """
            
            logger.debug(f"Creating table {table_name} with {len(column_defs)} columns")
            logger.debug(f"SQL: {create_sql[:200]}...")  # Log first 200 chars
            cursor.execute(create_sql)
            conn.commit()
            logger.info(f"Successfully created table: {table_name}")
            
            cursor.close()
            conn.close()
            
            return True
            
        except Exception as e:
            logger.error(f"Error creating table {table_name}: {e}", exc_info=True)
            return False
    
    def alter_table_add_columns(
        self,
        table_name: str,
        new_attributes: List[str],
        data: List[Dict[str, Any]]
    ) -> bool:
        """
        Add new columns to existing table.
        
        Args:
            table_name: Name of existing table
            new_attributes: List of new attribute names to add
            data: Sample data for type inference
        
        Returns:
            True if successful, False otherwise
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            for attr in new_attributes:
                # Get sample values
                sample_values = [
                    row.get(attr) for row in data[:100]
                    if attr in row
                ]
                
                pg_type = self.infer_column_type(sample_values, attr)
                normalized_attr = normalize_attribute(attr)
                
                # Check if column already exists
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT 1 
                        FROM information_schema.columns 
                        WHERE table_schema = 'public' 
                        AND table_name = %s 
                        AND column_name = %s
                    );
                """, (table_name, normalized_attr))
                
                exists = cursor.fetchone()[0]
                
                if not exists:
                    # Add column
                    alter_sql = f'ALTER TABLE "{table_name}" ADD COLUMN "{normalized_attr}" {pg_type};'
                    cursor.execute(alter_sql)
            
            conn.commit()
            cursor.close()
            conn.close()
            
            return True
            
        except Exception as e:
            print(f"Error altering table {table_name}: {e}")
            return False
    
    def ensure_jsonb_column(self, table_name: str, column_name: str = 'extra') -> bool:
        """
        Ensure table has a JSONB column for extra fields.
        
        Args:
            table_name: Name of table
            column_name: Name of JSONB column (default: 'extra')
        
        Returns:
            True if successful, False otherwise
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Check if column exists
            cursor.execute("""
                SELECT EXISTS (
                    SELECT 1 
                    FROM information_schema.columns 
                    WHERE table_schema = 'public' 
                    AND table_name = %s 
                    AND column_name = %s
                );
            """, (table_name, column_name))
            
            exists = cursor.fetchone()[0]
            
            if not exists:
                # Add JSONB column
                alter_sql = f'ALTER TABLE "{table_name}" ADD COLUMN "{column_name}" JSONB;'
                cursor.execute(alter_sql)
                conn.commit()
            
            cursor.close()
            conn.close()
            
            return True
            
        except Exception as e:
            print(f"Error ensuring JSONB column in {table_name}: {e}")
            return False
    
    def insert_rows(
        self,
        table_name: str,
        data: List[Dict[str, Any]],
        attribute_mapping: Optional[Dict[str, str]] = None,
        extra_fields: Optional[List[str]] = None,
        new_fields: Optional[List[str]] = None
    ) -> Tuple[int, Optional[str]]:
        """
        Insert rows into table.
        
        Args:
            table_name: Name of table
            data: List of dictionaries (rows) to insert
            attribute_mapping: Optional mapping from new_attr -> existing_attr
            extra_fields: Optional list of fields to store in JSONB 'extra' column
            new_fields: Optional list of new fields that were just added (to include in insertion)
        
        Returns:
            Tuple of (rows_inserted, error_message)
        """
        if not data:
            return 0, "No data to insert"
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Get table columns
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = 'public' 
                AND table_name = %s
                ORDER BY ordinal_position;
            """, (table_name,))
            
            table_columns = [row[0] for row in cursor.fetchall()]
            
            # Prepare data for insertion
            rows_to_insert = []
            columns_to_insert = []
            
            # Determine which columns to insert
            if attribute_mapping:
                # Use mapping - include mapped columns
                columns_to_insert = [
                    normalize_attribute(attr_mapped)
                    for attr_mapped in attribute_mapping.values()
                ]
                
                # Also include new_fields if they exist in table (for evolved_table)
                if new_fields:
                    for new_field in new_fields:
                        normalized_new_field = normalize_attribute(new_field)
                        # Only add if it exists in table and not already in columns_to_insert
                        if normalized_new_field in table_columns and normalized_new_field not in columns_to_insert:
                            columns_to_insert.append(normalized_new_field)
                            logger.debug(f"Including new field '{new_field}' (normalized: '{normalized_new_field}') in insertion")
            else:
                # Use all columns from first row
                first_row = data[0]
                columns_to_insert = [
                    normalize_attribute(attr)
                    for attr in first_row.keys()
                ]
                
                # Also include new_fields if provided
                if new_fields:
                    for new_field in new_fields:
                        normalized_new_field = normalize_attribute(new_field)
                        if normalized_new_field in table_columns and normalized_new_field not in columns_to_insert:
                            columns_to_insert.append(normalized_new_field)
            
            # Filter to only columns that exist in table
            columns_to_insert = [
                col for col in columns_to_insert
                if col in table_columns
            ]
            
            logger.debug(f"Columns to insert: {columns_to_insert}")
            
            # Check if 'extra' column exists for JSONB
            has_extra_column = 'extra' in table_columns
            
            # Prepare rows
            for row in data:
                values = []
                extra_data = {}
                
                # Map attributes if mapping provided
                if attribute_mapping:
                    mapped_row = {}
                    for new_attr, existing_attr in attribute_mapping.items():
                        if new_attr in row:
                            # mapped_row uses existing_attr (table column name) as key
                            mapped_row[normalize_attribute(existing_attr)] = row[new_attr]
                            logger.debug(f"Mapped '{new_attr}' → '{existing_attr}' (normalized: '{normalize_attribute(existing_attr)}'): {row[new_attr]}")
                    
                    # Add unmapped fields to extra if extra_fields specified (JSONB mode)
                    if extra_fields:
                        for field in extra_fields:
                            if field in row:
                                extra_data[normalize_attribute(field)] = row[field]
                    
                    # Add values for mapped columns
                    for col in columns_to_insert:
                        # Check if this column exists in mapped_row (mapped columns use normalized existing_attr as key)
                        if col in mapped_row:
                            # Value from mapped attribute
                            values.append(mapped_row[col])
                            logger.debug(f"Using mapped value for '{col}': {mapped_row[col]}")
                        elif new_fields and col in [normalize_attribute(f) for f in new_fields]:
                            # This is a new field that was just added - get value directly from row
                            # Find the original field name in new_fields
                            for new_field in new_fields:
                                if normalize_attribute(new_field) == col and new_field in row:
                                    values.append(row[new_field])
                                    logger.debug(f"Adding value for new field '{new_field}': {row[new_field]}")
                                    break
                            else:
                                values.append(None)
                                logger.debug(f"No value found for new field column '{col}'")
                        else:
                            # Try direct lookup in row (case-insensitive)
                            value = None
                            for attr, val in row.items():
                                if normalize_attribute(attr) == col:
                                    value = val
                                    break
                            values.append(value)
                            if value is not None:
                                logger.debug(f"Direct lookup found value for '{col}': {value}")
                            else:
                                logger.debug(f"No value found for column '{col}'")
                else:
                    # No mapping - use direct column names
                    for col in columns_to_insert:
                        # Find matching attribute in row (case-insensitive)
                        value = None
                        for attr, val in row.items():
                            if normalize_attribute(attr) == col:
                                value = val
                                break
                        values.append(value)
                
                # Add extra JSONB data if available
                if has_extra_column and extra_data:
                    values.append(Json(extra_data))
                    if 'extra' not in columns_to_insert:
                        columns_to_insert.append('extra')
                elif has_extra_column and not extra_data:
                    values.append(None)
                    if 'extra' not in columns_to_insert:
                        columns_to_insert.append('extra')
                
                rows_to_insert.append(tuple(values))
            
            # Get primary key column(s) for ON CONFLICT handling
            cursor.execute("""
                SELECT a.attname
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = %s::regclass
                AND i.indisprimary
                ORDER BY a.attnum;
            """, (table_name,))
            
            pk_columns = [row[0] for row in cursor.fetchall()]
            logger.debug(f"Primary key columns for {table_name}: {pk_columns}")
            
            # Check if primary key columns are missing from insert columns
            # If so, check if they have defaults or can be auto-generated
            normalized_columns = [normalize_attribute(col) for col in columns_to_insert]
            missing_pk_columns = []
            
            if pk_columns:
                for pk_col in pk_columns:
                    if normalize_attribute(pk_col) not in normalized_columns:
                        missing_pk_columns.append(pk_col)
            
            # If primary key is missing, check if it has a default value (e.g., SERIAL, sequence)
            if missing_pk_columns:
                logger.debug(f"Primary key column(s) missing from insert: {missing_pk_columns}")
                
                # Check column defaults and data types
                cursor.execute("""
                    SELECT column_name, column_default, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                    AND table_name = %s
                    AND column_name = ANY(%s);
                """, (table_name, missing_pk_columns))
                
                pk_info = {row[0]: {'default': row[1], 'type': row[2], 'nullable': row[3]} for row in cursor.fetchall()}
                
                # If PK has a default (e.g., nextval for SERIAL), we can omit it
                # Otherwise, we need to generate values or skip the insert
                can_omit_pk = all(
                    info['default'] is not None or info['nullable'] == 'YES'
                    for col, info in pk_info.items()
                )
                
                if can_omit_pk:
                    logger.debug(f"Primary key has default/generated value, omitting from insert: {missing_pk_columns}")
                    # Don't add missing PK columns - PostgreSQL will use default
                else:
                    # PK is NOT NULL and has no default - we need to generate values
                    logger.warning(f"Primary key column(s) {missing_pk_columns} are NOT NULL with no default. Generating values...")
                    
                    # For each missing PK column, generate next value using MAX+1 or sequence
                    for pk_col in missing_pk_columns:
                        # Try to get the sequence name for this column
                        cursor.execute("""
                            SELECT pg_get_serial_sequence(%s::text, %s::text);
                        """, (table_name, pk_col))
                        
                        seq_result = cursor.fetchone()
                        seq_name = seq_result[0] if seq_result and seq_result[0] else None
                        
                        if seq_name:
                            # Use sequence nextval - generate values using sequence
                            logger.debug(f"Using sequence {seq_name} for {pk_col}")
                            
                            # Get current sequence value and generate next values
                            cursor.execute(f"SELECT last_value, is_called FROM {seq_name};")
                            seq_result = cursor.fetchone()
                            if seq_result:
                                last_val, is_called = seq_result
                                start_value = last_val if not is_called else last_val + 1
                            else:
                                start_value = 1
                            
                            logger.debug(f"Sequence {seq_name} current value: {start_value}")
                            
                            # Add PK column to insert list
                            if normalize_attribute(pk_col) not in normalized_columns:
                                columns_to_insert.insert(0, normalize_attribute(pk_col))
                                normalized_columns.insert(0, normalize_attribute(pk_col))
                                
                                # Generate values using sequence for each row
                                for i, row_tuple in enumerate(rows_to_insert):
                                    new_value = start_value + i
                                    rows_to_insert[i] = (new_value,) + row_tuple
                                    logger.debug(f"Generated {pk_col} = {new_value} for row {i+1} using sequence")
                        else:
                            # No sequence - use MAX+1 approach
                            logger.debug(f"No sequence found for {pk_col}, will generate using MAX+1")
                            
                            # Get current max value
                            cursor.execute(f'SELECT COALESCE(MAX("{pk_col}"), 0) FROM "{table_name}";')
                            max_val = cursor.fetchone()[0] or 0
                            
                            # Generate values starting from max+1 for each row
                            logger.debug(f"Current MAX({pk_col}) = {max_val}, generating values from {max_val + 1}")
                            
                            # Add PK column to insert list
                            if normalize_attribute(pk_col) not in normalized_columns:
                                # Insert PK at the beginning
                                columns_to_insert.insert(0, normalize_attribute(pk_col))
                                normalized_columns.insert(0, normalize_attribute(pk_col))
                                logger.debug(f"Added {pk_col} to columns_to_insert")
                                
                                # Add generated PK values to each row (at the beginning)
                                start_value = max_val + 1
                                for i, row_tuple in enumerate(rows_to_insert):
                                    new_value = start_value + i
                                    # Insert at the beginning
                                    rows_to_insert[i] = (new_value,) + row_tuple
                                    logger.debug(f"Generated {pk_col} = {new_value} for row {i+1}")
            
            # Build INSERT SQL template with ON CONFLICT handling
            columns_str = ', '.join(f'"{col}"' for col in columns_to_insert)
            
            # Update normalized_columns after potentially adding PK columns
            normalized_columns = [normalize_attribute(col) for col in columns_to_insert]
            
            # If primary key exists, use ON CONFLICT DO NOTHING to skip duplicates
            # Otherwise, use regular INSERT
            if pk_columns:
                # Check if all PK columns are in columns_to_insert (after generation)
                pk_in_insert = all(
                    normalize_attribute(pk_col) in normalized_columns 
                    for pk_col in pk_columns
                )
                
                if pk_in_insert:
                    # Use ON CONFLICT to skip duplicate primary keys
                    pk_constraint = ', '.join(f'"{pk_col}"' for pk_col in pk_columns)
                    insert_template = f'INSERT INTO "{table_name}" ({columns_str}) VALUES %s ON CONFLICT ({pk_constraint}) DO NOTHING'
                    logger.debug(f"Using ON CONFLICT for primary key: {pk_constraint}")
                else:
                    # PK not in insert columns (should not happen if we generated them)
                    insert_template = f'INSERT INTO "{table_name}" ({columns_str}) VALUES %s'
                    logger.warning("Primary key columns not in insert columns after generation - this should not happen")
            else:
                # No primary key, use regular INSERT
                insert_template = f'INSERT INTO "{table_name}" ({columns_str}) VALUES %s'
                logger.debug("No primary key found, using regular INSERT")
            
            # Use execute_values for bulk insert
            execute_values(
                cursor,
                insert_template,
                rows_to_insert,
                template=None,
                page_size=100
            )
            
            # Get the actual number of rows inserted (after conflicts are ignored)
            # Note: execute_values doesn't return row count directly, so we check cursor.rowcount
            rows_inserted = cursor.rowcount
            logger.info(f"Successfully inserted {rows_inserted} out of {len(rows_to_insert)} rows into {table_name}")
            
            if rows_inserted < len(rows_to_insert):
                skipped = len(rows_to_insert) - rows_inserted
                logger.info(f"Skipped {skipped} duplicate row(s) (conflict on primary key)")
            
            conn.commit()
            
            cursor.close()
            conn.close()
            
            return rows_inserted, None
            
        except Exception as e:
            error_msg = f"Error inserting rows: {str(e)}"
            logger.error(error_msg, exc_info=True)
            print(error_msg)
            try:
                if 'conn' in locals() and conn:
                    conn.rollback()
            except:
                pass
            return 0, error_msg
    
    def generate_table_name(self, attributes: List[str], prefix: str = "table") -> str:
        """
        Generate a table name from attributes.
        
        Args:
            attributes: List of attribute names
            prefix: Prefix for table name
        
        Returns:
            Generated table name
        """
        # Use first few attributes to generate name
        if attributes:
            # Normalize and combine
            normalized = [normalize_attribute(attr) for attr in attributes[:3]]
            name_parts = [prefix] + normalized
            table_name = '_'.join(name_parts)
            # Limit length
            if len(table_name) > 50:
                table_name = table_name[:50]
            return table_name
        else:
            return f"{prefix}_unknown"


if __name__ == "__main__":
    # Test example
    from config import get_db_config
    
    print("=" * 60)
    print("SQL Executor - Test")
    print("=" * 60)
    
    db_config = get_db_config()
    executor = SQLExecutor(db_config)
    
    # Test data
    test_data = [
        {"product_name": "Shoes", "price": 500, "stock": 10},
        {"product_name": "T-Shirt", "price": 300, "stock": 20}
    ]
    
    test_attrs = ["product_name", "price", "stock"]
    
    print("\n1. Type Inference:")
    for attr in test_attrs:
        values = [row[attr] for row in test_data]
        pg_type = executor.infer_column_type(values, attr)
        print(f"   {attr}: {pg_type}")
    
    print("\n2. Table Name Generation:")
    table_name = executor.generate_table_name(test_attrs)
    print(f"   Generated: {table_name}")

