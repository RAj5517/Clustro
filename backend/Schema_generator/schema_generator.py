"""
Program A: Schema Generator

This program:
- Parses incoming non-media files
- Infers SQL schema from file structure
- Generates CREATE/ALTER TABLE SQL statements
- Compares with existing PostgreSQL schema
- Saves SQL statements to schema_jobs table (does NOT execute them)
"""

import json
import csv
import xml.etree.ElementTree as ET
import re
import psycopg2
from psycopg2 import sql
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
from collections import defaultdict

# Optional imports
try:
    import yaml
    HAS_YAML = True
except ImportError:
    HAS_YAML = False

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

try:
    from docx import Document
    HAS_DOCX = True
except ImportError:
    HAS_DOCX = False

try:
    import PyPDF2
    HAS_PDF = True
except ImportError:
    HAS_PDF = False


class SchemaGenerator:
    """Generates SQL schema from non-media files."""
    
    def __init__(self, db_config: Dict[str, Any]):
        """
        Initialize Schema Generator.
        
        Args:
            db_config: PostgreSQL connection configuration
        """
        self.db_config = db_config
        self.conn = None
        self.cursor = None
        self.file_type = None
        self.file_content = None
        self.base_table_name = None
        self.tables_structure = {}  # {table_name: {columns: [...], constraints: [...]}}
    
    def connect(self):
        """Connect to PostgreSQL."""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.cursor = self.conn.cursor()
        except psycopg2.Error as e:
            raise ConnectionError(f"Failed to connect to database: {e}")
    
    def close(self):
        """Close database connection."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
    
    def generate_schema(self, file_path: str) -> Dict[str, Any]:
        """
        Main method to generate schema from file.
        
        Args:
            file_path: Path to the file to process
            
        Returns:
            Dictionary with results:
            {
                'success': bool,
                'tables': List[str],
                'sql_statements': List[str],
                'jobs_created': int,
                'errors': List[str]
            }
        """
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        # Reset state
        self.tables_structure = {}
        self.base_table_name = self._sanitize_table_name(file_path.stem)
        
        # Detect and parse file
        self.file_type = self._detect_file_type(file_path)
        
        # Skip text-heavy files (they go to NoSQL)
        if self.file_type in ['text', 'log', 'md', 'pdf', 'docx']:
            return {
                'success': False,
                'tables': [],
                'sql_statements': [],
                'jobs_created': 0,
                'errors': [f"File type '{self.file_type}' is text-heavy and should be handled by NoSQL storage"]
            }
        
        try:
            self.file_content = self._parse_file(file_path)
        except Exception as e:
            return {
                'success': False,
                'tables': [],
                'sql_statements': [],
                'jobs_created': 0,
                'errors': [f"Failed to parse file: {str(e)}"]
            }
        
        # Connect to database
        try:
            self.connect()
        except Exception as e:
            return {
                'success': False,
                'tables': [],
                'sql_statements': [],
                'jobs_created': 0,
                'errors': [f"Database connection failed: {str(e)}"]
            }
        
        try:
            # Extract structure based on file type
            if self.file_type == 'json':
                self._extract_json_structure()
            elif self.file_type in ['csv', 'excel']:
                self._extract_tabular_structure()
            elif self.file_type == 'xml':
                self._extract_xml_structure()
            elif self.file_type == 'html':
                self._extract_html_structure()
            elif self.file_type == 'yaml':
                self._extract_yaml_structure()
            else:
                return {
                    'success': False,
                    'tables': [],
                    'sql_statements': [],
                    'jobs_created': 0,
                    'errors': [f"Unsupported file type: {self.file_type}"]
                }
            
            # Compare with existing schema and generate SQL
            sql_statements = []
            errors = []
            
            for table_name, table_info in self.tables_structure.items():
                try:
                    sql_stmt = self._generate_table_sql(table_name, table_info)
                    if sql_stmt:
                        sql_statements.append(sql_stmt)
                except Exception as e:
                    errors.append(f"Error generating SQL for {table_name}: {str(e)}")
            
            # Save SQL statements to schema_jobs table
            jobs_created = 0
            for sql_stmt in sql_statements:
                try:
                    table_name = self._extract_table_name_from_sql(sql_stmt)
                    self._save_sql_to_jobs(table_name, sql_stmt)
                    jobs_created += 1
                except Exception as e:
                    errors.append(f"Error saving SQL to jobs: {str(e)}")
            
            return {
                'success': len(errors) == 0,
                'tables': list(self.tables_structure.keys()),
                'sql_statements': sql_statements,
                'jobs_created': jobs_created,
                'errors': errors
            }
            
        finally:
            self.close()
    
    def _detect_file_type(self, file_path: Path) -> str:
        """Detect file type from extension."""
        ext = file_path.suffix.lower()
        
        type_map = {
            '.json': 'json',
            '.csv': 'csv',
            '.xlsx': 'excel',
            '.xls': 'excel',
            '.xml': 'xml',
            '.html': 'html',
            '.htm': 'html',
            '.txt': 'text',
            '.md': 'md',
            '.log': 'log',
            '.yaml': 'yaml',
            '.yml': 'yaml',
            '.pdf': 'pdf',
            '.docx': 'docx'
        }
        
        return type_map.get(ext, 'unknown')
    
    def _parse_file(self, file_path: Path) -> Any:
        """Parse file based on detected type."""
        if self.file_type == 'json':
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        
        elif self.file_type == 'csv':
            if HAS_PANDAS:
                return pd.read_csv(file_path)
            else:
                with open(file_path, 'r', encoding='utf-8') as f:
                    return list(csv.DictReader(f))
        
        elif self.file_type == 'excel':
            if HAS_PANDAS:
                return pd.read_excel(file_path)
            else:
                raise ImportError("pandas required for Excel file parsing")
        
        elif self.file_type == 'xml':
            return ET.parse(file_path).getroot()
        
        elif self.file_type == 'html':
            with open(file_path, 'r', encoding='utf-8') as f:
                return f.read()
        
        elif self.file_type == 'yaml':
            if HAS_YAML:
                with open(file_path, 'r', encoding='utf-8') as f:
                    return yaml.safe_load(f)
            else:
                raise ImportError("pyyaml required for YAML file parsing")
        
        else:
            raise ValueError(f"Unsupported file type for schema generation: {self.file_type}")
    
    def _extract_json_structure(self):
        """Extract table structure from JSON."""
        data = self.file_content
        
        if isinstance(data, list) and len(data) > 0:
            # Array of objects - main table
            self._process_object_list(data, self.base_table_name)
        elif isinstance(data, dict):
            # Single object - process it
            self._process_object(data, self.base_table_name, None)
        else:
            raise ValueError("Unsupported JSON structure for schema generation")
    
    def _extract_tabular_structure(self):
        """Extract table structure from CSV/Excel."""
        if HAS_PANDAS and isinstance(self.file_content, pd.DataFrame):
            df = self.file_content
            columns = []
            
            for col in df.columns:
                col_name = self._sanitize_column_name(col)
                col_type = self._infer_type_from_series(df[col])
                columns.append({
                    'name': col_name,
                    'type': col_type,
                    'nullable': df[col].isna().any()
                })
            
            self.tables_structure[self.base_table_name] = {
                'columns': columns,
                'parent_table': None,
                'parent_fk': None
            }
        else:
            # CSV DictReader
            if isinstance(self.file_content, list) and len(self.file_content) > 0:
                first_row = self.file_content[0]
                columns = []
                
                for col_name, value in first_row.items():
                    sanitized_name = self._sanitize_column_name(col_name)
                    # Infer type from first few rows
                    sample_values = [row.get(col_name) for row in self.file_content[:10] if col_name in row]
                    col_type = self._infer_type_from_values(sample_values)
                    nullable = any(v is None or v == '' for v in sample_values)
                    
                    columns.append({
                        'name': sanitized_name,
                        'type': col_type,
                        'nullable': nullable
                    })
                
                self.tables_structure[self.base_table_name] = {
                    'columns': columns,
                    'parent_table': None,
                    'parent_fk': None
                }
    
    def _extract_xml_structure(self):
        """Extract table structure from XML."""
        root = self.file_content
        
        # Find repeating elements
        children = list(root)
        if len(children) > 0:
            # Group by tag name
            tag_groups = defaultdict(list)
            for child in children:
                tag_groups[child.tag].append(child)
            
            # Process each tag group as a separate table
            for tag, elements in tag_groups.items():
                table_name = self._sanitize_table_name(tag)
                if len(elements) > 0:
                    self._process_xml_elements(elements, table_name)
        else:
            # Root element itself
            self._process_xml_element(root, self.base_table_name, None)
    
    def _extract_html_structure(self):
        """Extract table structure from HTML tables."""
        html_content = self.file_content
        
        # Find all tables
        table_pattern = r'<table[^>]*>(.*?)</table>'
        tables = re.findall(table_pattern, html_content, re.DOTALL | re.IGNORECASE)
        
        if not tables:
            raise ValueError("No HTML tables found in file")
        
        # Process first table (can be extended to handle multiple tables)
        table_html = tables[0]
        
        # Extract headers
        header_pattern = r'<th[^>]*>(.*?)</th>'
        headers = re.findall(header_pattern, table_html, re.IGNORECASE)
        
        # Extract rows
        row_pattern = r'<tr[^>]*>(.*?)</tr>'
        rows = re.findall(row_pattern, table_html, re.IGNORECASE)
        
        if len(headers) > 0:
            columns = []
            for header in headers:
                header_text = re.sub(r'<[^>]+>', '', header).strip()
                col_name = self._sanitize_column_name(header_text)
                # Default to VARCHAR(255) for HTML tables
                columns.append({
                    'name': col_name,
                    'type': 'VARCHAR(255)',
                    'nullable': True
                })
            
            self.tables_structure[self.base_table_name] = {
                'columns': columns,
                'parent_table': None,
                'parent_fk': None
            }
    
    def _extract_yaml_structure(self):
        """Extract table structure from YAML (similar to JSON)."""
        data = self.file_content
        
        if isinstance(data, list) and len(data) > 0:
            self._process_object_list(data, self.base_table_name)
        elif isinstance(data, dict):
            self._process_object(data, self.base_table_name, None)
        else:
            raise ValueError("Unsupported YAML structure for schema generation")
    
    def _process_object_list(self, obj_list: List[Dict], table_name: str, parent_table: Optional[str] = None):
        """Process a list of objects into table structure."""
        if not obj_list or not isinstance(obj_list[0], dict):
            return
        
        columns = []
        child_tables = defaultdict(list)
        
        # Analyze first object to determine structure
        first_obj = obj_list[0]
        
        for key, value in first_obj.items():
            col_name = self._sanitize_column_name(key)
            
            if isinstance(value, (dict, list)) and value:
                # Nested structure - create child table
                child_table_name = f"{table_name}_{col_name}"
                if isinstance(value, list) and len(value) > 0 and isinstance(value[0], dict):
                    # List of objects
                    child_tables[child_table_name] = value
                elif isinstance(value, dict):
                    # Single object
                    child_tables[child_table_name] = [value]
            else:
                # Regular column
                sample_values = [obj.get(key) for obj in obj_list[:10] if key in obj]
                col_type = self._infer_type_from_values(sample_values)
                nullable = any(v is None or v == '' for v in sample_values)
                
                columns.append({
                    'name': col_name,
                    'type': col_type,
                    'nullable': nullable
                })
        
        # Store main table
        self.tables_structure[table_name] = {
            'columns': columns,
            'parent_table': parent_table,
            'parent_fk': f"{parent_table}_id" if parent_table else None
        }
        
        # Process child tables
        for child_table_name, child_data in child_tables.items():
            if isinstance(child_data, list):
                self._process_object_list(child_data, child_table_name, table_name)
    
    def _process_object(self, obj: Dict, table_name: str, parent_table: Optional[str]):
        """Process a single object into table structure."""
        columns = []
        child_tables = defaultdict(list)
        
        for key, value in obj.items():
            col_name = self._sanitize_column_name(key)
            
            if isinstance(value, (dict, list)) and value:
                # Nested structure
                child_table_name = f"{table_name}_{col_name}"
                if isinstance(value, list) and len(value) > 0:
                    if isinstance(value[0], dict):
                        child_tables[child_table_name] = value
                    else:
                        # List of primitives - store as array or JSONB
                        columns.append({
                            'name': col_name,
                            'type': 'TEXT',  # Store as text representation or JSON
                            'nullable': False
                        })
                elif isinstance(value, dict):
                    child_tables[child_table_name] = [value]
            else:
                # Regular column
                col_type = self._infer_type_from_values([value])
                nullable = value is None or value == ''
                
                columns.append({
                    'name': col_name,
                    'type': col_type,
                    'nullable': nullable
                })
        
        # Store table
        self.tables_structure[table_name] = {
            'columns': columns,
            'parent_table': parent_table,
            'parent_fk': f"{parent_table}_id" if parent_table else None
        }
        
        # Process child tables
        for child_table_name, child_data in child_tables.items():
            if isinstance(child_data, list):
                self._process_object_list(child_data, child_table_name, table_name)
    
    def _process_xml_elements(self, elements: List[ET.Element], table_name: str, parent_table: Optional[str] = None):
        """Process XML elements into table structure."""
        if not elements:
            return
        
        columns = []
        child_tables = defaultdict(list)
        
        first_element = elements[0]
        
        # Process attributes
        for attr_name, attr_value in first_element.attrib.items():
            col_name = self._sanitize_column_name(attr_name)
            col_type = self._infer_type_from_values([attr_value])
            columns.append({
                'name': col_name,
                'type': col_type,
                'nullable': True
            })
        
        # Process child elements
        for child in list(first_element):
            col_name = self._sanitize_column_name(child.tag)
            
            # Check if this tag repeats
            child_count = sum(1 for e in elements if len([c for c in list(e) if c.tag == child.tag]) > 0)
            
            if child_count > 1:
                # Repeating child - create child table
                child_table_name = f"{table_name}_{col_name}"
                child_elements = []
                for e in elements:
                    child_elements.extend([c for c in list(e) if c.tag == child.tag])
                child_tables[child_table_name] = child_elements
            elif child.text and child.text.strip():
                # Text content - regular column
                sample_values = [list(e)[0].text for e in elements if len(list(e)) > 0 and list(e)[0].tag == child.tag]
                col_type = self._infer_type_from_values(sample_values)
                columns.append({
                    'name': col_name,
                    'type': col_type,
                    'nullable': True
                })
        
        # Store table
        self.tables_structure[table_name] = {
            'columns': columns,
            'parent_table': parent_table,
            'parent_fk': f"{parent_table}_id" if parent_table else None
        }
        
        # Process child tables
        for child_table_name, child_elements in child_tables.items():
            self._process_xml_elements(child_elements, child_table_name, table_name)
    
    def _process_xml_element(self, element: ET.Element, table_name: str, parent_table: Optional[str]):
        """Process a single XML element."""
        self._process_xml_elements([element], table_name, parent_table)
    
    def _infer_type_from_values(self, values: List[Any]) -> str:
        """Infer PostgreSQL type from sample values."""
        if not values:
            return 'VARCHAR(255)'
        
        # Filter out None and empty strings
        non_null_values = [v for v in values if v is not None and v != '']
        
        if not non_null_values:
            return 'VARCHAR(255)'  # Default
        
        # Try to infer type
        is_integer = True
        is_float = True
        is_boolean = True
        is_date = True
        max_length = 0
        
        for value in non_null_values:
            if isinstance(value, bool):
                is_float = False
                is_integer = False
            elif isinstance(value, int):
                is_boolean = False
            elif isinstance(value, float):
                is_integer = False
                is_boolean = False
            elif isinstance(value, str):
                is_boolean = False
                
                # Check if it's a number
                try:
                    int(value)
                    is_float = False
                except ValueError:
                    is_integer = False
                    try:
                        float(value)
                    except ValueError:
                        is_float = False
                
                # Check if it's a date
                if not self._looks_like_date(value):
                    is_date = False
                
                # Track max length
                max_length = max(max_length, len(value))
            else:
                is_integer = False
                is_float = False
                is_boolean = False
                is_date = False
        
        # Determine type
        if is_boolean and all(isinstance(v, bool) for v in non_null_values):
            return 'BOOLEAN'
        elif is_integer:
            return 'INTEGER'
        elif is_float:
            return 'FLOAT'
        elif is_date:
            return 'DATE'
        elif max_length <= 255:
            return 'VARCHAR(255)'
        else:
            return 'TEXT'
    
    def _infer_type_from_series(self, series: pd.Series) -> str:
        """Infer type from pandas Series."""
        non_null = series.dropna()
        
        if len(non_null) == 0:
            return 'VARCHAR(255)'
        
        dtype = non_null.dtype
        
        if pd.api.types.is_integer_dtype(dtype):
            return 'INTEGER'
        elif pd.api.types.is_float_dtype(dtype):
            return 'FLOAT'
        elif pd.api.types.is_bool_dtype(dtype):
            return 'BOOLEAN'
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            return 'DATE'
        else:
            max_length = non_null.astype(str).str.len().max()
            if max_length <= 255:
                return 'VARCHAR(255)'
            else:
                return 'TEXT'
    
    def _looks_like_date(self, value: str) -> bool:
        """Check if string looks like a date."""
        date_patterns = [
            r'^\d{4}-\d{2}-\d{2}$',  # YYYY-MM-DD
            r'^\d{2}/\d{2}/\d{4}$',  # MM/DD/YYYY
            r'^\d{2}-\d{2}-\d{4}$',  # MM-DD-YYYY
            r'^\d{4}/\d{2}/\d{2}$',  # YYYY/MM/DD
        ]
        return any(re.match(pattern, value) for pattern in date_patterns)
    
    def _sanitize_table_name(self, name: str) -> str:
        """Sanitize table name for PostgreSQL."""
        # Convert to lowercase, replace spaces/special chars with underscore
        sanitized = re.sub(r'[^a-z0-9_]+', '_', name.lower())
        # Remove leading/trailing underscores
        sanitized = sanitized.strip('_')
        # Ensure it doesn't start with a number
        if sanitized and sanitized[0].isdigit():
            sanitized = f"table_{sanitized}"
        return sanitized or 'table'
    
    def _sanitize_column_name(self, name: str) -> str:
        """Sanitize column name for PostgreSQL."""
        # Convert to lowercase, replace spaces/special chars with underscore
        sanitized = re.sub(r'[^a-z0-9_]+', '_', str(name).lower())
        # Remove leading/trailing underscores
        sanitized = sanitized.strip('_')
        # Ensure it doesn't start with a number
        if sanitized and sanitized[0].isdigit():
            sanitized = f"col_{sanitized}"
        return sanitized or 'column'
    
    def _table_exists(self, table_name: str) -> bool:
        """Check if table exists in PostgreSQL."""
        query = sql.SQL("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = %s
            );
        """)
        self.cursor.execute(query, (table_name,))
        return self.cursor.fetchone()[0]
    
    def _get_existing_columns(self, table_name: str) -> Dict[str, str]:
        """Get existing columns and their types from PostgreSQL."""
        query = sql.SQL("""
            SELECT column_name, data_type, character_maximum_length
            FROM information_schema.columns
            WHERE table_schema = 'public' 
            AND table_name = %s
            ORDER BY ordinal_position;
        """)
        self.cursor.execute(query, (table_name,))
        
        columns = {}
        for row in self.cursor.fetchall():
            col_name, data_type, max_length = row
            if max_length:
                columns[col_name] = f"{data_type}({max_length})"
            else:
                columns[col_name] = data_type
        
        return columns
    
    def _generate_table_sql(self, table_name: str, table_info: Dict) -> Optional[str]:
        """
        Generate CREATE or ALTER TABLE SQL statement.
        
        Returns:
            SQL statement string or None if no changes needed
        """
        table_exists = self._table_exists(table_name)
        existing_columns = self._get_existing_columns(table_name) if table_exists else {}
        
        columns = table_info['columns']
        parent_table = table_info.get('parent_table')
        parent_fk = table_info.get('parent_fk')
        
        if not table_exists:
            # Generate CREATE TABLE
            return self._generate_create_table(table_name, columns, parent_table, parent_fk)
        else:
            # Generate ALTER TABLE for missing columns
            return self._generate_alter_table(table_name, columns, existing_columns)
    
    def _generate_create_table(self, table_name: str, columns: List[Dict], 
                               parent_table: Optional[str], parent_fk: Optional[str]) -> str:
        """Generate CREATE TABLE SQL statement."""
        parts = [f"CREATE TABLE {table_name} ("]
        
        # Start with primary key
        parts.append("    id SERIAL PRIMARY KEY")
        
        # Add parent foreign key if needed
        if parent_fk and parent_table:
            parts.append(f",    {parent_fk} INTEGER REFERENCES {parent_table}(id)")
        
        # Add columns (exclude 'id' since we already have it as SERIAL PRIMARY KEY)
        for col in columns:
            col_name = col['name']
            # Skip 'id' column if it exists in data - we already have it as SERIAL PRIMARY KEY
            if col_name.lower() == 'id':
                continue
            col_type = col['type']
            nullable = "NULL" if col['nullable'] else "NOT NULL"
            parts.append(f",    {col_name} {col_type} {nullable}")
        
        parts.append(");")
        
        return "\n".join(parts)
    
    def _generate_alter_table(self, table_name: str, columns: List[Dict], 
                              existing_columns: Dict[str, str]) -> Optional[str]:
        """Generate ALTER TABLE SQL statements for missing columns."""
        missing_columns = []
        
        for col in columns:
            if col['name'] not in existing_columns:
                missing_columns.append(col)
        
        if not missing_columns:
            return None  # No changes needed
        
        # Generate ALTER TABLE statements
        alter_statements = []
        for col in missing_columns:
            col_name = col['name']
            col_type = col['type']
            nullable = "NULL" if col['nullable'] else "NOT NULL"
            alter_statements.append(
                f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_type} {nullable};"
            )
        
        return "\n".join(alter_statements)
    
    def _extract_table_name_from_sql(self, sql_stmt: str) -> str:
        """Extract table name from SQL statement."""
        # Try CREATE TABLE
        match = re.search(r'CREATE TABLE (\w+)', sql_stmt, re.IGNORECASE)
        if match:
            return match.group(1)
        
        # Try ALTER TABLE
        match = re.search(r'ALTER TABLE (\w+)', sql_stmt, re.IGNORECASE)
        if match:
            return match.group(1)
        
        return self.base_table_name
    
    def _save_sql_to_jobs(self, table_name: str, sql_stmt: str):
        """Save SQL statement to schema_jobs table."""
        query = sql.SQL("""
            INSERT INTO schema_jobs (table_name, sql_text, status)
            VALUES (%s, %s, 'pending')
        """)
        self.cursor.execute(query, (table_name, sql_stmt))
        self.conn.commit()


def generate_schema(file_path: str, db_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convenience function to generate schema from a file.
    
    Args:
        file_path: Path to the file
        db_config: Database configuration dictionary
        
    Returns:
        Results dictionary
    """
    generator = SchemaGenerator(db_config)
    return generator.generate_schema(file_path)


if __name__ == "__main__":
    # Example usage
    import sys
    import argparse
    
    parser = argparse.ArgumentParser(description='Schema Generator - Program A')
    parser.add_argument('file_path', help='Path to the file to process')
    parser.add_argument('--provider', type=str, help='Cloud provider (aws, gcp, azure, supabase, neon, heroku)')
    parser.add_argument('--config', type=str, help='Use custom config module')
    args = parser.parse_args()
    
    file_path = args.file_path
    
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
            print("❌ Invalid database configuration!")
            print()
            print("📋 Setup Instructions:")
            print("   1. Install PostgreSQL: https://www.postgresql.org/download/")
            print("   2. Create database: createdb clustro")
            print("   3. Set DB_PASSWORD environment variable")
            print("   4. Or use --provider flag for cloud databases")
            print()
            sys.exit(1)
    except ImportError:
        print("❌ Config module not found!")
        print("   Please make sure config.py is in the same directory.")
        sys.exit(1)
    
    print("=" * 60)
    print("Schema Generator - Program A")
    print("=" * 60)
    print(f"Processing file: {file_path}")
    print()
    
    result = generate_schema(file_path, db_config)
    
    print(f"Success: {result['success']}")
    print(f"Tables detected: {len(result['tables'])}")
    print(f"SQL statements generated: {len(result['sql_statements'])}")
    print(f"Jobs created in schema_jobs: {result['jobs_created']}")
    
    if result['tables']:
        print(f"\nTables: {', '.join(result['tables'])}")
    
    if result['sql_statements']:
        print(f"\nGenerated SQL Statements:")
        for i, sql_stmt in enumerate(result['sql_statements'], 1):
            print(f"\n--- Statement {i} ---")
            print(sql_stmt)
    
    if result['errors']:
        print(f"\nErrors:")
        for error in result['errors']:
            print(f"  - {error}")

