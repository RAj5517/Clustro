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

# Import Schema Similarity Engine
try:
    from Schema_generator.schema_similarity_engine import get_schema_similarity_engine
    HAS_SIMILARITY_ENGINE = True
except ImportError:
    HAS_SIMILARITY_ENGINE = False
    print("[WARNING] Schema Similarity Engine not available, using basic matching")
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
        self.tables_structure = {}  # {table_name: {columns: [...], constraints: [...], data: [...]}}
        
        # Initialize Schema Similarity Engine
        if HAS_SIMILARITY_ENGINE:
            self.similarity_engine = get_schema_similarity_engine()
        else:
            self.similarity_engine = None
    
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
        
        # Handle unknown file types by attempting to parse as text first
        # This allows files with non-standard extensions (like .docs) to be processed
        if self.file_type == 'unknown':
            print(f"[INFO] Unknown file type for '{file_path.name}', attempting to parse as text...")
            self.file_type = 'text'  # Treat as text temporarily
        
        # Try to parse the file first to check if it contains structured data
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
        
        # Check if text file (or unknown file treated as text) contains structured data (CSV/TSV)
        # If it does, treat it as CSV/TSV; otherwise skip it
        if self.file_type == 'text':
            if self._is_structured_text(self.file_content):
                # Re-detect as CSV if it contains comma-separated or tab-separated values
                delimiter = self._detect_delimiter(self.file_content)
                if delimiter in [',', '\t']:
                    # Change file type to CSV and re-parse with proper delimiter
                    self.file_type = 'csv'
                    if delimiter == ',':
                        print(f"[INFO] Text file '{file_path.name}' detected as CSV (comma-separated)")
                    else:
                        print(f"[INFO] Text file '{file_path.name}' detected as TSV (tab-separated)")
                    
                    # Re-parse the file as CSV with the detected delimiter
                    try:
                        self.file_content = self._parse_file_as_csv(file_path, delimiter)
                    except Exception as e:
                        return {
                            'success': False,
                            'tables': [],
                            'sql_statements': [],
                            'jobs_created': 0,
                            'errors': [f"Failed to parse text file as CSV/TSV: {str(e)}"]
                        }
                else:
                    # Not structured - skip it
                    return {
                        'success': False,
                        'tables': [],
                        'sql_statements': [],
                        'jobs_created': 0,
                        'errors': ["File type 'text' does not contain structured data (CSV/TSV). Should be handled by NoSQL storage"]
                    }
            else:
                # Unstructured text - skip it
                return {
                    'success': False,
                    'tables': [],
                    'sql_statements': [],
                    'jobs_created': 0,
                    'errors': ["File type 'text' is unstructured and should be handled by NoSQL storage"]
                }
        
        # Skip other text-heavy file types that can't contain structured data
        if self.file_type in ['log', 'md', 'pdf', 'docx']:
            return {
                'success': False,
                'tables': [],
                'sql_statements': [],
                'jobs_created': 0,
                'errors': [f"File type '{self.file_type}' is text-heavy and should be handled by NoSQL storage"]
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
            actual_tables = []  # Track actual table names used (may differ from original)
            
            for original_table_name, table_info in self.tables_structure.items():
                try:
                    # Check if a similar table exists and we should reuse it
                    similar_table = self._find_similar_existing_table(table_info['columns'])
                    
                    if similar_table and similar_table != original_table_name:
                        # Use existing similar table instead of creating new one
                        table_info['reuse_table'] = similar_table
                        actual_table_name = similar_table
                        print(f"[INFO] Reusing existing table '{similar_table}' instead of creating '{original_table_name}'")
                    else:
                        actual_table_name = original_table_name
                    
                    # Always track the actual table name (even if no SQL changes needed)
                    if actual_table_name not in actual_tables:
                        actual_tables.append(actual_table_name)
                    
                    # Generate SQL statement (CREATE or ALTER)
                    sql_stmt = self._generate_table_sql(actual_table_name, table_info)
                    if sql_stmt:
                        sql_statements.append(sql_stmt)
                except Exception as e:
                    errors.append(f"Error generating SQL for {original_table_name}: {str(e)}")
            
            # Save CREATE/ALTER TABLE statements to schema_jobs table
            jobs_created = 0
            table_sql_map = {}  # Map table names to their CREATE/ALTER SQL statements
            
            for sql_stmt in sql_statements:
                try:
                    table_name = self._extract_table_name_from_sql(sql_stmt)
                    self._save_sql_to_jobs(table_name, sql_stmt)
                    table_sql_map[table_name] = sql_stmt
                    jobs_created += 1
                except Exception as e:
                    errors.append(f"Error saving SQL to jobs: {str(e)}")
            
            # Generate and save INSERT statements for tables with data
            insert_jobs_created = 0
            for original_table_name, table_info in self.tables_structure.items():
                # Find the actual table name (may have been reused)
                actual_table_name = table_info.get('reuse_table', original_table_name)
                
                # Always insert data, whether table was created or reused
                data_rows = table_info.get('data', [])
                if not data_rows:
                    continue  # No data to insert
                
                try:
                    # Get existing columns if table is reused (for column mapping)
                    existing_columns_for_mapping = {}
                    if actual_table_name != original_table_name:
                        # Table was reused, get existing columns for mapping
                        existing_columns_for_mapping = self._get_existing_columns(actual_table_name)
                    
                    # Generate INSERT statements with column mapping
                    insert_statements = self._generate_insert_statements(
                        actual_table_name, 
                        table_info['columns'], 
                        data_rows,
                        existing_columns_for_mapping
                    )
                    
                    for insert_stmt in insert_statements:
                        self._save_sql_to_jobs(actual_table_name, insert_stmt)
                        insert_jobs_created += 1
                        sql_statements.append(insert_stmt)
                    
                except Exception as e:
                    errors.append(f"Error generating INSERT statements for {actual_table_name}: {str(e)}")
            
            total_jobs = jobs_created + insert_jobs_created
            
            return {
                'success': len(errors) == 0,
                'tables': actual_tables if actual_tables else list(self.tables_structure.keys()),
                'sql_statements': sql_statements,
                'jobs_created': total_jobs,
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
    
    def _is_structured_text(self, text_content: str) -> bool:
        """
        Check if text content contains structured data (CSV/TSV patterns).
        
        Args:
            text_content: The text content to analyze
            
        Returns:
            True if the text appears to be structured (CSV/TSV), False otherwise
        """
        if not text_content or not isinstance(text_content, str):
            return False
        
        lines = text_content.strip().split('\n')
        if len(lines) < 2:
            return False  # Need at least a header and one data row
        
        # Check for CSV pattern (comma-separated)
        first_line_fields = len([f for f in lines[0].split(',') if f.strip()])
        if first_line_fields > 1:
            # Check if multiple lines have consistent field counts
            consistent_csv = all(
                len([f for f in line.split(',') if f.strip()]) == first_line_fields 
                for line in lines[1:11]  # Check first 10 data rows
                if line.strip()
            )
            if consistent_csv:
                return True
        
        # Check for TSV pattern (tab-separated)
        first_line_tsv = len([f for f in lines[0].split('\t') if f.strip()])
        if first_line_tsv > 1:
            consistent_tsv = all(
                len([f for f in line.split('\t') if f.strip()]) == first_line_tsv
                for line in lines[1:11]  # Check first 10 data rows
                if line.strip()
            )
            if consistent_tsv:
                return True
        
        return False
    
    def _detect_delimiter(self, text_content: str) -> str:
        """
        Detect the delimiter used in structured text (CSV or TSV).
        
        Args:
            text_content: The text content to analyze
            
        Returns:
            The detected delimiter (',' or '\t'), or None if not detected
        """
        if not text_content or not isinstance(text_content, str):
            return None
        
        lines = text_content.strip().split('\n')
        if len(lines) < 2:
            return None
        
        # Check comma delimiter
        first_line_csv = len([f for f in lines[0].split(',') if f.strip()])
        if first_line_csv > 1:
            consistent_csv = all(
                len([f for f in line.split(',') if f.strip()]) == first_line_csv
                for line in lines[1:11]
                if line.strip()
            )
            if consistent_csv:
                return ','
        
        # Check tab delimiter
        first_line_tsv = len([f for f in lines[0].split('\t') if f.strip()])
        if first_line_tsv > 1:
            consistent_tsv = all(
                len([f for f in line.split('\t') if f.strip()]) == first_line_tsv
                for line in lines[1:11]
                if line.strip()
            )
            if consistent_tsv:
                return '\t'
        
        return None
    
    def _parse_file_as_csv(self, file_path: Path, delimiter: str = ',') -> Any:
        """
        Parse file as CSV with specified delimiter.
        
        Args:
            file_path: Path to the file
            delimiter: Delimiter to use (',' or '\t')
            
        Returns:
            Parsed CSV data (DataFrame or list of dicts)
        """
        if HAS_PANDAS:
            return pd.read_csv(file_path, delimiter=delimiter)
        else:
            with open(file_path, 'r', encoding='utf-8') as f:
                return list(csv.DictReader(f, delimiter=delimiter))
    
    def _parse_file(self, file_path: Path) -> Any:
        """Parse file based on detected type."""
        if self.file_type == 'json':
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        
        elif self.file_type == 'csv':
            # Default delimiter is comma
            return self._parse_file_as_csv(file_path, ',')
        
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
        
        elif self.file_type == 'text':
            # Read as plain text first - we'll check if it's structured CSV/TSV later
            # This also handles unknown file types that we treat as text
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    return f.read()
            except UnicodeDecodeError:
                # If UTF-8 fails, try with error handling
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
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
                # Make columns nullable by default (except id and critical fields)
                col_lower = col_name.lower()
                is_critical = col_lower in ['id', 'email', 'username', 'password', 'uuid', 'key']
                nullable = True if not is_critical else df[col].isna().any()
                
                columns.append({
                    'name': col_name,
                    'type': col_type,
                    'nullable': nullable
                })
            
            # Store data rows for insertion (convert DataFrame to list of dicts with sanitized column names)
            if HAS_PANDAS and isinstance(self.file_content, pd.DataFrame):
                data_rows = []
                for _, row in df.iterrows():
                    row_dict = {}
                    for col in df.columns:
                        sanitized_col = self._sanitize_column_name(col)
                        value = row[col]
                        # Convert pandas NaN to None
                        row_dict[sanitized_col] = None if pd.isna(value) else value
                    data_rows.append(row_dict)
            else:
                data_rows = []
            
            self.tables_structure[self.base_table_name] = {
                'columns': columns,
                'parent_table': None,
                'parent_fk': None,
                'data': data_rows  # Store data for insertion
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
                    # Make columns nullable by default (except id and critical fields)
                    col_lower = sanitized_name.lower()
                    is_critical = col_lower in ['id', 'email', 'username', 'password', 'uuid', 'key']
                    nullable = True if not is_critical else any(v is None or v == '' for v in sample_values)
                    
                    columns.append({
                        'name': sanitized_name,
                        'type': col_type,
                        'nullable': nullable
                    })
                
                # Store data rows for insertion
                data_rows = self.file_content if isinstance(self.file_content, list) else []
                
                self.tables_structure[self.base_table_name] = {
                    'columns': columns,
                    'parent_table': None,
                    'parent_fk': None,
                    'data': data_rows  # Store data for insertion
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
            
            # Store data rows for insertion (convert DataFrame to list of dicts with sanitized column names)
            if HAS_PANDAS and isinstance(self.file_content, pd.DataFrame):
                data_rows = []
                for _, row in df.iterrows():
                    row_dict = {}
                    for col in df.columns:
                        sanitized_col = self._sanitize_column_name(col)
                        value = row[col]
                        # Convert pandas NaN to None
                        row_dict[sanitized_col] = None if pd.isna(value) else value
                    data_rows.append(row_dict)
            else:
                data_rows = []
            
            self.tables_structure[self.base_table_name] = {
                'columns': columns,
                'parent_table': None,
                'parent_fk': None,
                'data': data_rows  # Store data for insertion
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
                # Make columns nullable by default (except id and critical fields)
                col_lower = col_name.lower()
                is_critical = col_lower in ['id', 'email', 'username', 'password', 'uuid', 'key']
                nullable = True if not is_critical else any(v is None or v == '' for v in sample_values)
                
                columns.append({
                    'name': col_name,
                    'type': col_type,
                    'nullable': nullable
                })
        
        # Store data rows for insertion (flatten nested objects for main table)
        data_rows = []
        for obj in obj_list:
            flat_obj = {}
            for key, value in obj.items():
                col_name = self._sanitize_column_name(key)
                # Only include non-nested values in main table data
                if not isinstance(value, (dict, list)) or not value:
                    flat_obj[col_name] = value
            data_rows.append(flat_obj)
        
        # Store main table
        self.tables_structure[table_name] = {
            'columns': columns,
            'parent_table': parent_table,
            'parent_fk': f"{parent_table}_id" if parent_table else None,
            'data': data_rows  # Store data for insertion
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
                        # Make nullable for flexibility
                        columns.append({
                            'name': col_name,
                            'type': 'TEXT',  # Store as text representation or JSON
                            'nullable': True
                        })
                elif isinstance(value, dict):
                    child_tables[child_table_name] = [value]
            else:
                # Regular column
                col_type = self._infer_type_from_values([value])
                # Make columns nullable by default (except id and critical fields)
                col_lower = col_name.lower()
                is_critical = col_lower in ['id', 'email', 'username', 'password', 'uuid', 'key']
                nullable = True if not is_critical else (value is None or value == '')
                
                columns.append({
                    'name': col_name,
                    'type': col_type,
                    'nullable': nullable
                })
        
        # Store data for single object (wrap in list)
        flat_obj = {}
        for key, value in obj.items():
            col_name = self._sanitize_column_name(key)
            # Only include non-nested values in main table data
            if not isinstance(value, (dict, list)) or not value:
                flat_obj[col_name] = value
        data_rows = [flat_obj] if flat_obj else []
        
        # Store table
        self.tables_structure[table_name] = {
            'columns': columns,
            'parent_table': parent_table,
            'parent_fk': f"{parent_table}_id" if parent_table else None,
            'data': data_rows  # Store data for insertion
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
        
        # Store data rows for insertion (extract from XML elements)
        data_rows = []
        for element in elements:
            row = {}
            # Add attributes
            for attr_name, attr_value in element.attrib.items():
                col_name = self._sanitize_column_name(attr_name)
                row[col_name] = attr_value
            # Add text content and child elements (non-repeating)
            for child in list(element):
                col_name = self._sanitize_column_name(child.tag)
                # Only include non-repeating child elements
                if child.tag not in [t for t, _ in child_tables.items()]:
                    row[col_name] = child.text if child.text else None
            if row:
                data_rows.append(row)
        
        # Store table
        self.tables_structure[table_name] = {
            'columns': columns,
            'parent_table': parent_table,
            'parent_fk': f"{parent_table}_id" if parent_table else None,
            'data': data_rows  # Store data for insertion
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
    
    def _get_existing_columns(self, table_name: str) -> Dict[str, Dict[str, Any]]:
        """Get existing columns and their types/nullability from PostgreSQL."""
        query = sql.SQL("""
            SELECT column_name, data_type, character_maximum_length, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'public' 
            AND table_name = %s
            ORDER BY ordinal_position;
        """)
        self.cursor.execute(query, (table_name,))
        
        columns = {}
        for row in self.cursor.fetchall():
            col_name, data_type, max_length, is_nullable = row
            type_str = f"{data_type}({max_length})" if max_length else data_type
            columns[col_name] = {
                'type': type_str,
                'nullable': is_nullable == 'YES'
            }
        
        return columns
    
    def _find_similar_existing_table(self, new_columns: List[Dict]) -> Optional[str]:
        """
        Find an existing table that can accommodate the new schema using intelligent similarity matching.
        
        Returns:
            Table name if found, None otherwise
        """
        # Get all existing tables
        self.cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_type = 'BASE TABLE'
            AND table_name != 'schema_jobs'
            ORDER BY table_name;
        """)
        
        existing_tables = [row[0] for row in self.cursor.fetchall()]
        
        if not existing_tables:
            return None
        
        if not new_columns:
            return None  # No columns to match
        
        # Build new schema dict
        new_schema = {
            'columns': new_columns,
            'parent_table': None  # Will be set by caller if needed
        }
        
        # Use Schema Similarity Engine if available
        if self.similarity_engine:
            # Build existing schemas list
            existing_schemas = []
            for table_name in existing_tables:
                existing_cols = self._get_existing_columns(table_name)
                # Convert to list format
                columns = []
                for col_name, col_info in existing_cols.items():
                    columns.append({
                        'name': col_name,
                        'type': col_info['type'],
                        'nullable': col_info['nullable']
                    })
                
                existing_schemas.append({
                    'table_name': table_name,
                    'columns': columns,
                    'parent_table': None
                })
            
            # Find best match using similarity engine
            match_result = self.similarity_engine.find_best_match(new_schema, existing_schemas)
            
            if match_result:
                table_name, similarity_score, decision = match_result
                print(f"[INFO] Schema similarity match: '{table_name}' (score: {similarity_score:.2%}, decision: {decision})")
                
                if decision in ['same_table', 'evolved_table']:
                    return table_name
                # If decision is 'new_table', fall through to return None
            
            return None  # No good match found
        
        else:
            # Fallback to basic matching if similarity engine not available
            new_col_names = set(col['name'].lower() for col in new_columns if col['name'].lower() != 'id')
            
            for table_name in existing_tables:
                existing_columns = self._get_existing_columns(table_name)
                existing_col_names = set(col.lower() for col in existing_columns.keys() if col.lower() != 'id')
                
                # If all new columns exist in this table, we can use it
                if new_col_names.issubset(existing_col_names):
                    return table_name  # Perfect match - all columns exist
                
                # Partial match: if most columns overlap, we can extend this table
                overlap = len(new_col_names & existing_col_names)
                overlap_ratio = overlap / len(new_col_names) if new_col_names else 0
                
                # If at least 50% of columns overlap and we have 2+ matching columns, reuse this table
                if overlap_ratio >= 0.5 and overlap >= 2:
                    return table_name  # Good match - can extend this table
            
            return None
    
    def _generate_table_sql(self, table_name: str, table_info: Dict) -> Optional[str]:
        """
        Generate CREATE or ALTER TABLE SQL statement.
        
        Args:
            table_name: Name of the table (already adjusted if reusing existing table)
            table_info: Table structure information
            
        Returns:
            SQL statement string or None if no changes needed
        """
        table_exists = self._table_exists(table_name)
        existing_columns_dict = self._get_existing_columns(table_name) if table_exists else {}
        
        columns = table_info['columns']
        parent_table = table_info.get('parent_table')
        parent_fk = table_info.get('parent_fk')
        
        if not table_exists:
            # Generate CREATE TABLE
            return self._generate_create_table(table_name, columns, parent_table, parent_fk)
        else:
            # Generate ALTER TABLE for missing columns
            return self._generate_alter_table(table_name, columns, existing_columns_dict)
    
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
                              existing_columns: Dict[str, Dict[str, Any]]) -> Optional[str]:
        """
        Generate ALTER TABLE SQL statements for missing columns.
        Uses similarity engine to avoid adding columns that are similar to existing ones.
        New columns are added as NULLABLE to allow existing rows.
        """
        missing_columns = []
        existing_col_names = set(existing_columns.keys()) if existing_columns else set()
        
        for col in columns:
            col_name = col['name']
            # Skip id column and columns that already exist
            if col_name.lower() == 'id':
                continue
            
            # Check exact match first
            if col_name in existing_col_names:
                continue
            
            # Check for similar column names using similarity engine
            is_similar = False
            if self.similarity_engine:
                for existing_col_name in existing_col_names:
                    # Skip id column from comparison
                    if existing_col_name.lower() == 'id':
                        continue
                    
                    # Check field name similarity
                    similarity = self.similarity_engine.field_name_similarity(col_name, existing_col_name)
                    
                    # If similarity is high (>=0.8), consider it the same column
                    if similarity >= 0.8:
                        # Also check type compatibility
                        existing_type = existing_columns[existing_col_name].get('type', '')
                        new_type = col.get('type', '')
                        type_comp = self.similarity_engine.type_compatibility(new_type, existing_type)
                        
                        # If both name and type are compatible, skip adding this column
                        if type_comp >= 0.7:
                            is_similar = True
                            print(f"[INFO] Skipping column '{col_name}' - similar to existing '{existing_col_name}' (similarity: {similarity:.2%}, type: {type_comp:.2%})")
                            break
            
            # Only add if not similar to any existing column
            if not is_similar:
                missing_columns.append(col)
        
        if not missing_columns:
            return None  # No changes needed
        
        # Generate ALTER TABLE statements
        # New columns are added as NULLABLE to accommodate existing data
        alter_statements = []
        for col in missing_columns:
            col_name = col['name']
            col_type = col['type']
            # Always add new columns as nullable to avoid breaking existing data
            # Critical fields can still be NOT NULL if needed
            col_lower = col_name.lower()
            is_critical = col_lower in ['id', 'email', 'username', 'password', 'uuid', 'key']
            nullable = "NULL" if not is_critical or col.get('nullable', True) else "NOT NULL"
            alter_statements.append(
                f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_type} {nullable};"
            )
        
        return "\n".join(alter_statements)
    
    def _generate_insert_statements(self, table_name: str, columns: List[Dict], data_rows: List[Dict], 
                                    existing_columns: Dict[str, Dict[str, Any]] = None) -> List[str]:
        """
        Generate INSERT statements from data rows.
        
        Args:
            table_name: Name of the table
            columns: List of column definitions
            data_rows: List of data dictionaries (rows)
            existing_columns: Existing columns dict (for column mapping when reusing tables)
            
        Returns:
            List of INSERT SQL statements (batched for efficiency)
        """
        if not data_rows:
            return []
        
        # Build column mapping: map new column names to existing column names
        # This handles cases where product_name maps to name, etc.
        column_mapping = {}  # {new_col_name: existing_col_name}
        target_column_names = []  # Columns that actually exist in the table
        
        existing_col_names = set(existing_columns.keys()) if existing_columns else set()
        
        for col in columns:
            col_name = col['name']
            if col_name.lower() == 'id':
                continue
            
            # If column exists exactly, use it
            if col_name in existing_col_names:
                column_mapping[col_name] = col_name
                if col_name not in target_column_names:
                    target_column_names.append(col_name)
            elif existing_columns and self.similarity_engine:
                # Check if similar column exists
                best_match = None
                best_similarity = 0.0
                
                for existing_col_name in existing_col_names:
                    if existing_col_name.lower() == 'id':
                        continue
                    
                    similarity = self.similarity_engine.field_name_similarity(col_name, existing_col_name)
                    if similarity > best_similarity and similarity >= 0.8:
                        # Also check type compatibility
                        existing_type = existing_columns[existing_col_name].get('type', '')
                        new_type = col.get('type', '')
                        type_comp = self.similarity_engine.type_compatibility(new_type, existing_type)
                        
                        if type_comp >= 0.7:
                            best_match = existing_col_name
                            best_similarity = similarity
                
                if best_match:
                    # Map new column to existing similar column
                    column_mapping[col_name] = best_match
                    if best_match not in target_column_names:
                        target_column_names.append(best_match)
                    print(f"[INFO] Mapping column '{col_name}' -> '{best_match}' for INSERT (similarity: {best_similarity:.2%})")
            else:
                # New column (not in existing table), use as-is
                column_mapping[col_name] = col_name
                if col_name not in target_column_names:
                    target_column_names.append(col_name)
        
        if not target_column_names:
            return []  # No columns to insert (only id)
        
        # Generate INSERT statements (batch multiple rows per statement for efficiency)
        insert_statements = []
        batch_size = 100  # Insert 100 rows per statement
        
        for i in range(0, len(data_rows), batch_size):
            batch = data_rows[i:i + batch_size]
            
            # Build column list (use target column names)
            cols_str = ", ".join([f'"{col}"' for col in target_column_names])
            
            # Build values list
            values_list = []
            for row in batch:
                row_values = []
                for target_col_name in target_column_names:
                    # Find the source column name in the data
                    # (reverse mapping: existing_col -> new_col in data)
                    source_col_name = None
                    for new_col, mapped_col in column_mapping.items():
                        if mapped_col == target_col_name:
                            source_col_name = new_col
                            break
                    
                    if not source_col_name:
                        source_col_name = target_col_name  # Fallback
                    
                    # Handle different row formats (dict or pandas Series)
                    if isinstance(row, dict):
                        value = row.get(source_col_name)
                    elif hasattr(row, 'get'):  # pandas Series
                        value = row.get(source_col_name)
                    else:
                        value = None
                    
                    # Handle different data types
                    if value is None:
                        row_values.append('NULL')
                    elif isinstance(value, bool):
                        row_values.append('TRUE' if value else 'FALSE')
                    elif isinstance(value, (int, float)):
                        row_values.append(str(value))
                    elif isinstance(value, str):
                        # Escape single quotes and wrap in quotes
                        escaped_value = value.replace("'", "''")
                        row_values.append(f"'{escaped_value}'")
                    else:
                        # Convert to string and escape
                        escaped_value = str(value).replace("'", "''")
                        row_values.append(f"'{escaped_value}'")
                
                values_list.append(f"({', '.join(row_values)})")
            
            # Build INSERT statement
            values_str = ",\n    ".join(values_list)
            insert_stmt = f"INSERT INTO {table_name} ({cols_str})\nVALUES\n    {values_str};"
            insert_statements.append(insert_stmt)
        
        return insert_statements
    
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
        
        # Try INSERT INTO
        match = re.search(r'INSERT INTO (\w+)', sql_stmt, re.IGNORECASE)
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

