"""
File Classifier for SQL/NoSQL Routing

This module classifies non-media files into SQL or NoSQL categories
based on structure, consistency, nestedness, and content patterns.
"""

import json
import csv
import xml.etree.ElementTree as ET
import re
import mimetypes
import sys
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional
from collections import Counter

# Add parent directory to path for logger import
sys.path.insert(0, str(Path(__file__).parent.parent))

from logger_config import get_logger
logger = get_logger('auraverse.file_classifier')

# Optional imports for advanced file types
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


class FileClassifier:
    """Classifies files as SQL or NoSQL based on structure and content analysis."""
    
    def __init__(self):
        self.file_type = None
        self.file_content = None
        self.sql_score = 0
        self.nosql_score = 0
        self.reasons = []
        logger.debug("FileClassifier initialized")
    
    def classify(self, file_path: str) -> Dict[str, Any]:
        """
        Main classification method.
        
        Args:
            file_path: Path to the file to classify
            
        Returns:
            Dictionary with classification results:
            {
                'classification': 'SQL' or 'NoSQL',
                'sql_score': float,
                'nosql_score': float,
                'confidence': float,
                'reasons': List[str],
                'file_type': str
            }
        """
        file_path = Path(file_path)
        logger.info(f"Classifying file: {file_path.name}")
        
        if not file_path.exists():
            logger.error(f"File not found: {file_path}")
            raise FileNotFoundError(f"File not found: {file_path}")
        
        # Detect file type
        self.file_type = self._detect_file_type(file_path)
        logger.debug(f"Detected file type: {self.file_type}")
        
        # Parse file based on type
        logger.debug(f"Parsing {self.file_type} file...")
        self.file_content = self._parse_file(file_path)
        logger.debug(f"File parsed successfully")
        
        # Reset scores
        self.sql_score = 0
        self.nosql_score = 0
        self.reasons = []
        
        # Analyze and score
        if self.file_type in ['json']:
            self._analyze_json()
        elif self.file_type in ['csv', 'excel']:
            self._analyze_tabular()
        elif self.file_type in ['xml']:
            self._analyze_xml()
        elif self.file_type in ['html']:
            self._analyze_html()
        elif self.file_type in ['yaml']:
            self._analyze_yaml()
        elif self.file_type in ['text', 'log', 'md']:
            self._analyze_text()
        elif self.file_type in ['pdf', 'docx']:
            self._analyze_document()
        else:
            # Unknown file type - try to analyze as text first
            if self.file_content is None:
                # Binary file or couldn't parse - lean towards NoSQL
                self.nosql_score += 2
                self.reasons.append("Unknown binary file type - defaulting to NoSQL (unstructured)")
            elif isinstance(self.file_content, str):
                # Analyze unknown file as text to detect any structured patterns
                self._analyze_unknown_text()
            else:
                # Non-text unknown file - lean towards NoSQL
                self.nosql_score += 2
                self.reasons.append("Unknown file type - defaulting to NoSQL (unstructured)")
        
        # Determine classification
        classification = 'SQL' if self.sql_score >= self.nosql_score else 'NoSQL'
        confidence = abs(self.sql_score - self.nosql_score) / max(1, max(self.sql_score, self.nosql_score))
        
        return {
            'classification': classification,
            'sql_score': round(self.sql_score, 2),
            'nosql_score': round(self.nosql_score, 2),
            'confidence': round(confidence, 2),
            'reasons': self.reasons,
            'file_type': self.file_type
        }
    
    def _detect_file_type(self, file_path: Path) -> str:
        """Detect file type from extension and mimetype."""
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
            '.ini': 'ini',
            '.cfg': 'ini',
            '.conf': 'ini',
            '.pdf': 'pdf',
            '.docx': 'docx',
            '.doc': 'docx'
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
        
        elif self.file_type in ['text', 'log', 'md']:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                return f.read()
        
        elif self.file_type == 'pdf':
            if HAS_PDF:
                with open(file_path, 'rb') as f:
                    reader = PyPDF2.PdfReader(f)
                    text = ""
                    for page in reader.pages:
                        text += page.extract_text()
                    return text
            else:
                raise ImportError("PyPDF2 required for PDF file parsing")
        
        elif self.file_type == 'docx':
            if HAS_DOCX:
                doc = Document(file_path)
                return '\n'.join([para.text for para in doc.paragraphs])
            else:
                raise ImportError("python-docx required for DOCX file parsing")
        
        elif self.file_type == 'ini':
            config = {}
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        config[key.strip()] = value.strip()
            return config
        
        else:
            # Try to read as text (for unknown file types)
            try:
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    return f.read()
            except (UnicodeDecodeError, ValueError):
                # Binary file - return None to indicate non-text
                return None
    
    def _analyze_json(self):
        """Analyze JSON structure and assign scores."""
        data = self.file_content
        
        # Check if it's a flat structure
        is_flat = self._is_flat_structure(data)
        if is_flat:
            self.sql_score += 4
            self.reasons.append("JSON is flat (no nested objects/arrays) - +4 SQL")
        else:
            nested_depth = self._get_nested_depth(data)
            if nested_depth > 0:
                self.nosql_score += 4
                self.reasons.append(f"JSON contains nested objects (depth: {nested_depth}) - +4 NoSQL")
        
        # Check if it's an array of objects
        if isinstance(data, list) and len(data) > 0:
            # Check if all objects have identical keys
            if isinstance(data[0], dict):
                keys_list = [set(obj.keys()) for obj in data if isinstance(obj, dict)]
                if len(keys_list) > 0:
                    if all(keys == keys_list[0] for keys in keys_list):
                        self.sql_score += 4
                        self.reasons.append("JSON array with identical keys in each object - +4 SQL")
                    else:
                        self.nosql_score += 3
                        self.reasons.append("JSON arrays with inconsistent object structure - +3 NoSQL")
        
        # Check for relational patterns
        if self._has_relational_patterns(data):
            self.sql_score += 1
            self.reasons.append("Contains relational patterns (IDs, foreign keys) - +1 SQL")
        
        # Check for dynamic keys
        if isinstance(data, (dict, list)):
            if self._has_dynamic_keys(data):
                self.nosql_score += 2
                self.reasons.append("Dynamic keys that change across objects - +2 NoSQL")
        
        # Check schema consistency
        if self._is_schema_consistent(data):
            self.sql_score += 2
            self.reasons.append("Schema is consistent and predictable - +2 SQL")
        else:
            self.nosql_score += 2
            self.reasons.append("Schema expected to grow or lack structure - +2 NoSQL")
        
        # Check for primitive types vs complex
        if self._is_mostly_primitive(data):
            self.sql_score += 1
            self.reasons.append("Mostly primitive fields (numbers, strings, booleans) - +1 SQL")
        
        # Check for large text fields
        if self._has_large_text_fields(data):
            self.nosql_score += 2
            self.reasons.append("Contains large free-text fields (content, description, html) - +2 NoSQL")
    
    def _analyze_tabular(self):
        """Analyze CSV/Excel files."""
        self.sql_score += 5
        self.reasons.append("File type is CSV/Excel (tabular) - +5 SQL")
        
        if isinstance(self.file_content, pd.DataFrame):
            # Check for consistent columns
            if len(self.file_content.columns) > 0:
                self.sql_score += 2
                self.reasons.append("Schema is consistent and predictable - +2 SQL")
            
            # Check for ID-like columns
            col_names = [col.lower() for col in self.file_content.columns]
            if any('id' in col for col in col_names):
                self.sql_score += 1
                self.reasons.append("Contains relational patterns (IDs) - +1 SQL")
        
        elif isinstance(self.file_content, list) and len(self.file_content) > 0:
            # DictReader result
            first_keys = set(self.file_content[0].keys())
            if all(set(row.keys()) == first_keys for row in self.file_content):
                self.sql_score += 2
                self.reasons.append("Schema is consistent and predictable - +2 SQL")
    
    def _analyze_xml(self):
        """Analyze XML structure."""
        root = self.file_content
        
        # Check depth
        max_depth = self._get_xml_depth(root)
        if max_depth > 2:
            self.nosql_score += 3
            self.reasons.append(f"Deeply nested XML (depth: {max_depth}) - +3 NoSQL")
        
        # Check for repeating elements (tabular pattern)
        children = list(root)
        if len(children) > 0:
            first_child_tags = set(child.tag for child in children)
            if len(first_child_tags) == 1 and len(children) > 1:
                # Same tag repeated - tabular pattern
                self.sql_score += 3
                self.reasons.append("XML has repeating tags (records with same structure) - +3 SQL")
            
            # Check if all children have same structure
            if len(children) > 1:
                first_attrs = set(children[0].attrib.keys())
                if all(set(child.attrib.keys()) == first_attrs for child in children[1:]):
                    self.sql_score += 2
                    self.reasons.append("Schema is consistent and predictable - +2 SQL")
    
    def _analyze_html(self):
        """Analyze HTML content."""
        html_content = self.file_content.lower()
        
        # Check for tables
        table_count = html_content.count('<table')
        if table_count > 0:
            self.sql_score += 3
            self.reasons.append(f"HTML contains well-formed tables ({table_count} tables) - +3 SQL")
        
        # Check if it's mostly content-heavy
        if len(html_content) > 5000 and table_count == 0:
            self.nosql_score += 1
            self.reasons.append("HTML without structured tables (content-heavy) - +1 NoSQL")
        
        # Check for large text content
        text_content = re.sub(r'<[^>]+>', '', html_content)
        if len(text_content) > 3000:
            self.nosql_score += 2
            self.reasons.append("Contains large free-text fields - +2 NoSQL")
    
    def _analyze_yaml(self):
        """Analyze YAML structure (similar to JSON)."""
        data = self.file_content
        
        # Similar analysis as JSON
        is_flat = self._is_flat_structure(data)
        if is_flat:
            self.sql_score += 4
            self.reasons.append("YAML is flat (no nested objects) - +4 SQL")
        else:
            nested_depth = self._get_nested_depth(data)
            if nested_depth > 0:
                self.nosql_score += 4
                self.reasons.append(f"YAML contains nested objects (depth: {nested_depth}) - +4 NoSQL")
        
        # Check consistency
        if self._is_schema_consistent(data):
            self.sql_score += 2
            self.reasons.append("Schema is consistent and predictable - +2 SQL")
    
    def _analyze_text(self):
        """Analyze plain text files."""
        text_content = self.file_content
        
        # Text files are unstructured
        self.nosql_score += 3
        self.reasons.append("File is text-based (.txt, .md, .log) - +3 NoSQL")
        
        # Check if it's very long (content-heavy)
        if len(text_content) > 5000:
            self.nosql_score += 2
            self.reasons.append("Contains large free-text fields - +2 NoSQL")
        
        # Check for structured patterns (CSV-like lines)
        lines = text_content.split('\n')
        if len(lines) > 1:
            # Check if lines look like CSV
            first_line_fields = len(lines[0].split(','))
            if first_line_fields > 1:
                consistent = all(len(line.split(',')) == first_line_fields for line in lines[:10] if line.strip())
                if consistent:
                    self.sql_score += 3
                    self.reasons.append("Text contains tabular patterns (CSV-like) - +3 SQL")
    
    def _analyze_document(self):
        """Analyze PDF/DOCX documents."""
        text_content = self.file_content
        
        # Documents are typically unstructured
        self.nosql_score += 3
        self.reasons.append("File is document-based (PDF/DOCX extracted text) - +3 NoSQL")
        
        if len(text_content) > 3000:
            self.nosql_score += 2
            self.reasons.append("Contains large free-text fields - +2 NoSQL")
    
    def _analyze_unknown_text(self):
        """Analyze unknown file types as text, trying to detect structured patterns."""
        text_content = self.file_content
        
        # Default: Unknown files lean towards NoSQL but we analyze first
        self.nosql_score += 2
        self.reasons.append("Unknown file type - defaulting to NoSQL (unstructured)")
        
        # Try to detect structured patterns
        lines = text_content.split('\n') if text_content else []
        
        # Check for CSV-like patterns (comma-separated)
        if len(lines) > 1:
            # Check if lines look like CSV
            first_line_fields = len(lines[0].split(','))
            if first_line_fields > 1:
                consistent = all(len(line.split(',')) == first_line_fields for line in lines[:10] if line.strip())
                if consistent:
                    self.sql_score += 3
                    self.reasons.append("Unknown file contains tabular patterns (CSV-like) - +3 SQL")
            
            # Check for TSV-like patterns (tab-separated)
            first_line_tsv = len(lines[0].split('\t'))
            if first_line_tsv > 1:
                consistent_tsv = all(len(line.split('\t')) == first_line_tsv for line in lines[:10] if line.strip())
                if consistent_tsv:
                    self.sql_score += 3
                    self.reasons.append("Unknown file contains tabular patterns (TSV-like) - +3 SQL")
        
        # Check for JSON-like patterns (even if not .json extension)
        text_stripped = text_content.strip()
        if (text_stripped.startswith('{') and text_stripped.endswith('}')) or \
           (text_stripped.startswith('[') and text_stripped.endswith(']')):
            try:
                parsed_json = json.loads(text_content)
                # Re-analyze as JSON if valid
                self.nosql_score -= 2  # Remove the unknown penalty
                self.reasons.pop()  # Remove the default reason
                self.reasons.append("Unknown file type but contains valid JSON - analyzing as JSON")
                self.file_content = parsed_json
                self._analyze_json()
                return
            except (json.JSONDecodeError, ValueError):
                pass  # Not valid JSON, continue as text
        
        # Check for XML-like patterns
        if text_content.strip().startswith('<?xml') or text_content.strip().startswith('<'):
            try:
                parsed_xml = ET.fromstring(text_content)
                self.nosql_score -= 2  # Remove the unknown penalty
                self.reasons.pop()  # Remove the default reason
                self.reasons.append("Unknown file type but contains valid XML - analyzing as XML")
                self.file_content = parsed_xml
                self._analyze_xml()
                return
            except ET.ParseError:
                pass  # Not valid XML, continue as text
        
        # If large text content, add NoSQL points
        if len(text_content) > 5000:
            self.nosql_score += 2
            self.reasons.append("Contains large free-text fields - +2 NoSQL")
    
    # Helper methods
    
    def _is_flat_structure(self, data: Any, depth: int = 0) -> bool:
        """Check if structure is flat (no nesting)."""
        if depth > 1:
            return False
        
        if isinstance(data, dict):
            return all(not isinstance(v, (dict, list)) for v in data.values())
        elif isinstance(data, list):
            return all(not isinstance(item, (dict, list)) for item in data)
        
        return True
    
    def _get_nested_depth(self, data: Any, current_depth: int = 0) -> int:
        """Get maximum nesting depth."""
        if not isinstance(data, (dict, list)):
            return current_depth
        
        if isinstance(data, dict):
            if not data:
                return current_depth
            return max(self._get_nested_depth(v, current_depth + 1) for v in data.values())
        elif isinstance(data, list):
            if not data:
                return current_depth
            return max(self._get_nested_depth(item, current_depth + 1) for item in data)
        
        return current_depth
    
    def _has_relational_patterns(self, data: Any) -> bool:
        """Check for relational patterns like IDs."""
        if isinstance(data, dict):
            keys = [k.lower() for k in data.keys()]
            id_patterns = ['id', '_id', 'id_', 'key', '_key', 'key_', 'pk', 'fk', 'foreign']
            if any(any(pattern in key for pattern in id_patterns) for key in keys):
                return True
            # Recursively check values
            return any(self._has_relational_patterns(v) for v in data.values() if isinstance(v, (dict, list)))
        elif isinstance(data, list):
            return any(self._has_relational_patterns(item) for item in data if isinstance(item, (dict, list)))
        
        return False
    
    def _has_dynamic_keys(self, data: Any) -> bool:
        """Check if keys vary across objects in arrays."""
        if isinstance(data, list) and len(data) > 1:
            if all(isinstance(item, dict) for item in data):
                keys_sets = [set(item.keys()) for item in data]
                return len(set(tuple(sorted(keys)) for keys in keys_sets)) > 1
        
        return False
    
    def _is_schema_consistent(self, data: Any) -> bool:
        """Check if schema is consistent across data."""
        if isinstance(data, list) and len(data) > 1:
            if all(isinstance(item, dict) for item in data):
                first_keys = set(data[0].keys())
                return all(set(item.keys()) == first_keys for item in data[1:])
        
        return True
    
    def _is_mostly_primitive(self, data: Any) -> bool:
        """Check if data is mostly primitive types."""
        if isinstance(data, dict):
            values = data.values()
        elif isinstance(data, list):
            values = data
        else:
            return isinstance(data, (str, int, float, bool, type(None)))
        
        primitive_count = sum(1 for v in values if isinstance(v, (str, int, float, bool, type(None))))
        total_count = len(values) if values else 1
        
        return primitive_count / total_count > 0.8
    
    def _has_large_text_fields(self, data: Any, threshold: int = 500) -> bool:
        """Check for large text fields."""
        text_fields = ['content', 'description', 'html', 'text', 'body', 'message']
        
        if isinstance(data, dict):
            for key, value in data.items():
                key_lower = key.lower()
                if any(field in key_lower for field in text_fields) and isinstance(value, str):
                    if len(value) > threshold:
                        return True
                if isinstance(value, (dict, list)):
                    if self._has_large_text_fields(value, threshold):
                        return True
        elif isinstance(data, list):
            for item in data:
                if isinstance(item, (dict, list)):
                    if self._has_large_text_fields(item, threshold):
                        return True
        
        return False
    
    def _get_xml_depth(self, element: ET.Element, current_depth: int = 0) -> int:
        """Get maximum depth of XML tree."""
        children = list(element)
        if not children:
            return current_depth
        
        return max(self._get_xml_depth(child, current_depth + 1) for child in children)


def classify_file(file_path: str) -> Dict[str, Any]:
    """
    Convenience function to classify a file.
    
    Args:
        file_path: Path to the file to classify
        
    Returns:
        Dictionary with classification results
    """
    classifier = FileClassifier()
    return classifier.classify(file_path)


if __name__ == "__main__":
    # Example usage
    import sys
    
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
        result = classify_file(file_path)
        
        print("\n" + "="*60)
        print("FILE CLASSIFICATION RESULT")
        print("="*60)
        print(f"File: {file_path}")
        print(f"Type: {result['file_type']}")
        print(f"\nClassification: {result['classification']}")
        print(f"SQL Score: {result['sql_score']}")
        print(f"NoSQL Score: {result['nosql_score']}")
        print(f"Confidence: {result['confidence']:.2%}")
        print(f"\nReasons:")
        for reason in result['reasons']:
            print(f"  - {reason}")
        print("="*60 + "\n")
    else:
        print("Usage: python file_classifier.py <file_path>")

