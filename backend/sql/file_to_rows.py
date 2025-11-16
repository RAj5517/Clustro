"""
File to Rows Converter

Converts any file into rows (list of dictionaries).
Handles:
- CSV / JSON array → already rows
- Logs / weird JSON → parse into rows
- PDFs / images / audio / video → extract text first, then parse
- Always preserves raw file
"""

import json
import csv
import hashlib
import sys
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
import xml.etree.ElementTree as ET

# Add parent directory to path for logger import
sys.path.insert(0, str(Path(__file__).parent.parent))

from logger_config import get_logger
logger = get_logger('auraverse.file_to_rows')

# Optional imports
try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

try:
    import yaml
    HAS_YAML = True
except ImportError:
    HAS_YAML = False

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


def compute_file_hash(file_path: Path) -> str:
    """Compute SHA256 hash of file content."""
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()


def file_to_rows(file_path: Path) -> Tuple[List[Dict[str, Any]], str, Optional[str]]:
    """
    Convert any file into rows (list of dictionaries).
    
    Args:
        file_path: Path to the file
    
    Returns:
        (rows, file_type, error_message)
        rows: List of dictionaries (each dict is a row)
        file_type: Detected file type
        error_message: None if successful, error string otherwise
    """
    file_path = Path(file_path)
    logger.info(f"Converting file to rows: {file_path.name}")
    
    if not file_path.exists():
        logger.error(f"File not found: {file_path}")
        return [], 'unknown', f"File not found: {file_path}"
    
    ext = file_path.suffix.lower()
    logger.debug(f"File extension: {ext}")
    
    try:
        # CSV files
        if ext == '.csv':
            logger.debug("Processing as CSV file")
            rows = _csv_to_rows(file_path)
            logger.info(f"Successfully converted CSV: {len(rows)} rows extracted")
            return rows, 'csv', None
        
        # JSON files
        elif ext == '.json':
            logger.debug("Processing as JSON file")
            rows = _json_to_rows(file_path)
            logger.info(f"Successfully converted JSON: {len(rows)} rows extracted")
            return rows, 'json', None
        
        # Excel files
        elif ext in ['.xlsx', '.xls']:
            logger.debug("Processing as Excel file")
            rows = _excel_to_rows(file_path)
            logger.info(f"Successfully converted Excel: {len(rows)} rows extracted")
            return rows, 'excel', None
        
        # XML files
        elif ext == '.xml':
            logger.debug("Processing as XML file")
            rows = _xml_to_rows(file_path)
            logger.info(f"Successfully converted XML: {len(rows)} rows extracted")
            return rows, 'xml', None
        
        # YAML files
        elif ext in ['.yaml', '.yml']:
            logger.debug("Processing as YAML file")
            rows = _yaml_to_rows(file_path)
            logger.info(f"Successfully converted YAML: {len(rows)} rows extracted")
            return rows, 'yaml', None
        
        # Text files (try to parse as CSV/TSV)
        elif ext == '.txt':
            return _text_to_rows(file_path), 'text', None
        
        # HTML files (extract tables)
        elif ext in ['.html', '.htm']:
            return _html_to_rows(file_path), 'html', None
        
        # PDF files (extract text, then parse)
        elif ext == '.pdf':
            return _pdf_to_rows(file_path), 'pdf', None
        
        # DOCX files (extract text, then parse)
        elif ext == '.docx':
            return _docx_to_rows(file_path), 'docx', None
        
        # Unknown - try as text
        else:
            return _text_to_rows(file_path), 'unknown', None
            
    except Exception as e:
        return [], 'unknown', f"Error converting file to rows: {str(e)}"


def _csv_to_rows(file_path: Path) -> List[Dict[str, Any]]:
    """Convert CSV to rows."""
    if HAS_PANDAS:
        df = pd.read_csv(file_path)
        return df.to_dict('records')
    else:
        with open(file_path, 'r', encoding='utf-8') as f:
            return list(csv.DictReader(f))


def _json_to_rows(file_path: Path) -> List[Dict[str, Any]]:
    """Convert JSON to rows."""
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    if isinstance(data, list):
        return data
    elif isinstance(data, dict):
        # Single object - wrap in list
        return [data]
    else:
        return []


def _excel_to_rows(file_path: Path) -> List[Dict[str, Any]]:
    """Convert Excel to rows."""
    if HAS_PANDAS:
        df = pd.read_excel(file_path)
        return df.to_dict('records')
    else:
        raise ImportError("pandas required for Excel file parsing")


def _xml_to_rows(file_path: Path) -> List[Dict[str, Any]]:
    """Convert XML to rows."""
    tree = ET.parse(file_path)
    root = tree.getroot()
    
    rows = []
    # Find repeating elements
    children = list(root)
    if children:
        for child in children:
            row = {}
            # Add attributes
            for attr, value in child.attrib.items():
                row[attr] = value
            # Add text content
            if child.text and child.text.strip():
                row['text'] = child.text.strip()
            # Add child elements
            for subchild in child:
                if subchild.text:
                    row[subchild.tag] = subchild.text.strip()
            if row:
                rows.append(row)
    else:
        # Root element itself
        row = {}
        for attr, value in root.attrib.items():
            row[attr] = value
        if root.text and root.text.strip():
            row['text'] = root.text.strip()
        if row:
            rows.append(row)
    
    return rows


def _yaml_to_rows(file_path: Path) -> List[Dict[str, Any]]:
    """Convert YAML to rows."""
    if HAS_YAML:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
        
        if isinstance(data, list):
            return data
        elif isinstance(data, dict):
            return [data]
        else:
            return []
    else:
        raise ImportError("pyyaml required for YAML file parsing")


def _text_to_rows(file_path: Path) -> List[Dict[str, Any]]:
    """Convert text file to rows (try CSV/TSV patterns)."""
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    lines = content.strip().split('\n')
    if len(lines) < 2:
        return []  # Need at least header + one row
    
    # Try CSV (comma-separated)
    first_line_fields = len([f for f in lines[0].split(',') if f.strip()])
    if first_line_fields > 1:
        consistent = all(
            len([f for f in line.split(',') if f.strip()]) == first_line_fields
            for line in lines[1:11] if line.strip()
        )
        if consistent:
            # Parse as CSV
            if HAS_PANDAS:
                df = pd.read_csv(file_path, delimiter=',')
                return df.to_dict('records')
            else:
                with open(file_path, 'r', encoding='utf-8') as f:
                    return list(csv.DictReader(f, delimiter=','))
    
    # Try TSV (tab-separated)
    first_line_tsv = len([f for f in lines[0].split('\t') if f.strip()])
    if first_line_tsv > 1:
        consistent = all(
            len([f for f in line.split('\t') if f.strip()]) == first_line_tsv
            for line in lines[1:11] if line.strip()
        )
        if consistent:
            if HAS_PANDAS:
                df = pd.read_csv(file_path, delimiter='\t')
                return df.to_dict('records')
            else:
                with open(file_path, 'r', encoding='utf-8') as f:
                    return list(csv.DictReader(f, delimiter='\t'))
    
    # Not structured - return empty (will be stored as document)
    return []


def _html_to_rows(file_path: Path) -> List[Dict[str, Any]]:
    """Extract tables from HTML."""
    try:
        from bs4 import BeautifulSoup
        with open(file_path, 'r', encoding='utf-8') as f:
            soup = BeautifulSoup(f.read(), 'html.parser')
        
        rows = []
        # Find first table
        table = soup.find('table')
        if table:
            headers = [th.get_text().strip() for th in table.find_all('th')]
            if not headers:
                # Try first row as headers
                first_row = table.find('tr')
                if first_row:
                    headers = [td.get_text().strip() for td in first_row.find_all(['td', 'th'])]
            
            if headers:
                for tr in table.find_all('tr')[1:]:  # Skip header row
                    tds = tr.find_all(['td', 'th'])
                    if len(tds) == len(headers):
                        row = {headers[i]: td.get_text().strip() for i, td in enumerate(tds)}
                        rows.append(row)
        
        return rows
    except ImportError:
        return []  # beautifulsoup4 not available


def _pdf_to_rows(file_path: Path) -> List[Dict[str, Any]]:
    """Extract text from PDF, then try to parse as structured data."""
    if not HAS_PDF:
        return []
    
    try:
        # Extract text
        with open(file_path, 'rb') as f:
            pdf_reader = PyPDF2.PdfReader(f)
            text = ""
            for page in pdf_reader.pages:
                text += page.extract_text() + "\n"
        
        # Try to parse as structured data (CSV/TSV patterns)
        # For now, return empty - can be enhanced with AI parsing later
        return []
    except Exception:
        return []


def _docx_to_rows(file_path: Path) -> List[Dict[str, Any]]:
    """Extract text from DOCX, then try to parse as structured data."""
    if not HAS_DOCX:
        return []
    
    try:
        doc = Document(file_path)
        text = "\n".join([para.text for para in doc.paragraphs])
        
        # Try to parse as structured data
        # For now, return empty - can be enhanced with AI parsing later
        return []
    except Exception:
        return []

