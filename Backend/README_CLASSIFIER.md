# File Classifier - SQL/NoSQL Router

A Python module that automatically classifies non-media files into **SQL** or **NoSQL** categories based on structure analysis, data consistency, schema stability, and query efficiency patterns.

## Features

- ✅ Supports multiple file types: JSON, CSV, Excel, XML, HTML, TXT, PDF, DOCX, YAML, LOG
- ✅ Weighted scoring system (not simple if-else)
- ✅ Detailed reasoning for each classification
- ✅ Confidence scoring
- ✅ No hard dependencies (graceful degradation)

## Installation

Install optional dependencies for full functionality:

```bash
pip install -r requirements.txt
```

**Core dependencies:**
- `pandas` - For CSV/Excel parsing
- `PyYAML` - For YAML file support
- `python-docx` - For DOCX file parsing
- `PyPDF2` - For PDF text extraction
- `openpyxl` - For Excel file support

The classifier will work without these, but with limited file type support.

## Usage

### Basic Usage

```python
from file_classifier import classify_file

result = classify_file("path/to/file.json")

print(f"Classification: {result['classification']}")  # 'SQL' or 'NoSQL'
print(f"SQL Score: {result['sql_score']}")
print(f"NoSQL Score: {result['nosql_score']}")
print(f"Confidence: {result['confidence']}")
print(f"Reasons: {result['reasons']}")
```

### Example Output

```python
{
    'classification': 'SQL',
    'sql_score': 12.0,
    'nosql_score': 0.0,
    'confidence': 1.0,
    'reasons': [
        'File type is CSV/Excel (tabular) - +5 SQL',
        'Schema is consistent and predictable - +2 SQL',
        'Contains relational patterns (IDs) - +1 SQL'
    ],
    'file_type': 'csv'
}
```

### Command Line Usage

```bash
python file_classifier.py path/to/file.json
```

### Running Tests

```bash
python test_classifier.py
```

## Classification Logic

### SQL Scoring

The classifier awards SQL points for:

| Condition | Points |
|-----------|--------|
| File type is CSV or Excel (tabular) | +5 |
| JSON is flat (no nested objects/arrays) | +4 |
| JSON array with identical keys in each object | +4 |
| XML has repeating tags (records with same structure) | +3 |
| HTML contains a well-formed table | +3 |
| Schema is consistent and predictable | +2 |
| Mostly primitive fields (numbers, strings, booleans) | +1 |
| Contains relational patterns like user_id, product_id | +1 |

### NoSQL Scoring

The classifier awards NoSQL points for:

| Condition | Points |
|-----------|--------|
| JSON contains nested objects | +4 |
| JSON arrays with inconsistent object structure | +3 |
| Deeply nested XML (depth > 2) | +3 |
| File is text-based (.txt, .md, .log, .docx extracted text) | +3 |
| Contains large free-text fields (content, description, html) | +2 |
| Dynamic keys that change across objects | +2 |
| Schema expected to grow or lack structure | +2 |
| HTML without structured tables (content-heavy) | +1 |

### Decision Rule

```python
if sql_score >= nosql_score:
    classification = "SQL"
else:
    classification = "NoSQL"
```

## Supported File Types

| Type | Extensions | Parser |
|------|-----------|--------|
| JSON | `.json` | `json` |
| CSV | `.csv` | `csv` or `pandas` |
| Excel | `.xlsx`, `.xls` | `pandas` |
| XML | `.xml` | `xml.etree.ElementTree` |
| HTML | `.html`, `.htm` | Text parser |
| Text | `.txt` | Text parser |
| Markdown | `.md` | Text parser |
| Log | `.log` | Text parser |
| YAML | `.yaml`, `.yml` | `pyyaml` |
| Config | `.ini`, `.cfg`, `.conf` | Custom parser |
| PDF | `.pdf` | `PyPDF2` |
| DOCX | `.docx`, `.doc` | `python-docx` |

## Integration Example

```python
from file_classifier import FileClassifier

classifier = FileClassifier()

# Process multiple files
files = ["users.json", "products.csv", "logs.txt"]

for file_path in files:
    result = classifier.classify(file_path)
    
    if result['classification'] == 'SQL':
        # Route to SQL schema generator
        print(f"{file_path} → SQL Database")
    else:
        # Route to NoSQL document store
        print(f"{file_path} → NoSQL Database")
```

## How It Works

1. **File Detection**: Identifies file type from extension
2. **Parsing**: Parses file content using appropriate parser
3. **Analysis**: Analyzes structure, depth, consistency, patterns
4. **Scoring**: Calculates SQL and NoSQL scores based on rules
5. **Classification**: Compares scores and makes decision
6. **Reasoning**: Provides detailed explanations for the decision

## Testing

The `test_classifier.py` script creates sample files and tests classification:

```bash
python test_classifier.py
```

This will:
- Create sample files (flat JSON, nested JSON, CSV, etc.)
- Run classification on each
- Display results with scores and reasons

## Notes

- The classifier is designed for **non-media files only**
- Media files (images, videos, audio) are not supported
- The scoring system is weighted and configurable
- Classification confidence is calculated as: `|sql_score - nosql_score| / max(sql_score, nosql_score)`

## License

Part of the Clustro intelligent storage system.

