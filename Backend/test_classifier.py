"""
Test script for the file classifier.
Creates sample files and tests classification.
"""

import json
import csv
from pathlib import Path
from file_classifier import classify_file

def create_test_files():
    """Create sample test files for classification."""
    test_dir = Path("test_files")
    test_dir.mkdir(exist_ok=True)
    
    # Test 1: Flat JSON (should be SQL)
    flat_json = [
        {"id": 1, "name": "Alice", "age": 30, "city": "NYC"},
        {"id": 2, "name": "Bob", "age": 25, "city": "LA"},
        {"id": 3, "name": "Charlie", "age": 35, "city": "Chicago"}
    ]
    with open(test_dir / "flat_users.json", "w") as f:
        json.dump(flat_json, f, indent=2)
    
    # Test 2: Nested JSON (should be NoSQL)
    nested_json = {
        "user": {
            "id": 1,
            "profile": {
                "name": "Alice",
                "address": {
                    "street": "123 Main St",
                    "city": "NYC"
                }
            },
            "posts": [
                {"title": "Post 1", "content": "Long content here..."},
                {"title": "Post 2", "content": "More content..."}
            ]
        }
    }
    with open(test_dir / "nested_user.json", "w") as f:
        json.dump(nested_json, f, indent=2)
    
    # Test 3: CSV (should be SQL)
    with open(test_dir / "products.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["product_id", "name", "price", "category"])
        writer.writerow([1, "Laptop", 999.99, "Electronics"])
        writer.writerow([2, "Mouse", 29.99, "Electronics"])
        writer.writerow([3, "Keyboard", 79.99, "Electronics"])
    
    # Test 4: Inconsistent JSON (should be NoSQL)
    inconsistent_json = [
        {"id": 1, "name": "Alice", "age": 30},
        {"id": 2, "name": "Bob", "email": "bob@example.com", "score": 85},
        {"id": 3, "title": "Charlie", "tags": ["tag1", "tag2"]}
    ]
    with open(test_dir / "inconsistent.json", "w") as f:
        json.dump(inconsistent_json, f, indent=2)
    
    # Test 5: Text file (should be NoSQL)
    with open(test_dir / "log.txt", "w") as f:
        f.write("""
2024-01-01 10:00:00 INFO Application started
2024-01-01 10:05:23 ERROR Failed to connect to database
2024-01-01 10:10:45 WARNING High memory usage detected
2024-01-01 10:15:12 INFO Request processed successfully
        """)
    
    print(f"Created test files in {test_dir}/")
    return test_dir


def test_classifier():
    """Test the classifier with sample files."""
    test_dir = create_test_files()
    
    test_files = [
        "flat_users.json",
        "nested_user.json",
        "products.csv",
        "inconsistent.json",
        "log.txt"
    ]
    
    print("\n" + "="*70)
    print("FILE CLASSIFIER TEST RESULTS")
    print("="*70)
    
    for filename in test_files:
        file_path = test_dir / filename
        if file_path.exists():
            try:
                result = classify_file(str(file_path))
                print(f"\n📄 File: {filename}")
                print(f"   Classification: {result['classification']}")
                print(f"   SQL Score: {result['sql_score']}, NoSQL Score: {result['nosql_score']}")
                print(f"   Confidence: {result['confidence']:.2%}")
                print(f"   Top Reasons:")
                for reason in result['reasons'][:3]:
                    print(f"     • {reason}")
            except Exception as e:
                print(f"\n❌ Error classifying {filename}: {e}")
    
    print("\n" + "="*70)


if __name__ == "__main__":
    test_classifier()

