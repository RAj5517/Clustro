"""
Schema Similarity Engine

Intelligently compares new inferred schemas with existing database schemas
using semantic matching, type compatibility, and structure analysis.

Scoring Dimensions:
1. Field Name Similarity (30%) - Levenshtein, token matching, synonyms
2. Field Count Ratio (25%) - How many fields overlap
3. Data Type Compatibility (25%) - Type matching and compatibility
4. Nested Structure Similarity (10%) - Array/Object patterns
5. Parent-Child Context (10%) - Relationship context
"""

from typing import Dict, List, Tuple, Optional, Set
import re
from difflib import SequenceMatcher


class SchemaSimilarityEngine:
    """Intelligently matches schemas to existing tables."""
    
    def __init__(self):
        """Initialize the similarity engine with field name normalization."""
        # Common field name synonyms/patterns
        self.field_synonyms = {
            'id': ['id', 'user_id', 'userId', 'userid', 'uid', 'identifier', 'pk'],
            'email': ['email', 'email_address', 'emailAddress', 'e_mail', 'mail', 'e_mail_address'],
            'name': ['name', 'product_name', 'productName', 'item_name', 'itemName', 'full_name', 'fullName', 'fullname', 'user_name', 'username', 'display_name'],
            'category': ['category', 'product_category', 'productCategory', 'item_category', 'itemCategory', 'type', 'classification'],
            'price': ['price', 'cost', 'amount', 'value', 'total', 'sum', 'product_price', 'productPrice'],
            'stock': ['stock', 'quantity', 'quantity_available', 'quantityAvailable', 'inventory', 'available_stock', 'availableStock', 'qty'],
            'phone': ['phone', 'phone_number', 'phoneNumber', 'tel', 'telephone', 'mobile', 'cell'],
            'address': ['address', 'addr', 'street_address', 'streetAddress', 'location'],
            'city': ['city', 'town', 'municipality'],
            'zip': ['zip', 'zip_code', 'zipCode', 'postal_code', 'postalCode', 'postcode'],
            'country': ['country', 'nation', 'country_code'],
            'gender': ['gender', 'sex', 'target_gender', 'targetGender', 'gender_type'],
            'created_at': ['created_at', 'createdAt', 'created', 'timestamp', 'date_created', 'creation_date'],
            'updated_at': ['updated_at', 'updatedAt', 'updated', 'modified_at', 'last_modified', 'modified'],
            'amount': ['amount', 'price', 'cost', 'total', 'value', 'sum'],
            'status': ['status', 'state', 'condition'],
            'description': ['description', 'desc', 'details', 'note', 'notes', 'comment', 'comments'],
        }
        
        # Type compatibility matrix
        self.type_compatible = {
            'INTEGER': ['INTEGER', 'BIGINT', 'SMALLINT', 'SERIAL'],
            'BIGINT': ['INTEGER', 'BIGINT', 'SMALLINT', 'SERIAL', 'BIGSERIAL'],
            'VARCHAR(255)': ['VARCHAR(255)', 'VARCHAR(500)', 'TEXT', 'CHAR(255)'],
            'TEXT': ['VARCHAR(255)', 'VARCHAR(500)', 'TEXT', 'CHAR(255)'],
            'DOUBLE PRECISION': ['DOUBLE PRECISION', 'REAL', 'NUMERIC', 'DECIMAL', 'FLOAT'],
            'BOOLEAN': ['BOOLEAN', 'BOOL'],
            'DATE': ['DATE', 'TIMESTAMP', 'TIMESTAMPTZ'],
            'TIMESTAMP': ['DATE', 'TIMESTAMP', 'TIMESTAMPTZ'],
        }
    
    def normalize_field_name(self, field_name: str) -> str:
        """Normalize field name to canonical form."""
        # Convert to lowercase and remove special chars
        normalized = re.sub(r'[^a-z0-9]', '_', field_name.lower())
        normalized = re.sub(r'_+', '_', normalized).strip('_')
        return normalized
    
    def field_name_similarity(self, field1: str, field2: str) -> float:
        """
        Calculate similarity between two field names.
        Returns: 0.0 to 1.0
        """
        # Normalize names
        norm1 = self.normalize_field_name(field1)
        norm2 = self.normalize_field_name(field2)
        
        # Exact match after normalization
        if norm1 == norm2:
            return 1.0
        
        # Check if they're synonyms
        for canonical, synonyms in self.field_synonyms.items():
            if norm1 in synonyms and norm2 in synonyms:
                return 0.95
        
        # Levenshtein-based similarity
        similarity_ratio = SequenceMatcher(None, norm1, norm2).ratio()
        
        # Check for token overlap (e.g., "email_address" vs "emailAddress")
        tokens1 = set(re.split(r'[_\s]+', norm1))
        tokens2 = set(re.split(r'[_\s]+', norm2))
        
        if tokens1 and tokens2:
            token_overlap = len(tokens1 & tokens2) / max(len(tokens1), len(tokens2))
            # Combine Levenshtein and token overlap
            similarity_ratio = max(similarity_ratio, token_overlap * 0.8)
        
        return similarity_ratio
    
    def type_compatibility(self, type1: str, type2: str) -> float:
        """
        Check compatibility between two data types.
        Returns: 0.0 to 1.0
        """
        # Normalize types (remove parameters for comparison)
        norm1 = type1.split('(')[0].strip().upper()
        norm2 = type2.split('(')[0].strip().upper()
        
        if norm1 == norm2:
            return 1.0
        
        # Check compatibility matrix
        for base_type, compatible in self.type_compatible.items():
            if norm1 == base_type.split('(')[0].strip().upper():
                if norm2 in [t.split('(')[0].strip().upper() for t in compatible]:
                    return 0.9  # Compatible but different
        
        # Check if both are numeric types
        numeric_types = ['INTEGER', 'BIGINT', 'SMALLINT', 'SERIAL', 'NUMERIC', 'DECIMAL', 'DOUBLE PRECISION', 'REAL', 'FLOAT']
        if norm1 in numeric_types and norm2 in numeric_types:
            return 0.7
        
        # Check if both are text types
        text_types = ['VARCHAR', 'TEXT', 'CHAR']
        if norm1 in text_types and norm2 in text_types:
            return 0.7
        
        # Check if both are date types
        date_types = ['DATE', 'TIMESTAMP', 'TIMESTAMPTZ']
        if norm1 in date_types and norm2 in date_types:
            return 0.7
        
        # Incompatible
        return 0.0
    
    def calculate_similarity(self, new_schema: Dict, existing_schema: Dict) -> float:
        """
        Calculate overall similarity score between new and existing schema.
        
        Args:
            new_schema: {'columns': [{'name': str, 'type': str}, ...], 'parent_table': str}
            existing_schema: {'columns': [{'name': str, 'type': str, 'nullable': bool}, ...]}
            
        Returns:
            Similarity score 0.0 to 1.0
        """
        new_cols = {col['name'].lower(): col for col in new_schema.get('columns', [])}
        existing_cols = {col['name'].lower(): col for col in existing_schema.get('columns', [])}
        
        # Remove 'id' from comparison (it's always present)
        new_cols.pop('id', None)
        existing_cols.pop('id', None)
        
        if not new_cols or not existing_cols:
            # If either has no columns (except id), similarity is low
            if not new_cols and not existing_cols:
                return 1.0  # Both empty (only id)
            return 0.2
        
        # 1. Field Name Similarity (30%)
        field_matches = []
        matched_existing = set()
        
        for new_name, new_col in new_cols.items():
            best_match_score = 0.0
            best_match_name = None
            
            for existing_name, existing_col in existing_cols.items():
                if existing_name in matched_existing:
                    continue
                
                similarity = self.field_name_similarity(new_name, existing_name)
                if similarity > best_match_score:
                    best_match_score = similarity
                    best_match_name = existing_name
            
            if best_match_name and best_match_score > 0.5:  # Threshold for matching
                matched_existing.add(best_match_name)
                # Also check type compatibility
                type_comp = self.type_compatibility(
                    new_col.get('type', ''), 
                    existing_cols[best_match_name].get('type', '')
                )
                # Combine name and type similarity
                combined_score = 0.7 * best_match_score + 0.3 * type_comp
                field_matches.append(combined_score)
            else:
                field_matches.append(0.0)  # No match found
        
        # Average field name similarity
        field_name_sim = sum(field_matches) / len(field_matches) if field_matches else 0.0
        
        # 2. Field Count Ratio (25%)
        total_fields = max(len(new_cols), len(existing_cols))
        matched_count = len(matched_existing)
        field_count_ratio = matched_count / total_fields if total_fields > 0 else 0.0
        
        # 3. Data Type Compatibility (25%)
        type_matches = []
        for new_name, new_col in new_cols.items():
            best_type_match = 0.0
            for existing_name, existing_col in existing_cols.items():
                type_sim = self.type_compatibility(
                    new_col.get('type', ''),
                    existing_col.get('type', '')
                )
                best_type_match = max(best_type_match, type_sim)
            type_matches.append(best_type_match)
        
        type_compatibility_score = sum(type_matches) / len(type_matches) if type_matches else 0.0
        
        # 4. Nested Structure Similarity (10%)
        # For now, assume 1.0 if both have similar column structures
        nested_sim = 1.0 if len(new_cols) > 0 and len(existing_cols) > 0 else 0.5
        
        # 5. Parent-Child Context (10%)
        new_parent = new_schema.get('parent_table')
        existing_parent = existing_schema.get('parent_table')
        
        if new_parent and existing_parent:
            parent_context = 1.0 if new_parent == existing_parent else 0.3
        elif not new_parent and not existing_parent:
            parent_context = 1.0  # Both are root tables
        else:
            parent_context = 0.5  # One has parent, other doesn't
        
        # Weighted combination
        final_score = (
            0.30 * field_name_sim +
            0.25 * field_count_ratio +
            0.25 * type_compatibility_score +
            0.10 * nested_sim +
            0.10 * parent_context
        )
        
        return final_score
    
    def find_best_match(self, new_schema: Dict, existing_schemas: List[Dict]) -> Optional[Tuple[str, float, str]]:
        """
        Find the best matching existing schema.
        
        Args:
            new_schema: New schema to match
            existing_schemas: List of existing schemas [{'table_name': str, 'columns': [...], ...}]
            
        Returns:
            (table_name, similarity_score, decision) or None
            decision: 'same_table', 'evolved_table', 'new_table'
        """
        if not existing_schemas:
            return None
        
        best_match = None
        best_score = 0.0
        best_table_name = None
        
        for existing in existing_schemas:
            table_name = existing.get('table_name')
            if not table_name:
                continue
            
            score = self.calculate_similarity(new_schema, existing)
            
            if score > best_score:
                best_score = score
                best_match = existing
                best_table_name = table_name
        
        if best_score < 0.50:
            return None  # No good match
        
        # Determine decision based on threshold
        if best_score >= 0.80:
            decision = 'same_table'
        elif best_score >= 0.50:
            decision = 'evolved_table'
        else:
            decision = 'new_table'
        
        return (best_table_name, best_score, decision)
    
    def get_missing_columns(self, new_schema: Dict, existing_schema: Dict) -> List[Dict]:
        """
        Get columns from new schema that don't exist in existing schema.
        
        Args:
            new_schema: New schema
            existing_schema: Existing schema
            
        Returns:
            List of missing column definitions
        """
        new_cols = {col['name'].lower(): col for col in new_schema.get('columns', [])}
        existing_col_names = {col['name'].lower() for col in existing_schema.get('columns', [])}
        
        missing = []
        for col_name, col_def in new_cols.items():
            if col_name not in existing_col_names:
                # Check if there's a similar column name (fuzzy match)
                has_similar = False
                for existing_name in existing_col_names:
                    if self.field_name_similarity(col_name, existing_name) >= 0.8:
                        has_similar = True
                        break
                
                if not has_similar:
                    missing.append(col_def)
        
        return missing


def get_schema_similarity_engine() -> SchemaSimilarityEngine:
    """Factory function to get a singleton instance."""
    return SchemaSimilarityEngine()

