"""
Semantic Attribute Matcher

Matches incoming file attributes to existing database attributes using:
1. Exact match
2. Synonym dictionary
3. Token overlap
4. Levenshtein distance
5. Type compatibility
"""

import re
from typing import Dict, List, Tuple, Any, Optional, Set
from difflib import SequenceMatcher


# Synonym groups for attribute matching
SYNONYM_GROUPS = {
    'name': ['name', 'product_name', 'productName', 'item_name', 'full_name', 'username', 'title'],
    'price': ['price', 'cost', 'amount', 'value', 'total', 'sum', 'price_amount'],
    'stock': ['stock', 'quantity', 'quantity_available', 'inventory', 'qty', 'stock_level'],
    'gender': ['gender', 'sex', 'target_gender', 'gender_type'],
    'email': ['email', 'email_address', 'emailAddress', 'e_mail', 'mail', 'email_id'],
    'phone': ['phone', 'phone_number', 'phoneNumber', 'mobile', 'telephone', 'contact_number'],
    'address': ['address', 'location', 'street_address', 'full_address', 'addr'],
    'date': ['date', 'created_date', 'date_created', 'timestamp', 'created_at', 'updated_at'],
    'id': ['id', '_id', 'id_', 'identifier', 'key', 'primary_key'],
    'description': ['description', 'desc', 'details', 'info', 'notes', 'comment'],
    'category': ['category', 'type', 'classification', 'group', 'tag'],
    'status': ['status', 'state', 'condition', 'active'],
    'age': ['age', 'years_old', 'years'],
    'country': ['country', 'nation', 'country_code'],
    'city': ['city', 'town', 'location_city'],
    'zip': ['zip', 'zipcode', 'postal_code', 'postcode'],
}


def is_id_attribute(attr: str) -> bool:
    """
    Check if an attribute is an ID attribute (case-insensitive).
    
    ID patterns include: id, _id, id_, identifier, key, primary_key, pk, etc.
    
    Args:
        attr: Attribute name to check
        
    Returns:
        True if attribute is an ID attribute, False otherwise
        
    Example:
        is_id_attribute("id") -> True
        is_id_attribute("user_id") -> True
        is_id_attribute("productId") -> True
        is_id_attribute("name") -> False
    """
    if not attr:
        return False
    
    normalized = normalize_attribute(attr)
    
    # Common ID patterns
    id_patterns = [
        'id',
        'identifier',
        'key',
        'primary_key',
        'pk',
    ]
    
    # Check exact match
    if normalized in id_patterns:
        return True
    
    # Check if it ends with '_id' or starts with 'id_'
    if normalized.endswith('_id') or normalized.startswith('id_'):
        return True
    
    # Check if it's exactly '_id'
    if normalized == '_id':
        return True
    
    return False


def normalize_attribute(attr: str) -> str:
    """
    Normalize attribute name for matching.
    
    - Converts to lowercase
    - Removes special characters
    - Replaces with underscores
    - Strips leading/trailing underscores
    
    Args:
        attr: Attribute name to normalize
        
    Returns:
        Normalized attribute name
        
    Example:
        normalize_attribute("Email Address") -> "email_address"
        normalize_attribute("productName") -> "productname"
    """
    if not attr:
        return ""
    
    # Convert to lowercase
    attr = attr.lower()
    
    # Replace special characters and spaces with underscores
    attr = re.sub(r'[^a-z0-9_]+', '_', attr)
    
    # Remove multiple consecutive underscores
    attr = re.sub(r'_+', '_', attr)
    
    # Strip leading and trailing underscores
    attr = attr.strip('_')
    
    return attr


def extract_attributes(data: List[Dict[str, Any]]) -> List[str]:
    """
    Extract attribute names (column names) from structured data.
    
    Args:
        data: List of dictionaries (rows)
        
    Returns:
        List of attribute names
        
    Example:
        data = [{"name": "John", "age": 30}, {"name": "Jane", "age": 25}]
        extract_attributes(data) -> ["name", "age"]
    """
    if not data or not isinstance(data, list):
        return []
    
    if len(data) == 0:
        return []
    
    # Get keys from first row
    first_row = data[0]
    if isinstance(first_row, dict):
        return list(first_row.keys())
    elif isinstance(first_row, (list, tuple)) and len(data) > 0:
        # Handle list of lists (need headers)
        return []
    
    return []


def get_synonym_group(attr: str) -> Optional[str]:
    """
    Find which synonym group an attribute belongs to.
    
    Args:
        attr: Normalized attribute name
        
    Returns:
        Synonym group name if found, None otherwise
        
    Example:
        get_synonym_group("cost") -> "price"
        get_synonym_group("qty") -> "stock"
    """
    normalized = normalize_attribute(attr)
    
    for group_name, synonyms in SYNONYM_GROUPS.items():
        normalized_synonyms = [normalize_attribute(s) for s in synonyms]
        if normalized in normalized_synonyms:
            return group_name
    
    return None


def are_synonyms(attr1: str, attr2: str) -> bool:
    """
    Check if two attributes are synonyms.
    
    Args:
        attr1: First attribute name
        attr2: Second attribute name
        
    Returns:
        True if attributes are synonyms, False otherwise
    """
    group1 = get_synonym_group(attr1)
    group2 = get_synonym_group(attr2)
    
    if group1 and group2:
        return group1 == group2
    
    return False


def tokenize(attr: str) -> Set[str]:
    """
    Tokenize attribute name by splitting on underscores and camelCase.
    
    Args:
        attr: Attribute name
        
    Returns:
        Set of tokens
        
    Example:
        tokenize("product_name") -> {"product", "name"}
        tokenize("productName") -> {"product", "name"}
    """
    # Split on underscores
    parts = attr.split('_')
    
    # Further split camelCase
    tokens = set()
    for part in parts:
        if part:
            # Split camelCase: "productName" -> ["product", "Name"]
            camel_parts = re.findall(r'[a-z]+|[A-Z][a-z]*', part)
            for cp in camel_parts:
                if cp:
                    tokens.add(cp.lower())
    
    return tokens


def calculate_token_overlap(attr1: str, attr2: str) -> float:
    """
    Calculate token overlap ratio between two attributes.
    
    Args:
        attr1: First attribute name
        attr2: Second attribute name
        
    Returns:
        Token overlap ratio (0.0 to 1.0)
        
    Example:
        calculate_token_overlap("product_name", "productName") -> 1.0
        calculate_token_overlap("email_address", "email") -> 0.5
    """
    tokens1 = tokenize(normalize_attribute(attr1))
    tokens2 = tokenize(normalize_attribute(attr2))
    
    if not tokens1 or not tokens2:
        return 0.0
    
    intersection = tokens1 & tokens2
    union = tokens1 | tokens2
    
    if not union:
        return 0.0
    
    return len(intersection) / len(union)


def calculate_levenshtein_similarity(attr1: str, attr2: str) -> float:
    """
    Calculate Levenshtein similarity using SequenceMatcher.
    
    Args:
        attr1: First attribute name
        attr2: Second attribute name
        
    Returns:
        Similarity ratio (0.0 to 1.0)
        
    Example:
        calculate_levenshtein_similarity("email_address", "emailaddress") -> ~0.85
    """
    norm1 = normalize_attribute(attr1)
    norm2 = normalize_attribute(attr2)
    
    if not norm1 or not norm2:
        return 0.0
    
    if norm1 == norm2:
        return 1.0
    
    return SequenceMatcher(None, norm1, norm2).ratio()


def infer_type(value: Any) -> str:
    """
    Infer PostgreSQL type from Python value.
    
    Args:
        value: Python value
        
    Returns:
        PostgreSQL type name
    """
    if value is None:
        return 'TEXT'  # Default for unknown
    
    if isinstance(value, bool):
        return 'BOOLEAN'
    elif isinstance(value, int):
        return 'INTEGER'
    elif isinstance(value, float):
        return 'NUMERIC'
    elif isinstance(value, str):
        # Check if it looks like a date/timestamp
        if len(value) > 10 and ('T' in value or '-' in value):
            return 'TIMESTAMP'
        # Check if it's a long text
        if len(value) > 255:
            return 'TEXT'
        return 'VARCHAR'
    else:
        return 'TEXT'


def calculate_type_compatibility(type1: str, type2: str) -> float:
    """
    Calculate type compatibility score.
    
    Args:
        type1: First type
        type2: Second type
        
    Returns:
        Compatibility score (0.0 to 1.0)
    """
    if type1 == type2:
        return 1.0
    
    # Compatible types
    compatible_groups = [
        {'INTEGER', 'NUMERIC', 'BIGINT'},
        {'VARCHAR', 'TEXT', 'CHAR'},
        {'TIMESTAMP', 'DATE', 'TIME'},
    ]
    
    for group in compatible_groups:
        if type1 in group and type2 in group:
            return 0.8
    
    # Numeric types are somewhat compatible
    if type1 in {'INTEGER', 'NUMERIC', 'BIGINT'} and type2 in {'INTEGER', 'NUMERIC', 'BIGINT'}:
        return 0.9
    
    # Text types are somewhat compatible
    if type1 in {'VARCHAR', 'TEXT'} and type2 in {'VARCHAR', 'TEXT'}:
        return 0.9
    
    return 0.3  # Low compatibility for different types


def calculate_attribute_similarity(
    new_attr: str,
    existing_attr: str,
    new_value: Any = None,
    existing_type: Optional[str] = None
) -> float:
    """
    Calculate combined similarity score between two attributes.
    
    Uses:
    - 70% name similarity (exact, synonym, token overlap, Levenshtein)
    - 30% type compatibility
    
    Note: Detailed logging is handled in match_attributes() to avoid excessive logs.
    
    Args:
        new_attr: New attribute name
        existing_attr: Existing attribute name
        new_value: Sample value from new data (for type inference)
        existing_type: Existing column type in database
        
    Returns:
        Combined similarity score (0.0 to 1.0)
    """
    norm_new = normalize_attribute(new_attr)
    norm_existing = normalize_attribute(existing_attr)
    
    # 1. Exact match
    if norm_new == norm_existing:
        name_similarity = 1.0
    # 2. Synonym match
    elif are_synonyms(new_attr, existing_attr):
        name_similarity = 0.95
    else:
        # 3. Calculate Levenshtein and token overlap
        levenshtein = calculate_levenshtein_similarity(new_attr, existing_attr)
        token_overlap = calculate_token_overlap(new_attr, existing_attr)
        
        # Use maximum of Levenshtein and token overlap (weighted)
        name_similarity = max(levenshtein, token_overlap * 0.8)
    
    # Type compatibility (if types are available)
    type_compatibility = 1.0  # Default: assume compatible
    if existing_type and new_value is not None:
        new_type = infer_type(new_value)
        type_compatibility = calculate_type_compatibility(new_type, existing_type)
    elif existing_type:
        # If we have existing type but no new value, assume moderate compatibility
        type_compatibility = 0.7
    
    # Combined score: 70% name, 30% type
    combined_score = 0.7 * name_similarity + 0.3 * type_compatibility
    
    return combined_score


def match_attributes(
    new_attributes: List[str],
    existing_attributes: List[str],
    new_data: Optional[List[Dict[str, Any]]] = None,
    existing_types: Optional[Dict[str, str]] = None,
    similarity_threshold: float = 0.6
) -> Tuple[Dict[str, str], List[str]]:
    """
    Match new attributes to existing attributes.
    
    Note: ID attributes are excluded from matching calculations.
    
    Args:
        new_attributes: List of new attribute names
        existing_attributes: List of existing attribute names
        new_data: Sample data for type inference (optional)
        existing_types: Dict mapping existing attributes to their types (optional)
        similarity_threshold: Minimum similarity score to consider a match (default: 0.6)
        
    Returns:
        Tuple of (mapping_dict, new_fields_list)
        - mapping_dict: Maps new_attr -> existing_attr
        - new_fields_list: List of new attributes that couldn't be matched
        
    Example:
        new_attrs = ["cost", "qty", "category"]
        existing_attrs = ["price", "stock", "name"]
        match_attributes(new_attrs, existing_attrs)
        -> ({"cost": "price", "qty": "stock"}, ["category"])
    """
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from logger_config import get_logger
    logger = get_logger('auraverse.semantic_matcher')
    
    mapping = {}
    new_fields = []
    
    logger.info("=" * 60)
    logger.info("ATTRIBUTE MATCHING STARTED")
    logger.info("=" * 60)
    logger.info(f"New attributes ({len(new_attributes)}): {new_attributes}")
    logger.info(f"Existing attributes ({len(existing_attributes)}): {existing_attributes}")
    logger.info(f"Similarity threshold: {similarity_threshold}")
    
    # Separate ID attributes from regular attributes (exclude from matching)
    new_id_attrs = [attr for attr in new_attributes if is_id_attribute(attr)]
    new_regular_attrs = [attr for attr in new_attributes if not is_id_attribute(attr)]
    
    existing_id_attrs = [attr for attr in existing_attributes if is_id_attribute(attr)]
    existing_regular_attrs = [attr for attr in existing_attributes if not is_id_attribute(attr)]
    
    logger.debug(f"ID attributes - New: {new_id_attrs}, Existing: {existing_id_attrs}")
    logger.debug(f"Regular attributes - New: {new_regular_attrs}, Existing: {existing_regular_attrs}")
    
    # Auto-match ID attributes if both exist
    logger.debug("Matching ID attributes...")
    for new_id_attr in new_id_attrs:
        norm_new_id = normalize_attribute(new_id_attr)
        matched = False
        
        # First, try exact normalized match
        for existing_id_attr in existing_id_attrs:
            norm_existing_id = normalize_attribute(existing_id_attr)
            if norm_new_id == norm_existing_id:
                mapping[new_id_attr] = existing_id_attr
                logger.info(f"✓ ID MATCH: '{new_id_attr}' → '{existing_id_attr}' (exact normalized match)")
                matched = True
                break
        
        # If no exact match and both sides have only one ID attribute, match them
        # (they likely represent the same concept - primary key)
        if not matched and len(new_id_attrs) == 1 and len(existing_id_attrs) == 1:
            mapping[new_id_attr] = existing_id_attrs[0]
            logger.info(f"✓ ID MATCH: '{new_id_attr}' → '{existing_id_attrs[0]}' (single ID on both sides - assumed same concept)")
            matched = True
        
        # If still no match, try partial match (e.g., 'employee_id' contains 'id')
        if not matched:
            for existing_id_attr in existing_id_attrs:
                norm_existing_id = normalize_attribute(existing_id_attr)
                # If both contain 'id' and one is a subset of the other (e.g., 'id' in 'employee_id')
                if 'id' in norm_new_id and 'id' in norm_existing_id:
                    # Check if one contains the other (e.g., 'id' is in 'employee_id' or vice versa)
                    if norm_existing_id in norm_new_id or norm_new_id in norm_existing_id:
                        mapping[new_id_attr] = existing_id_attr
                        logger.info(f"✓ ID MATCH: '{new_id_attr}' → '{existing_id_attr}' (partial match - both contain 'id')")
                        matched = True
                        break
        
        if not matched:
            logger.debug(f"✗ No ID match found for: '{new_id_attr}' - will be added as new field")
            if new_id_attr not in new_fields:
                new_fields.append(new_id_attr)
    
    # Normalize all regular attributes
    normalized_new = {normalize_attribute(attr): attr for attr in new_regular_attrs}
    normalized_existing = {normalize_attribute(attr): attr for attr in existing_regular_attrs}
    
    # Get sample values for type inference
    sample_values = {}
    if new_data and len(new_data) > 0:
        first_row = new_data[0]
        for attr in new_regular_attrs:
            if attr in first_row:
                sample_values[attr] = first_row[attr]
    
    logger.info(f"Matching {len(new_regular_attrs)} regular attributes...")
    
    # Match each new regular attribute (excluding ID attributes)
    for new_attr in new_regular_attrs:
        norm_new = normalize_attribute(new_attr)
        best_match = None
        best_score = 0.0
        all_scores = []
        
        logger.debug(f"Trying to match: '{new_attr}' (normalized: '{norm_new}')")
        
        # Try to find best match in existing regular attributes
        for existing_attr in existing_regular_attrs:
            norm_existing = normalize_attribute(existing_attr)
            
            # Skip if already mapped
            if norm_existing in [normalize_attribute(m) for m in mapping.values()]:
                logger.debug(f"  Skipping '{existing_attr}' (already mapped)")
                continue
            
            # Calculate similarity
            new_value = sample_values.get(new_attr)
            existing_type = existing_types.get(existing_attr) if existing_types else None
            
            score = calculate_attribute_similarity(
                new_attr,
                existing_attr,
                new_value,
                existing_type
            )
            
            all_scores.append((existing_attr, score))
            
            if score > best_score:
                best_score = score
                best_match = existing_attr
        
        # Log all scores for debugging
        if all_scores:
            sorted_scores = sorted(all_scores, key=lambda x: x[1], reverse=True)
            logger.debug(f"  Similarity scores for '{new_attr}':")
            for attr, score in sorted_scores[:5]:  # Log top 5 matches
                match_indicator = "✓ BEST" if attr == best_match else " "
                logger.debug(f"    {match_indicator} '{attr}': {score:.2%}")
        
        # If best match exceeds threshold, map it
        if best_match and best_score >= similarity_threshold:
            mapping[new_attr] = best_match
            logger.info(f"✓ MATCH: '{new_attr}' → '{best_match}' (score: {best_score:.2%}, threshold: {similarity_threshold:.2%})")
        else:
            if best_match:
                logger.warning(f"✗ NO MATCH: '{new_attr}' → '{best_match}' (score: {best_score:.2%} < threshold: {similarity_threshold:.2%})")
            else:
                logger.warning(f"✗ NO MATCH: '{new_attr}' (no suitable match found)")
            new_fields.append(new_attr)
    
    # Summary
    logger.info("=" * 60)
    logger.info("ATTRIBUTE MATCHING SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Total attributes to match: {len(new_attributes)}")
    logger.info(f"ID attributes: {len(new_id_attrs)} (matched: {len([a for a in new_id_attrs if a in mapping])})")
    logger.info(f"Regular attributes: {len(new_regular_attrs)} (matched: {len([a for a in new_regular_attrs if a in mapping])})")
    logger.info(f"Matched: {len(mapping)} attributes")
    logger.info(f"New fields: {len(new_fields)} attributes")
    
    if mapping:
        logger.info("Matches:")
        for new_attr, existing_attr in mapping.items():
            logger.info(f"  '{new_attr}' → '{existing_attr}'")
    
    if new_fields:
        logger.info("New fields (no match found):")
        for attr in new_fields:
            logger.info(f"  '{attr}'")
    
    logger.info("=" * 60)
    
    return mapping, new_fields


if __name__ == "__main__":
    # Test examples
    print("=" * 60)
    print("Semantic Attribute Matcher - Test")
    print("=" * 60)
    
    # Test normalization
    print("\n1. Normalization:")
    test_attrs = ["Email Address", "productName", "cost_per_unit", "  user_id  "]
    for attr in test_attrs:
        print(f"   '{attr}' -> '{normalize_attribute(attr)}'")
    
    # Test synonym matching
    print("\n2. Synonym Matching:")
    test_pairs = [
        ("price", "cost"),
        ("stock", "quantity_available"),
        ("email", "email_address"),
        ("name", "product_name"),
    ]
    for attr1, attr2 in test_pairs:
        is_syn = are_synonyms(attr1, attr2)
        print(f"   '{attr1}' <-> '{attr2}': {is_syn}")
    
    # Test similarity calculation
    print("\n3. Similarity Calculation:")
    test_similarities = [
        ("name", "name"),
        ("price", "cost"),
        ("product_name", "productName"),
        ("email_address", "emailAddress"),
        ("name", "age"),
    ]
    for attr1, attr2 in test_similarities:
        sim = calculate_attribute_similarity(attr1, attr2)
        print(f"   '{attr1}' vs '{attr2}': {sim:.2%}")
    
    # Test full matching
    print("\n4. Full Attribute Matching:")
    new_attrs = ["cost", "qty", "category", "product_name"]
    existing_attrs = ["price", "stock", "name", "description"]
    mapping, new_fields = match_attributes(new_attrs, existing_attrs)
    print(f"   New attributes: {new_attrs}")
    print(f"   Existing attributes: {existing_attrs}")
    print(f"   Mapping: {mapping}")
    print(f"   New fields: {new_fields}")


