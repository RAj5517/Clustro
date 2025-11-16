"""
Schema Evolution Engine

Handles:
1. Inverted index for table candidate retrieval
2. Table similarity calculation
3. Schema evolution decision rules
"""

import psycopg2
from typing import Dict, List, Tuple, Any, Optional, Set
from collections import defaultdict
import sys
from pathlib import Path

# Add parent directory to path for logger import
sys.path.insert(0, str(Path(__file__).parent.parent))

from logger_config import get_logger
logger = get_logger('auraverse.schema_evolution')

from semantic_attribute_matcher import match_attributes, normalize_attribute


class SchemaEvolutionEngine:
    """Manages schema evolution decisions and table matching."""
    
    def __init__(self, db_config: Dict[str, Any]):
        """
        Initialize schema evolution engine.
        
        Args:
            db_config: Database connection configuration
        """
        self.db_config = db_config
        self.inverted_index: Dict[str, Set[str]] = defaultdict(set)
        self.table_metadata: Dict[str, Dict[str, Any]] = {}
        logger.debug("SchemaEvolutionEngine initializing...")
        self._load_metadata()
        logger.info(f"SchemaEvolutionEngine initialized: {len(self.table_metadata)} tables loaded")
        if self.table_metadata:
            table_names = list(self.table_metadata.keys())
            logger.debug(f"Loaded tables: {table_names}")
            for table_name, info in self.table_metadata.items():
                logger.debug(f"  Table '{table_name}': {len(info['columns'])} columns - {info['columns']}")
    
    def _load_metadata(self):
        """Load table metadata and build inverted index from database."""
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            
            # Get all tables in public schema
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_type = 'BASE TABLE'
                AND table_name != 'schema_jobs';
            """)
            
            tables = [row[0] for row in cursor.fetchall()]
            
            # For each table, get columns and build index
            for table_name in tables:
                cursor.execute("""
                    SELECT 
                        column_name,
                        data_type,
                        character_maximum_length
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                    AND table_name = %s
                    ORDER BY ordinal_position;
                """, (table_name,))
                
                columns = cursor.fetchall()
                column_names = []
                column_types = {}
                
                # Import here to avoid circular dependency
                from semantic_attribute_matcher import is_id_attribute
                
                for col_name, data_type, max_length in columns:
                    normalized = normalize_attribute(col_name)
                    column_names.append(col_name)
                    column_types[col_name] = data_type
                    
                    # Exclude ID attributes from inverted index (they're not used for matching)
                    if not is_id_attribute(col_name):
                        # Add to inverted index
                        self.inverted_index[normalized].add(table_name)
                
                # Store table metadata
                self.table_metadata[table_name] = {
                    'columns': column_names,
                    'types': column_types,
                    'column_count': len(column_names)
                }
            
            cursor.close()
            conn.close()
            
        except Exception as e:
            # If database connection fails, start with empty index
            logger.warning(f"Could not load metadata from database: {e}", exc_info=True)
            self.inverted_index = defaultdict(set)
            self.table_metadata = {}
    
    def get_candidate_tables(
        self,
        new_attributes: List[str],
        min_matches: int = 1
    ) -> List[Tuple[str, int, Set[str]]]:
        """
        Get candidate tables using inverted index AND semantic matching.
        
        Note: ID attributes are excluded from candidate matching.
        Uses both exact normalized matches and semantic similarity (synonyms, token overlap).
        
        Args:
            new_attributes: List of new attribute names
            min_matches: Minimum number of matched attributes to consider
        
        Returns:
            List of tuples: (table_name, match_count, matched_fields)
            Sorted by match count (descending)
        """
        # Exclude ID attributes from candidate matching
        from semantic_attribute_matcher import is_id_attribute, normalize_attribute, calculate_attribute_similarity, are_synonyms
        regular_attributes = [attr for attr in new_attributes if not is_id_attribute(attr)]
        
        # Normalize regular attributes only
        normalized_new = [normalize_attribute(attr) for attr in regular_attributes]
        
        logger.debug(f"Finding candidate tables for {len(regular_attributes)} regular attributes: {regular_attributes}")
        logger.debug(f"Available tables in metadata: {list(self.table_metadata.keys())}")
        
        # Count matches per table using both exact and semantic matching
        table_matches: Dict[str, Set[str]] = defaultdict(set)
        
        # First pass: Exact normalized matches (fast lookup via inverted index)
        for norm_attr in normalized_new:
            # Find tables that contain this exact normalized attribute
            matching_tables = self.inverted_index.get(norm_attr, set())
            for table_name in matching_tables:
                table_matches[table_name].add(norm_attr)
        
        logger.debug(f"After exact match: Found {len(table_matches)} tables with exact matches")
        
        # Second pass: Semantic matching for tables not found in first pass
        # Also check all tables for potential semantic matches
        for new_attr in regular_attributes:
            norm_new = normalize_attribute(new_attr)
            
            # Check all tables for semantic matches
            for table_name, table_info in self.table_metadata.items():
                existing_columns = table_info['columns']
                existing_types = table_info.get('types', {})
                
                # Skip if already found via exact match
                if norm_new in table_matches.get(table_name, set()):
                    continue
                
                # Check for semantic matches in existing columns
                for existing_col in existing_columns:
                    # Skip ID attributes
                    if is_id_attribute(existing_col):
                        continue
                    
                    norm_existing = normalize_attribute(existing_col)
                    
                    # Check exact normalized match first (shouldn't happen, but just in case)
                    if norm_new == norm_existing:
                        table_matches[table_name].add(norm_new)
                        logger.debug(f"Semantic match: '{new_attr}' ≈ '{existing_col}' (exact normalized)")
                        break
                    
                    # Check if synonyms
                    if are_synonyms(new_attr, existing_col):
                        table_matches[table_name].add(norm_new)
                        logger.debug(f"Semantic match: '{new_attr}' ≈ '{existing_col}' (synonym)")
                        break
                    
                    # Check similarity score (lower threshold for candidate matching: 0.4)
                    similarity = calculate_attribute_similarity(new_attr, existing_col)
                    if similarity >= 0.4:  # Lower threshold for candidate discovery
                        table_matches[table_name].add(norm_new)
                        logger.debug(f"Semantic match: '{new_attr}' ≈ '{existing_col}' (similarity: {similarity:.2%})")
                        break
        
        logger.info(f"Found {len(table_matches)} candidate table(s) after semantic matching")
        
        # Log each candidate table
        for table_name, matched_fields in table_matches.items():
            logger.debug(f"  Candidate: '{table_name}' - {len(matched_fields)} matching attributes: {matched_fields}")
        
        # Filter by minimum matches and sort
        candidates = [
            (table_name, len(matched_fields), matched_fields)
            for table_name, matched_fields in table_matches.items()
            if len(matched_fields) >= min_matches
        ]
        
        # Sort by match count (descending)
        candidates.sort(key=lambda x: x[1], reverse=True)
        
        logger.info(f"Returning {len(candidates)} candidate table(s) after filtering (min_matches={min_matches})")
        
        return candidates
    
    def calculate_table_similarity(
        self,
        new_attributes: List[str],
        table_name: str,
        attribute_mapping: Optional[Dict[str, str]] = None,
        new_data: Optional[List[Dict[str, Any]]] = None
    ) -> Tuple[float, Dict[str, str], List[str]]:
        """
        Calculate similarity between new attributes and a table.
        
        Note: ID attributes are excluded from similarity calculations.
        
        Args:
            new_attributes: List of new attribute names
            table_name: Name of existing table
            attribute_mapping: Optional pre-computed mapping (new_attr -> existing_attr)
            new_data: Sample data for type inference (optional)
        
        Returns:
            Tuple of (match_ratio, mapping_dict, new_fields_list)
            - match_ratio: Percentage of matched attributes (0.0 to 1.0) - excludes ID attributes
            - mapping_dict: Maps new_attr -> existing_attr
            - new_fields_list: List of new attributes that couldn't be matched
        """
        if table_name not in self.table_metadata:
            return 0.0, {}, new_attributes
        
        existing_attrs = self.table_metadata[table_name]['columns']
        existing_types = self.table_metadata[table_name]['types']
        
        # Separate ID attributes from regular attributes (exclude from calculations)
        from semantic_attribute_matcher import is_id_attribute
        new_id_attrs = [attr for attr in new_attributes if is_id_attribute(attr)]
        new_regular_attrs = [attr for attr in new_attributes if not is_id_attribute(attr)]
        
        existing_id_attrs = [attr for attr in existing_attrs if is_id_attribute(attr)]
        existing_regular_attrs = [attr for attr in existing_attrs if not is_id_attribute(attr)]
        
        # If mapping not provided, compute it
        if attribute_mapping is None:
            # Match all attributes (including ID attributes) - match_attributes handles ID matching
            # Pass both regular and ID attributes, but match_attributes will separate them internally
            all_new_attrs = new_regular_attrs + new_id_attrs
            all_existing_attrs = existing_regular_attrs + existing_id_attrs
            
            mapping, new_fields = match_attributes(
                all_new_attrs,
                all_existing_attrs,
                new_data=new_data,
                existing_types=existing_types
            )
        else:
            mapping = attribute_mapping
            mapped_existing = set(mapping.values())
            new_fields = [
                attr for attr in new_attributes
                if attr not in mapping
            ]
        
        # Calculate match ratio EXCLUDING ID attributes
        # Only count regular attributes in the ratio calculation
        total_regular_attrs = len(new_regular_attrs)
        matched_regular_attrs = sum(1 for attr in mapping.keys() if attr in new_regular_attrs)
        
        if total_regular_attrs == 0:
            match_ratio = 1.0 if len(mapping) > 0 else 0.0  # If only IDs, consider it matched
        else:
            match_ratio = matched_regular_attrs / total_regular_attrs
        
        return match_ratio, mapping, new_fields
    
    def calculate_dynamic_threshold(self, num_attributes: int) -> float:
        """
        Calculate dynamic threshold based on number of attributes.
        
        Args:
            num_attributes: Number of incoming attributes
        
        Returns:
            Threshold value (0.0 to 1.0)
        """
        if num_attributes < 10:
            return 0.6  # 60% for small schemas
        else:
            return 0.8  # 80% for larger schemas
    
    def make_decision(
        self,
        new_attributes: List[str],
        new_data: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Make schema evolution decision.
        
        Args:
            new_attributes: List of new attribute names
            new_data: Sample data for type inference (optional)
        
        Returns:
            Decision dictionary with:
            {
                'decision': 'same_table' | 'evolved_table' | 'evolved_table_jsonb' | 'new_table',
                'table_name': str,
                'mapping': Dict[str, str],
                'new_fields': List[str],
                'match_ratio': float,
                'reason': str
            }
        """
        if not new_attributes:
            return {
                'decision': 'new_table',
                'table_name': None,
                'mapping': {},
                'new_fields': new_attributes,
                'match_ratio': 0.0,
                'reason': 'No attributes provided'
            }
        
        # Get candidate tables
        candidates = self.get_candidate_tables(new_attributes, min_matches=1)
        
        if not candidates:
            # No candidates found - create new table
            return {
                'decision': 'new_table',
                'table_name': None,
                'mapping': {},
                'new_fields': new_attributes,
                'match_ratio': 0.0,
                'reason': 'No matching tables found'
            }
        
        # Calculate similarity for top candidates
        threshold = self.calculate_dynamic_threshold(len(new_attributes))
        logger.info(f"Dynamic threshold calculated: {threshold:.2%} (based on {len(new_attributes)} attributes)")
        
        best_match = None
        best_score = 0.0
        best_mapping = {}
        best_new_fields = []
        
        logger.info(f"Evaluating {min(len(candidates), 3)} candidate table(s)...")
        
        # Check top 3 candidates
        for idx, (table_name, match_count, matched_fields) in enumerate(candidates[:3], 1):
            logger.info(f"Candidate #{idx}: '{table_name}' ({match_count} matching attributes: {matched_fields})")
            
            match_ratio, mapping, new_fields = self.calculate_table_similarity(
                new_attributes,
                table_name,
                new_data=new_data
            )
            
            logger.info(f"  Match ratio: {match_ratio:.2%} | Matched: {len(mapping)} | New fields: {len(new_fields)}")
            
            if mapping:
                logger.debug(f"  Attribute mappings for '{table_name}':")
                for new_attr, existing_attr in mapping.items():
                    logger.debug(f"    '{new_attr}' → '{existing_attr}'")
            
            if new_fields:
                logger.debug(f"  New fields for '{table_name}': {new_fields}")
            
            if match_ratio > best_score:
                best_score = match_ratio
                best_match = table_name
                best_mapping = mapping
                best_new_fields = new_fields
                logger.info(f"  ✓ New best match: '{table_name}' (score: {best_score:.2%})")
            else:
                logger.debug(f"  Score {match_ratio:.2%} <= current best {best_score:.2%}")
        
        # Make decision based on rules
        logger.info("=" * 60)
        logger.info("SCHEMA EVOLUTION DECISION")
        logger.info("=" * 60)
        
        if best_match is None:
            logger.warning("No suitable match found - creating new table")
            return {
                'decision': 'new_table',
                'table_name': None,
                'mapping': {},
                'new_fields': new_attributes,
                'match_ratio': 0.0,
                'reason': 'No suitable match found'
            }
        
        num_new_fields = len(best_new_fields)
        
        logger.info(f"Best match: '{best_match}'")
        logger.info(f"Match ratio: {best_score:.2%} (threshold: {threshold:.2%})")
        logger.info(f"Matched attributes: {len(best_mapping)}")
        logger.info(f"New fields: {num_new_fields}")
        
        if best_mapping:
            logger.info("Attribute mappings:")
            for new_attr, existing_attr in best_mapping.items():
                logger.info(f"  '{new_attr}' → '{existing_attr}'")
        
        if best_new_fields:
            logger.info("New fields (will be added):")
            for field in best_new_fields:
                logger.info(f"  '{field}'")
        
        # Decision rules
        if best_score >= threshold and num_new_fields == 0:
            # Perfect match - same table
            decision = 'same_table'
            reason = f'Perfect match ({best_score:.0%}) with no new fields'
            logger.info(f"✓ DECISION: {decision} - {reason}")
        
        elif best_score >= 0.5 and num_new_fields <= 3:
            # Schema evolution - add columns
            decision = 'evolved_table'
            reason = f'Good match ({best_score:.0%}) with {num_new_fields} new fields (≤3)'
            logger.info(f"✓ DECISION: {decision} - {reason}")
        
        elif best_score >= 0.5 and num_new_fields > 3:
            # Schema evolution with JSONB for extra fields
            decision = 'evolved_table_jsonb'
            reason = f'Good match ({best_score:.0%}) with {num_new_fields} new fields (>3, using JSONB)'
            logger.info(f"✓ DECISION: {decision} - {reason}")
        
        else:
            # Low match - create new table
            decision = 'new_table'
            reason = f'Low match ({best_score:.0%}) - creating new table'
            logger.warning(f"✗ DECISION: {decision} - {reason} (score {best_score:.2%} < threshold {threshold:.2%})")
            best_match = None
            best_mapping = {}
            best_new_fields = new_attributes
        
        logger.info("=" * 60)
        
        return {
            'decision': decision,
            'table_name': best_match,
            'mapping': best_mapping,
            'new_fields': best_new_fields,
            'match_ratio': best_score,
            'reason': reason
        }
    
    def refresh_metadata(self):
        """Reload metadata from database (call after schema changes)."""
        logger.debug("Refreshing metadata from database...")
        old_tables = set(self.table_metadata.keys())
        self.inverted_index = defaultdict(set)
        self.table_metadata = {}
        self._load_metadata()
        new_tables = set(self.table_metadata.keys())
        
        logger.info(f"Metadata refreshed: {len(old_tables)} → {len(new_tables)} tables")
        if new_tables != old_tables:
            added = new_tables - old_tables
            removed = old_tables - new_tables
            if added:
                logger.info(f"New tables detected: {list(added)}")
            if removed:
                logger.info(f"Removed tables: {list(removed)}")
        
        # Log all current tables and their columns
        for table_name, info in self.table_metadata.items():
            logger.debug(f"  Table '{table_name}': {len(info['columns'])} columns - {info['columns']}")


if __name__ == "__main__":
    # Test example
    from config import get_db_config
    
    print("=" * 60)
    print("Schema Evolution Engine - Test")
    print("=" * 60)
    
    db_config = get_db_config()
    engine = SchemaEvolutionEngine(db_config)
    
    print(f"\nLoaded {len(engine.table_metadata)} tables")
    print(f"Inverted index has {len(engine.inverted_index)} attributes")
    
    # Test candidate retrieval
    print("\n1. Candidate Table Retrieval:")
    new_attrs = ["name", "price", "stock", "category"]
    candidates = engine.get_candidate_tables(new_attrs)
    print(f"   New attributes: {new_attrs}")
    for table_name, match_count, matched_fields in candidates[:5]:
        print(f"   {table_name}: {match_count} matches ({matched_fields})")
    
    # Test decision making
    print("\n2. Schema Decision:")
    decision = engine.make_decision(new_attrs)
    print(f"   Decision: {decision['decision']}")
    print(f"   Table: {decision['table_name']}")
    print(f"   Match ratio: {decision['match_ratio']:.0%}")
    print(f"   Mapping: {decision['mapping']}")
    print(f"   New fields: {decision['new_fields']}")
    print(f"   Reason: {decision['reason']}")

