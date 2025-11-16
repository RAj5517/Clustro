"""
Database configuration for SQL operations.

Loads configuration from environment variables or .env file.
"""

import os
from typing import Dict, Any
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file if it exists
# Check in sql folder first, then backend folder
sql_dir = Path(__file__).parent
backend_dir = sql_dir.parent

# Try sql folder first
env_file = sql_dir / '.env'
if env_file.exists():
    load_dotenv(env_file)
# Fallback to backend folder
else:
    env_file = backend_dir / '.env'
    if env_file.exists():
        load_dotenv(env_file)


def get_db_config() -> Dict[str, Any]:
    """
    Get database configuration from environment variables.
    
    Environment variables:
        DB_HOST: Database host (default: localhost)
        DB_PORT: Database port (default: 5432)
        DB_NAME: Database name (default: auraverse)
        DB_USER: Database user (default: postgres)
        DB_PASSWORD: Database password (required)
    
    Returns:
        Dictionary with database connection parameters
    
    Raises:
        ValueError: If required environment variables are missing
    """
    host = os.getenv('DB_HOST', 'localhost')
    port = int(os.getenv('DB_PORT', '5432'))
    database = os.getenv('DB_NAME', os.getenv('DB_DATABASE', 'auraverse'))
    user = os.getenv('DB_USER', 'postgres')
    password = os.getenv('DB_PASSWORD', '')
    
    if not password:
        # Try alternative env var names
        password = os.getenv('DB_PASS', '')
    
    config = {
        'host': host,
        'port': port,
        'database': database,
        'user': user,
        'password': password,
    }
    
    return config
