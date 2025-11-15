"""
Database configuration for PostgreSQL connections.

Defaults to local PostgreSQL. Can be configured for cloud providers.
"""

import os
from typing import Dict, Any

# Try to load .env file if python-dotenv is available
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # python-dotenv is optional


def get_db_config() -> Dict[str, Any]:
    """
    Get database configuration from environment variables.
    Defaults to local PostgreSQL if no environment variables set.
    
    Environment variables supported:
    - DB_HOST: Database host (default: localhost)
    - DB_PORT: Database port (default: 5432)
    - DB_NAME: Database name (default: clustro)
    - DB_USER: Database user (default: postgres)
    - DB_PASSWORD: Database password (required)
    - DB_SSLMODE: SSL mode (default: prefer, use 'disable' for local)
    
    Returns:
        Dictionary with database connection parameters (defaults to local)
    """
    # Get configuration from environment variables, default to local
    host = os.getenv('DB_HOST', 'localhost')
    config = {
        'host': host,
        'port': int(os.getenv('DB_PORT', '5432')),
        'database': os.getenv('DB_NAME', 'clustro'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', ''),
    }
    
    # For local connections (localhost/127.0.0.1), always disable SSL
    # For cloud connections, use 'require' or 'prefer'
    is_local = host in ['localhost', '127.0.0.1', '::1']
    
    if is_local:
        # Local connections: don't set sslmode (PostgreSQL will not use SSL)
        # Explicitly do not add sslmode to config for local connections
        pass
    else:
        # Cloud connections: use SSL mode from environment or default to 'prefer'
        sslmode = os.getenv('DB_SSLMODE', 'prefer')
        if sslmode and sslmode != 'disable':
            config['sslmode'] = sslmode
    
    return config


def get_cloud_db_config(provider: str = 'supabase') -> Dict[str, Any]:
    """
    Get cloud database configuration for specific providers.
    Defaults to Supabase (free tier, recommended).
    
    Args:
        provider: Cloud provider name ('supabase', 'neon', 'aws', 'gcp', 'azure', 'heroku')
                 Default: 'supabase'
    
    Returns:
        Dictionary with database connection parameters
    """
    if provider is None:
        provider = 'supabase'
    
    provider = provider.lower()
    
    # AWS RDS PostgreSQL
    if provider == 'aws' or provider == 'rds':
        return {
            'host': os.getenv('AWS_RDS_HOST', ''),
            'port': int(os.getenv('AWS_RDS_PORT', '5432')),
            'database': os.getenv('AWS_RDS_DATABASE', 'clustro'),
            'user': os.getenv('AWS_RDS_USER', 'postgres'),
            'password': os.getenv('AWS_RDS_PASSWORD', ''),
            'sslmode': 'require',  # AWS RDS requires SSL
        }
    
    # Google Cloud SQL (PostgreSQL)
    elif provider == 'gcp' or provider == 'cloudsql':
        return {
            'host': os.getenv('GCP_CLOUDSQL_HOST', ''),
            'port': int(os.getenv('GCP_CLOUDSQL_PORT', '5432')),
            'database': os.getenv('GCP_CLOUDSQL_DATABASE', 'clustro'),
            'user': os.getenv('GCP_CLOUDSQL_USER', 'postgres'),
            'password': os.getenv('GCP_CLOUDSQL_PASSWORD', ''),
            'sslmode': 'require',
            # For Cloud SQL Proxy, use localhost with proxy port
            # 'host': '127.0.0.1',
            # 'port': 5432,  # Proxy port
        }
    
    # Azure Database for PostgreSQL
    elif provider == 'azure':
        return {
            'host': os.getenv('AZURE_DB_HOST', ''),
            'port': int(os.getenv('AZURE_DB_PORT', '5432')),
            'database': os.getenv('AZURE_DB_NAME', 'clustro'),
            'user': os.getenv('AZURE_DB_USER', ''),
            'password': os.getenv('AZURE_DB_PASSWORD', ''),
            'sslmode': 'require',
        }
    
    # Supabase (PostgreSQL)
    elif provider == 'supabase':
        # Supabase connection string format: postgresql://postgres:[password]@[host]:5432/postgres
        # Extract from DATABASE_URL if provided
        db_url = os.getenv('DATABASE_URL', '')
        if db_url:
            return parse_connection_string(db_url)
        
        return {
            'host': os.getenv('SUPABASE_DB_HOST', ''),
            'port': int(os.getenv('SUPABASE_DB_PORT', '5432')),
            'database': os.getenv('SUPABASE_DB_NAME', 'postgres'),
            'user': os.getenv('SUPABASE_DB_USER', 'postgres'),
            'password': os.getenv('SUPABASE_DB_PASSWORD', ''),
            'sslmode': 'require',
        }
    
    # Neon (Serverless PostgreSQL)
    elif provider == 'neon':
        db_url = os.getenv('DATABASE_URL', '')
        if db_url:
            config = parse_connection_string(db_url)
            config['sslmode'] = 'require'
            return config
        
        return {
            'host': os.getenv('NEON_HOST', ''),
            'port': int(os.getenv('NEON_PORT', '5432')),
            'database': os.getenv('NEON_DATABASE', 'neondb'),
            'user': os.getenv('NEON_USER', ''),
            'password': os.getenv('NEON_PASSWORD', ''),
            'sslmode': 'require',
        }
    
    # Heroku Postgres
    elif provider == 'heroku':
        db_url = os.getenv('DATABASE_URL', '')
        if db_url:
            config = parse_connection_string(db_url)
            config['sslmode'] = 'require'
            return config
        
        return {
            'host': os.getenv('HEROKU_DB_HOST', ''),
            'port': int(os.getenv('HEROKU_DB_PORT', '5432')),
            'database': os.getenv('HEROKU_DB_NAME', ''),
            'user': os.getenv('HEROKU_DB_USER', ''),
            'password': os.getenv('HEROKU_DB_PASSWORD', ''),
            'sslmode': 'require',
        }
    
    # Railway, Render, Vercel Postgres (uses DATABASE_URL)
    elif provider in ['railway', 'render', 'vercel']:
        db_url = os.getenv('DATABASE_URL', '')
        if db_url:
            config = parse_connection_string(db_url)
            config['sslmode'] = 'require'
            return config
        else:
            raise ValueError(f"DATABASE_URL environment variable required for {provider}")
    
    else:
        raise ValueError(f"Unknown cloud provider: {provider}")


def parse_connection_string(connection_string: str) -> Dict[str, Any]:
    """
    Parse PostgreSQL connection string (postgresql:// or postgres://).
    
    Format: postgresql://[user[:password]@][host][:port][/database][?params]
    
    Args:
        connection_string: PostgreSQL connection URL
        
    Returns:
        Dictionary with connection parameters
    """
    from urllib.parse import urlparse, parse_qs, unquote
    
    parsed = urlparse(connection_string)
    
    # Decode URL-encoded password (e.g., %40 becomes @)
    password = unquote(parsed.password) if parsed.password else ''
    
    config = {
        'host': parsed.hostname or 'localhost',
        'port': parsed.port or 5432,
        'database': parsed.path.lstrip('/') if parsed.path else 'clustro',
        'user': unquote(parsed.username) if parsed.username else 'postgres',
        'password': password,
    }
    
    # Parse query parameters (e.g., ?sslmode=require)
    params = parse_qs(parsed.query)
    if 'sslmode' in params:
        config['sslmode'] = params['sslmode'][0]
    
    return config


def validate_db_config(config: Dict[str, Any]) -> bool:
    """
    Validate database configuration.
    
    Args:
        config: Database configuration dictionary
        
    Returns:
        True if valid, False otherwise
    """
    required = ['host', 'port', 'database', 'user', 'password']
    
    for key in required:
        if key not in config or not config[key]:
            return False
    
    return True


# Example configurations for reference (cloud only)
EXAMPLES = {
    'supabase': {
        'host': 'db.xxxxx.supabase.co',
        'port': 5432,
        'database': 'postgres',
        'user': 'postgres',
        'password': 'your_password',
        'sslmode': 'require',
    },
    'neon': {
        'host': 'ep-xxxxx.us-east-1.aws.neon.tech',
        'port': 5432,
        'database': 'neondb',
        'user': 'neondb_owner',
        'password': 'your_password',
        'sslmode': 'require',
    },
}


if __name__ == "__main__":
    """Test configuration loading."""
    print("=" * 60)
    print("Database Configuration Test")
    print("=" * 60)
    
    # Test environment variable config
    print("\n1. Environment Variable Config:")
    config = get_db_config()
    config_safe = config.copy()
    config_safe['password'] = '***' if config_safe['password'] else '(not set)'
    for key, value in config_safe.items():
        print(f"   {key}: {value}")
    
    # Test cloud configs
    print("\n2. Cloud Provider Configs:")
    providers = ['aws', 'gcp', 'azure', 'supabase', 'neon', 'heroku']
    for provider in providers:
        try:
            cloud_config = get_cloud_db_config(provider)
            cloud_safe = cloud_config.copy()
            cloud_safe['password'] = '***' if cloud_safe['password'] else '(not set)'
            print(f"\n   {provider.upper()}:")
            for key, value in cloud_safe.items():
                print(f"     {key}: {value}")
        except Exception as e:
            print(f"\n   {provider.upper()}: Error - {e}")
    
    # Test connection string parsing
    print("\n3. Connection String Parsing:")
    test_url = "postgresql://user:pass@host:5432/db?sslmode=require"
    parsed = parse_connection_string(test_url)
    parsed_safe = parsed.copy()
    parsed_safe['password'] = '***'
    for key, value in parsed_safe.items():
        print(f"   {key}: {value}")

