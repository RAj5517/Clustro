"""
Stub replacements for the removed SQL schema generator module.

These helpers keep the rest of the backend importable without performing
any database work.
"""

SQL_PIPELINE_MESSAGE = "SQL schema generator has been removed from this build."


def get_db_config():
    """Return an empty configuration placeholder."""
    return {}


def generate_schema(*_args, **_kwargs):
    """No-op schema generation placeholder."""
    return {
        "success": False,
        "status": "disabled",
        "message": SQL_PIPELINE_MESSAGE,
        "tables": [],
        "jobs_created": 0,
    }


def execute_pending_jobs(*_args, **_kwargs):
    """No-op schema execution placeholder."""
    return {
        "status": "disabled",
        "completed": 0,
        "failed": 0,
        "message": SQL_PIPELINE_MESSAGE,
    }
