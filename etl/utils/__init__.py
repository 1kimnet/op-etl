"""Utilities package for ETL operations."""

try:
    from . import workspace
except ImportError as e:
    import logging
    logging.warning(f"Failed to import workspace utilities: {e}")
    workspace = None
