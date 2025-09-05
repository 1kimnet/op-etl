"""Utilities package for ETL operations."""

# Import workspace utilities
try:
    from . import workspace
except ImportError as e:
    import logging
    logging.warning(f"Failed to import workspace utilities: {e}")
    workspace = None

# Provide backward compatibility for the renamed naming_utils module
try:
    from ..naming_utils import *
    from .. import naming_utils
except ImportError as e:
    import logging
    logging.warning(f"Failed to import naming_utils for backward compatibility: {e}")
