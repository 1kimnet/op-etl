"""Utils - backward compatibility module.

This module provides backward compatibility for utility functions.
Functions are imported from naming_utils.py and etl.utils package.
"""

# Import naming utilities for backward compatibility
try:
    from .naming_utils import *
except ImportError as e:
    import logging
    logging.warning(f"Failed to import naming utilities: {e}")

# Also import common utilities from utils package
try:
    from .utils.utils import *
except ImportError as e:
    import logging
    logging.warning(f"Failed to import utils package utilities: {e}")