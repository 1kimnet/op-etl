"""Config utilities - backward compatibility module.

This module provides backward compatibility for config utilities.
The actual implementation is in etl.utils.config.
"""

# Import all config utilities for backward compatibility
try:
    from .utils.config import *
except ImportError as e:
    import logging
    logging.warning(f"Failed to import config utilities: {e}")