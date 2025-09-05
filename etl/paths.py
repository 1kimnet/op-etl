"""Path utilities - backward compatibility module.

This module provides backward compatibility for path utilities.
The actual implementation is in etl.utils.paths.
"""

# Import all path utilities for backward compatibility
try:
    from .utils.paths import *
except ImportError as e:
    import logging
    logging.warning(f"Failed to import path utilities: {e}")