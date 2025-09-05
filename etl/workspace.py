"""Workspace utilities - backward compatibility module.

This module provides backward compatibility for workspace utilities.
The actual implementation is in etl.utils.workspace.
"""

# Import all workspace utilities for backward compatibility
try:
    from .utils.workspace import *
except ImportError as e:
    import logging
    logging.warning(f"Failed to import workspace utilities: {e}")