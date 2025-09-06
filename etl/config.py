"""Config utilities - backward compatibility module.

This module provides backward compatibility for config utilities.
The actual implementation is in etl.utils.config.
"""

# Import all config utilities for backward compatibility
import importlib
import logging
from typing import TYPE_CHECKING

logger = logging.getLogger(__name__)

try:
    _mod = importlib.import_module(".utils.config", __package__)
except ImportError as e:
    logger.error("Failed to import etl.utils.config: %s", e)
    raise

__all__ = getattr(
    _mod,
    "__all__",
    [n for n in dir(_mod) if not n.startswith("_")]
)
globals().update({name: getattr(_mod, name) for name in __all__})

if TYPE_CHECKING:
    from .utils.config import *  # noqa: F401,F403  # for static type checkers