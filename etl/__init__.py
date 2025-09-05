"""
OP-ETL: Organized ETL modules for improved ArcPy handling and code organization.

This package provides backward compatibility imports while organizing code into
dedicated directories: downloaders, staging, geoprocess, sde, and utils.
"""

# Backward compatibility imports - maintain existing import patterns
from etl.downloaders import download_atom, download_http, download_ogc, download_rest, download_wfs
from etl.geoprocess import process
from etl.sde import load_sde
from etl.staging import stage_files

# Additional backward compatibility imports for utilities
try:
    from . import config
except ImportError:
    pass

try:
    from . import paths
except ImportError:
    pass

try:
    from . import workspace
except ImportError:
    pass

try:
    from . import utils
except ImportError:
    pass

# Re-export modules at package level for backward compatibility
__all__ = [
    'download_atom',
    'download_http',
    'download_ogc',
    'download_rest',
    'download_wfs',
    'process',
    'load_sde',
    'stage_files',
    'config',
    'paths',
    'workspace', 
    'utils',
]