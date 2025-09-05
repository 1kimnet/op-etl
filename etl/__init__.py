"""
OP-ETL: Organized ETL modules for improved ArcPy handling and code organization.

This package provides backward compatibility imports while organizing code into
dedicated directories: downloaders, staging, geoprocess, sde, and utils.
"""

import sys

# Check if we're in the refactored structure or the legacy structure
try:
    # Try refactored imports (for when PR #51 is merged)
    from etl.downloaders import download_atom, download_http, download_ogc, download_rest, download_wfs
    from etl.geoprocess import process
    from etl.sde import load_sde
    from etl.staging import stage_files
    _is_refactored = True
except ImportError:
    # Fall back to legacy imports (current main branch structure)
    from . import download_atom, download_http, download_ogc, download_rest, download_wfs
    from . import process, load_sde, stage_files
    _is_refactored = False

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
]

# Register modules in sys.modules for legacy dotted imports
# This ensures both 'from etl import download_atom' and 'import etl.download_atom' work
sys.modules['etl.download_atom'] = download_atom
sys.modules['etl.download_http'] = download_http
sys.modules['etl.download_ogc'] = download_ogc
sys.modules['etl.download_rest'] = download_rest
sys.modules['etl.download_wfs'] = download_wfs
sys.modules['etl.process'] = process
sys.modules['etl.load_sde'] = load_sde
sys.modules['etl.stage_files'] = stage_files