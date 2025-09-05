"""
OP-ETL: Organized ETL modules for improved ArcPy handling and code organization.

This package provides backward compatibility imports while organizing code into
dedicated directories: downloaders, staging, geoprocess, sde, and utils.
"""

import sys
import importlib

# Attempt to import each module from the refactored structure if available, else fall back to legacy
def _import_module(refactored, legacy, attr_name):
    spec = importlib.util.find_spec(refactored)
    if spec is not None:
        module = importlib.import_module(refactored)
        return getattr(module, attr_name)
    else:
        module = importlib.import_module(legacy, __package__)
        return module

download_atom = _import_module('etl.downloaders.download_atom', '.download_atom', 'download_atom')
download_http = _import_module('etl.downloaders.download_http', '.download_http', 'download_http')
download_ogc = _import_module('etl.downloaders.download_ogc', '.download_ogc', 'download_ogc')
download_rest = _import_module('etl.downloaders.download_rest', '.download_rest', 'download_rest')
download_wfs = _import_module('etl.downloaders.download_wfs', '.download_wfs', 'download_wfs')
process = _import_module('etl.geoprocess.process', '.process', 'process')
load_sde = _import_module('etl.sde.load_sde', '.load_sde', 'load_sde')
stage_files = _import_module('etl.staging.stage_files', '.stage_files', 'stage_files')

# Determine if refactored structure is used (all refactored modules found)
_is_refactored = all(
    importlib.util.find_spec(m) is not None for m in [
        'etl.downloaders.download_atom',
        'etl.downloaders.download_http',
        'etl.downloaders.download_ogc',
        'etl.downloaders.download_rest',
        'etl.downloaders.download_wfs',
        'etl.geoprocess.process',
        'etl.sde.load_sde',
        'etl.staging.stage_files',
    ]
)
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