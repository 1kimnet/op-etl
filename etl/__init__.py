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