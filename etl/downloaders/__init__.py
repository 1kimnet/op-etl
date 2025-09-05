"""ETL downloaders package for handling various data sources."""

from . import download_atom, download_http, download_ogc, download_rest, download_wfs

__all__ = [
    'download_atom',
    'download_http',
    'download_ogc',
    'download_rest',
    'download_wfs',
]
