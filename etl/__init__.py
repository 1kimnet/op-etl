"""ETL package with backward-compatible imports."""

# Core module imports with individual error handling to avoid masking failures
try:
    from . import download_atom
except ImportError as e:
    import logging
    logging.warning(f"Failed to import download_atom: {e}")
    download_atom = None

try:
    from . import download_http  
except ImportError as e:
    import logging
    logging.warning(f"Failed to import download_http: {e}")
    download_http = None

try:
    from . import download_ogc
except ImportError as e:
    import logging
    logging.warning(f"Failed to import download_ogc: {e}")
    download_ogc = None

try:
    from . import download_rest
except ImportError as e:
    import logging
    logging.warning(f"Failed to import download_rest: {e}")
    download_rest = None

try:
    from . import download_wfs
except ImportError as e:
    import logging
    logging.warning(f"Failed to import download_wfs: {e}")
    download_wfs = None

try:
    from . import stage_files
except ImportError as e:
    import logging
    logging.warning(f"Failed to import stage_files: {e}")
    stage_files = None

try:
    from . import process
except ImportError as e:
    import logging
    logging.warning(f"Failed to import process: {e}")
    process = None

try:
    from . import load_sde
except ImportError as e:
    import logging
    logging.warning(f"Failed to import load_sde: {e}")
    load_sde = None

# Backward-compatibility exports for relocated modules
try:
    from . import config
except ImportError as e:
    import logging
    logging.warning(f"Failed to import config: {e}")
    config = None

try:
    from . import paths
except ImportError as e:
    import logging
    logging.warning(f"Failed to import paths: {e}")
    paths = None

try:
    from .utils import workspace
except ImportError as e:
    import logging
    logging.warning(f"Failed to import workspace: {e}")
    workspace = None

# Backward-compatibility for the renamed utils module
try:
    from . import naming_utils as utils
except ImportError as e:
    import logging
    logging.warning(f"Failed to import naming_utils (utils): {e}")
    utils = None
