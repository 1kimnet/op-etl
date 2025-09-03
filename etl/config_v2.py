"""
Configuration System v2 for OP-ETL

This module implements a simplified, explicit configuration system that replaces
the legacy split configuration (legacy/config.yaml + legacy/sources.yaml) with a single, validated configuration.

Key principles:
- Explicit over implicit (no auto-detection)
- Single configuration file
- Required geometry types for GeoJSON sources
- Comprehensive validation with clear error messages
"""

import logging
import re
from typing import Dict, List, Optional, Literal, Any
from dataclasses import dataclass
from pathlib import Path
import yaml

logger = logging.getLogger(__name__)

# Type aliases for clarity
GeometryType = Literal["POINT", "POLYLINE", "POLYGON", "MULTIPOINT"]
SourceType = Literal["http", "rest", "ogc", "wfs", "atom", "file"]
BoundingBox = List[float]  # [xmin, ymin, xmax, ymax]

@dataclass
class WorkspaceConfig:
    """Workspace path configuration with validation."""
    downloads: Path
    staging_gdb: Path
    sde_connection: Path

    def __post_init__(self) -> None:
        """
        Ensure workspace path attributes are converted to absolute pathlib.Path objects.
        
        Converts the `downloads`, `staging_gdb`, and `sde_connection` attributes to
        pathlib.Path instances and resolves them to absolute paths in-place.
        """
        self.downloads = Path(self.downloads).resolve()
        self.staging_gdb = Path(self.staging_gdb).resolve()
        self.sde_connection = Path(self.sde_connection).resolve()

@dataclass
class ProcessingConfig:
    """Processing parameters with coordinate system settings."""
    target_wkid: int
    aoi_bbox: BoundingBox
    aoi_bbox_wkid: int

    def __post_init__(self) -> None:
        """
        Validate ProcessingConfig fields after initialization.
        
        Raises:
            ValueError: If `aoi_bbox` does not contain exactly four coordinates [xmin, ymin, xmax, ymax].
        
        Notes:
            - Logs a warning if `target_wkid` or `aoi_bbox_wkid` fall outside the typical EPSG numeric range (1000–9999).
        """
        if len(self.aoi_bbox) != 4:
            raise ValueError("aoi_bbox must contain exactly 4 coordinates [xmin, ymin, xmax, ymax]")

        if not (1000 <= self.target_wkid <= 9999):
            logger.warning(f"target_wkid {self.target_wkid} may not be a valid EPSG code")

        if not (1000 <= self.aoi_bbox_wkid <= 9999):
            logger.warning(f"aoi_bbox_wkid {self.aoi_bbox_wkid} may not be a valid EPSG code")

@dataclass
class SourceConfig:
    """Individual source configuration with type-specific validation."""
    name: str
    type: SourceType
    url: str
    authority: str
    geometry: Optional[GeometryType] = None
    enabled: bool = True

    # Type-specific optional fields
    collections: Optional[List[str]] = None  # OGC/WFS
    layer_ids: Optional[List[int]] = None    # REST

    # Source types that require geometry specification
    REQUIRES_GEOMETRY = {"ogc", "wfs"}

    def __post_init__(self) -> None:
        """
        Validate and normalize a SourceConfig after initialization.
        
        Performs type-specific checks and normalization:
        - Requires a geometry for source types in REQUIRES_GEOMETRY (raises ValueError if missing).
        - Emits a warning if an "ogc" or "wfs" source has no collections.
        - Validates the URL:
          - For "file" sources accepts file:// URLs, absolute filesystem paths, or non-http(s) URLs; otherwise raises ValueError.
          - For non-"file" sources requires an http:// or https:// URL; otherwise raises ValueError.
        - Sanitizes and replaces the `name` and `authority` fields (non-alphanumeric characters collapsed to underscores, trimmed).
        
        Raises:
            ValueError: If a required geometry is missing or if the URL is invalid for the source type.
        """
        # Geometry required for sources that return GeoJSON
        if self.type in self.REQUIRES_GEOMETRY and not self.geometry:
            raise ValueError(f"geometry field required for {self.type} sources. Valid values: {', '.join(GeometryType.__args__)}")

        # Collections required for OGC/WFS
        if self.type in {"ogc", "wfs"} and not self.collections:
            logger.warning(f"No collections specified for {self.type} source {self.name}")

        # URL validation
        if self.type == "file":
            # Accept file:// URLs or valid filesystem paths
            if not (
                self.url.startswith("file://")
                or Path(self.url).is_absolute()
                or not any(self.url.startswith(proto) for proto in ("http://", "https://"))
            ):
                raise ValueError(f"Invalid file source URL or path for source {self.name}: {self.url}")
        else:
            if not self.url.startswith(("http://", "https://")):
                raise ValueError(f"Invalid URL for source {self.name}: {self.url}")

        # Name sanitization
        self.name = self._sanitize_name(self.name)
        self.authority = self._sanitize_name(self.authority)

    @staticmethod
    def _sanitize_name(name: str) -> str:
        """
        Sanitize a name for filesystem and database compatibility.
        
        Replaces non-alphanumeric characters with underscores, collapses consecutive underscores,
        and strips leading/trailing underscores.
        
        Parameters:
            name (str): Input name to sanitize.
        
        Returns:
            str: Sanitized string safe for use in filenames and identifier fields.
        """
        # Replace non-alphanumeric with underscores
        sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', name)
        # Remove multiple underscores
        sanitized = re.sub(r'_+', '_', sanitized)
        # Remove leading/trailing underscores
        return sanitized.strip('_')

@dataclass
class PipelineConfig:
    """Complete pipeline configuration with comprehensive validation."""
    workspace: WorkspaceConfig
    processing: ProcessingConfig
    sources: List[SourceConfig]

    def __post_init__(self) -> None:
        """
        Run post-initialization validation for the PipelineConfig.
        
        Performs workspace path validation, enforces that source names are unique within the same authority
        (raises ValueError on duplicates), and emits a summary of enabled sources by type. This method
        is called automatically after dataclass initialization and may produce warnings for non-fatal
        issues (missing files, invalid parent directories).
        """
        self._validate_workspace_paths()
        self._validate_source_names_unique()
        self._log_configuration_summary()

    def _validate_workspace_paths(self) -> None:
        """
        Validate required workspace filesystem paths.
        
        Raises:
            ValueError: If the parent directory of `workspace.downloads` or the parent directory
                of `workspace.staging_gdb` does not exist.
            
        Notes:
            If the `workspace.sde_connection` file itself is missing, the method does not raise;
            it emits a warning instead.
        """
        # Check parent directories exist
        if not self.workspace.downloads.parent.exists():
            raise ValueError(f"Downloads parent directory does not exist: {self.workspace.downloads.parent}")

        if not self.workspace.staging_gdb.parent.exists():
            raise ValueError(f"Staging GDB parent directory does not exist: {self.workspace.staging_gdb.parent}")

        # SDE connection file should exist
        if not self.workspace.sde_connection.exists():
            logger.warning(f"SDE connection file does not exist: {self.workspace.sde_connection}")

    def _validate_source_names_unique(self) -> None:
        """
        Ensure all source names are unique within the same authority.
        
        Iterates over configured sources and raises a ValueError if two sources share the same name under the same authority.
        Raises:
            ValueError: If a duplicate source name is found for an authority.
        """
        authority_names: Dict[str, List[str]] = {}

        for source in self.sources:
            if source.authority not in authority_names:
                authority_names[source.authority] = []

            if source.name in authority_names[source.authority]:
                raise ValueError(f"Duplicate source name '{source.name}' in authority '{source.authority}'")

            authority_names[source.authority].append(source.name)

    def _log_configuration_summary(self) -> None:
        """
        Log a brief summary of enabled sources in the pipeline.
        
        Writes an info-level summary that includes the total number of enabled sources and a per-source-type count of enabled sources.
        """
        enabled_sources = [s for s in self.sources if s.enabled]
        by_type = {}

        for source in enabled_sources:
            by_type[source.type] = by_type.get(source.type, 0) + 1

        logger.info(f"[CONFIG] Configuration loaded: {len(enabled_sources)} enabled sources")
        for source_type, count in by_type.items():
            logger.info(f"   {source_type}: {count} sources")

    def get_enabled_sources(self) -> List[SourceConfig]:
        """
        Return the list of sources that are enabled.
        
        Returns:
            List[SourceConfig]: Enabled sources (preserves original order).
        """
        return [source for source in self.sources if source.enabled]

def load_config(config_path: Path) -> PipelineConfig:
    """
    Load and validate a pipeline configuration from a YAML file.
    
    Reads the YAML at `config_path`, constructs and validates a PipelineConfig
    (WorkspaceConfig, ProcessingConfig, and SourceConfig items), and returns it.
    
    Parameters:
        config_path (Path): Path to the YAML configuration file.
    
    Returns:
        PipelineConfig: Validated pipeline configuration.
    
    Raises:
        ValueError: If the file cannot be read, contains invalid YAML, is missing
            required sections, is empty, or fails validation.
    """
    logger.info(f"📖 Loading configuration from {config_path}")

    try:
        with config_path.open('r', encoding='utf-8') as f:
            raw_config = yaml.safe_load(f)

        if not raw_config:
            raise ValueError("Configuration file is empty")

        # Convert to structured configuration
        config = PipelineConfig(
            workspace=WorkspaceConfig(**raw_config['workspace']),
            processing=ProcessingConfig(**raw_config['processing']),
            sources=[SourceConfig(**source) for source in raw_config['sources']]
        )

        logger.info(f"✅ Configuration validated successfully")
        return config

    except (IOError, yaml.YAMLError) as e:
        logger.error(f"Failed to load configuration: {e}")
        raise ValueError(f"Configuration load error: {e}") from e
    except KeyError as e:
        logger.error(f"Missing required configuration section: {e}")
        raise ValueError(f"Missing required configuration: {e}") from e
    except Exception as e:
        logger.error(f"Configuration validation failed: {e}")
        raise ValueError(f"Invalid configuration: {e}") from e

def create_example_config() -> Dict[str, Any]:
    """
    Return a representative example configuration dictionary for the v2 pipeline.
    
    The returned dictionary matches the structure expected by load_config()/PipelineConfig and is suitable for serializing to YAML as a starter/example config. It includes:
    
    - workspace: filesystem paths for 'downloads', 'staging_gdb', and 'sde_connection'.
    - processing: projection and area-of-interest settings:
        - target_wkid (int): output spatial reference (example: 3006 SWEREF99 TM).
        - aoi_bbox (list[float]): [xmin, ymin, xmax, ymax].
        - aoi_bbox_wkid (int): spatial reference of the AOI bbox.
    - sources: a list of source definitions. Each source contains keys used by SourceConfig, for example:
        - An 'ogc' source with 'geometry' and 'collections'.
        - A 'rest' source with 'layer_ids'.
    
    Returns:
        dict: Example configuration dictionary matching the module's schema.
    """
    return {
        'workspace': {
            'downloads': './data/downloads',
            'staging_gdb': './data/staging.gdb',
            'sde_connection': './data/prod.sde'
        },
        'processing': {
            'target_wkid': 3006,  # SWEREF99 TM
            'aoi_bbox': [610000, 6550000, 700000, 6650000],
            'aoi_bbox_wkid': 3006
        },
        'sources': [
            {
                'name': 'erosion_areas',
                'type': 'ogc',
                'url': 'https://api.sgu.se/features/v1/',
                'authority': 'SGU',
                'geometry': 'POLYGON',
                'collections': ['erosion'],
                'enabled': True
            },
            {
                'name': 'riksintressen',
                'type': 'rest',
                'url': 'https://services.example.com/arcgis/rest/services/Data/MapServer',
                'authority': 'LST',
                'layer_ids': [0, 1],
                'enabled': True
            }
        ]
    }

# Migration utilities
def migrate_old_config(old_config_path: Path, old_sources_path: Path,
                      output_path: Path) -> PipelineConfig:
    """
                      Migrate legacy two-file configuration (old config + old sources) into the new unified PipelineConfig format and return the validated, loaded configuration.
                      
                      Reads the legacy YAML files at old_config_path and old_sources_path, maps workspace and processing fields to the new layout, converts each legacy source to the new source schema (including copying collections or layer_ids when present), and writes the resulting unified YAML to output_path. For OGC/WFS sources the function emits a warning that geometry must be specified manually and defaults the migrated geometry to "POLYGON". Finally, the generated config is loaded and validated via load_config and the resulting PipelineConfig is returned.
                      
                      Parameters:
                          old_config_path (Path): Path to the legacy global config YAML (workspaces, geoprocessing, etc.).
                          old_sources_path (Path): Path to the legacy sources YAML (list or dict of sources and defaults).
                          output_path (Path): Path where the migrated unified YAML should be written.
                      
                      Returns:
                          PipelineConfig: The migrated configuration loaded and validated with load_config.
                      """
    logger.info(f"[MIGRATING] Migrating configuration from {old_config_path} + {old_sources_path}")

    # Load old configuration files
    with old_config_path.open('r') as f:
        old_config = yaml.safe_load(f)

    with old_sources_path.open('r') as f:
        old_sources = yaml.safe_load(f)

    # Convert to new format
    new_config = {
        'workspace': {
            'downloads': old_config['workspaces']['downloads'],
            'staging_gdb': old_config['workspaces']['staging_gdb'],
            'sde_connection': old_config['workspaces'].get('sde_conn', './data/prod.sde')
        },
        'processing': {
            'target_wkid': old_config.get('geoprocess', {}).get('target_srid', 3006),
            'aoi_bbox': old_sources.get('defaults', {}).get('bbox', [610000, 6550000, 700000, 6650000]),
            'aoi_bbox_wkid': old_sources.get('defaults', {}).get('bbox_sr', 3006)
        },
        'sources': []
    }

    # Migrate sources
    if isinstance(old_sources, dict):
        sources_list = old_sources.get('sources', [])
    else:
        sources_list = old_sources

    for old_source in sources_list:
        new_source = {
            'name': old_source['name'],
            'type': old_source['type'],
            'url': old_source['url'],
            'authority': old_source.get('authority', 'unknown'),
            'enabled': old_source.get('enabled', True)
        }

        # Add type-specific fields
        raw = old_source.get('raw', {})
        if old_source['type'] in ['ogc', 'wfs']:
            new_source['collections'] = raw.get('collections', [])
            # Geometry type must be specified manually
            logger.warning(f"Manual intervention required: specify geometry type for {old_source['name']}")
            new_source['geometry'] = 'POLYGON'  # Default assumption
        elif old_source['type'] == 'rest':
            new_source['layer_ids'] = raw.get('layer_ids', [])

        new_config['sources'].append(new_source)

    # Save new configuration
    with output_path.open('w') as f:
        yaml.dump(new_config, f, default_flow_style=False, indent=2)

    logger.info(f"✅ Migration complete: {output_path}")

    # Load and validate migrated configuration
    return load_config(output_path)

if __name__ == "__main__":
    """Example usage and testing."""
    import tempfile
    
    # Create example configuration
    example_config = create_example_config()
    
    # Save to temporary file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(example_config, f, default_flow_style=False, indent=2)
        temp_path = Path(f.name)
    
    try:
        # Test loading
        config = load_config(temp_path)
        print(f"✅ Example configuration loaded successfully")
        print(f"   Workspace: {config.workspace.downloads}")
        print(f"   Target WKID: {config.processing.target_wkid}")
        print(f"   Sources: {len(config.sources)}")
        
    finally:
        # Cleanup
        temp_path.unlink()