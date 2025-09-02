"""
New Configuration System for OP-ETL v2.0

This module implements a simplified, explicit configuration system that replaces
the legacy split configuration (legacy/config.yaml + legacy/sources.yaml) with a single, validated configuration.

Key principles:
- Explicit over implicit (no auto-detection)
- Single configuration file
- Required geometry types for GeoJSON sources
- Comprehensive validation with clear error messages
"""

import logging
from typing import Dict, List, Optional, Literal, Union, Any
from dataclasses import dataclass, field
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
        """Validate workspace paths on initialization."""
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
        """Validate processing configuration."""
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
        """Validate source configuration based on type."""
        # Geometry required for sources that return GeoJSON
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
        """Sanitize name for filesystem and database compatibility."""
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
        """Validate complete configuration on initialization."""
        self._validate_workspace_paths()
        self._validate_source_names_unique()
        self._log_configuration_summary()

    def _validate_workspace_paths(self) -> None:
        """Validate that required workspace paths are accessible."""
        # Check parent directories exist
        if not self.workspace.downloads.parent.exists():
            raise ValueError(f"Downloads parent directory does not exist: {self.workspace.downloads.parent}")

        if not self.workspace.staging_gdb.parent.exists():
            raise ValueError(f"Staging GDB parent directory does not exist: {self.workspace.staging_gdb.parent}")

        # SDE connection file should exist
        if not self.workspace.sde_connection.exists():
            logger.warning(f"SDE connection file does not exist: {self.workspace.sde_connection}")

    def _validate_source_names_unique(self) -> None:
        """Ensure all source names are unique within authorities."""
        authority_names: Dict[str, List[str]] = {}

        for source in self.sources:
            if source.authority not in authority_names:
                authority_names[source.authority] = []

            if source.name in authority_names[source.authority]:
                raise ValueError(f"Duplicate source name '{source.name}' in authority '{source.authority}'")

            authority_names[source.authority].append(source.name)

    def _log_configuration_summary(self) -> None:
        """Log configuration summary for debugging."""
        enabled_sources = [s for s in self.sources if s.enabled]
        by_type = {}

        for source in enabled_sources:
            by_type[source.type] = by_type.get(source.type, 0) + 1

        logger.info(f"📋 Configuration loaded: {len(enabled_sources)} enabled sources")
        for source_type, count in by_type.items():
            logger.info(f"   {source_type}: {count} sources")

    def get_enabled_sources(self) -> List[SourceConfig]:
        """Get only enabled sources."""
        return [source for source in self.sources if source.enabled]

def load_config(config_path: Path) -> PipelineConfig:
    """Load and validate pipeline configuration from YAML file."""
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
    """Create example configuration for documentation."""
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
    """Migrate existing legacy configuration files to new unified format."""
    logger.info(f"🔄 Migrating configuration from {old_config_path} + {old_sources_path}")

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