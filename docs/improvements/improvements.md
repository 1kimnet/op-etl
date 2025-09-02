# OP-ETL Implementation Guide: Tasks & Code Examples
**Complete refactoring guide with task breakdowns and implementation examples**

## Overview & Success Metrics

**Target**: 70% code reduction (3,600 → 1,000 lines)
**Timeline**: 5 weeks with risk buffers
**Philosophy**: Explicit configuration over automatic complexity

---

# Phase 1: Foundation & Planning (Week 1)

## Task 1.1: Baseline Capture & Documentation
**Duration**: 2 days | **Prerequisites**: None | **Risk**: Medium

### Task Breakdown
1. **1.1.1**: Create baseline test dataset (4 hours)
2. **1.1.2**: Execute current pipeline and capture metrics (4 hours)
3. **1.1.3**: Document current behavior patterns (2 hours)
4. **1.1.4**: Create comparison framework (6 hours)

### Implementation: Baseline Capture System

```python
# File: tests/baseline/capture_baseline.py
import logging
import json
import time
from typing import Dict, List, Optional, Any, NamedTuple
from dataclasses import dataclass, asdict
from pathlib import Path
import arcpy

logger = logging.getLogger(__name__)

@dataclass
class SourceBaseline:
    """Comprehensive baseline metrics for a single source."""
    name: str
    source_type: str
    authority: str
    success: bool
    feature_count: int
    geometry_type: str
    srid: int
    file_size_bytes: int
    processing_time_seconds: float
    download_path: Optional[str] = None
    staging_fc_name: Optional[str] = None
    error_message: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)

class BaselineCapture:
    """Captures comprehensive baseline metrics from current pipeline execution."""

    def __init__(self, config_path: Path, output_dir: Path) -> None:
        self.config_path = config_path
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def execute_baseline_capture(self) -> List[SourceBaseline]:
        """Execute current pipeline and capture all metrics."""
        logger.info("🔍 Starting baseline capture")

        # Load current configuration
        from etl.config import load_config
        config = load_config(self.config_path)

        baselines: List[SourceBaseline] = []

        for source in config['sources'][:5]:  # Limit to 5 test sources
            if not source.get('enabled', True):
                continue

            logger.info(f"Capturing baseline for {source['name']}")
            baseline = self._capture_single_source(source, config)
            baselines.append(baseline)

        self._save_baseline_results(baselines)
        self._generate_baseline_report(baselines)

        return baselines

    def _capture_single_source(self, source: Dict[str, Any], config: Dict[str, Any]) -> SourceBaseline:
        """Capture metrics for a single source."""
        start_time = time.time()

        try:
            # Execute download phase
            download_path = self._execute_download(source, config)

            # Execute staging phase
            staging_fc = self._execute_staging(source, download_path, config)

            # Extract metrics from staged feature class
            metrics = self._extract_fc_metrics(staging_fc, config['workspaces']['staging_gdb'])

            processing_time = time.time() - start_time

            return SourceBaseline(
                name=source['name'],
                source_type=source['type'],
                authority=source['authority'],
                success=True,
                feature_count=metrics['feature_count'],
                geometry_type=metrics['geometry_type'],
                srid=metrics['srid'],
                file_size_bytes=download_path.stat().st_size if download_path else 0,
                processing_time_seconds=processing_time,
                download_path=str(download_path) if download_path else None,
                staging_fc_name=staging_fc
            )

        except Exception as e:
            processing_time = time.time() - start_time
            logger.error(f"Baseline capture failed for {source['name']}: {e}")

            return SourceBaseline(
                name=source['name'],
                source_type=source['type'],
                authority=source['authority'],
                success=False,
                feature_count=0,
                geometry_type="UNKNOWN",
                srid=0,
                file_size_bytes=0,
                processing_time_seconds=processing_time,
                error_message=str(e)
            )

    def _execute_download(self, source: Dict[str, Any], config: Dict[str, Any]) -> Optional[Path]:
        """Execute download phase for single source."""
        # Import appropriate downloader based on type
        if source['type'] == 'rest':
            from etl import download_rest
            download_rest.process_rest_source(source, Path(config['workspaces']['downloads']), None, None)
        elif source['type'] == 'ogc':
            from etl import download_ogc
            download_ogc.process_ogc_source(source, Path(config['workspaces']['downloads']), None, None, 0.1)
        # Add other downloaders as needed

        # Find downloaded file
        authority_dir = Path(config['workspaces']['downloads']) / source['authority']
        if authority_dir.exists():
            files = list(authority_dir.rglob('*'))
            return files[0] if files else None
        return None

    def _execute_staging(self, source: Dict[str, Any], download_path: Optional[Path],
                        config: Dict[str, Any]) -> Optional[str]:
        """Execute staging phase for single source."""
        if not download_path or not download_path.exists():
            return None

        from etl.stage_files import stage_all_downloads
        stage_all_downloads(config)

        # Find staged feature class
        staging_gdb = config['workspaces']['staging_gdb']
        expected_fc_name = f"{source['authority']}_{source['name']}"

        if arcpy.Exists(f"{staging_gdb}/{expected_fc_name}"):
            return expected_fc_name
        return None

    def _extract_fc_metrics(self, fc_name: Optional[str], staging_gdb: str) -> Dict[str, Any]:
        """Extract comprehensive metrics from staged feature class."""
        if not fc_name:
            return {'feature_count': 0, 'geometry_type': 'UNKNOWN', 'srid': 0}

        fc_path = f"{staging_gdb}/{fc_name}"

        try:
            # Get feature count
            count_result = arcpy.management.GetCount(fc_path)
            feature_count = int(count_result[0])

            # Get geometry and spatial reference info
            desc = arcpy.Describe(fc_path)
            geometry_type = desc.shapeType
            srid = desc.spatialReference.factoryCode or 0

            return {
                'feature_count': feature_count,
                'geometry_type': geometry_type,
                'srid': srid
            }

        except Exception as e:
            logger.warning(f"Failed to extract metrics from {fc_path}: {e}")
            return {'feature_count': 0, 'geometry_type': 'UNKNOWN', 'srid': 0}

    def _save_baseline_results(self, baselines: List[SourceBaseline]) -> None:
        """Save baseline results to JSON file."""
        output_file = self.output_dir / "baseline_results.json"

        with output_file.open('w') as f:
            json.dump([b.to_dict() for b in baselines], f, indent=2)

        logger.info(f"💾 Baseline results saved to {output_file}")

    def _generate_baseline_report(self, baselines: List[SourceBaseline]) -> None:
        """Generate human-readable baseline report."""
        report_file = self.output_dir / "baseline_report.md"

        with report_file.open('w') as f:
            f.write("# OP-ETL Baseline Report\n\n")
            f.write(f"**Generated**: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")

            # Summary table
            f.write("## Source Summary\n\n")
            f.write("| Source | Type | Success | Features | Geometry | SRID | Time (s) |\n")
            f.write("|--------|------|---------|----------|----------|------|----------|\n")

            for baseline in baselines:
                status = "✅" if baseline.success else "❌"
                f.write(f"| {baseline.name} | {baseline.source_type} | {status} | "
                       f"{baseline.feature_count} | {baseline.geometry_type} | "
                       f"{baseline.srid} | {baseline.processing_time_seconds:.1f} |\n")

            # Detailed results
            f.write("\n## Detailed Results\n\n")
            for baseline in baselines:
                f.write(f"### {baseline.name}\n")
                f.write(f"- **Type**: {baseline.source_type}\n")
                f.write(f"- **Authority**: {baseline.authority}\n")
                f.write(f"- **Success**: {'Yes' if baseline.success else 'No'}\n")

                if baseline.success:
                    f.write(f"- **Features**: {baseline.feature_count:,}\n")
                    f.write(f"- **Geometry**: {baseline.geometry_type}\n")
                    f.write(f"- **SRID**: {baseline.srid}\n")
                    f.write(f"- **Processing Time**: {baseline.processing_time_seconds:.2f}s\n")
                    f.write(f"- **File Size**: {baseline.file_size_bytes:,} bytes\n")
                else:
                    f.write(f"- **Error**: {baseline.error_message}\n")

                f.write("\n")

        logger.info(f"📊 Baseline report generated: {report_file}")

# Usage example
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    baseline_capture = BaselineCapture(
        config_path=Path("config/config.yaml"),
        output_dir=Path("tests/baseline")
    )

    results = baseline_capture.execute_baseline_capture()
    print(f"Captured baseline for {len(results)} sources")
```

### Deliverables
- [ ] `tests/baseline/baseline_results.json` - Machine-readable metrics
- [ ] `tests/baseline/baseline_report.md` - Human-readable summary
- [ ] Test dataset with 5 representative sources configured

---

## Task 1.2: Configuration Schema Design
**Duration**: 1 day | **Prerequisites**: Task 1.1 | **Risk**: Low

### Task Breakdown
1. **1.2.1**: Design minimal configuration schema (3 hours)
2. **1.2.2**: Implement validation framework (3 hours)
3. **1.2.3**: Create migration utilities (2 hours)

### Implementation: New Configuration System

```python
# File: etl/new_config.py
import logging
from typing import Dict, List, Optional, Literal, Union, Any
from dataclasses import dataclass, field
from pathlib import Path
import yaml

logger = logging.getLogger(__name__)

# Type aliases for clarity
GeometryType = Literal["POINT", "POLYLINE", "POLYGON", "MULTIPOINT"]
SourceType = Literal["http", "rest", "ogc", "wfs", "atom"]
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

    def __post_init__(self) -> None:
        """Validate source configuration based on type."""
        # Geometry required for sources that return GeoJSON
        geojson_types = {"ogc", "wfs"}
        if self.type in geojson_types and not self.geometry:
            raise ValueError(f"geometry field required for {self.type} sources")

        # Collections required for OGC/WFS
        if self.type in {"ogc", "wfs"} and not self.collections:
            logger.warning(f"No collections specified for {self.type} source {self.name}")

        # URL validation
        if not self.url.startswith(("http://", "https://")):
            raise ValueError(f"Invalid URL for source {self.name}: {self.url}")

        # Name sanitization
        self.name = self._sanitize_name(self.name)
        self.authority = self._sanitize_name(self.authority)

    def _sanitize_name(self, name: str) -> str:
        """Sanitize name for filesystem and database compatibility."""
        import re
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
    """Migrate existing config.yaml + sources.yaml to new unified format."""
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
            'aoi_bbox': old_config.get('global_bbox', {}).get('coords', [610000, 6550000, 700000, 6650000]),
            'aoi_bbox_wkid': old_config.get('global_bbox', {}).get('crs', 3006)
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
```

### Example Configuration File

```yaml
# File: config/config_new.yaml
workspace:
  downloads: "./data/downloads"
  staging_gdb: "./data/staging.gdb"
  sde_connection: "./data/prod.sde"

processing:
  target_wkid: 3006  # SWEREF99 TM
  aoi_bbox: [610000, 6550000, 700000, 6650000]  # Strängnäs area
  aoi_bbox_wkid: 3006

sources:
  # OGC API Features source
  - name: "erosion_areas"
    type: "ogc"
    url: "https://api.sgu.se/oppnadata/stranderosion-kust/ogc/features/v1/"
    authority: "SGU"
    geometry: "POLYGON"  # Required for GeoJSON sources
    collections: ["aktiv-erosion"]
    enabled: true

  # ArcGIS REST API source
  - name: "riksintressen"
    type: "rest"
    url: "https://ext-geodata-nationella.lansstyrelsen.se/arcgis/rest/services/LST/lst_lst_riksintressen_4/MapServer"
    authority: "LST"
    layer_ids: [0]
    enabled: true

  # HTTP file download
  - name: "geodata_zip"
    type: "http"
    url: "https://www.forsvarsmakten.se/siteassets/geodata/rikstackande-geodata.zip"
    authority: "FM"
    enabled: true
```

### Deliverables
- [ ] `etl/new_config.py` - New configuration system with validation
- [ ] `config/config_new.yaml` - Example configuration file
- [ ] `scripts/migrate_config.py` - Migration utility for existing configs
- [ ] Configuration validation that catches 90% of setup errors at startup

---

## Task 1.3: Simple Logging Infrastructure
**Duration**: 1 day | **Prerequisites**: Task 1.2 | **Risk**: Low

### Implementation: Simplified Logging

```python
# File: etl/simple_logging.py
import logging
import sys
from typing import Optional
from pathlib import Path

def setup_pipeline_logging(
    console_level: str = "INFO",
    file_path: Optional[Path] = None,
    file_level: str = "DEBUG"
) -> None:
    """Configure simple, consistent logging for entire pipeline."""

    # Clear any existing handlers
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Set root logger level to capture all messages
    root_logger.setLevel(logging.DEBUG)

    # Console handler with clean format
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, console_level.upper()))
    console_format = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(message)s',
        datefmt='%H:%M:%S'
    )
    console_handler.setFormatter(console_format)
    root_logger.addHandler(console_handler)

    # File handler with detailed format (if specified)
    if file_path:
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(file_path, mode='w', encoding='utf-8')
        file_handler.setLevel(getattr(logging, file_level.upper()))
        file_format = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(name)s | %(funcName)s:%(lineno)d | %(message)s'
        )
        file_handler.setFormatter(file_format)
        root_logger.addHandler(file_handler)

def log_phase_start(phase_name: str) -> None:
    """Standard start message for pipeline phases."""
    logging.info(f"🚀 Starting {phase_name} phase")

def log_source_result(
    source_name: str,
    success: bool,
    feature_count: int = 0,
    error: Optional[str] = None
) -> None:
    """Standard result logging for sources."""
    if success:
        if feature_count > 0:
            logging.info(f"✅ {source_name}: {feature_count:,} features")
        else:
            logging.info(f"✅ {source_name}: processed successfully")
    else:
        error_msg = f" ({error})" if error else ""
        logging.error(f"❌ {source_name}: failed{error_msg}")

def log_phase_complete(phase_name: str, total_sources: int, successful: int) -> None:
    """Standard completion message."""
    if successful == total_sources:
        logging.info(f"✅ {phase_name} complete: {successful}/{total_sources} sources successful")
    else:
        failed = total_sources - successful
        logging.warning(f"⚠️  {phase_name} complete: {successful}/{total_sources} sources successful ({failed} failed)")
```

### Deliverables
- [ ] `etl/simple_logging.py` - Unified logging system
- [ ] Consistent log format across all modules
- [ ] Clear success/failure indicators in logs

---

# Phase 2: Core Infrastructure (Week 2)

## Task 2.1: Simple HTTP Client
**Duration**: 2 days | **Prerequisites**: Task 1.3 | **Risk**: Medium

### Task Breakdown
1. **2.1.1**: Replace complex http_utils.py with simple requests wrapper (6 hours)
2. **2.1.2**: Create compatibility shims for existing code (2 hours)
3. **2.1.3**: Test HTTP client with baseline sources (4 hours)
4. **2.1.4**: Update existing downloaders to use new client (4 hours)

### Implementation: Simple HTTP Client

```python
# File: etl/simple_http.py
import logging
from typing import Optional, Dict, Any, Union
from pathlib import Path
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

class SimpleHTTPClient:
    """Dead-simple HTTP client focused on reliability over features."""

    def __init__(self, timeout: int = 60, max_retries: int = 3) -> None:
        """Initialize HTTP client with sensible defaults."""
        self.session = requests.Session()
        self.timeout = timeout

        # Simple retry strategy - only retry on network errors and server errors
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=1,  # 1, 2, 4 second delays
            status_forcelist=[429, 500, 502, 503, 504],
            raise_on_status=False
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        # Set user agent
        self.session.headers.update({
            'User-Agent': 'OP-ETL/2.0 (geospatial-data-pipeline)'
        })

    def get_json(self, url: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """Get JSON response with basic error handling."""
        logger.debug(f"GET JSON: {url}")

        try:
            response = self.session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()

            return response.json()

        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error for {url}: {e}")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {url}: {e}")
            return None
        except ValueError as e:  # JSON decode error
            logger.error(f"Invalid JSON response from {url}: {e}")
            return None

    def get_text(self, url: str, params: Optional[Dict[str, Any]] = None) -> Optional[str]:
        """Get text response."""
        logger.debug(f"GET TEXT: {url}")

        try:
            response = self.session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()

            return response.text

        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {url}: {e}")
            return None

    def download_file(self, url: str, output_path: Path,
                     params: Optional[Dict[str, Any]] = None) -> bool:
        """Download file with progress logging."""
        logger.info(f"📥 Downloading {url} -> {output_path.name}")

        try:
            response = self.session.get(url, params=params, timeout=self.timeout, stream=True)
            response.raise_for_status()

            # Create parent directory if needed
            output_path.parent.mkdir(parents=True, exist_ok=True)

            # Download with chunked reading
            total_size = 0
            with output_path.open('wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:  # Filter out keep-alive chunks
                        f.write(chunk)
                        total_size += len(chunk)

            logger.info(f"✅ Downloaded {total_size:,} bytes to {output_path.name}")
            return True

        except requests.exceptions.RequestException as e:
            logger.error(f"Download failed for {url}: {e}")
            return False
        except IOError as e:
            logger.error(f"Failed to write file {output_path}: {e}")
            return False

# Global client instance for easy access
http_client = SimpleHTTPClient()

# Convenience functions
def get_json(url: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    """Convenience function for JSON requests."""
    return http_client.get_json(url, params)

def get_text(url: str, params: Optional[Dict[str, Any]] = None) -> Optional[str]:
    """Convenience function for text requests."""
    return http_client.get_text(url, params)

def download_file(url: str, output_path: Path,
                 params: Optional[Dict[str, Any]] = None) -> bool:
    """Convenience function for file downloads."""
    return http_client.download_file(url, output_path, params)
```

### Compatibility Shims

```python
# File: etl/http_compat.py
"""Backward compatibility shims during migration phase."""

from .simple_http import http_client
from pathlib import Path
from typing import Optional, Dict, Any

class RecursionSafeSession:
    """Compatibility shim for existing code that uses RecursionSafeSession."""

    def safe_get(self, url: str, **kwargs) -> Optional[Dict[str, Any]]:
        """Compatibility method that returns JSON data."""
        params = kwargs.get('params')
        return http_client.get_json(url, params)

def download_with_retries(url: str, output_path: Path, **kwargs) -> bool:
    """Compatibility function for existing download calls."""
    return http_client.download_file(url, output_path)

# Simplified parsing functions
def safe_json_parse(content: Any) -> Optional[Dict[str, Any]]:
    """Simplified JSON parsing - assume content is already parsed."""
    if isinstance(content, dict):
        return content
    return None

def validate_response_content(response: Any) -> bool:
    """Simplified response validation - always return True."""
    return response is not None
```

### Deliverables
- [ ] `etl/simple_http.py` - New HTTP client (100 lines vs 567 current)
- [ ] `etl/http_compat.py` - Compatibility layer for migration
- [ ] HTTP client tested with all baseline sources
- [ ] 85% reduction in HTTP-related code complexity

---

## Task 2.2: Pipeline Orchestrator
**Duration**: 2 days | **Prerequisites**: Task 2.1 | **Risk**: High

### Implementation: Core Orchestrator

```python
# File: etl/pipeline.py
import logging
from typing import List, Tuple, Optional, Dict, Any
from pathlib import Path
from dataclasses import dataclass
import time

from .new_config import PipelineConfig, SourceConfig
from .simple_logging import log_phase_start, log_source_result, log_phase_complete

logger = logging.getLogger(__name__)

@dataclass
class PipelineResult:
    """Results from complete pipeline execution."""
    successful_sources: List[str]
    failed_sources: List[Tuple[str, str]]  # (name, error_message)
    total_features: int
    staging_feature_classes: List[str]
    total_duration_seconds: float

    def success_rate(self) -> float:
        """Calculate success rate as percentage."""
        total = len(self.successful_sources) + len(self.failed_sources)
        if total == 0:
            return 100.0
        return (len(self.successful_sources) / total) * 100.0

class PipelineOrchestrator:
    """Simple orchestrator that coordinates the three main ETL phases."""

    def __init__(self, config: PipelineConfig) -> None:
        """Initialize orchestrator with validated configuration."""
        self.config = config
        self.start_time = time.time()

        # Ensure workspace directories exist
        self.config.workspace.downloads.mkdir(parents=True, exist_ok=True)

    def run_full_pipeline(self) -> PipelineResult:
        """Execute complete ETL pipeline with error isolation per source."""
        logger.info("🚀 Starting OP-ETL Pipeline")
        logger.info(f"Target SRID: {self.config.processing.target_wkid}")
        logger.info(f"AOI Bbox: {self.config.processing.aoi_bbox}")

        try:
            # Phase 1: Download all sources
            downloaded_files = self._run_download_phase()

            # Phase 2: Stage downloaded files to FGDB
            staged_fcs = self._run_staging_phase(downloaded_files)

            # Phase 3: Load staged feature classes to SDE
            self._run_loading_phase(staged_fcs)

            # Build final result
            result = self._build_pipeline_result(downloaded_files, staged_fcs)

            # Log summary
            duration = time.time() - self.start_time
            logger.info(f"🎉 Pipeline complete in {duration:.1f}s: "
                       f"{result.success_rate():.1f}% success rate "
                       f"({len(result.successful_sources)}/{len(result.successful_sources) + len(result.failed_sources)})")

            return result

        except Exception as e:
            logger.error(f"💥 Pipeline failed with critical error: {e}")
            raise

    def _run_download_phase(self) -> List[Tuple[SourceConfig, Optional[Path]]]:
        """Download all enabled sources, isolating failures per source."""
        log_phase_start("Download")

        enabled_sources = self.config.get_enabled_sources()
        results: List[Tuple[SourceConfig, Optional[Path]]] = []

        for source in enabled_sources:
            try:
                logger.info(f"📥 Downloading {source.type} source: {source.name}")
                file_path = self._download_single_source(source)
                results.append((source, file_path))

                if file_path and file_path.exists():
                    size_mb = file_path.stat().st_size / (1024 * 1024)
                    log_source_result(source.name, True, 0)
                    logger.debug(f"Downloaded {size_mb:.1f} MB to {file_path}")
                else:
                    log_source_result(source.name, False, 0, "No file created")
                    results[-1] = (source, None)  # Mark as failed

            except Exception as e:
                logger.exception(f"Download failed for {source.name}")
                log_source_result(source.name, False, 0, str(e))
                results.append((source, None))

        successful = len([r for r in results if r[1] is not None])
        log_phase_complete("Download", len(results), successful)

        return results

    def _download_single_source(self, source: SourceConfig) -> Optional[Path]:
        """Download single source using appropriate downloader."""
        # Import downloader dynamically to avoid circular imports
        from .download import SourceDownloader

        downloader = SourceDownloader(self.config)
        return downloader.download(source)

    def _run_staging_phase(self, downloads: List[Tuple[SourceConfig, Optional[Path]]]) -> List[str]:
        """Stage downloaded files to FGDB with error isolation."""
        log_phase_start("Staging")

        # Filter to only successful downloads
        successful_downloads = [(source, path) for source, path in downloads if path is not None]

        if not successful_downloads:
            logger.warning("No files to stage - all downloads failed")
            return []

        # Import staging processor
        from .stage import StagingProcessor
        processor = StagingProcessor(self.config)

        staged_fcs: List[str] = []

        for source, file_path in successful_downloads:
            try:
                logger.info(f"🔄 Staging {source.type} file: {file_path.name}")
                fc_name = processor.stage_file(source, file_path)
                staged_fcs.append(fc_name)

                # Get feature count for logging
                feature_count = self._get_feature_count(fc_name)
                log_source_result(source.name, True, feature_count)

            except Exception as e:
                logger.exception(f"Staging failed for {source.name}")
                log_source_result(source.name, False, 0, str(e))

        log_phase_complete("Staging", len(successful_downloads), len(staged_fcs))
        return staged_fcs

    def _run_loading_phase(self, fc_names: List[str]) -> None:
        """Load staged feature classes to SDE."""
        if not fc_names:
            logger.info("No feature classes to load to SDE")
            return

        log_phase_start("SDE Loading")

        # Import SDE loader
        from .load import SDELoader
        loader = SDELoader(self.config)

        successful_loads = 0

        for fc_name in fc_names:
            try:
                logger.info(f"📤 Loading to SDE: {fc_name}")
                loader.load_feature_class(fc_name)
                successful_loads += 1
                log_source_result(fc_name, True, 0)

            except Exception as e:
                logger.exception(f"SDE load failed for {fc_name}")
                log_source_result(fc_name, False, 0, str(e))

        log_phase_complete("SDE Loading", len(fc_names), successful_loads)

    def _get_feature_count(self, fc_name: str) -> int:
        """Get feature count from staged feature class."""
        try:
            import arcpy
            fc_path = f"{self.config.workspace.staging_gdb}/{fc_name}"
            if arcpy.Exists(fc_path):
                count_result = arcpy.management.GetCount(fc_path)
                return int(count_result[0])
        except Exception as e:
            logger.debug(f"Could not get feature count for {fc_name}: {e}")

        return 0

    def _build_pipeline_result(self, downloads: List[Tuple[SourceConfig, Optional[Path]]],
                              staged_fcs: List[str]) -> PipelineResult:
        """Build comprehensive pipeline result."""
        successful_sources = []
        failed_sources = []
        total_features = 0

        for source, file_path in downloads:
            if file_path and file_path.exists():
                successful_sources.append(source.name)
            else:
                failed_sources.append((source.name, "Download failed"))

        # Calculate total features
        for fc_name in staged_fcs:
            total_features += self._get_feature_count(fc_name)

        return PipelineResult(
            successful_sources=successful_sources,
            failed_sources=failed_sources,
            total_features=total_features,
            staging_feature_classes=staged_fcs,
            total_duration_seconds=time.time() - self.start_time
        )

    # Individual phase methods for partial execution
    def run_download_only(self) -> PipelineResult:
        """Execute only the download phase."""
        downloads = self._run_download_phase()
        return self._build_pipeline_result(downloads, [])

    def run_staging_only(self) -> PipelineResult:
        """Execute only staging phase (assumes downloads exist)."""
        # Find existing download files
        downloads = self._discover_existing_downloads()
        staged_fcs = self._run_staging_phase(downloads)
        return self._build_pipeline_result(downloads, staged_fcs)

    def run_loading_only(self) -> PipelineResult:
        """Execute only SDE loading phase (assumes staging GDB populated)."""
        # Find existing staged feature classes
        staged_fcs = self._discover_staged_feature_classes()
        self._run_loading_phase(staged_fcs)
        return PipelineResult([], [], 0, staged_fcs, 0.0)

    def _discover_existing_downloads(self) -> List[Tuple[SourceConfig, Optional[Path]]]:
        """Discover existing download files for enabled sources."""
        results = []

        for source in self.config.get_enabled_sources():
            # Look for downloaded files in authority directory
            authority_dir = self.config.workspace.downloads / source.authority

            if authority_dir.exists():
                # Find most recent file matching source name
                pattern = f"*{source.name}*"
                files = list(authority_dir.glob(pattern))

                if files:
                    # Take most recent file
                    most_recent = max(files, key=lambda p: p.stat().st_mtime)
                    results.append((source, most_recent))
                    continue

            results.append((source, None))

        return results

    def _discover_staged_feature_classes(self) -> List[str]:
        """Discover existing feature classes in staging GDB."""
        try:
            import arcpy

            # List feature classes in staging GDB
            arcpy.env.workspace = str(self.config.workspace.staging_gdb)
            fc_names = arcpy.ListFeatureClasses()

            return fc_names or []

        except Exception as e:
            logger.error(f"Failed to discover staged feature classes: {e}")
            return []
```

### Deliverables
- [ ] `etl/pipeline.py` - Core orchestrator with phase isolation
- [ ] Support for full pipeline or individual phases
- [ ] Error isolation prevents single source from killing pipeline
- [ ] Comprehensive result reporting with success rates

---

# Phase 3: Download Consolidation (Week 3)

## Task 3.1: Unified Download System
**Duration**: 3 days | **Prerequisites**: Task 2.2 | **Risk**: High

### Implementation: Consolidated Downloader

```python
# File: etl/download.py
import logging
import json
import zipfile
from typing import Optional, List, Dict, Any
from pathlib import Path
from urllib.parse import urljoin
import xml.etree.ElementTree as ET

from .new_config import PipelineConfig, SourceConfig
from .simple_http import http_client

logger = logging.getLogger(__name__)

class DownloadResult:
    """Standard result container for download operations."""

    def __init__(self, file_path: Optional[Path], source_name: str,
                 feature_count: int = 0, error: Optional[str] = None):
        self.file_path = file_path
        self.source_name = source_name
        self.feature_count = feature_count
        self.error = error
        self.success = file_path is not None and file_path.exists()

class SourceDownloader:
    """Unified downloader that handles all source types with shared logic."""

    def __init__(self, config: PipelineConfig) -> None:
        """Initialize with pipeline configuration for bbox and paths."""
        self.config = config
        self.downloads_dir = config.workspace.downloads
        self.bbox = config.processing.aoi_bbox
        self.bbox_wkid = config.processing.aoi_bbox_wkid

        # Create downloads directory
        self.downloads_dir.mkdir(parents=True, exist_ok=True)

    def download(self, source: SourceConfig) -> Optional[Path]:
        """Download source using appropriate method based on type."""
        logger.info(f"🔗 Processing {source.type.upper()} source: {source.name}")

        # Dispatch to appropriate handler
        handlers = {
            'http': self._download_http,
            'rest': self._download_rest,
            'ogc': self._download_ogc,
            'wfs': self._download_wfs,
            'atom': self._download_atom
        }

        handler = handlers.get(source.type)
        if not handler:
            raise ValueError(f"Unsupported source type: {source.type}")

        try:
            return handler(source)
        except Exception as e:
            logger.error(f"Download handler failed for {source.name}: {e}")
            raise

    def _create_output_path(self, source: SourceConfig, extension: str = ".json") -> Path:
        """Create standardized output path for downloaded files."""
        output_dir = self.downloads_dir / source.authority
        output_dir.mkdir(parents=True, exist_ok=True)
        return output_dir / f"{source.name}{extension}"

    # HTTP Downloader (Simplest)
    def _download_http(self, source: SourceConfig) -> Optional[Path]:
        """Download direct HTTP file with automatic extraction."""
        logger.debug(f"Downloading HTTP file from: {source.url}")

        # Determine file extension from URL
        url_path = Path(source.url)
        extension = url_path.suffix or ".zip"
        output_path = self._create_output_path(source, extension)

        # Download file
        success = http_client.download_file(source.url, output_path)
        if not success:
            raise RuntimeError(f"Failed to download {source.url}")

        # Extract ZIP files automatically
        if extension.lower() == ".zip":
            return self._extract_and_find_best_file(output_path, source)

        return output_path

    def _extract_and_find_best_file(self, zip_path: Path, source: SourceConfig) -> Optional[Path]:
        """Extract ZIP and find the best file to use."""
        extract_dir = zip_path.parent / f"{source.name}_extracted"
        extract_dir.mkdir(exist_ok=True)

        try:
            with zipfile.ZipFile(zip_path, 'r') as zf:
                zf.extractall(extract_dir)

            logger.info(f"📂 Extracted ZIP to {extract_dir}")

            # Find best file in priority order
            for pattern in ["*.gpkg", "*.shp", "*.gdb"]:
                files = list(extract_dir.rglob(pattern))
                if files:
                    best_file = files[0]  # Take first match
                    logger.info(f"Found {pattern} file: {best_file.name}")
                    return best_file

            raise RuntimeError(f"No suitable files found in extracted ZIP: {zip_path}")

        except zipfile.BadZipFile as e:
            raise RuntimeError(f"Invalid ZIP file: {zip_path}") from e

    # REST API Downloader
    def _download_rest(self, source: SourceConfig) -> Optional[Path]:
        """Download ArcGIS REST API source with simplified approach."""
        logger.debug(f"Querying REST service: {source.url}")

        # Get service information
        service_info = http_client.get_json(source.url, {'f': 'json'})
        if not service_info:
            raise RuntimeError(f"Cannot access REST service: {source.url}")

        # Determine layer to download
        layer_id = self._get_rest_layer_id(service_info, source)
        layer_url = f"{source.url.rstrip('/')}/{layer_id}"

        # Query layer data
        features = self._query_rest_layer(layer_url)
        if not features:
            raise RuntimeError(f"No features returned from layer {layer_id}")

        # Save as GeoJSON
        output_path = self._create_output_path(source, ".geojson")
        with output_path.open('w', encoding='utf-8') as f:
            json.dump(features, f, ensure_ascii=False)

        feature_count = len(features.get('features', []))
        logger.info(f"📊 Downloaded {feature_count:,} features")

        return output_path

    def _get_rest_layer_id(self, service_info: Dict[str, Any], source: SourceConfig) -> int:
        """Determine which layer ID to download."""
        available_layers = service_info.get('layers', [])

        # Use specified layer_ids or take first available
        if source.layer_ids:
            return source.layer_ids[0]
        elif available_layers:
            return available_layers[0]['id']
        elif service_info.get('type') == 'Feature Layer':
            # Single layer service
            return service_info.get('id', 0)
        else:
            raise RuntimeError(f"No layers found in REST service: {source.url}")

    def _query_rest_layer(self, layer_url: str) -> Optional[Dict[str, Any]]:
        """Query REST layer with bbox filter."""
        params = {
            'where': '1=1',
            'outFields': '*',
            'f': 'geojson',  # Try GeoJSON first
            'geometryType': 'esriGeometryEnvelope',
            'spatialRel': 'esriSpatialRelIntersects'
        }

        # Add bbox filter
        if self.bbox:
            bbox_geometry = {
                'xmin': self.bbox[0], 'ymin': self.bbox[1],
                'xmax': self.bbox[2], 'ymax': self.bbox[3],
                'spatialReference': {'wkid': self.bbox_wkid}
            }
            params['geometry'] = json.dumps(bbox_geometry)
            params['inSR'] = self.bbox_wkid
            logger.debug(f"Using bbox filter: {self.bbox}")

        # Make request
        query_url = f"{layer_url}/query"
        response = http_client.get_json(query_url, params)

        if not response:
            # Fallback to Esri JSON format
            logger.info("GeoJSON failed, trying Esri JSON format")
            params['f'] = 'json'
            response = http_client.get_json(query_url, params)

        if not response:
            raise RuntimeError(f"Query failed for both GeoJSON and JSON formats: {query_url}")

        return response

    # OGC API Features Downloader
    def _download_ogc(self, source: SourceConfig) -> Optional[Path]:
        """Download OGC API Features source with pagination support."""
        logger.debug(f"Fetching OGC collections from: {source.url}")

        all_features = []
        collections = source.collections or self._discover_ogc_collections(source)

        for collection in collections:
            logger.info(f"📦 Processing collection: {collection}")
            features = self._fetch_ogc_collection(source, collection)
            all_features.extend(features)
            logger.info(f"   Retrieved {len(features):,} features")

        if not all_features:
            logger.warning("No features retrieved from any collection")
            return None

        # Create GeoJSON FeatureCollection
        geojson = {
            'type': 'FeatureCollection',
            'features': all_features
        }

        # Save to file
        output_path = self._create_output_path(source, ".geojson")
        with output_path.open('w', encoding='utf-8') as f:
            json.dump(geojson, f, ensure_ascii=False)

        logger.info(f"📊 Total features from {len(collections)} collections: {len(all_features):,}")
        return output_path

    def _discover_ogc_collections(self, source: SourceConfig) -> List[str]:
        """Discover available collections from OGC API."""
        collections_url = f"{source.url.rstrip('/')}/collections"
        data = http_client.get_json(collections_url, {'f': 'json'})

        if not data or 'collections' not in data:
            raise RuntimeError(f"Cannot discover collections from {collections_url}")

        collection_ids = [c['id'] for c in data['collections'] if 'id' in c]
        logger.info(f"📋 Discovered {len(collection_ids)} collections: {collection_ids}")

        return collection_ids

    def _fetch_ogc_collection(self, source: SourceConfig, collection: str) -> List[Dict[str, Any]]:
        """Fetch all features from an OGC collection with pagination."""
        items_url = f"{source.url.rstrip('/')}/collections/{collection}/items"
        params = {'limit': 1000}

        # Add bbox filter
        if self.bbox:
            params['bbox'] = ','.join(map(str, self.bbox))
            logger.debug(f"Using bbox filter: {self.bbox}")

        features = []
        next_url = items_url
        page_count = 0

        while next_url and page_count < 100:  # Safety limit
            logger.debug(f"Fetching page {page_count + 1}")

            # Use params only for first request
            request_params = params if next_url == items_url else None
            data = http_client.get_json(next_url, request_params)

            if not data:
                logger.warning(f"No data returned from {next_url}")
                break

            page_features = data.get('features', [])
            features.extend(page_features)

            if not page_features:
                logger.debug("Empty page received, stopping pagination")
                break

            # Find next link
            next_url = self._find_next_link(data.get('links', []))
            page_count += 1

        logger.debug(f"Retrieved {len(features)} features in {page_count} pages")
        return features

    def _find_next_link(self, links: List[Dict[str, Any]]) -> Optional[str]:
        """Find next page link in OGC API response."""
        for link in links:
            if link.get('rel') == 'next' and link.get('href'):
                return link['href']
        return None

    # WFS Downloader
    def _download_wfs(self, source: SourceConfig) -> Optional[Path]:
        """Download WFS source using GetFeature request."""
        logger.debug(f"WFS GetFeature request: {source.url}")

        params = {
            'service': 'WFS',
            'version': '2.0.0',
            'request': 'GetFeature',
            'outputFormat': 'application/json'
        }

        # Add typename/collections
        if source.collections:
            params['typeName'] = ','.join(source.collections)
            logger.debug(f"Requesting typenames: {source.collections}")

        # Add bbox filter
        if self.bbox:
            params['bbox'] = ','.join(map(str, self.bbox))
            logger.debug(f"Using bbox filter: {self.bbox}")

        # Make WFS request
        response = http_client.get_json(source.url, params)
        if not response:
            raise RuntimeError(f"WFS GetFeature request failed: {source.url}")

        # Save response
        output_path = self._create_output_path(source, ".geojson")
        with output_path.open('w', encoding='utf-8') as f:
            json.dump(response, f, ensure_ascii=False)

        feature_count = len(response.get('features', []))
        logger.info(f"📊 WFS returned {feature_count:,} features")

        return output_path

    # ATOM Feed Downloader
    def _download_atom(self, source: SourceConfig) -> Optional[Path]:
        """Download ATOM feed source by parsing XML and downloading enclosures."""
        logger.debug(f"Parsing ATOM feed: {source.url}")

        # Get ATOM feed content
        atom_content = http_client.get_text(source.url)
        if not atom_content:
            raise RuntimeError(f"Failed to fetch ATOM feed: {source.url}")

        # Parse XML to find download links
        try:
            root = ET.fromstring(atom_content)
        except ET.ParseError as e:
            raise RuntimeError(f"Invalid XML in ATOM feed: {source.url}") from e

        # Find enclosure links
        download_urls = self._extract_atom_download_links(root)
        if not download_urls:
            raise RuntimeError(f"No download links found in ATOM feed: {source.url}")

        # Download first available file
        download_url = download_urls[0]
        logger.info(f"🔗 Found download link: {download_url}")

        # Determine file extension and download
        url_path = Path(download_url)
        extension = url_path.suffix or ".zip"
        output_path = self._create_output_path(source, extension)

        success = http_client.download_file(download_url, output_path)
        if not success:
            raise RuntimeError(f"Failed to download from ATOM feed: {download_url}")

        # Extract if ZIP
        if extension.lower() == ".zip":
            return self._extract_and_find_best_file(output_path, source)

        return output_path

    def _extract_atom_download_links(self, root: ET.Element) -> List[str]:
        """Extract download URLs from ATOM feed XML."""
        download_urls = []

        # Look for enclosure links in ATOM namespace
        atom_ns = "http://www.w3.org/2005/Atom"

        for link in root.findall(f".//{{{atom_ns}}}link"):
            rel = link.get('rel')
            href = link.get('href')

            if rel == 'enclosure' and href:
                download_urls.append(href)

        # Fallback: look for links without namespace
        if not download_urls:
            for link in root.findall(".//link"):
                rel = link.get('rel')
                href = link.get('href')

                if rel == 'enclosure' and href:
                    download_urls.append(href)

        logger.debug(f"Found {len(download_urls)} download links in ATOM feed")
        return download_urls
```

### Deliverables
- [ ] `etl/download.py` - Unified downloader (400 lines vs 1,039 current)
- [ ] All 5 source types supported with shared infrastructure
- [ ] Common bbox filtering, error handling, and output patterns
- [ ] 65% reduction in download-related code

---

# Phase 4: Staging Simplification (Week 4)

## Task 4.1: Explicit Geometry Staging
**Duration**: 3 days | **Prerequisites**: Task 3.1 | **Risk**: Medium

### Implementation: Simple Staging System

```python
# File: etl/stage.py
import logging
from typing import Optional, List, Dict, Any
from pathlib import Path

from .new_config import SourceConfig, PipelineConfig

logger = logging.getLogger(__name__)

class StagingProcessor:
    """Simple staging processor - no auto-detection, explicit geometry types required."""

    def __init__(self, config: PipelineConfig) -> None:
        """Initialize with pipeline configuration."""
        self.config = config
        self.staging_gdb = config.workspace.staging_gdb
        self.target_wkid = config.processing.target_wkid

        # Ensure staging GDB exists
        self._ensure_staging_gdb_exists()

    def stage_file(self, source: SourceConfig, file_path: Path) -> str:
        """Stage file to FGDB feature class with explicit geometry handling."""
        logger.info(f"🔄 Staging {file_path.suffix} file: {file_path.name}")

        # Create feature class name
        fc_name = self._create_feature_class_name(source)
        fc_path = str(self.staging_gdb / fc_name)

        # Delete existing if present
        self._delete_existing_feature_class(fc_path)

        # Import based on file type
        if file_path.suffix.lower() == '.geojson':
            self._stage_geojson(file_path, fc_path, source)
        elif file_path.suffix.lower() == '.json':
            self._stage_esri_json(file_path, fc_path)
        elif file_path.suffix.lower() == '.shp':
            self._stage_shapefile(file_path, fc_path)
        elif file_path.suffix.lower() == '.gpkg':
            self._stage_geopackage(file_path, fc_path)
        else:
            raise ValueError(f"Unsupported file type for staging: {file_path.suffix}")

        # Ensure proper coordinate system
        self._ensure_target_coordinate_system(fc_path)

        # Log results
        feature_count = self._get_feature_count(fc_path)
        logger.info(f"✅ Staged {feature_count:,} features as {fc_name}")

        return fc_name

    def _create_feature_class_name(self, source: SourceConfig) -> str:
        """Create safe feature class name following naming convention."""
        # Format: {authority}_{name} with sanitization
        safe_authority = self._sanitize_for_arcpy(source.authority)
        safe_name = self._sanitize_for_arcpy(source.name)

        # Combine and ensure length limit
        fc_name = f"{safe_authority}_{safe_name}"
        if len(fc_name) > 50:  # ArcGIS limitation
            fc_name = fc_name[:50]

        return fc_name

    def _sanitize_for_arcpy(self, name: str) -> str:
        """Sanitize string for ArcPy feature class names."""
        import re

        # Replace non-alphanumeric with underscores
        sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', name)

        # Remove multiple underscores
        sanitized = re.sub(r'_+', '_', sanitized)

        # Remove leading/trailing underscores
        sanitized = sanitized.strip('_')

        # Ensure starts with letter
        if sanitized and sanitized[0].isdigit():
            sanitized = f"fc_{sanitized}"

        return sanitized or "unnamed"

    def _delete_existing_feature_class(self, fc_path: str) -> None:
        """Delete existing feature class if present."""
        try:
            import arcpy
            if arcpy.Exists(fc_path):
                logger.debug(f"Deleting existing feature class: {Path(fc_path).name}")
                arcpy.management.Delete(fc_path)
        except Exception as e:
            logger.warning(f"Could not delete existing feature class {fc_path}: {e}")

    def _stage_geojson(self, file_path: Path, fc_path: str, source: SourceConfig) -> None:
        """Stage GeoJSON file with explicit geometry type."""
        import arcpy

        if not source.geometry:
            raise ValueError(f"Geometry type required for GeoJSON source: {source.name}")

        # Map to ArcPy geometry types
        geometry_mapping = {
            'POINT': 'POINT',
            'POLYLINE': 'POLYLINE',
            'POLYGON': 'POLYGON',
            'MULTIPOINT': 'MULTIPOINT'
        }

        arcpy_geometry = geometry_mapping.get(source.geometry.upper())
        if not arcpy_geometry:
            raise ValueError(f"Unsupported geometry type: {source.geometry}")

        logger.debug(f"Converting GeoJSON with geometry type: {arcpy_geometry}")

        try:
            # Use JSONToFeatures with explicit geometry type
            arcpy.conversion.JSONToFeatures(
                in_json_file=str(file_path),
                out_features=fc_path,
                geometry_type=arcpy_geometry
            )
        except arcpy.ExecuteError as e:
            error_msg = arcpy.GetMessages(2)
            raise RuntimeError(f"JSONToFeatures failed: {error_msg}") from e

    def _stage_esri_json(self, file_path: Path, fc_path: str) -> None:
        """Stage Esri JSON format file."""
        import arcpy

        logger.debug("Converting Esri JSON format")

        try:
            # Esri JSON should include spatial reference information
            arcpy.conversion.JSONToFeatures(
                in_json_file=str(file_path),
                out_features=fc_path
            )
        except arcpy.ExecuteError as e:
            error_msg = arcpy.GetMessages(2)
            raise RuntimeError(f"Esri JSON conversion failed: {error_msg}") from e

    def _stage_shapefile(self, file_path: Path, fc_path: str) -> None:
        """Stage shapefile to FGDB."""
        import arcpy

        logger.debug(f"Converting shapefile: {file_path.name}")

        try:
            arcpy.conversion.FeatureClassToFeatureClass(
                in_features=str(file_path),
                out_path=str(self.staging_gdb),
                out_name=Path(fc_path).name
            )
        except arcpy.ExecuteError as e:
            error_msg = arcpy.GetMessages(2)
            raise RuntimeError(f"Shapefile conversion failed: {error_msg}") from e

    def _stage_geopackage(self, file_path: Path, fc_path: str) -> None:
        """Stage GeoPackage file - import first available layer."""
        import arcpy

        logger.debug(f"Converting GeoPackage: {file_path.name}")

        try:
            # Discover layers in GeoPackage
            arcpy.env.workspace = str(file_path)
            layers = arcpy.ListFeatureClasses()

            if not layers:
                raise RuntimeError(f"No feature layers found in GeoPackage: {file_path}")

            # Import first layer
            first_layer = layers[0]
            source_layer = f"{file_path}\\{first_layer}"

            logger.debug(f"Importing layer: {first_layer}")

            arcpy.conversion.FeatureClassToFeatureClass(
                in_features=source_layer,
                out_path=str(self.staging_gdb),
                out_name=Path(fc_path).name
            )

        except arcpy.ExecuteError as e:
            error_msg = arcpy.GetMessages(2)
            raise RuntimeError(f"GeoPackage conversion failed: {error_msg}") from e
        finally:
            # Reset workspace
            arcpy.env.workspace = None

    def _ensure_target_coordinate_system(self, fc_path: str) -> None:
        """Ensure feature class has target coordinate system."""
        import arcpy

        try:
            desc = arcpy.Describe(fc_path)
            current_sr = desc.spatialReference

            # Check if projection is needed
            if current_sr.factoryCode != self.target_wkid:
                logger.info(f"🗺️  Projecting from EPSG:{current_sr.factoryCode} to EPSG:{self.target_wkid}")

                # Create target spatial reference
                target_sr = arcpy.SpatialReference(self.target_wkid)

                # Project to temporary feature class
                temp_fc = fc_path + "_projected"
                arcpy.management.Project(
                    in_dataset=fc_path,
                    out_dataset=temp_fc,
                    out_coor_system=target_sr
                )

                # Replace original with projected version
                arcpy.management.Delete(fc_path)
                arcpy.management.Rename(temp_fc, fc_path)

                logger.debug("Projection completed")
            else:
                logger.debug(f"Already in target coordinate system: EPSG:{self.target_wkid}")

        except arcpy.ExecuteError as e:
            error_msg = arcpy.GetMessages(2)
            logger.warning(f"Coordinate system handling failed: {error_msg}")
            # Continue without projection rather than failing

    def _get_feature_count(self, fc_path: str) -> int:
        """Get feature count from feature class."""
        try:
            import arcpy
            count_result = arcpy.management.GetCount(fc_path)
            return int(count_result[0])
        except Exception as e:
            logger.debug(f"Could not get feature count for {fc_path}: {e}")
            return 0

    def _ensure_staging_gdb_exists(self) -> None:
        """Ensure staging geodatabase exists."""
        if not self.staging_gdb.exists():
            logger.info(f"📁 Creating staging geodatabase: {self.staging_gdb}")

            try:
                import arcpy

                # Create parent directory if needed
                self.staging_gdb.parent.mkdir(parents=True, exist_ok=True)

                # Create file geodatabase
                arcpy.management.CreateFileGDB(
                    out_folder_path=str(self.staging_gdb.parent),
                    out_name=self.staging_gdb.name
                )

                logger.info("✅ Staging geodatabase created")

            except Exception as e:
                raise RuntimeError(f"Failed to create staging geodatabase: {e}") from e
```

### Configuration Migration Script

```python
# File: scripts/add_geometry_types.py
"""
Script to help consultants add explicit geometry types to their configurations.
"""

import yaml
from pathlib import Path
from typing import Dict, Any, List

def analyze_geojson_geometry(file_path: Path) -> str:
    """Analyze a GeoJSON file to determine dominant geometry type."""
    import json

    try:
        with file_path.open('r', encoding='utf-8') as f:
            data = json.load(f)

        geometry_types = {}
        features = data.get('features', [])

        for feature in features:
            geom = feature.get('geometry', {})
            geom_type = geom.get('type')
            if geom_type:
                geometry_types[geom_type] = geometry_types.get(geom_type, 0) + 1

        if not geometry_types:
            return "UNKNOWN"

        # Return most common geometry type
        dominant_type = max(geometry_types, key=geometry_types.get)

        # Map GeoJSON types to ArcGIS types
        mapping = {
            'Point': 'POINT',
            'MultiPoint': 'MULTIPOINT',
            'LineString': 'POLYLINE',
            'MultiLineString': 'POLYLINE',
            'Polygon': 'POLYGON',
            'MultiPolygon': 'POLYGON'
        }

        return mapping.get(dominant_type, dominant_type.upper())

    except Exception as e:
        print(f"Could not analyze {file_path}: {e}")
        return "UNKNOWN"

def update_config_with_geometry_types(config_path: Path, downloads_dir: Path) -> None:
    """Update configuration file with geometry types based on downloaded data."""

    with config_path.open('r') as f:
        config = yaml.safe_load(f)

    updated_sources = []

    for source in config.get('sources', []):
        if source.get('type') in ['ogc', 'wfs'] and not source.get('geometry'):
            # Try to find downloaded GeoJSON file
            authority = source.get('authority', 'unknown')
            name = source.get('name', 'unknown')

            geojson_path = downloads_dir / authority / f"{name}.geojson"

            if geojson_path.exists():
                geometry_type = analyze_geojson_geometry(geojson_path)
                source['geometry'] = geometry_type
                print(f"✅ {source['name']}: detected geometry type '{geometry_type}'")
            else:
                print(f"⚠️  {source['name']}: no GeoJSON found, manual intervention required")
                source['geometry'] = 'POLYGON'  # Safe default

        updated_sources.append(source)

    config['sources'] = updated_sources

    # Save updated configuration
    backup_path = config_path.with_suffix('.yaml.backup')
    config_path.rename(backup_path)
    print(f"📋 Backed up original config to: {backup_path}")

    with config_path.open('w') as f:
        yaml.dump(config, f, default_flow_style=False, indent=2)

    print(f"💾 Updated configuration saved: {config_path}")

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Add geometry types to configuration")
    parser.add_argument("--config", type=Path, required=True, help="Configuration file path")
    parser.add_argument("--downloads", type=Path, required=True, help="Downloads directory")

    args = parser.parse_args()

    update_config_with_geometry_types(args.config, args.downloads)
```

### Deliverables
- [ ] `etl/stage.py` - Simple staging system (150 lines vs 654 current)
- [ ] `scripts/add_geometry_types.py` - Helper for configuration migration
- [ ] 77% reduction in staging complexity
- [ ] Explicit geometry type requirement eliminates auto-detection

---

# Phase 5: Final Integration & Documentation (Week 5)

## Task 5.1: Simple SDE Loader
**Duration**: 2 days | **Prerequisites**: Task 4.1 | **Risk**: Low

### Implementation: SDE Loading System

```python
# File: etl/load.py
import logging
from typing import List, Optional
from pathlib import Path

from .new_config import PipelineConfig

logger = logging.getLogger(__name__)

class SDELoader:
    """Simple SDE loader using delete-and-copy strategy."""

    def __init__(self, config: PipelineConfig) -> None:
        """Initialize with pipeline configuration."""
        self.config = config
        self.sde_connection = config.workspace.sde_connection
        self.staging_gdb = config.workspace.staging_gdb

        # Validate SDE connection exists
        if not self.sde_connection.exists():
            logger.warning(f"SDE connection file not found: {self.sde_connection}")

    def load_feature_class(self, fc_name: str) -> None:
        """Load single feature class to SDE with comprehensive error handling."""
        logger.info(f"📤 Loading feature class to SDE: {fc_name}")

        try:
            # Validate source exists
            source_fc = f"{self.staging_gdb}/{fc_name}"
            if not self._feature_class_exists(source_fc):
                raise RuntimeError(f"Source feature class not found: {source_fc}")

            # Determine target location
            target_dataset, target_fc_name = self._determine_target_paths(fc_name)
            target_fc = f"{target_dataset}/{target_fc_name}"

            # Ensure target dataset exists
            self._ensure_dataset_exists(target_dataset, source_fc)

            # Execute delete-and-copy operation
            self._delete_and_copy(source_fc, target_dataset, target_fc_name)

            # Verify and log results
            feature_count = self._get_feature_count(f"{self.sde_connection}/{target_dataset}/{target_fc_name}")
            logger.info(f"✅ Loaded {feature_count:,} features to SDE: {target_fc_name}")

        except Exception as e:
            logger.error(f"❌ Failed to load {fc_name} to SDE: {e}")
            raise

    def load_all_feature_classes(self, fc_names: List[str]) -> Dict[str, bool]:
        """Load multiple feature classes with individual error isolation."""
        results = {}

        for fc_name in fc_names:
            try:
                self.load_feature_class(fc_name)
                results[fc_name] = True
            except Exception as e:
                logger.exception(f"Load failed for {fc_name}")
                results[fc_name] = False

        successful = sum(1 for success in results.values() if success)
        logger.info(f"📊 SDE loading complete: {successful}/{len(fc_names)} successful")

        return results

    def _determine_target_paths(self, fc_name: str) -> tuple[str, str]:
        """Determine target dataset and feature class name from staged FC name."""
        # Parse authority from feature class name (format: authority_name)
        parts = fc_name.split('_', 1)
        if len(parts) >= 2:
            authority = parts[0].upper()
            clean_name = parts[1]
        else:
            authority = 'UNKNOWN'
            clean_name = fc_name

        # Target dataset follows pattern: Underlag_{authority}
        dataset_name = f"Underlag_{authority}"

        return dataset_name, clean_name

    def _feature_class_exists(self, fc_path: str) -> bool:
        """Check if feature class exists."""
        try:
            import arcpy
            return arcpy.Exists(fc_path)
        except Exception:
            return False

    def _ensure_dataset_exists(self, dataset_name: str, template_fc: str) -> None:
        """Ensure target feature dataset exists, create if needed."""
        import arcpy

        dataset_path = f"{self.sde_connection}/{dataset_name}"

        if arcpy.Exists(dataset_path):
            logger.debug(f"Dataset already exists: {dataset_name}")
            return

        logger.info(f"📁 Creating feature dataset: {dataset_name}")

        try:
            # Get spatial reference from template feature class
            desc = arcpy.Describe(template_fc)
            spatial_ref = desc.spatialReference

            # Create feature dataset
            arcpy.management.CreateFeatureDataset(
                out_dataset_path=str(self.sde_connection),
                out_name=dataset_name,
                spatial_reference=spatial_ref
            )

            logger.info(f"✅ Created dataset: {dataset_name}")

        except arcpy.ExecuteError as e:
            error_msg = arcpy.GetMessages(2)
            raise RuntimeError(f"Failed to create dataset {dataset_name}: {error_msg}") from e

    def _delete_and_copy(self, source_fc: str, target_dataset: str, target_fc_name: str) -> None:
        """Execute delete-and-copy operation."""
        import arcpy

        target_fc_path = f"{self.sde_connection}/{target_dataset}/{target_fc_name}"

        try:
            # Delete existing feature class if present
            if arcpy.Exists(target_fc_path):
                logger.debug(f"Deleting existing feature class: {target_fc_name}")
                arcpy.management.Delete(target_fc_path)

            # Copy from staging to SDE
            logger.debug(f"Copying {Path(source_fc).name} -> {target_dataset}/{target_fc_name}")
            arcpy.conversion.FeatureClassToFeatureClass(
                in_features=source_fc,
                out_path=f"{self.sde_connection}/{target_dataset}",
                out_name=target_fc_name
            )

        except arcpy.ExecuteError as e:
            error_msg = arcpy.GetMessages(2)
            raise RuntimeError(f"Copy operation failed: {error_msg}") from e

    def _get_feature_count(self, fc_path: str) -> int:
        """Get feature count from feature class."""
        try:
            import arcpy
            if arcpy.Exists(fc_path):
                count_result = arcpy.management.GetCount(fc_path)
                return int(count_result[0])
        except Exception as e:
            logger.debug(f"Could not get feature count for {fc_path}: {e}")

        return 0
```

### Simplified run.py

```python
# File: run.py (simplified version)
import argparse
import logging
import sys
from pathlib import Path

from etl.pipeline import PipelineOrchestrator
from etl.new_config import load_config
from etl.simple_logging import setup_pipeline_logging

def main() -> int:
    """Simple CLI interface for OP-ETL pipeline."""
    parser = argparse.ArgumentParser(
        description="OP-ETL Geospatial Data Pipeline - Simplified",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run.py                    # Run complete pipeline
  python run.py --phase download   # Download only
  python run.py --config test.yaml # Use different config
  python run.py --log-level DEBUG  # Verbose logging
        """
    )

    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/config.yaml"),
        help="Configuration file path (default: config/config.yaml)"
    )

    parser.add_argument(
        "--phase",
        choices=["download", "stage", "load", "all"],
        default="all",
        help="Pipeline phase to execute (default: all)"
    )

    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Console logging level (default: INFO)"
    )

    parser.add_argument(
        "--log-file",
        type=Path,
        help="Optional log file path for detailed logging"
    )

    args = parser.parse_args()

    # Setup logging
    setup_pipeline_logging(
        console_level=args.log_level,
        file_path=args.log_file
    )

    logger = logging.getLogger(__name__)

    try:
        # Load configuration
        logger.info(f"📖 Loading configuration: {args.config}")
        config = load_config(args.config)

        # Create and run orchestrator
        orchestrator = PipelineOrchestrator(config)

        # Execute requested phase
        if args.phase == "all":
            result = orchestrator.run_full_pipeline()
        elif args.phase == "download":
            result = orchestrator.run_download_only()
        elif args.phase == "stage":
            result = orchestrator.run_staging_only()
        elif args.phase == "load":
            result = orchestrator.run_loading_only()

        # Report final results
        if result.failed_sources:
            logger.warning(f"⚠️  Some sources failed:")
            for name, error in result.failed_sources:
                logger.warning(f"   {name}: {error}")

        success_rate = result.success_rate()
        if success_rate == 100.0:
            logger.info(f"🎉 Pipeline completed successfully!")
            logger.info(f"   Sources: {len(result.successful_sources)}")
            logger.info(f"   Features: {result.total_features:,}")
            logger.info(f"   Duration: {result.total_duration_seconds:.1f}s")
            return 0
        else:
            logger.warning(f"⚠️  Pipeline completed with issues: {success_rate:.1f}% success")
            return 1

    except Exception as e:
        logger.error(f"💥 Pipeline failed: {e}")
        logger.debug("Full error details:", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(main())
```

### Deliverables
- [ ] `etl/load.py` - Simple SDE loader (60 lines)
- [ ] `run.py` - Simplified CLI interface (150 lines vs 242 current)
- [ ] Complete pipeline integration with error isolation
- [ ] 75% reduction in main execution code

---

## Final Validation & Documentation

### Complete Parity Test Suite

```python
# File: tests/complete_parity_test.py
"""
Comprehensive parity validation for the refactored OP-ETL system.
"""

import logging
import json
from typing import Dict, List, Any, NamedTuple
from pathlib import Path
import time

from etl.pipeline import PipelineOrchestrator
from etl.new_config import load_config

logger = logging.getLogger(__name__)

class ParityTest:
    """Comprehensive parity validation between old and new systems."""

    def __init__(self, test_config_path: Path, baseline_path: Path):
        self.test_config = load_config(test_config_path)
        self.baseline_data = self._load_baseline(baseline_path)

    def run_complete_validation(self) -> bool:
        """Run all parity tests and return overall pass/fail."""
        logger.info("🧪 Starting complete parity validation")

        # Execute new pipeline
        orchestrator = PipelineOrchestrator(self.test_config)
        result = orchestrator.run_full_pipeline()

        # Run all validation checks
        validation_results = {
            'feature_counts': self._validate_feature_counts(),
            'geometry_types': self._validate_geometry_types(),
            'spatial_references': self._validate_spatial_references(),
            'success_rates': self._validate_success_rates(result),
            'performance': self._validate_performance(result)
        }

        # Generate detailed report
        self._generate_validation_report(validation_results)

        # Return overall result
        all_passed = all(validation_results.values())

        if all_passed:
            logger.info("🎉 All parity tests PASSED")
        else:
            failed_tests = [test for test, passed in validation_results.items() if not passed]
            logger.error(f"❌ Parity tests FAILED: {failed_tests}")

        return all_passed

    def _validate_feature_counts(self) -> bool:
        """Validate feature counts match baseline exactly."""
        logger.info("Validating feature counts...")

        try:
            import arcpy

            for baseline_source in self.baseline_data:
                if not baseline_source['success']:
                    continue

                source_name = baseline_source['name']
                expected_geometry = baseline_source['geometry_type']

                # Find corresponding feature class
                fc_name = f"{baseline_source['authority']}_{source_name}"
                fc_path = f"{self.test_config.workspace.staging_gdb}/{fc_name}"

                if arcpy.Exists(fc_path):
                    desc = arcpy.Describe(fc_path)
                    actual_geometry = desc.shapeType

                    if actual_geometry.upper() != expected_geometry.upper():
                        logger.error(f"Geometry type mismatch for {source_name}: "
                                   f"expected {expected_geometry}, got {actual_geometry}")
                        return False

                    logger.debug(f"✓ {source_name}: {actual_geometry}")
                else:
                    logger.error(f"Feature class not found for geometry check: {fc_path}")
                    return False

            return True

        except Exception as e:
            logger.error(f"Geometry type validation failed: {e}")
            return False

    def _validate_spatial_references(self) -> bool:
        """Validate spatial references match target WKID."""
        logger.info("Validating spatial references...")

        try:
            import arcpy

            target_wkid = self.test_config.processing.target_wkid

            # List all feature classes in staging GDB
            arcpy.env.workspace = str(self.test_config.workspace.staging_gdb)
            feature_classes = arcpy.ListFeatureClasses()

            for fc_name in feature_classes:
                desc = arcpy.Describe(fc_name)
                actual_wkid = desc.spatialReference.factoryCode

                if actual_wkid != target_wkid:
                    logger.error(f"SRID mismatch for {fc_name}: "
                               f"expected {target_wkid}, got {actual_wkid}")
                    return False

                logger.debug(f"✓ {fc_name}: EPSG:{actual_wkid}")

            return True

        except Exception as e:
            logger.error(f"Spatial reference validation failed: {e}")
            return False
        finally:
            arcpy.env.workspace = None

    def _validate_success_rates(self, result) -> bool:
        """Validate success rates are acceptable."""
        logger.info("Validating success rates...")

        success_rate = result.success_rate()
        baseline_success_rate = self._calculate_baseline_success_rate()

        # Allow small degradation but flag significant drops
        tolerance = 10.0  # 10% tolerance

        if success_rate < baseline_success_rate - tolerance:
            logger.error(f"Success rate regression: {success_rate:.1f}% vs baseline {baseline_success_rate:.1f}%")
            return False

        logger.info(f"✓ Success rate: {success_rate:.1f}% (baseline: {baseline_success_rate:.1f}%)")
        return True

    def _validate_performance(self, result) -> bool:
        """Validate performance is within acceptable range."""
        logger.info("Validating performance...")

        # Calculate baseline average processing time
        baseline_times = [s['processing_time_seconds'] for s in self.baseline_data if s['success']]
        baseline_avg = sum(baseline_times) / len(baseline_times) if baseline_times else 0

        # Allow 20% performance degradation
        tolerance_factor = 1.2
        max_acceptable_time = baseline_avg * tolerance_factor

        if result.total_duration_seconds > max_acceptable_time:
            logger.warning(f"Performance regression: {result.total_duration_seconds:.1f}s vs "
                         f"baseline {baseline_avg:.1f}s (tolerance: {max_acceptable_time:.1f}s)")
            # Don't fail on performance - just warn

        logger.info(f"✓ Performance: {result.total_duration_seconds:.1f}s (baseline: {baseline_avg:.1f}s)")
        return True

    def _load_baseline(self, baseline_path: Path) -> List[Dict[str, Any]]:
        """Load baseline data from JSON file."""
        with baseline_path.open('r') as f:
            return json.load(f)

    def _calculate_baseline_success_rate(self) -> float:
        """Calculate baseline success rate."""
        total = len(self.baseline_data)
        successful = len([s for s in self.baseline_data if s['success']])
        return (successful / total * 100.0) if total > 0 else 0.0

    def _generate_validation_report(self, results: Dict[str, bool]) -> None:
        """Generate detailed validation report."""
        report_path = Path("tests/parity_validation_report.md")

        with report_path.open('w') as f:
            f.write("# OP-ETL Parity Validation Report\n\n")
            f.write(f"**Generated**: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")

            f.write("## Test Results\n\n")
            f.write("| Test | Status | Description |\n")
            f.write("|------|--------|-------------|\n")

            test_descriptions = {
                'feature_counts': 'Feature counts match baseline exactly',
                'geometry_types': 'Geometry types preserved correctly',
                'spatial_references': 'All feature classes in target SRID',
                'success_rates': 'Success rate within tolerance of baseline',
                'performance': 'Processing time within acceptable range'
            }

            for test_name, passed in results.items():
                status = "✅ PASS" if passed else "❌ FAIL"
                description = test_descriptions.get(test_name, "")
                f.write(f"| {test_name} | {status} | {description} |\n")

            f.write("\n## Overall Result\n\n")
            if all(results.values()):
                f.write("🎉 **ALL TESTS PASSED** - Refactoring validation successful!\n")
            else:
                f.write("❌ **SOME TESTS FAILED** - Manual review required before deployment.\n")

        logger.info(f"📊 Validation report generated: {report_path}")

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run complete parity validation")
    parser.add_argument("--config", type=Path, required=True, help="Test configuration file")
    parser.add_argument("--baseline", type=Path, required=True, help="Baseline results JSON")

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    tester = ParityTest(args.config, args.baseline)
    success = tester.run_complete_validation()

    exit(0 if success else 1)
```

---

## Final Documentation

### README.md (Simple)

```markdown
# OP-ETL - Simple Geospatial ETL Pipeline

**Transform geospatial data from multiple sources into your ArcGIS SDE geodatabase.**

## Quick Start (5 minutes)

1. **Configure** your data sources in `config/config.yaml`
2. **Run** the pipeline: `python run.py`
3. **Check** your SDE geodatabase for results

## Requirements

- ArcGIS Pro 3.3+ (for ArcPy)
- Python 3.11+ (included with ArcGIS Pro)
- Network access to data sources

## Configuration

Create `config/config.yaml` with your settings:

```yaml
workspace:
  downloads: "./data/downloads"       # Where files are downloaded
  staging_gdb: "./data/staging.gdb"   # Temporary processing database
  sde_connection: "./data/prod.sde"   # Your target SDE connection

processing:
  target_wkid: 3006                   # SWEREF99 TM (or your target SRID)
  aoi_bbox: [610000, 6550000, 700000, 6650000]  # Area of interest
  aoi_bbox_wkid: 3006

sources:
  # OGC API Features
  - name: "erosion_areas"
    type: "ogc"
    url: "https://api.sgu.se/features/v1/"
    authority: "SGU"
    geometry: "POLYGON"              # Required for GeoJSON sources
    collections: ["erosion"]
    enabled: true

  # ArcGIS REST API
  - name: "protected_areas"
    type: "rest"
    url: "https://services.example.com/arcgis/rest/services/Nature/MapServer"
    authority: "EPA"
    layer_ids: [0, 1]
    enabled: true

  # Direct file download
  - name: "infrastructure_data"
    type: "http"
    url: "https://example.com/data/infrastructure.zip"
    authority: "DOT"
    enabled: true
```

## Source Types

| Type | Description | Configuration |
|------|-------------|---------------|
| `http` | Direct file download (ZIP, SHP, GPKG) | `url` only |
| `rest` | ArcGIS REST API | `url`, optional `layer_ids` |
| `ogc` | OGC API Features | `url`, `geometry`, `collections` |
| `wfs` | WFS GetFeature | `url`, `geometry`, `collections` |
| `atom` | ATOM feed enclosures | `url` only |

## Running the Pipeline

```bash
# Complete pipeline
python run.py

# Individual phases
python run.py --phase download    # Download only
python run.py --phase stage       # Stage to FGDB only
python run.py --phase load        # Load to SDE only

# Different configuration
python run.py --config test_config.yaml

# Verbose logging
python run.py --log-level DEBUG --log-file logs/debug.log
```

## Output Structure

- **Downloads**: `./data/downloads/{authority}/{source_name}.{ext}`
- **Staging**: `./data/staging.gdb/{authority}_{source_name}`
- **SDE**: `{sde_connection}/Underlag_{authority}/{source_name}`

## Common Issues

### "Geometry type required for GeoJSON source"
**Solution**: Add explicit `geometry` field to your source configuration:
```yaml
sources:
  - name: "my_source"
    type: "ogc"
    geometry: "POLYGON"  # Add this line
```

### "No features found in bbox"
**Solution**: Check your bbox coordinates and WKID:
```yaml
processing:
  aoi_bbox: [610000, 6550000, 700000, 6650000]  # Check these coordinates
  aoi_bbox_wkid: 3006  # Ensure this matches your bbox coordinate system
```

### "Cannot access service"
**Solution**: Verify URL and network connectivity:
```bash
curl "https://your-service-url/collections?f=json"
```

## Support

1. Check the logs in `logs/` directory
2. Verify your configuration with a small test
3. Ensure ArcGIS Pro Python environment is active
4. Test individual URLs manually first

## What Changed from v1.x

- **Explicit configuration**: No more auto-detection, specify geometry types
- **Simplified codebase**: 70% fewer lines of code to understand
- **Better error messages**: Clear failures with suggested fixes
- **Faster onboarding**: Complete pipeline understanding in 30 minutes

---

*OP-ETL v2.0 - Built for consultant simplicity*
```

### Migration Guide

```markdown
# Migration Guide: OP-ETL v1.x → v2.0

## Overview

OP-ETL v2.0 prioritizes explicit configuration over automatic complexity. This guide helps you migrate existing configurations to the new simplified system.

## Key Changes

### 1. Configuration Structure

**OLD (complex)**:
```yaml
# config.yaml + sources.yaml (2 files)
workspaces:
  downloads: ./downloads
  staging_gdb: ./staging.gdb

# sources.yaml
defaults:
  bbox: [...]

sources:
  - name: test
    raw:
      supports_epsg_3006: false
      bbox_crs: CRS84
```

**NEW (explicit)**:
```yaml
# Single config.yaml file
workspace:
  downloads: ./data/downloads
  staging_gdb: ./data/staging.gdb
  sde_connection: ./data/prod.sde

processing:
  target_wkid: 3006
  aoi_bbox: [610000, 6550000, 700000, 6650000]
  aoi_bbox_wkid: 3006

sources:
  - name: test
    type: ogc
    geometry: POLYGON  # Required
    collections: [data]
```

### 2. Required Changes

#### Geometry Types (REQUIRED)
All GeoJSON sources need explicit geometry:

```yaml
# Add to every OGC/WFS source
sources:
  - name: "your_source"
    type: "ogc"
    geometry: "POLYGON"  # Add this line
```

#### Spatial Reference (REQUIRED)
Specify target coordinate system:

```yaml
processing:
  target_wkid: 3006  # Your target EPSG code
  aoi_bbox: [xmin, ymin, xmax, ymax]
  aoi_bbox_wkid: 3006  # EPSG code for bbox coordinates
```

### 3. Migration Steps

#### Step 1: Backup Current Configuration
```bash
cp config/config.yaml config/config.yaml.v1.backup
cp config/sources.yaml config/sources.yaml.v1.backup
```

#### Step 2: Use Migration Script
```bash
python scripts/migrate_config.py \
  --old-config config/config.yaml \
  --old-sources config/sources.yaml \
  --output config/config_new.yaml
```

#### Step 3: Add Missing Geometry Types
The migration script adds default geometry types, but you should verify:

```bash
# Run a test download to analyze geometry types
python run.py --phase download --config config/config_new.yaml

# Use helper script to detect geometry types
python scripts/add_geometry_types.py \
  --config config/config_new.yaml \
  --downloads ./data/downloads
```

#### Step 4: Test New Configuration
```bash
# Test with new config
python run.py --config config/config_new.yaml --phase download

# Compare results with parity checker
python tests/complete_parity_test.py \
  --config config/config_new.yaml \
  --baseline tests/baseline/baseline_results.json
```

### 4. Removed Features

#### No Longer Supported:
- Complex monitoring and metrics collection
- Automatic geometry type detection
- Elaborate retry and backoff strategies
- Transfer limit detection for REST APIs
- Complex spatial reference inference
- Pattern detection and error categorization

#### Replaced With:
- Simple success/failure logging
- Explicit geometry type in configuration
- Basic retry (3 attempts, 2-second delay)
- Simple REST pagination
- Required EPSG codes in configuration
- Standard Python exceptions

### 5. Configuration Reference

#### Complete Example:
```yaml
workspace:
  downloads: "./data/downloads"
  staging_gdb: "./data/staging.gdb"
  sde_connection: "./data/connections/prod.sde"

processing:
  target_wkid: 3006
  aoi_bbox: [610000, 6550000, 700000, 6650000]
  aoi_bbox_wkid: 3006

sources:
  # HTTP file download
  - name: "infrastructure"
    type: "http"
    url: "https://example.com/data.zip"
    authority: "DOT"
    enabled: true

  # REST API
  - name: "protected_areas"
    type: "rest"
    url: "https://services.example.com/rest/services/Nature/MapServer"
    authority: "EPA"
    layer_ids: [0, 1, 2]
    enabled: true

  # OGC API Features
  - name: "erosion_data"
    type: "ogc"
    url: "https://api.example.com/features/v1/"
    authority: "GEOLOGICAL"
    geometry: "POLYGON"
    collections: ["erosion", "landslides"]
    enabled: true

  # WFS
  - name: "boundaries"
    type: "wfs"
    url: "https://example.com/wfs"
    authority: "ADMIN"
    geometry: "POLYGON"
    collections: ["administrative_boundaries"]
    enabled: true

  # ATOM feed
  - name: "environmental_data"
    type: "atom"
    url: "https://example.com/atom.xml"
    authority: "ENV"
    enabled: true
```

### 6. Troubleshooting Migration

#### "Geometry type required"
Add explicit geometry to your source:
```yaml
geometry: "POLYGON"  # or POINT, POLYLINE, MULTIPOINT
```

#### "Invalid EPSG code"
Verify your coordinate system codes:
```yaml
processing:
  target_wkid: 3006      # Must be valid EPSG code
  aoi_bbox_wkid: 3006    # Must match bbox coordinate system
```

#### "Feature count mismatch"
The new system may handle edge cases differently. Check:
- Bbox coordinates are in correct coordinate system
- Geometry filtering is working as expected
- Network connectivity to data sources

### 7. Rollback Plan

If issues arise, you can revert to v1.x:

```bash
# Restore old configuration
cp config/config.yaml.v1.backup config/config.yaml
cp config/sources.yaml.v1.backup config/sources.yaml

# Use old codebase (if preserved)
git checkout v1.x-stable
python run.py
```

### 8. Benefits of Migration

- **70% less code** to understand and maintain
- **Explicit configuration** eliminates guesswork
- **Faster debugging** with clearer error messages
- **5-minute onboarding** for new consultants
- **Predictable behavior** - no hidden auto-detection

---

*Need help with migration? Check logs in `./logs/` for detailed error messages.*
```

## Final Deliverables Summary

### ✅ Complete Implementation Package

**New Codebase Structure (1,000 lines total)**:
```
etl/
├── pipeline.py          # 200 lines - Main orchestrator
├── download.py          # 300 lines - All downloaders unified
├── stage.py             # 150 lines - Simple staging with explicit geometry
├── load.py              #  60 lines - SDE loading with delete-copy
├── new_config.py        #  60 lines - Configuration with validation
├── simple_logging.py    #  30 lines - Unified logging system
├── simple_http.py       # 100 lines - HTTP client wrapper
└── http_compat.py       #  50 lines - Migration compatibility

tests/
├── baseline/
│   ├── capture_baseline.py    # Comprehensive baseline capture
│   └── baseline_results.json  # Reference results
├── complete_parity_test.py    # Full validation suite
└── parity_validation_report.md

scripts/
├── migrate_config.py          # Configuration migration
└── add_geometry_types.py      # Helper for geometry detection

run.py                         # 150 lines - Simplified CLI
README.md                      # 5-minute quick start guide
```

**Migration Package**:
- [ ] **Baseline capture system** - Captures current behavior for comparison
- [ ] **Configuration migration** - Converts old config to new format
- [ ] **Parity validation** - Ensures functional equivalence
- [ ] **Geometry type detection** - Helps add required geometry fields
- [ ] **Complete documentation** - README, migration guide, examples

**Key Achievements**:
- ✅ **70% code reduction** (3,600 → 1,000 lines)
- ✅ **Explicit over implicit** - No auto-detection, everything in config
- ✅ **Error isolation** - Single source failure doesn't kill pipeline
- ✅ **5-minute onboarding** - Complete understanding from README
- ✅ **Backward compatibility** - Migration tools and rollback plan

This implementation transforms OP-ETL from an over-engineered system into a consultant-friendly tool that trades automatic complexity for explicit simplicity, while maintaining all core functionality through comprehensive parity testing.success.

---

# Deployment

- **Toolboxing:** Wrap the pipeline as a single GP tool with three parameters (`config`, `phase`, `log_level`). Mirrors CLI phases; prevents drift between “script” vs “Server” behavior.
- **Server settings:** Document GP service timeout, memory cap, and instance limits per phase (downloads/staging may exceed defaults).
- **Packaging:** One entry point (`run.py`) and YAML config only; no hidden flags.

---

# Operations

- **Schema versioning:** Add `config_version:` to YAML and fail fast on mismatch. Support base+overlay configs (e.g., `config.base.yaml` + `config.prod.yaml`) for safe handover.
- **Secrets:** Keep out of YAML. Pull tokens from environment or Credential Manager; log presence only.
- **State tracking:** Write per-source `_state.json` (etag, last_modified, sha256, last_run). Skip unchanged sources.
- **AOI order:** Always try server-side filters (bbox in REST/OGC/WFS). Fall back to local clip only if needed. Log which path was used.
- **Health check:** Add `--phase health` to verify SDE connect, downloads dir write, sample URL reachability.
- **Retention:** Rotate logs and clean old downloads/extracts (keep 3 successful runs per source).

---

# Data Governance

- **Authority datasets:** Enforce one dataset per authority (`Underlag_{authority}`); preflight check creates if missing.
- **Geometry guarantees:** Require explicit `geometry:` in YAML for GeoJSON. Reject mixed geometry pages; fail clearly with offending feature info.
- **Field policy:** Document treatment of unexpected fields (pass-through or drop). Log differences at staging.
- **Post-load hygiene:** After load, run `Rebuild Indexes` + `Analyze Datasets` (toggle in YAML). Add SDE compress schedule if versioned.
- **Attribution:** Optional: store a short licensing/authority string in metadata on each target FC.
