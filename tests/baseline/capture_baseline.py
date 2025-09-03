#!/usr/bin/env python3
"""
Baseline Capture System for OP-ETL Refactoring

This script captures comprehensive baseline metrics from the current pipeline
to validate the refactored system maintains functional parity.

Usage:
    python tests/baseline/capture_baseline.py --config config/config.yaml --sources config/sources.yaml
"""

import argparse
import json
import logging
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

logger = logging.getLogger(__name__)

@dataclass
class SourceBaseline:
    """Comprehensive baseline metrics for a single source."""
    name: str = ""
    source_type: str = ""
    authority: str = ""
    success: bool = False
    feature_count: int = 0
    geometry_type: str = ""
    srid: int = 0
    file_size_bytes: int = 0
    processing_time_seconds: float = 0.0
    download_path: Optional[str] = None
    staging_fc_name: Optional[str] = None
    error_message: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)

class BaselineCapture:
    """Captures comprehensive baseline metrics from current pipeline execution."""

    def __init__(self, config_path: Path, sources_path: Path, output_dir: Path) -> None:
        self.config_path = config_path
        self.sources_path = sources_path
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def execute_baseline_capture(self, max_sources: int = 5) -> List[SourceBaseline]:
        """Execute current pipeline and capture all metrics."""
        logger.info("Starting baseline capture")

        # Load current configuration
        from etl.config import load_config
        config = load_config(str(self.config_path), str(self.sources_path))

        baselines: List[SourceBaseline] = []

        # Select first N enabled sources for testing
        enabled_sources = [s for s in config['sources'] if s.get('enabled', True)]
        test_sources = enabled_sources[:max_sources]

        logger.info(f"Testing {len(test_sources)} sources: {[s['name'] for s in test_sources]}")

        for source in test_sources:
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
            # Clean start - ensure staging GDB is fresh
            self._prepare_clean_staging(config)

            # Execute download and staging for single source
            self._execute_single_source_pipeline(source, config)

            # Extract metrics from staged feature class
            staging_fc = self._find_staged_feature_class(source, config)
            metrics = self._extract_fc_metrics(staging_fc, config) if staging_fc else {}

            # Find download file
            download_path = self._find_download_file(source, config)

            processing_time = time.time() - start_time

            return SourceBaseline(
                name=source['name'],
                source_type=source['type'],
                authority=source.get('authority', 'unknown'),
                success=staging_fc is not None,
                feature_count=metrics.get('feature_count', 0),
                geometry_type=metrics.get('geometry_type', 'UNKNOWN'),
                srid=metrics.get('srid', 0),
                file_size_bytes=download_path.stat().st_size if download_path and download_path.exists() else 0,
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
                authority=source.get('authority', 'unknown'),
                success=False,
                feature_count=0,
                geometry_type="UNKNOWN",
                srid=0,
                file_size_bytes=0,
                processing_time_seconds=processing_time,
                error_message=str(e)
            )

    def _prepare_clean_staging(self, config: Dict[str, Any]) -> None:
        """Ensure staging GDB is clean for testing."""
        try:
            import arcpy
            staging_gdb = config['workspaces']['staging_gdb']

            # List and delete all feature classes
            if Path(staging_gdb).exists():
                # Use arcpy.da.Walk to avoid relying on env.workspace
                to_delete = []
                for dirpath, dirnames, filenames in arcpy.da.Walk(staging_gdb, datatype="FeatureClass"):
                    for fc in filenames:
                        to_delete.append(f"{staging_gdb}/{fc}")

                deleted = 0
                for fc_path in to_delete:
                    try:
                        if arcpy.Exists(fc_path):
                            arcpy.management.Delete(fc_path)
                            deleted += 1
                    except arcpy.ExecuteError:
                        logger.debug(f"ArcPy ExecuteError deleting {fc_path}: {arcpy.GetMessages(2)}")
                    except Exception as e:
                        logger.debug(f"Could not delete {fc_path}: {e}")

                logger.info(f"[BASELINE] Cleared {deleted} feature classes from staging")
        except Exception as e:
            logger.debug(f"Staging cleanup warning: {e}")

    def _prepare_clean_downloads(self, config: Dict[str, Any], source: Dict[str, Any]) -> None:
        """Remove all previous downloads to force a fresh download for this source."""
        try:
            downloads_dir = Path(config['workspaces']['downloads'])
            if downloads_dir.exists():
                import shutil
                shutil.rmtree(downloads_dir, ignore_errors=True)
                logger.info(f"[BASELINE] Removed previous downloads: {downloads_dir}")
        except Exception as e:
            logger.debug(f"Downloads cleanup warning: {e}")

    def _execute_single_source_pipeline(self, source: Dict[str, Any], config: Dict[str, Any]) -> None:
        """Execute download and staging for a single source."""
        # Create filtered config with only this source
        filtered_config = config.copy()
        filtered_config['sources'] = [source]

        # Ensure we force a fresh download for this source
        self._prepare_clean_downloads(filtered_config, source)

        # Registry mapping source types to downloader modules
        downloader_registry = {
            'rest': ('etl.download_rest', 'run'),
            'ogc': ('etl.download_ogc', 'run'),
            'http': ('etl.download_http', 'run'),
            'file': ('etl.download_http', 'run'),
            'wfs': ('etl.download_wfs', 'run'),
            'atom': ('etl.download_atom', 'run'),
        }

        source_type = source['type']
        if source_type not in downloader_registry:
            raise ValueError(f"Unknown source type: {source_type}")

        module_path, func_name = downloader_registry[source_type]
        import importlib
        downloader_module = importlib.import_module(module_path)
        run_func = getattr(downloader_module, func_name)
        run_func(filtered_config)
        # Run staging
        from etl.stage_files import stage_all_downloads
        stage_all_downloads(filtered_config)

    def _find_staged_feature_class(self, source: Dict[str, Any], config: Dict[str, Any]) -> Optional[str]:
        """Find the staged feature class for this source."""
        try:
            import arcpy
            staging_gdb = config['workspaces']['staging_gdb']

            if not Path(staging_gdb).exists():
                return None

            # List FCs using da.Walk for reliability without mutating env
            fcs: List[str] = []
            for _dirpath, _dirnames, filenames in arcpy.da.Walk(staging_gdb, datatype="FeatureClass"):
                fcs.extend(filenames)

            if not fcs:
                return None

            # Look for FC that matches source name pattern
            source_name = source['name']
            authority = source.get('authority', '')

            # Try different naming patterns
            candidates = [
                f"{authority}_{source_name}",
                source_name,
                f"{source_name}_{authority}",
            ]

            for fc in fcs:
                fc_lower = fc.lower()
                for candidate in candidates:
                    candidate_lower = candidate.lower()
                    if candidate_lower in fc_lower or fc_lower in candidate_lower:
                        logger.debug(f"Found staged FC: {fc} for source {source_name}")
                        return fc

            # If no pattern match, return first FC (might be the right one)
            if fcs:
                logger.debug(f"Using first FC: {fcs[0]} for source {source_name}")
                return fcs[0]

            return None

        except Exception as e:
            logger.debug(f"Error finding staged FC: {e}")
            return None

    def _extract_fc_metrics(self, fc_name: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """Extract comprehensive metrics from staged feature class."""
        try:
            import arcpy
            staging_gdb = config['workspaces']['staging_gdb']
            fc_path = f"{staging_gdb}/{fc_name}"

            # Get feature count
            count_result = arcpy.management.GetCount(fc_path)
            feature_count = int(str(count_result[0]))

            # Get geometry and spatial reference info
            desc = arcpy.Describe(fc_path)
            geometry_type = desc.shapeType
            srid = desc.spatialReference.factoryCode or 0

            logger.debug(f"FC {fc_name}: {feature_count} features, {geometry_type}, EPSG:{srid}")

            return {
                'feature_count': feature_count,
                'geometry_type': geometry_type,
                'srid': srid
            }

        except Exception as e:
            logger.warning(f"Failed to extract metrics from {fc_name}: {e}")
            return {'feature_count': 0, 'geometry_type': 'UNKNOWN', 'srid': 0}

    def _find_download_file(self, source: Dict[str, Any], config: Dict[str, Any]) -> Optional[Path]:
        """Find the downloaded file for this source."""
        try:
            downloads_dir = Path(config['workspaces']['downloads'])
            authority = source.get('authority', 'unknown')
            source_name = source['name']

            # Look in authority subdirectory
            authority_dir = downloads_dir / authority
            if authority_dir.exists():
                # Find files that might match this source
                patterns = [f"*{source_name}*", f"*{authority}*"]
                for pattern in patterns:
                    files = list(authority_dir.glob(pattern))
                    if files:
                        # Return most recent file
                        most_recent = max(files, key=lambda p: p.stat().st_mtime)
                        return most_recent

            return None

        except Exception as e:
            logger.debug(f"Error finding download file: {e}")
            return None

    def _save_baseline_results(self, baselines: List[SourceBaseline]) -> None:
        """Save baseline results to JSON file."""
        output_file = self.output_dir / "baseline_results.json"

        # Use UTF-8 to support non-ASCII characters in names/paths
        with output_file.open('w', encoding='utf-8') as f:
            json.dump([b.to_dict() for b in baselines], f, indent=2, default=str, ensure_ascii=False)
        logger.info(f"Baseline results saved to {output_file}")

    def _generate_baseline_report(self, baselines: List[SourceBaseline]) -> None:
        """Generate human-readable baseline report."""
        report_file = self.output_dir / "baseline_report.md"

        # Write Markdown with UTF-8 to allow emojis and locale-specific characters
        with report_file.open('w', encoding='utf-8') as f:
            f.write("# OP-ETL Baseline Report\n\n")
            f.write(f"**Generated**: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")

            # Summary table
            f.write("## Source Summary\n\n")
            f.write("| Source | Type | Success | Features | Geometry | SRID | Time (s) |\n")
            f.write("|--------|------|---------|----------|----------|------|----------|\n")

            for baseline in baselines:
                status = "✅" if baseline.success else "❌"
                f.write(f"| {baseline.name} | {baseline.source_type} | {status} | "
                       f"{baseline.feature_count:,} | {baseline.geometry_type} | "
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
                    if baseline.staging_fc_name:
                        f.write(f"- **Staged FC**: {baseline.staging_fc_name}\n")
                else:
                    f.write(f"- **Error**: {baseline.error_message}\n")

                f.write("\n")

            # Summary statistics
            f.write("## Summary Statistics\n\n")
            successful = [b for b in baselines if b.success]
            total_features = sum(b.feature_count for b in successful)
            avg_time = sum(b.processing_time_seconds for b in successful) / len(successful) if successful else 0
            success_rate = len(successful) / len(baselines) * 100 if baselines else 0

            f.write(f"- **Success Rate**: {success_rate:.1f}% ({len(successful)}/{len(baselines)})\n")
            f.write(f"- **Total Features**: {total_features:,}\n")
            f.write(f"- **Average Processing Time**: {avg_time:.2f}s\n")
        logger.info(f"Baseline report generated: {report_file}")

def main():
    """Main entry point for baseline capture."""
    parser = argparse.ArgumentParser(description="Capture OP-ETL baseline metrics")
    parser.add_argument("--config", type=Path, default=Path("config/config.yaml"),
                       help="Path to config.yaml")
    parser.add_argument("--sources", type=Path, default=Path("config/sources.yaml"),
                       help="Path to sources.yaml")
    parser.add_argument("--output", type=Path, default=Path("tests/baseline"),
                       help="Output directory for baseline results")
    parser.add_argument("--max-sources", type=int, default=5,
                       help="Maximum number of sources to test")
    parser.add_argument("--log-level", default="INFO",
                       choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                       help="Logging level")

    args = parser.parse_args()

    # Setup centralized logging
    from etl.logging import setup_pipeline_logging
    setup_pipeline_logging(console_level=args.log_level, file_path=Path("logs") / "baseline_tests.log")

    try:
        baseline_capture = BaselineCapture(
            config_path=args.config,
            sources_path=args.sources,
            output_dir=args.output
        )

        results = baseline_capture.execute_baseline_capture(args.max_sources)

        successful = len([r for r in results if r.success])
        total = len(results)

        print("\nBaseline capture complete!")
        print(f"   Sources tested: {total}")
        print(f"   Successful: {successful} ({successful/total*100:.1f}%)")
        print(f"   Results: {args.output}/baseline_results.json")
        print(f"   Report: {args.output}/baseline_report.md")

        return 0 if successful > 0 else 1

    except Exception as e:
        logger.error(f"Baseline capture failed: {e}")
        logger.debug("Full error details:", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(main())
