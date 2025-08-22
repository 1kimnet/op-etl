"""
Enhanced logging and monitoring utilities for OP-ETL pipeline.
Provides detailed tracking of success rates and error patterns.
"""

import logging
import time
import json
from typing import Dict, List, Optional, Any
from pathlib import Path
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta

log = logging.getLogger(__name__)


@dataclass
class SourceMetrics:
    """Metrics for a single data source."""
    name: str
    authority: str
    source_type: str
    start_time: float
    end_time: Optional[float] = None
    success: bool = False
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    features_downloaded: int = 0
    files_downloaded: int = 0
    response_time_ms: float = 0
    response_size_bytes: int = 0
    retry_count: int = 0
    
    @property
    def duration_seconds(self) -> float:
        """Calculate duration in seconds."""
        if self.end_time:
            return self.end_time - self.start_time
        return 0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        result = asdict(self)
        result['duration_seconds'] = self.duration_seconds
        result['start_time_iso'] = datetime.fromtimestamp(self.start_time).isoformat()
        if self.end_time:
            result['end_time_iso'] = datetime.fromtimestamp(self.end_time).isoformat()
        return result


class PipelineMonitor:
    """Monitor pipeline execution and collect metrics."""
    
    def __init__(self):
        self.metrics: List[SourceMetrics] = []
        self.pipeline_start_time = time.time()
        self.current_source: Optional[SourceMetrics] = None
        
    def start_source(self, name: str, authority: str, source_type: str) -> SourceMetrics:
        """Start monitoring a data source."""
        self.current_source = SourceMetrics(
            name=name,
            authority=authority,
            source_type=source_type,
            start_time=time.time()
        )
        
        log.info(f"[MONITOR] Starting {source_type.upper()} source: {name} ({authority})")
        return self.current_source
    
    def end_source(self, success: bool, error_type: Optional[str] = None, 
                   error_message: Optional[str] = None, features: int = 0, 
                   files: int = 0, response_size: int = 0, retries: int = 0) -> None:
        """End monitoring current data source."""
        if not self.current_source:
            log.warning("[MONITOR] No current source to end")
            return
        
        self.current_source.end_time = time.time()
        self.current_source.success = success
        self.current_source.error_type = error_type
        self.current_source.error_message = error_message
        self.current_source.features_downloaded = features
        self.current_source.files_downloaded = files
        self.current_source.response_size_bytes = response_size
        self.current_source.retry_count = retries
        self.current_source.response_time_ms = self.current_source.duration_seconds * 1000
        
        status = "SUCCESS" if success else "FAILED"
        duration = self.current_source.duration_seconds
        
        log.info(f"[MONITOR] Completed {self.current_source.source_type.upper()} source: "
                f"{self.current_source.name} - {status} ({duration:.2f}s)")
        
        if not success and error_type:
            log.warning(f"[MONITOR] Error details: {error_type} - {error_message}")
        
        self.metrics.append(self.current_source)
        self.current_source = None
    
    def get_summary(self) -> Dict[str, Any]:
        """Get pipeline execution summary."""
        total_duration = time.time() - self.pipeline_start_time
        
        # Group by source type
        by_type: Dict[str, Dict[str, Any]] = {}
        
        for metric in self.metrics:
            source_type = metric.source_type
            if source_type not in by_type:
                by_type[source_type] = {
                    'total': 0,
                    'successful': 0,
                    'failed': 0,
                    'total_features': 0,
                    'total_files': 0,
                    'avg_duration': 0,
                    'error_types': {}
                }
            
            stats = by_type[source_type]
            stats['total'] += 1
            
            if metric.success:
                stats['successful'] += 1
            else:
                stats['failed'] += 1
                if metric.error_type:
                    error_type = metric.error_type
                    stats['error_types'][error_type] = stats['error_types'].get(error_type, 0) + 1
            
            stats['total_features'] += metric.features_downloaded
            stats['total_files'] += metric.files_downloaded
            stats['avg_duration'] += metric.duration_seconds
        
        # Calculate averages and success rates
        for source_type, stats in by_type.items():
            if stats['total'] > 0:
                stats['success_rate'] = (stats['successful'] / stats['total']) * 100
                stats['avg_duration'] = stats['avg_duration'] / stats['total']
            else:
                stats['success_rate'] = 0
        
        # Overall summary
        total_sources = len(self.metrics)
        successful_sources = sum(1 for m in self.metrics if m.success)
        
        summary = {
            'pipeline_start_time': datetime.fromtimestamp(self.pipeline_start_time).isoformat(),
            'total_duration_seconds': total_duration,
            'total_sources': total_sources,
            'successful_sources': successful_sources,
            'failed_sources': total_sources - successful_sources,
            'overall_success_rate': (successful_sources / total_sources * 100) if total_sources > 0 else 0,
            'by_source_type': by_type,
            'individual_sources': [m.to_dict() for m in self.metrics]
        }
        
        return summary
    
    def log_summary(self) -> None:
        """Log a human-readable summary."""
        summary = self.get_summary()
        
        log.info("="*60)
        log.info("PIPELINE EXECUTION SUMMARY")
        log.info("="*60)
        log.info(f"Total Duration: {summary['total_duration_seconds']:.2f} seconds")
        log.info(f"Overall Success Rate: {summary['overall_success_rate']:.1f}% "
                f"({summary['successful_sources']}/{summary['total_sources']})")
        
        for source_type, stats in summary['by_source_type'].items():
            log.info(f"\n{source_type.upper()} sources: {stats['success_rate']:.1f}% success "
                    f"({stats['successful']}/{stats['total']})")
            
            if stats['failed'] > 0:
                log.info(f"  Failed with errors: {dict(stats['error_types'])}")
            
            if stats['total_features'] > 0:
                log.info(f"  Total features downloaded: {stats['total_features']:,}")
            
            if stats['total_files'] > 0:
                log.info(f"  Total files downloaded: {stats['total_files']}")
            
            log.info(f"  Average duration: {stats['avg_duration']:.2f}s")
    
    def save_metrics(self, output_path: Path) -> None:
        """Save metrics to JSON file."""
        summary = self.get_summary()
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
        
        log.info(f"[MONITOR] Metrics saved to {output_path}")
    
    def detect_patterns(self) -> Dict[str, Any]:
        """Detect error patterns and performance issues."""
        patterns = {
            'recursion_errors': [],
            'timeout_errors': [],
            'network_errors': [],
            'parsing_errors': [],
            'slow_sources': [],  # > 30 seconds
            'large_responses': []  # > 10MB
        }
        
        for metric in self.metrics:
            if not metric.success and metric.error_message:
                msg = metric.error_message.lower()
                
                if 'recursion' in msg:
                    patterns['recursion_errors'].append(metric.name)
                elif 'timeout' in msg or 'timed out' in msg:
                    patterns['timeout_errors'].append(metric.name)
                elif 'connection' in msg or 'network' in msg or 'resolve' in msg:
                    patterns['network_errors'].append(metric.name)
                elif 'parse' in msg or 'json' in msg or 'xml' in msg:
                    patterns['parsing_errors'].append(metric.name)
            
            if metric.duration_seconds > 30:
                patterns['slow_sources'].append({
                    'name': metric.name,
                    'duration': metric.duration_seconds
                })
            
            if metric.response_size_bytes > 10 * 1024 * 1024:  # 10MB
                patterns['large_responses'].append({
                    'name': metric.name,
                    'size_mb': metric.response_size_bytes / (1024 * 1024)
                })
        
        return patterns


# Global monitor instance
monitor = PipelineMonitor()


def start_monitoring_source(name: str, authority: str, source_type: str) -> SourceMetrics:
    """Start monitoring a data source."""
    return monitor.start_source(name, authority, source_type)


def end_monitoring_source(success: bool, error_type: Optional[str] = None, 
                         error_message: Optional[str] = None, features: int = 0, 
                         files: int = 0, response_size: int = 0, retries: int = 0) -> None:
    """End monitoring current data source."""
    monitor.end_source(success, error_type, error_message, features, files, response_size, retries)


def log_pipeline_summary() -> None:
    """Log pipeline execution summary."""
    monitor.log_summary()


def save_pipeline_metrics(output_path: Path) -> None:
    """Save pipeline metrics to file."""
    monitor.save_metrics(output_path)


def get_error_patterns() -> Dict[str, Any]:
    """Get detected error patterns."""
    return monitor.detect_patterns()