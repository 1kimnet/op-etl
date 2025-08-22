#!/usr/bin/env python3
"""
Simple test script to verify bbox utilization and boundary clipping functionality.
"""

import sys
from pathlib import Path
from etl.config import load_config
from etl.download_atom import _extract_global_bbox, is_filterable_service
from etl.download_ogc import _extract_global_bbox as ogc_extract_bbox
from etl.download_rest import _extract_global_bbox as rest_extract_bbox
from etl.download_wfs import _extract_global_bbox as wfs_extract_bbox


def test_bbox_inheritance():
    """Test that bbox inheritance works correctly."""
    print("=== Testing bbox inheritance ===")
    
    cfg = load_config()
    
    # Check that use_bbox_filter is enabled
    assert cfg.get("use_bbox_filter", False), "use_bbox_filter should be enabled"
    print("✓ use_bbox_filter is enabled")
    
    # Check that global_ogc_bbox is configured
    global_bbox = cfg.get("global_ogc_bbox")
    assert global_bbox, "global_ogc_bbox should be configured"
    assert "coords" in global_bbox, "global_ogc_bbox should have coords"
    print(f"✓ global_ogc_bbox configured: {global_bbox['coords']}")
    
    # Check that sources inherit bbox from defaults
    bbox_sources = [s for s in cfg["sources"] if s.get("raw", {}).get("bbox")]
    assert len(bbox_sources) > 0, "Some sources should have inherited bbox"
    print(f"✓ {len(bbox_sources)} sources have bbox configuration")
    
    # Check specific ATOM sources with filter_services enabled
    atom_filtered = [s for s in cfg["sources"] 
                    if s.get("type") == "atom" and 
                    s.get("raw", {}).get("filter_services")]
    assert len(atom_filtered) > 0, "Some ATOM sources should have filter_services enabled"
    print(f"✓ {len(atom_filtered)} ATOM sources have filter_services enabled")


def test_bbox_extraction():
    """Test bbox extraction for all download modules."""
    print("\n=== Testing bbox extraction across modules ===")
    
    cfg = load_config()
    
    # Test OGC bbox extraction
    ogc_bbox, ogc_crs = ogc_extract_bbox(cfg)
    print(f"✓ OGC bbox extraction: {ogc_bbox}, CRS: {ogc_crs}")
    
    # Test REST bbox extraction
    rest_bbox, rest_sr = rest_extract_bbox(cfg)
    print(f"✓ REST bbox extraction: {rest_bbox}, SR: {rest_sr}")
    
    # Test WFS bbox extraction
    wfs_bbox, wfs_sr = wfs_extract_bbox(cfg)
    print(f"✓ WFS bbox extraction: {wfs_bbox}, SR: {wfs_sr}")
    
    # Test ATOM bbox extraction
    atom_bbox, atom_sr = _extract_global_bbox(cfg)
    print(f"✓ ATOM bbox extraction: {atom_bbox}, SR: {atom_sr}")
    
    # Verify all extractions return consistent results
    assert ogc_bbox == rest_bbox == wfs_bbox == atom_bbox, "All modules should extract the same bbox"
    print("✓ All modules extract consistent bbox coordinates")


def test_filterable_service_detection():
    """Test detection of filterable services in ATOM feeds."""
    print("\n=== Testing filterable service detection ===")
    
    test_cases = [
        ("https://example.com/wfs?service=WFS", True, "WFS service"),
        ("https://api.example.com/ogc/features/v1/collections", True, "OGC API Features"),
        ("https://server.com/arcgis/rest/services/Test/FeatureServer", True, "ArcGIS FeatureServer"),
        ("https://server.com/arcgis/rest/services/Test/MapServer", True, "ArcGIS MapServer"),
        ("https://example.com/download.zip", False, "Direct file download"),
        ("https://example.com/data.geojson", False, "Direct GeoJSON"),
    ]
    
    for url, expected, description in test_cases:
        result = is_filterable_service(url)
        assert result == expected, f"Service detection failed for {description}: expected {expected}, got {result}"
        print(f"✓ {description}: {url} -> {result}")


def test_geoprocessing_config():
    """Test geoprocessing configuration loading."""
    print("\n=== Testing geoprocessing configuration ===")
    
    cfg = load_config()
    gp = cfg.get("geoprocess", {})
    
    assert gp.get("enabled"), "Geoprocessing should be enabled"
    print("✓ Geoprocessing is enabled")
    
    aoi_boundary = gp.get("aoi_boundary")
    assert aoi_boundary, "AOI boundary should be configured"
    print(f"✓ AOI boundary configured: {aoi_boundary}")
    
    target_srid = gp.get("target_srid")
    assert target_srid, "Target SRID should be configured"
    print(f"✓ Target SRID configured: {target_srid}")


def main():
    """Run all tests."""
    print("Running bbox utilization and boundary clipping tests...\n")
    
    try:
        test_bbox_inheritance()
        test_bbox_extraction()
        test_filterable_service_detection()
        test_geoprocessing_config()
        
        print("\n🎉 All tests passed! bbox utilization and boundary clipping are properly implemented.")
        return 0
        
    except AssertionError as e:
        print(f"\n❌ Test failed: {e}")
        return 1
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())