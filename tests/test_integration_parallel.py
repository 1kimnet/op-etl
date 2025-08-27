"""
Integration test for REST parallel OID-batch downloading.
Demonstrates the complete workflow from configuration to execution.
"""
import json
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import sys
import os

from etl.download_rest import download_layer, process_rest_source


class MockSession:
    """Mock session that simulates a large REST service with OID support."""
    
    def __init__(self, total_features=5000):
        self.total_features = total_features
        self.call_count = 0
    
    def safe_get(self, url, params=None, timeout=None):
        """Mock REST API responses."""
        self.call_count += 1
        mock_response = Mock()
        mock_response.headers = {}
        
        if "/query" in url and params:
            if params.get("returnIdsOnly") == "true":
                # Return object IDs
                object_ids = list(range(1, self.total_features + 1))
                mock_response.content = json.dumps({
                    "objectIds": object_ids,
                    "objectIdFieldName": "OBJECTID"
                }).encode()
                return mock_response
            
            elif "IN (" in params.get("where", ""):
                # Return batch of features
                features = []
                # Extract OIDs from WHERE clause for realistic response
                where = params.get("where", "")
                if "OBJECTID IN" in where:
                    # Simple extraction - in real world this would be more complex
                    features = [
                        {
                            "type": "Feature",
                            "properties": {"OBJECTID": i, "NAME": f"Feature {i}"},
                            "geometry": {"type": "Point", "coordinates": [12.0 + i*0.001, 59.0 + i*0.001]}
                        }
                        for i in range(1, min(1001, 100))  # Simulate batch of features
                    ]
                
                mock_response.content = json.dumps({
                    "type": "FeatureCollection",
                    "features": features
                }).encode()
                return mock_response
        
        elif url.endswith("MapServer/1"):
            # Layer info response
            mock_response.content = json.dumps({
                "id": 1,
                "name": "TestLayer",
                "type": "Feature Layer",
                "supportsQuery": True,
                "supportsAdvancedQueries": True,
                "objectIdField": "OBJECTID",
                "capabilities": "Query"
            }).encode()
            return mock_response
        
        return None


def test_integration_parallel_oid_download():
    """Test the complete workflow with parallel OID downloading enabled."""
    print("\n=== Integration Test: Parallel OID Download ===")
    
    # Mock the session
    mock_session = MockSession(total_features=2500)  # 2500 features -> 3 batches with page_size=1000
    
    with tempfile.TemporaryDirectory() as temp_dir:
        out_dir = Path(temp_dir)
        
        # Configuration with parallel OID sweep enabled
        raw_config = {
            "use_oid_sweep": True,
            "page_size": 1000,
            "max_workers": 3,
            "out_sr": 3006,
            "where": "1=1"
        }
        
        layer_url = "https://mock-server.com/rest/services/Test/MapServer/1"
        layer_name = "test_layer"
        
        with patch('etl.download_rest.RecursionSafeSession', return_value=mock_session), \
             patch('etl.download_rest.validate_response_content', return_value=True), \
             patch('etl.download_rest.safe_json_parse') as mock_parse:
            
            # Set up the mock_parse to return appropriate responses
            def side_effect(content):
                return json.loads(content.decode() if isinstance(content, bytes) else content)
            mock_parse.side_effect = side_effect
            
            # Run the download
            print(f"Downloading from {layer_url} with parallel OID sweep enabled...")
            print(f"Configuration: {raw_config}")
            
            feature_count = download_layer(
                layer_url=layer_url,
                out_dir=out_dir,
                layer_name=layer_name,
                raw_config=raw_config,
                global_bbox=None,
                global_sr=None
            )
            
            print(f"Download completed: {feature_count} features")
            print(f"Total API calls made: {mock_session.call_count}")
            
            # Verify the output file was created
            output_file = out_dir / f"{layer_name}.geojson"
            assert output_file.exists(), f"Output file not created: {output_file}"
            
            # Verify the content
            with open(output_file, 'r') as f:
                geojson_data = json.load(f)
            
            print(f"Features in output file: {len(geojson_data.get('features', []))}")
            
            # Expected behavior:
            # 1 call for layer info + 1 call for OID discovery + 3 calls for batches = 5 total calls
            expected_min_calls = 5  # This varies based on mock implementation
            print(f"Expected minimum API calls: {expected_min_calls}")
            
            assert feature_count > 0, "No features were downloaded"
            assert mock_session.call_count >= expected_min_calls, f"Expected at least {expected_min_calls} API calls, got {mock_session.call_count}"
            
            print("✅ Integration test passed!")


def test_integration_fallback_behavior():
    """Test fallback behavior when use_oid_sweep is disabled."""
    print("\n=== Integration Test: Fallback Behavior ===")
    
    mock_session = MockSession(total_features=100)
    
    with tempfile.TemporaryDirectory() as temp_dir:
        out_dir = Path(temp_dir)
        
        # Configuration with parallel OID sweep disabled (default behavior)
        raw_config = {
            "use_oid_sweep": False,  # Explicitly disabled
            "out_sr": 3006,
            "where": "1=1"
        }
        
        layer_url = "https://mock-server.com/rest/services/Test/MapServer/1"
        layer_name = "test_layer_fallback"
        
        # Mock offset pagination response
        def mock_offset_response(url, params=None, timeout=None):
            mock_session.call_count += 1
            mock_response = Mock()
            mock_response.headers = {}
            
            if url.endswith("MapServer/1"):
                # Layer info
                mock_response.content = json.dumps({
                    "id": 1,
                    "name": "TestLayer",
                    "supportsQuery": True,
                    "supportsAdvancedQueries": True,
                    "objectIdField": "OBJECTID"
                }).encode()
                return mock_response
            elif "/query" in url and params:
                # Simulate offset pagination - return empty to stop pagination
                mock_response.content = json.dumps({
                    "type": "FeatureCollection",
                    "features": [],
                    "exceededTransferLimit": False
                }).encode()
                return mock_response
            return None
        
        with patch('etl.download_rest.RecursionSafeSession') as mock_session_class, \
             patch('etl.download_rest.validate_response_content', return_value=True), \
             patch('etl.download_rest.safe_json_parse') as mock_parse:
            
            mock_session_instance = Mock()
            mock_session_instance.safe_get = mock_offset_response
            mock_session_class.return_value = mock_session_instance
            
            def side_effect(content):
                return json.loads(content.decode() if isinstance(content, bytes) else content)
            mock_parse.side_effect = side_effect
            
            print(f"Testing fallback behavior with use_oid_sweep=False...")
            
            feature_count = download_layer(
                layer_url=layer_url,
                out_dir=out_dir,
                layer_name=layer_name,
                raw_config=raw_config,
                global_bbox=None,
                global_sr=None
            )
            
            print(f"Fallback download completed: {feature_count} features")
            print("✅ Fallback behavior test passed!")


if __name__ == '__main__':
    # Run integration tests
    try:
        test_integration_parallel_oid_download()
        test_integration_fallback_behavior()
        print("\n🎉 All integration tests passed!")
    except Exception as e:
        print(f"\n❌ Integration test failed: {e}")
        import traceback
        traceback.print_exc()
        exit(1)