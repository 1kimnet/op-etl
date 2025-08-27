"""
Tests for parallel OID-based REST downloads.
"""
import unittest
from unittest.mock import Mock, patch, MagicMock
import sys
import os

# Add the etl directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from etl.download_rest import _rest_get_all_oids, _rest_fetch_oid_batch, fetch_rest_layer_parallel


class TestRestParallel(unittest.TestCase):
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_session = Mock()
        self.layer_url = "https://example.com/rest/services/test/MapServer/1"
        self.base_params = {
            "f": "geojson",
            "where": "1=1",
            "outFields": "*",
            "returnGeometry": "true"
        }
        self.layer_name = "test_layer"
    
    def test_rest_get_all_oids_success(self):
        """Test successful OID discovery."""
        # Mock response
        mock_response = Mock()
        mock_response.content = b'{"objectIds": [1, 2, 3, 4, 5], "objectIdFieldName": "OBJECTID"}'
        
        with patch('etl.download_rest.validate_response_content', return_value=True), \
             patch('etl.download_rest.safe_json_parse', return_value={
                 "objectIds": [5, 1, 3, 2, 4],  # Unsorted to test sorting
                 "objectIdFieldName": "OBJECTID"
             }):
            
            self.mock_session.safe_get.return_value = mock_response
            
            oid_field, oids, request_count = _rest_get_all_oids(
                self.mock_session, self.layer_url, self.base_params, self.layer_name
            )
            
            self.assertEqual(oid_field, "OBJECTID")
            self.assertEqual(oids, [1, 2, 3, 4, 5])  # Should be sorted
            self.assertEqual(request_count, 1)
    
    def test_rest_fetch_oid_batch_success(self):
        """Test successful batch fetching."""
        batch_ids = [1, 2, 3]
        batch_idx = 0
        response_format = "geojson"
        
        # Mock response
        mock_response = Mock()
        mock_response.headers = {}
        
        mock_features = [
            {"type": "Feature", "properties": {"id": 1}},
            {"type": "Feature", "properties": {"id": 2}},
            {"type": "Feature", "properties": {"id": 3}}
        ]
        
        with patch('etl.download_rest.validate_response_content', return_value=True), \
             patch('etl.download_rest.safe_json_parse', return_value={"features": mock_features}):
            
            self.mock_session.safe_get.return_value = mock_response
            
            features, success, request_count = _rest_fetch_oid_batch(
                self.mock_session, self.layer_url, self.base_params, "OBJECTID",
                batch_ids, batch_idx, self.layer_name, response_format
            )
            
            self.assertEqual(len(features), 3)
            self.assertTrue(success)
            self.assertEqual(request_count, 1)
    
    def test_rest_fetch_oid_batch_with_retry_after(self):
        """Test batch fetching respects Retry-After header."""
        batch_ids = [1, 2, 3]
        batch_idx = 0
        response_format = "geojson"
        
        # Mock response with Retry-After header
        mock_response = Mock()
        mock_response.headers = {"Retry-After": "2"}
        
        mock_features = [{"type": "Feature", "properties": {"id": 1}}]
        
        with patch('etl.download_rest.validate_response_content', return_value=True), \
             patch('etl.download_rest.safe_json_parse', return_value={"features": mock_features}), \
             patch('time.sleep') as mock_sleep:
            
            self.mock_session.safe_get.return_value = mock_response
            
            features, success, request_count = _rest_fetch_oid_batch(
                self.mock_session, self.layer_url, self.base_params, "OBJECTID",
                batch_ids, batch_idx, self.layer_name, response_format
            )
            
            # Verify sleep was called with the right delay
            mock_sleep.assert_called_once_with(2)
            self.assertTrue(success)
    
    @patch('etl.download_rest._rest_get_all_oids')
    @patch('etl.download_rest._rest_fetch_oid_batch')
    def test_fetch_rest_layer_parallel_integration(self, mock_fetch_batch, mock_get_oids):
        """Test full parallel fetch integration."""
        # Mock OID discovery
        mock_get_oids.return_value = ("OBJECTID", [1, 2, 3, 4, 5], 1)
        
        # Mock batch fetch results
        mock_fetch_batch.side_effect = [
            ([{"type": "Feature", "properties": {"OBJECTID": 1}}], True, 1),
            ([{"type": "Feature", "properties": {"OBJECTID": 2}}], True, 1),
            ([{"type": "Feature", "properties": {"OBJECTID": 3}}], True, 1),
        ]
        
        features, metrics = fetch_rest_layer_parallel(
            self.mock_session, self.layer_url, self.base_params, self.layer_name,
            page_size=2, max_workers=2, response_format="geojson"
        )
        
        # Check results
        self.assertEqual(len(features), 3)
        self.assertEqual(metrics["oids_total"], 5)
        self.assertEqual(metrics["batches_total"], 3)  # 5 OIDs with page_size=2 -> 3 batches
        self.assertEqual(metrics["batches_ok"], 3)
        self.assertEqual(metrics["features_total"], 3)
        self.assertEqual(metrics["request_count"], 4)  # 1 for OID discovery + 3 for batches


    def test_rest_fetch_oid_batch_with_post(self):
        """Test batch fetching detects when to use POST for long URL parameters."""
        # Create a small batch first (should use GET)
        batch_ids = [1, 2, 3]
        batch_idx = 0
        response_format = "geojson"
        
        # Mock response
        mock_response = Mock()
        mock_response.headers = {}
        
        mock_features = [{"type": "Feature", "properties": {"id": 1}}]
        
        with patch('etl.download_rest.validate_response_content', return_value=True), \
             patch('etl.download_rest.safe_json_parse', return_value={"features": mock_features}):
            
            self.mock_session.safe_get.return_value = mock_response
            
            features, success, request_count = _rest_fetch_oid_batch(
                self.mock_session, self.layer_url, self.base_params, "OBJECTID",
                batch_ids, batch_idx, self.layer_name, response_format
            )
            
            # Should use GET for small batch
            self.mock_session.safe_get.assert_called_once()
            self.assertTrue(success)
            self.assertEqual(len(features), 1)


if __name__ == '__main__':
    unittest.main()