#!/usr/bin/env python3
"""
Test script for REST pagination functionality.
Tests the new transfer-limit stop conditions and OID-batch paginator.
"""

import json
import logging
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

# Set up logging
logging.basicConfig(level=logging.INFO)

# Import the module we're testing
from etl.download_rest import (
    TransferLimitExceededError,
    _download_with_offset_pagination,
    _download_with_oid_pagination,
    download_layer
)


def create_mock_response(features, exceeded_transfer_limit=False, object_ids=None):
    """Create a mock response with the given features and transfer limit status."""
    response = Mock()
    response.content = json.dumps({
        "features": features,
        "exceededTransferLimit": exceeded_transfer_limit,
        "objectIds": object_ids or []
    }).encode('utf-8')
    return response


@patch('etl.download_rest.validate_response_content', return_value=True)
@patch('etl.download_rest.safe_json_parse')
def test_offset_pagination_normal(mock_json_parse, mock_validate):
    """Test normal offset pagination that completes successfully."""
    print("\n=== Testing normal offset pagination ===")
    
    # Mock session
    session = Mock()
    
    # Simulate 3 pages: 1000, 1000, 500 features (last page short)
    mock_responses = [
        {"features": [{"id": i} for i in range(1000)], "exceededTransferLimit": False},  # Page 1
        {"features": [{"id": i} for i in range(1000, 2000)], "exceededTransferLimit": False},  # Page 2
        {"features": [{"id": i} for i in range(2000, 2500)], "exceededTransferLimit": False},  # Page 3
    ]
    
    mock_json_parse.side_effect = mock_responses
    session.safe_get.return_value = Mock()  # Just need a truthy response
    
    base_params = {"where": "1=1", "outFields": "*"}
    
    features, request_count = _download_with_offset_pagination(session, "http://test/layer/0", base_params, "test_layer")
    
    assert len(features) == 2500, f"Expected 2500 features, got {len(features)}"
    assert request_count == 3, f"Expected 3 requests, got {request_count}"
    print(f"✓ Normal pagination: {len(features)} features in {request_count} requests")


@patch('etl.download_rest.validate_response_content', return_value=True)
@patch('etl.download_rest.safe_json_parse')
def test_offset_pagination_transfer_limit(mock_json_parse, mock_validate):
    """Test offset pagination that hits transfer limits."""
    print("\n=== Testing offset pagination with transfer limits ===")
    
    session = Mock()
    
    # Simulate transfer limit exceeded on all pages until the last short page
    mock_responses = [
        {"features": [{"id": i} for i in range(1000)], "exceededTransferLimit": True},  # Page 1: limit exceeded
        {"features": [{"id": i} for i in range(1000, 2000)], "exceededTransferLimit": True},  # Page 2: limit exceeded
        {"features": [{"id": i} for i in range(2000, 2500)], "exceededTransferLimit": False},  # Page 3: no limit
    ]
    
    mock_json_parse.side_effect = mock_responses
    session.safe_get.return_value = Mock()
    
    base_params = {"where": "1=1", "outFields": "*"}
    
    features, request_count = _download_with_offset_pagination(session, "http://test/layer/0", base_params, "test_layer")
    
    assert len(features) == 2500, f"Expected 2500 features, got {len(features)}"
    assert request_count == 3, f"Expected 3 requests, got {request_count}"
    print(f"✓ Transfer limit pagination: {len(features)} features in {request_count} requests")


@patch('etl.download_rest.validate_response_content', return_value=True)
@patch('etl.download_rest.safe_json_parse')
def test_oid_pagination(mock_json_parse, mock_validate):
    """Test OID-based pagination."""
    print("\n=== Testing OID-based pagination ===")
    
    session = Mock()
    
    # Mock responses: first for getting OIDs, then for batches of features
    mock_responses = [
        {"objectIds": list(range(1, 251))},  # 250 object IDs
        {"features": [{"id": i, "OBJECTID": i} for i in range(1, 101)]},  # Batch 1
        {"features": [{"id": i, "OBJECTID": i} for i in range(101, 201)]},  # Batch 2
        {"features": [{"id": i, "OBJECTID": i} for i in range(201, 251)]},  # Batch 3
    ]
    
    mock_json_parse.side_effect = mock_responses
    session.safe_get.return_value = Mock()
    
    base_params = {"where": "1=1", "outFields": "*"}
    
    features, request_count = _download_with_oid_pagination(session, "http://test/layer/0", base_params, "OBJECTID", "test_layer")
    
    assert len(features) == 250, f"Expected 250 features, got {len(features)}"
    assert request_count == 4, f"Expected 4 requests (1 for OIDs + 3 batches), got {request_count}"
    print(f"✓ OID pagination: {len(features)} features in {request_count} requests")


@patch('etl.download_rest.validate_response_content', return_value=True)
@patch('etl.download_rest.safe_json_parse')
def test_transfer_limit_exception(mock_json_parse, mock_validate):
    """Test that TransferLimitExceededError is raised correctly."""
    print("\n=== Testing transfer limit exception ===")
    
    session = Mock()
    
    # Simulate a scenario where transfer limit is exceeded with a short page
    mock_responses = [
        {"features": [{"id": i} for i in range(500)], "exceededTransferLimit": True}  # Short page with transfer limit
    ]
    
    mock_json_parse.side_effect = mock_responses
    session.safe_get.return_value = Mock()
    
    base_params = {"where": "1=1", "outFields": "*"}
    
    try:
        _download_with_offset_pagination(session, "http://test/layer/0", base_params, "test_layer")
        assert False, "Expected TransferLimitExceededError to be raised"
    except TransferLimitExceededError:
        print("✓ TransferLimitExceededError raised correctly")


def run_tests():
    """Run all tests."""
    print("Starting REST pagination tests...")
    
    try:
        test_offset_pagination_normal()
        test_offset_pagination_transfer_limit()
        test_oid_pagination()
        test_transfer_limit_exception()
        
        print("\n🎉 All tests passed!")
        return True
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    run_tests()