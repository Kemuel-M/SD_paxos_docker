"""
File: acceptor/tests/unit/test_api.py
Unit tests for the Acceptor API endpoints.
"""
import os
import json
import time
import pytest
from fastapi.testclient import TestClient
from unittest.mock import MagicMock, patch, AsyncMock

import warnings
warnings.filterwarnings("always", category=RuntimeWarning)

# Configure environment for testing
os.environ["DEBUG"] = "true"
os.environ["DEBUG_LEVEL"] = "basic"
os.environ["NODE_ID"] = "1"
os.environ["LEARNERS"] = "learner-1:8080,learner-2:8080"
os.environ["LOG_DIR"] = "/tmp/paxos-test-logs"

# Create test directory
os.makedirs(os.environ["LOG_DIR"], exist_ok=True)

# Fixture for setup and teardown of test environment
@pytest.fixture(scope="module", autouse=True)
def setup_module():
    """Setup function executed once before all tests."""
    # Initialize logging
    from common.logging import setup_logging
    setup_logging("acceptor-test", debug=True, debug_level="basic", log_dir=os.environ["LOG_DIR"])
    
    yield
    
    # Cleanup after tests
    import shutil
    try:
        shutil.rmtree(os.environ["LOG_DIR"])
    except:
        pass

@pytest.fixture
def mock_acceptor():
    """Fixture that creates a mock acceptor."""
    acceptor = AsyncMock()
    
    # Mock synchronous methods
    acceptor.get_status = MagicMock(return_value={
        "node_id": 1,
        "state": "running",
        "learners": 2,
        "active_instances": 2,
        "accepted_instances": 1,
        "prepare_requests_processed": 10,
        "accept_requests_processed": 5,
        "promises_made": 8,
        "proposals_accepted": 3,
        "timestamp": int(time.time() * 1000)
    })
    
    acceptor.get_instance_info = MagicMock(side_effect=lambda instance_id: (
        {"instanceId": 1, "highestPromised": 10, "accepted": True, "proposalNumber": 10, "value": {"value": "test"}}
        if instance_id == 1 else
        {"instanceId": 2, "highestPromised": 20, "accepted": False}
        if instance_id == 2 else
        {}
    ))
    
    # Mock asynchronous methods
    acceptor.process_prepare = AsyncMock(return_value={"accepted": True, "proposalNumber": 42, "instanceId": 1, "acceptorId": 1})
    acceptor.process_accept = AsyncMock(return_value={"accepted": True, "proposalNumber": 42, "instanceId": 1, "acceptorId": 1})
    
    return acceptor

@pytest.fixture
def mock_persistence():
    """Fixture that creates a mock persistence manager."""
    persistence = MagicMock()
    
    # Configure synchronous methods
    persistence.load_state = MagicMock(return_value={
        "promises": {"1": 10},
        "accepted": {"1": [10, {"value": "test"}]},
        "prepare_requests_processed": 10,
        "accept_requests_processed": 5,
        "promises_made": 8,
        "proposals_accepted": 3
    })
    
    # Configure asynchronous methods
    persistence.save_state = AsyncMock(return_value=True)
    
    return persistence

@pytest.fixture
def api_client(mock_acceptor, mock_persistence):
    """Fixture that creates a test client for the API."""
    from api import create_api
    
    # Create API with mock components
    app = create_api(mock_acceptor, mock_persistence)
    
    # Return test client
    return TestClient(app)

def test_prepare_endpoint(api_client, mock_acceptor):
    """Test the /prepare endpoint."""
    # Create prepare request
    prepare_request = {
        "type": "PREPARE",
        "proposalNumber": 20,
        "instanceId": 1,
        "proposerId": 2
    }
    
    # Reset the mock
    mock_acceptor.process_prepare.reset_mock()
    
    # Send request
    response = api_client.post("/prepare", json=prepare_request)
    
    # Check response
    assert response.status_code == 200
    
    # Check if acceptor method was called
    mock_acceptor.process_prepare.assert_called_once()
    args, kwargs = mock_acceptor.process_prepare.call_args

    assert args[0] == prepare_request

def test_accept_endpoint(api_client, mock_acceptor):
    """Test the /accept endpoint."""
    # Create accept request
    accept_request = {
        "type": "ACCEPT",
        "proposalNumber": 20,
        "instanceId": 1,
        "proposerId": 2,
        "value": {"clientId": "client-1", "value": "test"}
    }
    
    # Reset the mock
    mock_acceptor.process_accept.reset_mock()
    
    # Send request
    response = api_client.post("/accept", json=accept_request)
    
    # Check response
    assert response.status_code == 200
    
    # Check if acceptor method was called
    mock_acceptor.process_accept.assert_called_once()
    args, kwargs = mock_acceptor.process_accept.call_args
    assert args[0] == accept_request

def test_status_endpoint(api_client, mock_acceptor):
    """Test the /status endpoint with synchronous get_status method."""
    # Send request
    response = api_client.get("/status")
    
    # Check response
    assert response.status_code == 200
    data = response.json()
    assert data["node_id"] == 1
    assert data["state"] == "running"
    assert data["active_instances"] == 2
    assert data["accepted_instances"] == 1
    assert data["prepare_requests_processed"] == 10
    assert data["accept_requests_processed"] == 5
    assert data["promises_made"] == 8
    assert data["proposals_accepted"] == 3
    assert "uptime" in data
    
    # Verify that the synchronous method was called
    mock_acceptor.get_status.assert_called_once()

def test_health_endpoint(api_client):
    """Test the /health endpoint."""
    # Send request
    response = api_client.get("/health")
    
    # Check response
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert "timestamp" in data
    assert "debug_enabled" in data
    assert "debug_level" in data

def test_instance_endpoint_existing(api_client, mock_acceptor):
    """Test the /instance/{instance_id} endpoint for an existing instance."""
    # Reset the mock
    mock_acceptor.get_instance_info.reset_mock()
    
    # Send request
    response = api_client.get("/instance/1")
    
    # Check response
    assert response.status_code == 200
    data = response.json()
    assert data["instanceId"] == 1
    assert data["highestPromised"] == 10
    assert data["accepted"] == True
    assert data["proposalNumber"] == 10
    assert data["value"] == {"value": "test"}
    
    # Check if the synchronous method was called correctly
    mock_acceptor.get_instance_info.assert_called_once_with(1)

def test_instance_endpoint_nonexistent(api_client, mock_acceptor):
    """Test the /instance/{instance_id} endpoint for a nonexistent instance."""
    # Configure mock to return empty for nonexistent instance
    mock_acceptor.get_instance_info.return_value = {}
    
    # Send request
    response = api_client.get("/instance/999")
    
    # Check response
    assert response.status_code == 404
    assert "detail" in response.json()
    
    # Check if acceptor method was called
    mock_acceptor.get_instance_info.assert_called_with(999)

def test_instances_endpoint(api_client, mock_acceptor):
    """Test the /instances endpoint."""
    # Configure mock to return instances list
    mock_acceptor.promises = {1: 10, 2: 20}
    
    # Send request
    response = api_client.get("/instances")
    
    # Check response
    assert response.status_code == 200
    data = response.json()
    assert "instances" in data
    assert "total" in data
    assert "offset" in data
    assert "limit" in data

def test_logs_endpoint(api_client):
    """Test the /logs endpoint."""
    # Send request
    response = api_client.get("/logs")
    
    # Check response
    assert response.status_code == 200
    data = response.json()
    assert "logs" in data

def test_logs_important_endpoint(api_client):
    """Test the /logs/important endpoint."""
    # Send request
    response = api_client.get("/logs/important")
    
    # Check response
    assert response.status_code == 200
    data = response.json()
    assert "logs" in data

def test_stats_endpoint(api_client, mock_acceptor):
    """Test the /stats endpoint."""
    # Send request
    response = api_client.get("/stats")
    
    # Check response
    assert response.status_code == 200
    data = response.json()
    assert "stats" in data
    stats = data["stats"]
    assert "uptime" in stats
    assert "node_id" in stats
    assert "prepare_requests_processed" in stats
    assert "accept_requests_processed" in stats
    assert "promises_made" in stats
    assert "proposals_accepted" in stats
    assert "active_instances" in stats
    assert "accepted_instances" in stats

def test_debug_config_endpoint(api_client):
    """Test the /debug/config endpoint."""
    with patch("common.logging.set_debug_level") as mock_set_debug:
        # Create config request
        config = {
            "enabled": True,
            "level": "advanced"
        }
        
        # Send request
        response = api_client.post("/debug/config", json=config)
        
        # Check response
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "success"
        assert data["debug"]["enabled"] == True
        assert data["debug"]["level"] == "advanced"
        
        # Check if set_debug_level was called
        mock_set_debug.assert_called_once_with(True, "advanced")

def test_prepare_endpoint_missing_fields(api_client):
    """Test the /prepare endpoint with missing required fields."""
    # Create invalid prepare request
    invalid_request = {
        "type": "PREPARE",
        "proposalNumber": 20
        # Missing instanceId and proposerId
    }
    
    # Send request
    response = api_client.post("/prepare", json=invalid_request)
    
    # Should get validation error from FastAPI
    assert response.status_code in [400, 422]

def test_accept_endpoint_missing_fields(api_client):
    """Test the /accept endpoint with missing required fields."""
    # Create invalid accept request
    invalid_request = {
        "type": "ACCEPT",
        "proposalNumber": 20,
        "instanceId": 1
        # Missing proposerId and value
    }
    
    # Send request
    response = api_client.post("/accept", json=invalid_request)
    
    # Should get validation error from FastAPI
    assert response.status_code in [400, 422]