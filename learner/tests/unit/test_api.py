"""
File: learner/tests/unit/test_api.py
Unit tests for the Learner API endpoints.
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
os.environ["ACCEPTORS"] = "acceptor-1:8080,acceptor-2:8080,acceptor-3:8080,acceptor-4:8080,acceptor-5:8080"
os.environ["STORES"] = "store-1:8080,store-2:8080,store-3:8080"
os.environ["LOG_DIR"] = "/tmp/paxos-test-logs"

# Create test directory
os.makedirs(os.environ["LOG_DIR"], exist_ok=True)

# Fixture for setup and teardown of test environment
@pytest.fixture(scope="module", autouse=True)
def setup_module():
    """Setup function executed once before all tests."""
    # Initialize logging
    from common.logging import setup_logging
    setup_logging("learner-test", debug=True, debug_level="basic", log_dir=os.environ["LOG_DIR"])
    
    yield
    
    # Cleanup after tests
    import shutil
    try:
        shutil.rmtree(os.environ["LOG_DIR"])
    except:
        pass

@pytest.fixture
def mock_learner():
    """Fixture that creates a mock learner."""
    learner = AsyncMock()
    
    # Mock synchronous methods
    learner.get_status = MagicMock(return_value={
        "node_id": 1,
        "state": "running",
        "active_instances": 5,
        "decided_instances": 3,
        "notifications_processed": 10,
        "decisions_made": 3,
        "client_notifications_sent": 2,
        "client_notifications_failed": 1,
        "use_cluster_store": True,
        "timestamp": int(time.time() * 1000)
    })
    
    # Configure asynchronous methods
    learner.process_learn_notification = AsyncMock(return_value={
        "accepted": True,
        "instanceId": 1,
        "learnerId": 1
    })
    
    return learner

@pytest.fixture
def mock_consensus_manager():
    """Fixture that creates a mock consensus manager."""
    manager = MagicMock()
    
    # Mock methods
    manager.get_instance_info = MagicMock(side_effect=lambda instance_id: (
        {
            "instanceId": 1,
            "decided": True,
            "proposalNumber": 42,
            "value": {"data": "test"}
        }
        if instance_id == 1 else
        {
            "instanceId": 2,
            "decided": False,
            "votes": {
                "42": {
                    "value-hash-1": [1, 2]
                }
            }
        }
        if instance_id == 2 else
        {}
    ))
    
    manager.get_all_instance_ids = MagicMock(return_value=[1, 2, 3, 4, 5])
    
    return manager

@pytest.fixture
def mock_rowa_manager():
    """Fixture that creates a mock rowa manager."""
    manager = MagicMock()
    
    # Mock methods
    manager.get_stats = MagicMock(return_value={
        "reads_processed": 2,
        "writes_processed": 3,
        "write_successes": 2,
        "write_failures": 1,
        "success_rate": 0.67
    })
    
    return manager

@pytest.fixture
def api_client(mock_learner, mock_consensus_manager, mock_rowa_manager):
    """Fixture that creates a test client for the API."""
    from api import create_api
    
    # Create API with mock components
    app = create_api(mock_learner, mock_consensus_manager, mock_rowa_manager)
    
    # Return test client
    return TestClient(app)

def test_learn_endpoint(api_client, mock_learner):
    """Test the /learn endpoint."""
    # Create learn request
    learn_request = {
        "type": "LEARN",
        "proposalNumber": 42,
        "instanceId": 1,
        "acceptorId": 1,
        "accepted": True,
        "value": {"data": "test"},
        "timestamp": int(time.time() * 1000)
    }

    # Reset the mock
    mock_learner.process_learn_notification.reset_mock()

    # Send request
    response = api_client.post("/learn", json=learn_request)

    # Check response
    assert response.status_code == 200
    data = response.json()
    assert data["accepted"] == True
    assert data["instanceId"] == 1
    assert data["learnerId"] == 1

    # Check if learner method was called
    mock_learner.process_learn_notification.assert_called_once()
    args, kwargs = mock_learner.process_learn_notification.call_args
    assert args[0] == learn_request

def test_status_endpoint(api_client, mock_learner):
    """Test the /status endpoint."""
    # Send request
    response = api_client.get("/status")
    
    # Check response
    assert response.status_code == 200
    data = response.json()
    assert data["node_id"] == 1
    assert data["state"] == "running"
    assert data["active_instances"] == 5
    assert data["decided_instances"] == 3
    assert data["notifications_processed"] == 10
    assert data["decisions_made"] == 3
    assert "uptime" in data
    
    # Verify that the synchronous method was called
    mock_learner.get_status.assert_called_once()

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

def test_instance_endpoint_existing(api_client, mock_consensus_manager):
    """Test the /instance/{instance_id} endpoint for an existing instance."""
    # Reset the mock
    mock_consensus_manager.get_instance_info.reset_mock()
    
    # Send request
    response = api_client.get("/instance/1")
    
    # Check response
    assert response.status_code == 200
    data = response.json()
    assert data["instanceId"] == 1
    assert data["decided"] == True
    assert data["proposalNumber"] == 42
    assert data["value"]["data"] == "test"
    
    # Check if the method was called correctly
    mock_consensus_manager.get_instance_info.assert_called_once_with(1)

def test_instance_endpoint_nonexistent(api_client, mock_consensus_manager):
    """Test the /instance/{instance_id} endpoint for a nonexistent instance."""
    # Configure mock to return empty for nonexistent instance
    mock_consensus_manager.get_instance_info.return_value = {}
    
    # Send request
    response = api_client.get("/instance/999")
    
    # Check response
    assert response.status_code == 404
    assert "detail" in response.json()
    
    # Check if manager method was called
    mock_consensus_manager.get_instance_info.assert_called_with(999)

def test_instances_endpoint(api_client, mock_consensus_manager):
    """Test the /instances endpoint."""
    # Reset the mock
    mock_consensus_manager.get_all_instance_ids.reset_mock()
    mock_consensus_manager.get_instance_info.reset_mock()
    
    # Send request
    response = api_client.get("/instances")
    
    # Check response
    assert response.status_code == 200
    data = response.json()
    assert "instances" in data
    assert "total" in data
    assert "offset" in data
    assert "limit" in data
    
    # Check if methods were called
    mock_consensus_manager.get_all_instance_ids.assert_called_once()
    assert mock_consensus_manager.get_instance_info.call_count > 0

def test_instances_endpoint_decided_only(api_client, mock_consensus_manager):
    """Test the /instances endpoint with decided_only=True."""
    # Reset the mock
    mock_consensus_manager.get_all_instance_ids.reset_mock()
    
    # Configure mock to return fewer instances for decided_only=True
    mock_consensus_manager.get_all_instance_ids.side_effect = lambda decided_only: (
        [1, 3, 5] if decided_only else [1, 2, 3, 4, 5]
    )
    
    # Send request
    response = api_client.get("/instances?decided_only=true")
    
    # Check response
    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 3  # Only the decided instances
    
    # Check if method was called correctly
    mock_consensus_manager.get_all_instance_ids.assert_called_once_with(True)

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

def test_stats_endpoint(api_client, mock_learner, mock_rowa_manager):
    """Test the /stats endpoint."""
    # Send request
    response = api_client.get("/stats")
    
    # Check response
    assert response.status_code == 200
    data = response.json()
    assert "stats" in data
    
    stats = data["stats"]
    assert "node_id" in stats
    assert "notifications_processed" in stats
    assert "decisions_made" in stats
    assert "active_instances" in stats
    assert "decided_instances" in stats
    assert "uptime" in stats
    assert "use_cluster_store" in stats
    assert "rowa" in stats
    
    # Check ROWA stats
    rowa_stats = stats["rowa"]
    assert "reads_processed" in rowa_stats
    assert "writes_processed" in rowa_stats
    assert "write_successes" in rowa_stats
    assert "write_failures" in rowa_stats

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

def test_learn_endpoint_validation(api_client):
    """Test validation in the /learn endpoint."""
    # Create invalid learn request
    invalid_request = {
        "type": "LEARN",
        "proposalNumber": 42,
        "instanceId": 1,
        # Missing acceptorId
        "accepted": True,
        # Missing value
        "timestamp": int(time.time() * 1000)
    }
    
    # Send request
    response = api_client.post("/learn", json=invalid_request)
    
    # Check response (should get validation error)
    assert response.status_code in [400, 422]
