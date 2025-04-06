"""
File: client/tests/unit/test_api.py
Unit tests for the client API endpoints.
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
os.environ["CLIENT_ID"] = "client-1"
os.environ["LOG_DIR"] = "/tmp/paxos-test-logs"

# Create test directory
os.makedirs(os.environ["LOG_DIR"], exist_ok=True)

# Fixture for setup and teardown of test environment
@pytest.fixture(scope="module", autouse=True)
def setup_module():
    """Setup function executed once before all tests."""
    # Initialize logging
    from common.logging import setup_logging
    setup_logging("client-test", debug=True, debug_level="basic", log_dir=os.environ["LOG_DIR"])
    
    yield
    
    # Cleanup after tests
    import shutil
    try:
        shutil.rmtree(os.environ["LOG_DIR"])
    except:
        pass

@pytest.fixture
def mock_client():
    """Fixture that creates a mock client."""
    client = MagicMock()
    
    # Mock methods
    client.process_notification = MagicMock(return_value={
        "status": "acknowledged",
        "known": True,
        "operation_id": 1
    })
    
    client.get_status = MagicMock(return_value={
        "client_id": "client-1",
        "proposer_url": "http://proposer-1:8080",
        "total_operations": 20,
        "completed": 5,
        "failed": 2,
        "in_progress": 3,
        "avg_latency": 2.5,
        "runtime": 60.0,
        "running": True
    })
    
    client.get_history = MagicMock(return_value=[
        {"id": 1, "status": "COMMITTED"},
        {"id": 2, "status": "NOT_COMMITTED"}
    ])
    
    client.get_operation = MagicMock(side_effect=lambda op_id: (
        {"id": op_id, "status": "COMMITTED"} if op_id == 1 else
        {"id": op_id, "status": "NOT_COMMITTED"} if op_id == 2 else
        None
    ))
    
    client.start = AsyncMock()
    client.stop = AsyncMock()
    client._send_operation = AsyncMock()
    client.running = True
    client.next_operation_id = 10
    
    return client

@pytest.fixture
def api_client(mock_client):
    """Fixture that creates a test client for the API."""
    from api import create_api
    
    # Create API with mock client
    app = create_api(mock_client)
    
    # Return test client
    return TestClient(app)

def test_notification_endpoint(api_client, mock_client):
    """Test the /notification endpoint."""
    # Create notification
    notification = {
        "status": "COMMITTED",
        "instanceId": 1,
        "resource": "R",
        "timestamp": int(time.time() * 1000)
    }
    
    # Reset the mock
    mock_client.process_notification.reset_mock()
    
    # Send request
    response = api_client.post("/notification", json=notification)
    
    # Check response
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "acknowledged"
    assert data["known"] == True
    assert data["operation_id"] == 1
    
    # Check if client method was called
    mock_client.process_notification.assert_called_once_with(notification)

def test_status_endpoint(api_client, mock_client):
    """Test the /status endpoint."""
    # Send request
    response = api_client.get("/status")
    
    # Check response
    assert response.status_code == 200
    data = response.json()
    assert data["client_id"] == "client-1"
    assert data["total_operations"] == 20
    assert data["completed"] == 5
    assert data["failed"] == 2
    
    # Verify that the method was called
    mock_client.get_status.assert_called_once()

def test_health_endpoint(api_client):
    """Test the /health endpoint."""
    # Send request
    response = api_client.get("/health")
    
    # Check response
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert "timestamp" in data

def test_history_endpoint(api_client, mock_client):
    """Test the /history endpoint."""
    # Reset the mock
    mock_client.get_history.reset_mock()
    
    # Send request
    response = api_client.get("/history")
    
    # Check response
    assert response.status_code == 200
    data = response.json()
    assert "operations" in data
    assert len(data["operations"]) == 2
    
    # Check if method was called
    mock_client.get_history.assert_called_once_with(100)  # Default limit
    
    # Test with custom limit
    response = api_client.get("/history?limit=5")
    mock_client.get_history.assert_called_with(5)

def test_operation_endpoint(api_client, mock_client):
    """Test the /operation/{operation_id} endpoint."""
    # Reset the mock
    mock_client.get_operation.reset_mock()
    
    # Send request for existing operation
    response = api_client.get("/operation/1")
    
    # Check response
    assert response.status_code == 200
    data = response.json()
    assert data["id"] == 1
    assert data["status"] == "COMMITTED"
    
    # Check if method was called
    mock_client.get_operation.assert_called_with(1)
    
    # Test with non-existent operation
    response = api_client.get("/operation/3")
    assert response.status_code == 404
    assert "detail" in response.json()

def test_create_operation_endpoint(api_client, mock_client):
    """Test the POST /operation endpoint."""
    # Create operation request
    operation = {
        "data": "test data"
    }
    
    # Send request
    response = api_client.post("/operation", json=operation)
    
    # Check response
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "initiated"
    assert "operation_id" in data

def test_create_operation_stopped_client(api_client, mock_client):
    """Test the POST /operation endpoint with stopped client."""
    # Set client as stopped
    mock_client.running = False
    
    # Create operation request
    operation = {
        "data": "test data"
    }
    
    # Send request
    response = api_client.post("/operation", json=operation)
    
    # Check response - should fail
    assert response.status_code == 400
    assert "detail" in response.json()

def test_start_endpoint(api_client, mock_client):
    """Test the /start endpoint."""
    # Reset the mock
    mock_client.start.reset_mock()
    
    # Send request
    response = api_client.post("/start")
    
    # Check response
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "started"
    
    # Check if method was called
    mock_client.start.assert_called_once()

def test_stop_endpoint(api_client, mock_client):
    """Test the /stop endpoint."""
    # Reset the mock
    mock_client.stop.reset_mock()
    
    # Send request
    response = api_client.post("/stop")
    
    # Check response
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "stopped"
    
    # Check if method was called
    mock_client.stop.assert_called_once()

def test_logs_endpoint(api_client):
    """Test the /logs endpoint."""
    # Send request
    response = api_client.get("/logs")
    
    # Check response
    assert response.status_code == 200
    data = response.json()
    assert "logs" in data