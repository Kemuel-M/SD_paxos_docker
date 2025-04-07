"""
File: client/tests/unit/test_web_server.py
Unit tests for the client web server.
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
os.environ["STORES"] = "cluster-store-1:8080,cluster-store-2:8080,cluster-store-3:8080"

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
        {"id": 1, "start_time": time.time(), "instance_id": 101, "status": "COMMITTED", "latency": 1.5},
        {"id": 2, "start_time": time.time(), "instance_id": 102, "status": "NOT_COMMITTED", "latency": 2.0}
    ])
    
    # Configurar corretamente os métodos assíncronos
    client.start = AsyncMock()
    client.stop = AsyncMock()
    # Importante: não criar o _operation_loop diretamente como um atributo
    client._send_operation = AsyncMock()
    
    client.client_id = "client-1"
    
    return client

@pytest.fixture
def mock_http_client():
    """Fixture that creates a mock HTTP client."""
    mock = AsyncMock()
    
    # Mock resource data
    resource_data = {
        "data": "Test resource data",
        "version": 5,
        "timestamp": int(time.time() * 1000),
        "node_id": 1
    }
    
    # Mock logs data
    logs_data = {
        "logs": [
            {"timestamp": int(time.time() * 1000), "level": "INFO", "module": "test", "message": "Test log message"},
            {"timestamp": int(time.time() * 1000), "level": "ERROR", "module": "test", "message": "Test error message"}
        ]
    }
    
    # Configure mocks
    mock.get = AsyncMock(side_effect=lambda url, **kwargs: (
        MagicMock(status_code=200, json=MagicMock(return_value=resource_data)) if "/resource/" in url else
        MagicMock(status_code=200, json=MagicMock(return_value=logs_data)) if "/logs" in url else
        MagicMock(status_code=200, json=MagicMock(return_value={"status": "healthy"}))
    ))
    
    return mock

@pytest.fixture
def web_server(mock_client, mock_http_client):
    """Fixture that creates a web server for tests."""
    from web_server import WebServer
    
    # Create web server
    server = WebServer(mock_client, host="127.0.0.1", port=8081)
    
    # Replace HTTP client
    server.http_client = mock_http_client
    
    return server

@pytest.fixture
def web_client(web_server):
    """Fixture that creates a test client for the web server."""
    return TestClient(web_server.app)

def test_index_route(web_client, mock_client):
    """Test the index route."""
    # Send request
    response = web_client.get("/")
    
    # Check response
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    
    # Check content - should contain client ID
    content = response.content.decode()
    assert "client-1" in content
    
    # Check if client method was called
    mock_client.get_status.assert_called_once()
    mock_client.get_history.assert_called_once()

def test_resources_route(web_client, mock_http_client):
    """Test the resources route."""
    # Reset the mock
    mock_http_client.get.reset_mock()
    
    # Send request
    response = web_client.get("/resources")
    
    # Check response
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    
    # Check content - corrigido para procurar por "Recursos" (português) em vez de "Resources"
    content = response.content.decode()
    assert "Recursos" in content
    
    # Check if HTTP client method was called
    assert mock_http_client.get.call_count >= 1
    
    # Check if request was made to store
    any_store_request = any("cluster-store" in str(call) for call in mock_http_client.get.call_args_list)
    assert any_store_request, "No request was made to cluster store"

def test_resource_route(web_client, mock_http_client):
    """Test the resource route."""
    # Reset the mock
    mock_http_client.get.reset_mock()
    
    # Send request
    response = web_client.get("/resource/R")
    
    # Check response
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    
    # Check content
    content = response.content.decode()
    assert "Recurso R" in content
    
    # Check if HTTP client method was called
    assert mock_http_client.get.call_count >= 1
    
    # Check if request was made to store
    any_store_request = any("cluster-store" in str(call) and "/resource/R" in str(call) 
                          for call in mock_http_client.get.call_args_list)
    assert any_store_request, "No request was made to cluster store for resource R"

def test_history_route(web_client, mock_client):
    """Test the history route."""
    # Reset the mock
    mock_client.get_history.reset_mock()
    
    # Send request
    response = web_client.get("/history")
    
    # Check response
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    
    # Check content
    content = response.content.decode()
    assert "Histórico de Operações" in content
    
    # Check if client method was called
    mock_client.get_history.assert_called_once()
    args, kwargs = mock_client.get_history.call_args
    assert kwargs.get("limit", 100) == 100  # Default limit

def test_logs_route(web_client):
    """Test the logs route."""
    # Send request
    response = web_client.get("/logs")
    
    # Check response
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    
    # Check content
    content = response.content.decode()
    assert "Logs do Cliente" in content

def test_system_logs_route(web_client, mock_http_client):
    """Test the system-logs route."""
    # Reset the mock
    mock_http_client.get.reset_mock()
    
    # Configure mock to return logs for all components
    mock_http_client.get.return_value = MagicMock(
        status_code=200,
        json=MagicMock(return_value={"logs": [
            {"timestamp": int(time.time() * 1000), "level": "INFO", "component": "proposer", "module": "test", "message": "Test proposer log"},
            {"timestamp": int(time.time() * 1000), "level": "ERROR", "component": "acceptor", "module": "test", "message": "Test acceptor error"}
        ]})
    )
    
    # Send request
    response = web_client.get("/system-logs")
    
    # Check response
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    
    # Check content
    content = response.content.decode()
    assert "Logs do Sistema" in content
    
    # Check if HTTP client method was called multiple times (to fetch logs from different components)
    assert mock_http_client.get.call_count > 0

def test_system_logs_important_route(web_client, mock_http_client):
    """Test the system-logs/important route."""
    # Reset the mock
    mock_http_client.get.reset_mock()
    
    # Configure mock to return logs for all components
    mock_http_client.get.return_value = MagicMock(
        status_code=200,
        json=MagicMock(return_value={"logs": [
            {"timestamp": int(time.time() * 1000), "level": "IMPORTANT", "component": "proposer", "module": "test", "message": "Important proposer log"},
            {"timestamp": int(time.time() * 1000), "level": "ERROR", "component": "acceptor", "module": "test", "message": "Error acceptor log"}
        ]})
    )
    
    # Send request
    response = web_client.get("/system-logs/important")
    
    # Check response
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    
    # Check content
    content = response.content.decode()
    assert "Logs Importantes do Sistema" in content
    
    # Check if HTTP client method was called
    assert mock_http_client.get.call_count > 0

def test_api_status_route(web_client, mock_client):
    """Test the API status route."""
    # Reset the mock
    mock_client.get_status.reset_mock()
    
    # Send request
    response = web_client.get("/api/status")
    
    # Check response
    assert response.status_code == 200
    data = response.json()
    assert data["client_id"] == "client-1"
    
    # Check if client method was called
    mock_client.get_status.assert_called_once()

def test_api_history_route(web_client, mock_client):
    """Test the API history route."""
    # Reset the mock
    mock_client.get_history.reset_mock()
    
    # Send request
    response = web_client.get("/api/history")
    
    # Check response
    assert response.status_code == 200
    data = response.json()
    assert "operations" in data
    
    # Check if client method was called
    mock_client.get_history.assert_called_once()
    args, kwargs = mock_client.get_history.call_args
    assert kwargs.get("limit", 100) == 100  # Default limit

def test_api_start_route(web_client, mock_client):
    """Test the API start route."""
    # Reset the mock
    mock_client.start.reset_mock()
    
    # Send request
    response = web_client.post("/api/start")
    
    # Check response
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "started"
    
    # Check if client method was called
    mock_client.start.assert_called_once()

def test_api_stop_route(web_client, mock_client):
    """Test the API stop route."""
    # Reset the mock
    mock_client.stop.reset_mock()
    
    # Send request
    response = web_client.post("/api/stop")
    
    # Check response
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "stopped"
    
    # Check if client method was called
    mock_client.stop.assert_called_once()

def test_api_operation_route(web_client, mock_client):
    """Test the API operation route."""
    # Set client as running
    mock_client.running = True
    
    # Create mock for asyncio.create_task
    with patch('asyncio.create_task') as mock_create_task:
        # Send request
        response = web_client.post("/api/operation", json="test data")
        
        # Check response
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "initiated"
        assert "operation_id" in data
        
        # Check if task was created
        mock_create_task.assert_called_once()