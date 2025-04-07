"""
File: client/tests/integration/test_integration.py
Integration tests for the client component.
"""
import os
import json
import time
import pytest
import asyncio
import tempfile
import shutil
from unittest.mock import MagicMock, patch, AsyncMock
from fastapi.testclient import TestClient

import warnings
warnings.filterwarnings("always", category=RuntimeWarning)

# Configure environment for testing
os.environ["DEBUG"] = "true"
os.environ["DEBUG_LEVEL"] = "basic"
os.environ["NODE_ID"] = "1"
os.environ["CLIENT_ID"] = "client-1"
os.environ["PROPOSER"] = "localhost:8080"
os.environ["LOG_DIR"] = "/tmp/paxos-test-logs"

# Create test directory
os.makedirs(os.environ["LOG_DIR"], exist_ok=True)

# Fixture for setup and teardown of test environment
@pytest.fixture(scope="module")
def setup_integration():
    """Setup function executed once before all integration tests."""
    # Create temporary data directory
    temp_dir = tempfile.mkdtemp()
    
    # Configure mock for HTTP client
    http_client_mock = AsyncMock()
    http_client_mock.post.return_value = MagicMock(
        status_code=202,
        json=MagicMock(return_value={"instanceId": 42})
    )
    http_client_mock.get.return_value = MagicMock(
        status_code=200,
        json=MagicMock(return_value={"data": "test", "version": 1})
    )
    
    # Import necessary modules
    from client import PaxosClient
    from api import create_api
    from web_server import WebServer
    from common.logging import setup_logging
    from common.communication import HttpClient
    
    # Patch HTTP client
    with patch.object(HttpClient, 'post', new=http_client_mock.post), \
         patch.object(HttpClient, 'get', new=http_client_mock.get):
        # Setup logging
        setup_logging("client-test", debug=True, debug_level="basic", log_dir=os.environ["LOG_DIR"])
        
        # Create client
        client = PaxosClient(
            client_id="client-1",
            proposer_url=f"http://{os.environ['PROPOSER']}",
            callback_url=f"http://client-1:8080/notification",
            num_operations=20
        )
        
        # Create API
        app = create_api(client)
        api_client = TestClient(app)
        
        # Create web server
        web_server = WebServer(client, host="127.0.0.1", port=8081)
        web_client = TestClient(web_server.app)
        
        # Start client in a non-blocking way
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(client.start())
        
        # Return components for tests
        yield {
            "temp_dir": temp_dir,
            "client": client,
            "api_client": api_client,
            "web_client": web_client,
            "http_client_mock": http_client_mock,
            "loop": loop
        }
        
        # Cleanup
        try:
            if not loop.is_closed():
                loop.run_until_complete(client.stop())
        except RuntimeError as e:
            if "Event loop is closed" in str(e):
                # Create a new loop if the previous one is closed
                new_loop = asyncio.new_event_loop()
                asyncio.set_event_loop(new_loop)
                new_loop.run_until_complete(client.stop())
                new_loop.close()
            else:
                raise
        
        # Close the original loop if still open
        if not loop.is_closed():
            loop.close()

        try:
            shutil.rmtree(temp_dir)
            shutil.rmtree(os.environ["LOG_DIR"])
        except:
            pass

def test_client_operation_flow(setup_integration):
    """Test the complete flow of client operations."""
    client = setup_integration["client"]
    api_client = setup_integration["api_client"]
    http_client_mock = setup_integration["http_client_mock"]
    
    # Reset HTTP client mock
    http_client_mock.post.reset_mock()
    
    # Configure mock to return success
    http_client_mock.post.return_value = MagicMock(
        status_code=202,
        json=MagicMock(return_value={"instanceId": 42})
    )

    # Garantir que o cliente HTTP é o mock
    assert client.http_client is http_client_mock
    
    # Create test event for synchronization
    operation_complete = asyncio.Event()
    original_process_notification = client.process_notification
    
    def process_notification_with_signal(notification):
        result = original_process_notification(notification)
        # Signal completion for synchronization
        loop = asyncio.get_event_loop()
        loop.call_soon_threadsafe(operation_complete.set)
        return result
    
    # Replace process_notification method
    client.process_notification = process_notification_with_signal
    
    # Clear operation history
    client.history = []
    client.operations_completed = 0
    client.operations_failed = 0
    client.operations_in_progress = {}
    
    # First check status API
    response = api_client.get("/status")
    assert response.status_code == 200
    status_data = response.json()
    assert status_data["running"] == True
    
    # Submit a manual operation
    operation_data = "test data"
    response = api_client.post("/operation", json={"data": operation_data})
    assert response.status_code == 200
    operation_result = response.json()
    assert operation_result["status"] == "initiated"
    assert "operation_id" in operation_result
    operation_id = operation_result["operation_id"]
    
    # Check that HTTP client was called for propose
    http_client_mock.post.assert_called_once()
    call_args = http_client_mock.post.call_args
    assert "propose" in str(call_args)
    
    # Check that operation is in progress
    response = api_client.get("/history")
    assert response.status_code == 200
    history_data = response.json()
    
    # Check if operation is in progress
    operation_in_history = any(op.get("id") == operation_id for op in history_data.get("operations", []))
    if not operation_in_history:
        # It might not be in history yet if it's still in operations_in_progress
        assert 42 in client.operations_in_progress
        assert client.operations_in_progress[42]["id"] == operation_id
    
    # Send notification to simulate learner response
    notification = {
        "status": "COMMITTED",
        "instanceId": 42,
        "resource": "R",
        "timestamp": int(time.time() * 1000)
    }
    
    # Reset HTTP client mock
    http_client_mock.post.reset_mock()
    
    # Send notification
    response = api_client.post("/notification", json=notification)
    assert response.status_code == 200
    notification_result = response.json()
    assert notification_result["status"] == "acknowledged"
    assert notification_result["known"] == True
    
    # Wait a bit for async processing
    time.sleep(0.1)
    
    # Check that operation was completed
    assert client.operations_completed == 1
    assert client.operations_failed == 0
    assert 42 not in client.operations_in_progress
    
    # Check history again
    response = api_client.get("/history")
    assert response.status_code == 200
    history_data = response.json()
    
    # Find operation in history
    operation = next((op for op in history_data.get("operations", []) if op.get("id") == operation_id), None)
    assert operation is not None
    assert operation["status"] == "COMMITTED"
    
    # Restore original method
    client.process_notification = original_process_notification

def test_web_interface_integration(setup_integration):
    """Test the web interface integration with client."""
    client = setup_integration["client"]
    web_client = setup_integration["web_client"]
    http_client_mock = setup_integration["http_client_mock"]
    
    # Reset HTTP client mock
    http_client_mock.get.reset_mock()
    
    # Configure mock for resource data
    http_client_mock.get.return_value = MagicMock(
        status_code=200,
        json=MagicMock(return_value={
            "data": "Test resource data",
            "version": 5,
            "timestamp": int(time.time() * 1000),
            "node_id": 1
        })
    )
    
    # Add some operations to history
    client.history = [
        {"id": 1, "start_time": time.time() - 30, "status": "COMMITTED", "latency": 1.5, "instance_id": 101},
        {"id": 2, "start_time": time.time() - 20, "status": "NOT_COMMITTED", "latency": 2.0, "instance_id": 102}
    ]
    
    # Test dashboard
    response = web_client.get("/")
    assert response.status_code == 200
    
    # Test resources page
    response = web_client.get("/resources")
    assert response.status_code == 200
    
    # Test history page
    response = web_client.get("/history")
    assert response.status_code == 200
    assert "Histórico de Operações" in response.content.decode()
    
    # Verify both operations appear in history
    content = response.content.decode()
    assert "COMMITTED" in content
    assert "NOT_COMMITTED" in content
    
    # Test logs page
    response = web_client.get("/logs")
    assert response.status_code == 200
    
    # Test system logs
    response = web_client.get("/system-logs")
    assert response.status_code == 200
    
    # Test resource page
    response = web_client.get("/resource/R")
    assert response.status_code == 200
    assert "Test resource data" in response.content.decode()
    
    # Check HTTP client was called to fetch resource
    assert http_client_mock.get.call_count > 0
    any_resource_request = any("/resource/R" in str(call) for call in http_client_mock.get.call_args_list)
    assert any_resource_request, "No request was made to fetch resource R"

def test_retries_and_timeouts(setup_integration):
    """Test retry mechanism and timeout handling."""
    client = setup_integration["client"]
    api_client = setup_integration["api_client"]
    http_client_mock = setup_integration["http_client_mock"]
    
    # Reset HTTP client mock
    http_client_mock.post.reset_mock()
    
    # Configure mock to fail first, then succeed
    http_client_mock.post.side_effect = [
        MagicMock(
            status_code=500,
            text="Internal error"
        ),
        MagicMock(
            status_code=202,
            json=MagicMock(return_value={"instanceId": 43})
        )
    ]
    
    # Submit an operation
    with patch('asyncio.sleep', AsyncMock()):  # Mock sleep to speed up test
        response = api_client.post("/operation", json={"data": "retry test"})
    
    assert response.status_code == 200
    operation_result = response.json()
    assert operation_result["status"] == "initiated"
    
    # Check that HTTP client was called twice
    assert http_client_mock.post.call_count == 2
    
    # Test timeout handling
    # Create a pending operation
    instance_id = 100
    operation_info = {
        "id": 99,
        "start_time": time.time() - 2.0,
        "payload": {"data": "timeout test"},
        "status": "in_progress"
    }
    client.operations_in_progress[instance_id] = operation_info
    
    # Call timeout handler manually with patched sleep
    with patch('asyncio.sleep', AsyncMock()):
        asyncio.run_coroutine_threadsafe(
            client._handle_operation_timeout(instance_id),
            setup_integration["loop"]
        )
    
    # Wait a bit for async processing
    time.sleep(0.1)
    
    # Check that operation was marked as timeout
    assert instance_id not in client.operations_in_progress
    timeout_operation = next((op for op in client.history if op.get("id") == 99), None)
    assert timeout_operation is not None
    assert timeout_operation["status"] == "timeout"

def test_notification_handling(setup_integration):
    """Test handling of notifications from learners."""
    client = setup_integration["client"]
    api_client = setup_integration["api_client"]
    
    # Clear operations first
    client.operations_in_progress = {}
    client.history = []
    client.operations_completed = 0
    client.operations_failed = 0
    
    # Create a pending operation
    instance_id = 200
    operation_info = {
        "id": 50,
        "start_time": time.time() - 1.0,
        "payload": {"data": "notification test"},
        "status": "in_progress"
    }
    client.operations_in_progress[instance_id] = operation_info
    
    # Send committed notification
    notification1 = {
        "status": "COMMITTED",
        "instanceId": instance_id,
        "resource": "R",
        "timestamp": int(time.time() * 1000)
    }
    
    response = api_client.post("/notification", json=notification1)
    assert response.status_code == 200
    
    # Check that operation was completed
    assert instance_id not in client.operations_in_progress
    assert client.operations_completed == 1
    assert client.operations_failed == 0
    
    # Create another pending operation
    instance_id = 201
    operation_info = {
        "id": 51,
        "start_time": time.time() - 1.0,
        "payload": {"data": "notification failure test"},
        "status": "in_progress"
    }
    client.operations_in_progress[instance_id] = operation_info
    
    # Send not committed notification
    notification2 = {
        "status": "NOT_COMMITTED",
        "instanceId": instance_id,
        "resource": "R",
        "timestamp": int(time.time() * 1000)
    }
    
    response = api_client.post("/notification", json=notification2)
    assert response.status_code == 200
    
    # Check that operation was failed
    assert instance_id not in client.operations_in_progress
    assert client.operations_completed == 1
    assert client.operations_failed == 1
    
    # Send notification for unknown instance
    notification3 = {
        "status": "COMMITTED",
        "instanceId": 999,
        "resource": "R",
        "timestamp": int(time.time() * 1000)
    }
    
    response = api_client.post("/notification", json=notification3)
    assert response.status_code == 200
    result = response.json()
    assert result["known"] == False
    
    # Check that stats didn't change
    assert client.operations_completed == 1
    assert client.operations_failed == 1

# Fixture for setup and teardown of test environment
@pytest.fixture(scope="module")
def setup_integration():
    """Setup function executed once before all integration tests."""
    # Create temporary data directory
    temp_dir = tempfile.mkdtemp()
    
    # Configure mock for HTTP client
    http_client_mock = AsyncMock()
    http_client_mock.post.return_value = MagicMock(
        status_code=202,
        json=MagicMock(return_value={"instanceId": 42})
    )
    http_client_mock.get.return_value = MagicMock(
        status_code=200,
        json=MagicMock(return_value={"data": "test", "version": 1})
    )
    
    # Import necessary modules
    from client import PaxosClient
    from api import create_api
    from web_server import WebServer
    from common.logging import setup_logging
    from common.communication import HttpClient
    
    # Patch HTTP client
    with patch.object(HttpClient, 'post', new=http_client_mock.post), \
         patch.object(HttpClient, 'get', new=http_client_mock.get):
        # Setup logging
        setup_logging("client-test", debug=True, debug_level="basic", log_dir=os.environ["LOG_DIR"])
        
        # Create client
        client = PaxosClient(
            client_id="client-1",
            proposer_url=f"http://{os.environ['PROPOSER']}",
            callback_url=f"http://client-1:8080/notification",
            num_operations=20
        )
        
        # Create API
        app = create_api(client)
        api_client = TestClient(app)
        
        # Create web server
        web_server = WebServer(client, host="127.0.0.1", port=8081)
        web_client = TestClient(web_server.app)
        
        # Start client in a non-blocking way
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(client.start())
        
        # Return components for tests
        yield {
            "temp_dir": temp_dir,
            "client": client,
            "api_client": api_client,
            "web_client": web_client,
            "http_client_mock": http_client_mock,
            "loop": loop
        }
        
        # Cleanup
        try:
            if not loop.is_closed():
                loop.run_until_complete(client.stop())
        except RuntimeError as e:
            if "Event loop is closed" in str(e):
                # Create a new loop if the previous one is closed
                new_loop = asyncio.new_event_loop()
                asyncio.set_event_loop(new_loop)
                new_loop.run_until_complete(client.stop())
                new_loop.close()
            else:
                raise
        
        # Close the original loop if still open
        if not loop.is_closed():
            loop.close()

        try:
            shutil.rmtree(temp_dir)
            shutil.rmtree(os.environ["LOG_DIR"])
        except:
            pass

def test_web_interface_operation_form(setup_integration):
    """Test the manual operation form in web interface."""
    client = setup_integration["client"]
    web_client = setup_integration["web_client"]
    http_client_mock = setup_integration["http_client_mock"]
    
    # Configure mock to succeed
    http_client_mock.post.reset_mock()
    http_client_mock.post.return_value = MagicMock(
        status_code=202,
        json=MagicMock(return_value={"instanceId": 200})
    )
    
    # First get the form page
    response = web_client.get("/")
    assert response.status_code == 200
    
    # Simulate form submission via AJAX
    response = web_client.post("/api/operation", json="test operation data")
    assert response.status_code == 200
    
    # Check response contains operation ID
    result = response.json()
    assert "status" in result
    assert result["status"] == "initiated"
    assert "operation_id" in result
    
    # Check HTTP client was called
    assert http_client_mock.post.call_count > 0
    
    # Wait a bit for async processing
    time.sleep(0.1)
    
    # Check that operation is in progress
    assert 200 in client.operations_in_progress

def test_resource_editing_flow(setup_integration):
    """Test the resource editing flow."""
    client = setup_integration["client"]
    web_client = setup_integration["web_client"]
    http_client_mock = setup_integration["http_client_mock"]
    
    # Configure mock to return resource data
    http_client_mock.get.return_value = MagicMock(
        status_code=200,
        json=MagicMock(return_value={
            "data": "Original resource data",
            "version": 5,
            "timestamp": int(time.time() * 1000),
            "node_id": 1
        })
    )
    
    # Reset HTTP client mock for POST
    http_client_mock.post.reset_mock()
    http_client_mock.post.return_value = MagicMock(
        status_code=202,
        json=MagicMock(return_value={"instanceId": 201})
    )
    
    # First get the resource edit page
    response = web_client.get("/resource/R")
    assert response.status_code == 200
    assert "Original resource data" in response.content.decode()
    
    # Simulate form submission via AJAX
    response = web_client.post("/api/operation", json="Updated resource data")
    assert response.status_code == 200
    
    # Check response contains operation ID
    result = response.json()
    assert "operation_id" in result
    
    # Check HTTP client was called
    assert http_client_mock.post.call_count > 0
    
    # Wait a bit for async processing
    time.sleep(0.1)
    
    # Check that operation is in progress
    assert 201 in client.operations_in_progress

def test_system_logs_display(setup_integration):
    """Test the system logs display functionality."""
    web_client = setup_integration["web_client"]
    http_client_mock = setup_integration["http_client_mock"]
    
    # Configure mock to return logs for important view
    http_client_mock.get.reset_mock()
    
    # Setup a response that returns different logs for each component type
    def get_mock_response(url, **kwargs):
        if "proposer" in url:
            return MagicMock(
                status_code=200, 
                json=MagicMock(return_value={"logs": [
                    {"timestamp": int(time.time() * 1000), "level": "IMPORTANT", "component": "proposer", "module": "test", "message": "Proposer log"}
                ]})
            )
        elif "acceptor" in url:
            return MagicMock(
                status_code=200, 
                json=MagicMock(return_value={"logs": [
                    {"timestamp": int(time.time() * 1000), "level": "ERROR", "component": "acceptor", "module": "test", "message": "Acceptor error"}
                ]})
            )
        elif "store" in url:
            return MagicMock(
                status_code=200, 
                json=MagicMock(return_value={"logs": [
                    {"timestamp": int(time.time() * 1000), "level": "WARNING", "component": "store", "module": "test", "message": "Store warning"}
                ]})
            )
        else:
            return MagicMock(status_code=200, json=MagicMock(return_value={"logs": []}))
    
    http_client_mock.get.side_effect = get_mock_response
    
    # Get the important logs page
    response = web_client.get("/system-logs/important")
    assert response.status_code == 200
    
    content = response.content.decode()
    
    # Check that logs from different components are displayed
    assert "Proposer log" in content
    assert "Acceptor error" in content
    assert "Store warning" in content
    
    # Check that the proper classes are applied
    assert "level-IMPORTANT" in content
    assert "level-ERROR" in content
    assert "level-WARNING" in content

def test_client_stop_during_operation(setup_integration):
    """Test stopping the client during an operation."""
    client = setup_integration["client"]
    api_client = setup_integration["api_client"]
    http_client_mock = setup_integration["http_client_mock"]
    
    # Reset HTTP client mock
    http_client_mock.post.reset_mock()
    http_client_mock.post.return_value = MagicMock(
        status_code=202,
        json=MagicMock(return_value={"instanceId": 300})
    )
    
    # Start an operation
    response = api_client.post("/operation", json={"data": "operation during stop test"})
    assert response.status_code == 200
    
    # Wait a bit for async processing
    time.sleep(0.1)
    
    # Check that operation is in progress
    assert 300 in client.operations_in_progress
    
    # Stop the client
    response = api_client.post("/stop")
    assert response.status_code == 200
    
    # Wait a bit for async processing
    time.sleep(0.1)
    
    # Client should be stopped
    assert client.running is False
    
    # Operation should still be in progress (stop doesn't cancel pending operations)
    assert 300 in client.operations_in_progress
    
    # Send a notification for the operation
    notification = {
        "status": "COMMITTED",
        "instanceId": 300,
        "resource": "R",
        "timestamp": int(time.time() * 1000)
    }
    
    response = api_client.post("/notification", json=notification)
    assert response.status_code == 200
    
    # Wait a bit for async processing
    time.sleep(0.1)
    
    # Operation should be completed despite client being stopped
    assert 300 not in client.operations_in_progress
    assert client.operations_completed == 1

def test_client_restart_after_stop(setup_integration):
    """Test restarting the client after stopping it."""
    client = setup_integration["client"]
    api_client = setup_integration["api_client"]
    
    # Make sure client is stopped
    client.running = False
    
    # Start client
    response = api_client.post("/start")
    assert response.status_code == 200
    
    # Wait a bit for async processing
    time.sleep(0.1)
    
    # Client should be running
    assert client.running is True
    
    # Stop client again for cleanup
    response = api_client.post("/stop")
    assert response.status_code == 200