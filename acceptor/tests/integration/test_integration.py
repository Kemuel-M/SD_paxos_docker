"""
File: acceptor/tests/integration/test_integration.py
Integration tests for the Acceptor component.
"""
import os
import json
import time
import pytest
import asyncio
import tempfile
import shutil
import threading
from pathlib import Path
from unittest.mock import MagicMock, patch, AsyncMock
from fastapi.testclient import TestClient
from concurrent.futures import ThreadPoolExecutor

import warnings
warnings.filterwarnings("always", category=RuntimeWarning)

# Configure environment for testing
os.environ["DEBUG"] = "true"
os.environ["DEBUG_LEVEL"] = "basic"
os.environ["NODE_ID"] = "1"
os.environ["LEARNERS"] = "localhost:8091,localhost:8092"
os.environ["LOG_DIR"] = "/tmp/paxos-test-logs"

# Create directories
os.makedirs(os.environ["LOG_DIR"], exist_ok=True)

# Fixture for setup and teardown of test environment
@pytest.fixture(scope="module")
def setup_integration():
    """Setup function executed once before all integration tests."""
    # Create temporary data directory
    temp_dir = tempfile.mkdtemp()
    
    # Configure mock for HTTP client
    http_client_mock = AsyncMock()
    http_client_mock.post.return_value = {"status": "ok"}
    
    # Import necessary modules
    from acceptor import Acceptor
    from persistence import AcceptorPersistence
    from api import create_api
    from common.logging import setup_logging
    from common.communication import HttpClient
    
    # Patch HTTP client
    with patch.object(HttpClient, 'post', new=http_client_mock.post):
        # Setup logging
        setup_logging("acceptor-test", debug=True, debug_level="basic", log_dir=os.environ["LOG_DIR"])
        
        # Create persistence
        persistence = AcceptorPersistence(node_id=1, data_dir=temp_dir)
        
        # Create acceptor
        acceptor = Acceptor(
            node_id=1,
            learners=os.environ["LEARNERS"].split(","),
            persistence=persistence
        )
        
        # Create API
        app = create_api(acceptor, persistence)
        client = TestClient(app)
        
        # Start acceptor
        loop = asyncio.get_event_loop()
        loop.run_until_complete(acceptor.start())
        
        # Return components for tests
        yield {
            "temp_dir": temp_dir,
            "acceptor": acceptor,
            "persistence": persistence,
            "client": client,
            "http_client_mock": http_client_mock
        }
        
        # Cleanup
        loop = asyncio.get_event_loop()
        loop.run_until_complete(acceptor.stop())
        loop.run_until_complete(asyncio.sleep(0.1))  # Dê tempo para tarefas finalizarem
        tasks = asyncio.all_tasks(loop)
        for task in tasks:
            if not task.done() and task != asyncio.current_task():
                task.cancel()
        loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
        try:
            shutil.rmtree(temp_dir)
            shutil.rmtree(os.environ["LOG_DIR"])
        except:
            pass

def test_full_prepare_flow(setup_integration):
    """Test the complete prepare flow."""
    client = setup_integration["client"]
    acceptor = setup_integration["acceptor"]
    
    # Prepare request
    prepare_request = {
        "type": "PREPARE",
        "proposalNumber": 42,
        "instanceId": 100,
        "proposerId": 2,
        "clientRequest": {
            "clientId": "client-1",
            "timestamp": int(time.time() * 1000),
            "operation": "WRITE",
            "resource": "R",
            "data": "test data"
        }
    }
    
    # Send prepare request
    response = client.post("/prepare", json=prepare_request)
    
    # Check response
    assert response.status_code == 200
    data = response.json()
    assert data["accepted"] == True
    assert data["highestAccepted"] == -1
    assert data["instanceId"] == 100
    assert data["acceptorId"] == 1
    
    # Check state
    assert 100 in acceptor.promises
    assert acceptor.promises[100] == 42
    assert acceptor.prepare_requests_processed >= 1
    assert acceptor.promises_made >= 1
    
    # Send second prepare request with lower number
    prepare_request2 = {
        "type": "PREPARE",
        "proposalNumber": 20,
        "instanceId": 100,
        "proposerId": 3
    }
    
    response2 = client.post("/prepare", json=prepare_request2)
    
    # Check response
    assert response2.status_code == 200
    data2 = response2.json()
    assert data2["accepted"] == False
    assert data2["highestPromised"] == 42
    
    # Check state (shouldn't change)
    assert acceptor.promises[100] == 42
    
    # Send third prepare request with higher number
    prepare_request3 = {
        "type": "PREPARE",
        "proposalNumber": 50,
        "instanceId": 100,
        "proposerId": 3
    }
    
    response3 = client.post("/prepare", json=prepare_request3)
    
    # Check response
    assert response3.status_code == 200
    data3 = response3.json()
    assert data3["accepted"] == True
    assert data3["highestAccepted"] == -1
    
    # Check state (should update)
    assert acceptor.promises[100] == 50

def test_full_accept_flow(setup_integration):
    """Test the complete accept flow."""
    client = setup_integration["client"]
    acceptor = setup_integration["acceptor"]
    http_client_mock = setup_integration["http_client_mock"]
    
    # Reset HTTP client mock
    http_client_mock.post.reset_mock()
    
    # Accept request
    accept_request = {
        "type": "ACCEPT",
        "proposalNumber": 60,
        "instanceId": 200,
        "proposerId": 2,
        "value": {
            "clientId": "client-1",
            "timestamp": int(time.time() * 1000),
            "operation": "WRITE",
            "resource": "R",
            "data": "test accepted data"
        }
    }
    
    # Send accept request
    response = client.post("/accept", json=accept_request)
    
    # Check response
    assert response.status_code == 200
    data = response.json()
    assert data["accepted"] == True
    assert data["proposalNumber"] == 60
    assert data["instanceId"] == 200
    assert data["acceptorId"] == 1
    
    # Check state
    assert 200 in acceptor.promises
    assert 200 in acceptor.accepted
    assert acceptor.promises[200] == 60
    assert acceptor.accepted[200][0] == 60
    assert acceptor.accepted[200][1]["data"] == "test accepted data"
    assert acceptor.accept_requests_processed >= 1
    assert acceptor.proposals_accepted >= 1
    
    # Verify notification to learners
    # Wait a bit for the notification to complete
    time.sleep(0.1)
    assert http_client_mock.post.call_count == 2  # Both learners
    
    # Check that notification was sent with correct data
    for call in http_client_mock.post.call_args_list:
        args, kwargs = call
        url = args[0]
        notification = kwargs.get("json", {})
        
        assert url.endswith("/learn")
        assert notification["type"] == "LEARN"
        assert notification["instanceId"] == 200
        assert notification["proposalNumber"] == 60
        assert notification["acceptorId"] == 1
        assert notification["accepted"] == True
        assert notification["value"]["data"] == "test accepted data"
        assert "timestamp" in notification
    
    # Send second accept request with lower number (should be rejected)
    accept_request2 = {
        "type": "ACCEPT",
        "proposalNumber": 40,
        "instanceId": 200,
        "proposerId": 3,
        "value": {
            "clientId": "client-2",
            "timestamp": int(time.time() * 1000),
            "operation": "WRITE",
            "resource": "R",
            "data": "different data"
        }
    }
    
    # Reset HTTP client mock
    http_client_mock.post.reset_mock()
    
    # Send request
    response2 = client.post("/accept", json=accept_request2)
    
    # Check response
    assert response2.status_code == 200
    data2 = response2.json()
    assert data2["accepted"] == False
    assert data2["highestPromised"] == 60
    
    # Check state (shouldn't change)
    assert acceptor.accepted[200][1]["data"] == "test accepted data"
    
    # No notifications should have been sent
    assert http_client_mock.post.call_count == 0

def test_persistence(setup_integration):
    """Test that state is persisted and can be loaded."""
    acceptor = setup_integration["acceptor"]
    persistence = setup_integration["persistence"]
    temp_dir = setup_integration["temp_dir"]

    # Setup some state
    acceptor.promises = {300: 70, 301: 80}
    acceptor.accepted = {300: (70, {"data": "persistent data"})}
    acceptor.prepare_requests_processed = 10
    acceptor.accept_requests_processed = 5
    acceptor.promises_made = 8
    acceptor.proposals_accepted = 3

    # Save state
    loop = asyncio.get_event_loop()
    loop.run_until_complete(persistence.save_state({
        "promises": acceptor.promises,
        "accepted": acceptor.accepted,
        "prepare_requests_processed": acceptor.prepare_requests_processed,
        "accept_requests_processed": acceptor.accept_requests_processed,
        "promises_made": acceptor.promises_made,
        "proposals_accepted": acceptor.proposals_accepted
    }))

    # Check if state file exists
    state_file = Path(temp_dir) / f"acceptor_1_state.json"
    assert state_file.exists()

    # Create a new persistence instance and load state
    new_persistence = setup_integration["persistence"].__class__(node_id=1, data_dir=temp_dir)
    
    # Carregamento síncrono
    loaded_state = new_persistence.load_state()

    # Check loaded state (keys are converted to strings in JSON)
    assert str(300) in loaded_state["promises"]
    assert str(301) in loaded_state["promises"]
    assert str(300) in loaded_state["accepted"]
    assert loaded_state["promises"][str(300)] == 70
    assert loaded_state["promises"][str(301)] == 80
    assert loaded_state["accepted"][str(300)][0] == 70
    assert loaded_state["accepted"][str(300)][1]["data"] == "persistent data"
    assert loaded_state["prepare_requests_processed"] == 10
    assert loaded_state["accept_requests_processed"] == 5
    assert loaded_state["promises_made"] == 8
    assert loaded_state["proposals_accepted"] == 3

def test_recovery_after_crash(setup_integration):
    """Test that the acceptor can recover from a crash by loading persisted state."""
    acceptor = setup_integration["acceptor"]
    persistence = setup_integration["persistence"]
    temp_dir = setup_integration["temp_dir"]
    client = setup_integration["client"]
    
    # Clear current state and set up test state
    instance_id = 400
    original_proposal_num = 100
    original_value = {"data": "recovery test data"}
    
    acceptor.promises = {instance_id: original_proposal_num}
    acceptor.accepted = {instance_id: (original_proposal_num, original_value)}
    
    # Save state to persistence
    loop = asyncio.get_event_loop()
    loop.run_until_complete(persistence.save_state({
        "promises": acceptor.promises,
        "accepted": acceptor.accepted,
        "prepare_requests_processed": acceptor.prepare_requests_processed,
        "accept_requests_processed": acceptor.accept_requests_processed,
        "promises_made": acceptor.promises_made,
        "proposals_accepted": acceptor.proposals_accepted
    }))
    
    # Simulate crash by creating new acceptor instance
    from acceptor import Acceptor
    
    # Create new acceptor with same persistence
    new_acceptor = Acceptor(
        node_id=1,
        learners=os.environ["LEARNERS"].split(","),
        persistence=persistence
    )
    
    # Start new acceptor (this should load state from persistence)
    loop.run_until_complete(new_acceptor.start())
    
    # Verify state was loaded correctly
    assert instance_id in new_acceptor.promises
    assert instance_id in new_acceptor.accepted
    assert new_acceptor.promises[instance_id] == original_proposal_num
    assert new_acceptor.accepted[instance_id][0] == original_proposal_num
    assert new_acceptor.accepted[instance_id][1]["data"] == "recovery test data"
    
    # Now test continue working with loaded state
    # Send a prepare request with higher number
    # Replace acceptor in client app
    from api import create_api
    app = create_api(new_acceptor, persistence)
    client = TestClient(app)
    
    prepare_request = {
        "type": "PREPARE",
        "proposalNumber": 150,  # Higher than original
        "instanceId": instance_id,
        "proposerId": 2
    }
    
    # Send prepare request
    response = client.post("/prepare", json=prepare_request)
    
    # Check response
    assert response.status_code == 200
    data = response.json()
    assert data["accepted"] == True
    assert data["highestAccepted"] == original_proposal_num
    assert "acceptedValue" in data
    assert data["acceptedValue"]["data"] == "recovery test data"
    
    # Clean up
    loop.run_until_complete(new_acceptor.stop())

def test_identical_proposal_numbers(setup_integration):
    """Test behavior when receiving requests with identical proposal numbers."""
    client = setup_integration["client"]
    acceptor = setup_integration["acceptor"]
    
    instance_id = 500
    proposal_num = 200
    
    # First prepare with this number
    prepare_request1 = {
        "type": "PREPARE",
        "proposalNumber": proposal_num,
        "instanceId": instance_id,
        "proposerId": 1
    }
    
    response1 = client.post("/prepare", json=prepare_request1)
    
    # Check response
    assert response1.status_code == 200
    data1 = response1.json()
    assert data1["accepted"] == True
    
    # Second prepare with same number but different proposer
    prepare_request2 = {
        "type": "PREPARE",
        "proposalNumber": proposal_num,
        "instanceId": instance_id,
        "proposerId": 2
    }
    
    response2 = client.post("/prepare", json=prepare_request2)
    
    # Check response - should be rejected as it's not greater than current promise
    assert response2.status_code == 200
    data2 = response2.json()
    assert data2["accepted"] == False
    
    # Now accept with the same number as promised
    accept_request = {
        "type": "ACCEPT",
        "proposalNumber": proposal_num,
        "instanceId": instance_id,
        "proposerId": 1,
        "value": {"data": "identical number test"}
    }
    
    response3 = client.post("/accept", json=accept_request)
    
    # Check response - should be accepted as it's the same as promised
    assert response3.status_code == 200
    data3 = response3.json()
    assert data3["accepted"] == True
    
    # Another accept with same number but different proposer
    accept_request2 = {
        "type": "ACCEPT",
        "proposalNumber": proposal_num,
        "instanceId": instance_id,
        "proposerId": 2,
        "value": {"data": "different value"}
    }
    
    response4 = client.post("/accept", json=accept_request2)
    
    # Check response - should be accepted as the number matches (per Paxos spec)
    assert response4.status_code == 200
    data4 = response4.json()
    assert data4["accepted"] == True

def test_concurrent_requests(setup_integration):
    """Test handling of concurrent prepare/accept requests."""
    client = setup_integration["client"]
    
    # Function to send a prepare request
    def send_prepare(instance_id, proposal_number, proposer_id):
        prepare_request = {
            "type": "PREPARE",
            "proposalNumber": proposal_number,
            "instanceId": instance_id,
            "proposerId": proposer_id
        }
        return client.post("/prepare", json=prepare_request)
    
    # Function to send an accept request
    def send_accept(instance_id, proposal_number, proposer_id, value):
        accept_request = {
            "type": "ACCEPT",
            "proposalNumber": proposal_number,
            "instanceId": instance_id,
            "proposerId": proposer_id,
            "value": {"data": value}
        }
        return client.post("/accept", json=accept_request)
    
    # Test concurrent prepare requests for the same instance
    instance_id = 600
    
    # Define parameters for concurrent requests
    prepare_params = [
        (instance_id, 300, 1),
        (instance_id, 310, 2),
        (instance_id, 305, 3),
        (instance_id, 315, 4),
        (instance_id, 320, 5)
    ]
    
    # Send concurrent prepare requests
    with ThreadPoolExecutor(max_workers=5) as executor:
        prepare_futures = [executor.submit(send_prepare, *params) for params in prepare_params]
        prepare_responses = [future.result() for future in prepare_futures]
    
    # Check responses
    accepted_count = sum(1 for resp in prepare_responses if resp.json().get("accepted", False))
    rejected_count = len(prepare_responses) - accepted_count
    
    # Only the highest proposal numbers should be accepted
    assert accepted_count >= 1
    assert rejected_count >= 0
    
    # Test concurrent accept requests
    accept_params = [
        (instance_id, 320, 5, "value1"),  # Same as highest prepare
        (instance_id, 325, 1, "value2"),  # Higher than any prepare
        (instance_id, 330, 2, "value3"),  # Even higher
        (instance_id, 315, 3, "value4"),  # Lower than highest accepted
        (instance_id, 310, 4, "value5")   # Even lower
    ]
    
    # Send concurrent accept requests
    with ThreadPoolExecutor(max_workers=5) as executor:
        accept_futures = [executor.submit(send_accept, *params) for params in accept_params]
        accept_responses = [future.result() for future in accept_futures]
    
    # Check responses
    accepted_count = sum(1 for resp in accept_responses if resp.json().get("accepted", False))
    rejected_count = len(accept_responses) - accepted_count
    
    # Only proposals with numbers >= highest promise should be accepted
    assert accepted_count >= 1
    assert rejected_count >= 0

def test_malformed_requests(setup_integration):
    """Test handling of malformed or invalid requests."""
    client = setup_integration["client"]
    
    # Missing required fields
    prepare_missing_fields = {
        "type": "PREPARE",
        # Missing proposalNumber
        "instanceId": 700,
        "proposerId": 1
    }
    
    response = client.post("/prepare", json=prepare_missing_fields)
    # Should get validation error
    assert response.status_code in [400, 422]
    
    # Invalid types
    prepare_invalid_types = {
        "type": "PREPARE",
        "proposalNumber": "not_a_number",  # Should be integer
        "instanceId": 700,
        "proposerId": 1
    }
    
    response = client.post("/prepare", json=prepare_invalid_types)
    # Should get validation error
    assert response.status_code in [400, 422]
    
    # Empty body
    response = client.post("/prepare", json={})
    # Should get validation error
    assert response.status_code in [400, 422]
    
    # Invalid endpoint
    response = client.post("/invalid_endpoint", json={"test": "data"})
    # Should get not found
    assert response.status_code == 404

def test_full_api_integration(setup_integration):
    """Test all API endpoints together."""
    client = setup_integration["client"]
    
    # Test health endpoint
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "healthy"
    
    # Test status endpoint
    response = client.get("/status")
    assert response.status_code == 200
    assert response.json()["node_id"] == 1
    
    # Test instances endpoint
    response = client.get("/instances")
    assert response.status_code == 200
    assert "instances" in response.json()
    
    # Test logs endpoint
    response = client.get("/logs")
    assert response.status_code == 200
    assert "logs" in response.json()
    
    # Test stats endpoint
    response = client.get("/stats")
    assert response.status_code == 200
    assert "stats" in response.json()
    
    # Test debug config endpoint
    debug_config = {"enabled": True, "level": "advanced"}
    response = client.post("/debug/config", json=debug_config)
    assert response.status_code == 200
    assert response.json()["debug"]["level"] == "advanced"
    
    # Test specific instance that was created in previous tests
    response = client.get("/instance/200")
    assert response.status_code == 200
    assert response.json()["instanceId"] == 200
    assert response.json()["accepted"] == True