"""
File: learner/tests/integration/test_integration.py
Integration tests for the Learner component.
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
os.environ["ACCEPTORS"] = "localhost:8091,localhost:8092,localhost:8093,localhost:8094,localhost:8095"
os.environ["STORES"] = "localhost:8081,localhost:8082,localhost:8083"
os.environ["LOG_DIR"] = "/tmp/paxos-test-logs"
os.environ["USE_CLUSTER_STORE"] = "false"  # Start with Part 1 simulation

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
    http_client_mock.get.return_value = {"data": "test", "version": 1}
    
    # Import necessary modules
    from learner import Learner
    from consensus import ConsensusManager
    from rowa import RowaManager
    from two_phase import TwoPhaseCommitManager
    from api import create_api
    from common.logging import setup_logging
    from common.communication import HttpClient
    
    # Patch HTTP client
    with patch.object(HttpClient, 'post', new=http_client_mock.post), \
         patch.object(HttpClient, 'get', new=http_client_mock.get):
        # Setup logging
        setup_logging("learner-test", debug=True, debug_level="basic", log_dir=os.environ["LOG_DIR"])
        
        # Create components
        consensus_manager = ConsensusManager(node_id=1)
        two_phase_manager = TwoPhaseCommitManager(node_id=1, stores=os.environ["STORES"].split(","))
        rowa_manager = RowaManager(node_id=1, stores=os.environ["STORES"].split(","), two_phase_manager=two_phase_manager)
        
        # Create learner
        learner = Learner(
            node_id=1,
            acceptors=os.environ["ACCEPTORS"].split(","),
            stores=os.environ["STORES"].split(","),
            consensus_manager=consensus_manager,
            rowa_manager=rowa_manager,
            use_cluster_store=(os.environ["USE_CLUSTER_STORE"].lower() in ("true", "1", "yes"))
        )
        
        # Create API
        app = create_api(learner, consensus_manager, rowa_manager)
        client = TestClient(app)
        
        # Start learner
        loop = asyncio.get_event_loop()
        loop.run_until_complete(learner.start())
        
        # Return components for tests
        yield {
            "temp_dir": temp_dir,
            "learner": learner,
            "consensus_manager": consensus_manager,
            "rowa_manager": rowa_manager,
            "two_phase_manager": two_phase_manager,
            "client": client,
            "http_client_mock": http_client_mock
        }
        
        # Cleanup
        loop = asyncio.get_event_loop()
        loop.run_until_complete(learner.stop())
        try:
            shutil.rmtree(temp_dir)
            shutil.rmtree(os.environ["LOG_DIR"])
        except:
            pass

def test_process_learn_notification_flow(setup_integration):
    """Test the complete flow of processing learn notifications."""
    client = setup_integration["client"]
    consensus_manager = setup_integration["consensus_manager"]
    
    # Create learn notifications for the same instance but different acceptors
    notifications = []
    for acceptor_id in range(1, 6):
        notifications.append({
            "type": "LEARN",
            "proposalNumber": 42,
            "instanceId": 100,
            "acceptorId": acceptor_id,
            "accepted": True,
            "value": {"clientId": "client-1", "data": "test data", "resource": "R"},
            "timestamp": int(time.time() * 1000)
        })
    
    # Send first notification (should not reach consensus yet)
    response1 = client.post("/learn", json=notifications[0])
    assert response1.status_code == 200
    assert response1.json()["accepted"] == True
    
    # Check instance state
    response_instance = client.get("/instance/100")
    assert response_instance.status_code == 200
    data = response_instance.json()
    assert data["decided"] == False
    
    # Send second notification (still not enough for consensus)
    response2 = client.post("/learn", json=notifications[1])
    assert response2.status_code == 200

    # Check instance state again
    response_instance = client.get("/instance/100")
    assert response_instance.status_code == 200
    data = response_instance.json()
    assert data["decided"] == False
    
    # Send third notification (should reach consensus)
    response3 = client.post("/learn", json=notifications[2])
    assert response3.status_code == 200
    
    # Wait a moment for async processing
    time.sleep(0.1)
    
    # Check instance state again - should be decided now
    response_instance = client.get("/instance/100")
    assert response_instance.status_code == 200
    data = response_instance.json()
    assert data["decided"] == True
    assert data["proposalNumber"] == 42
    assert data["value"]["clientId"] == "client-1"
    assert data["value"]["data"] == "test data"

def test_conflicting_values(setup_integration):
    """Test handling of conflicting values from different acceptors."""
    client = setup_integration["client"]
    
    # Create learn notifications for the same instance but different values
    notification1 = {
        "type": "LEARN",
        "proposalNumber": 42,
        "instanceId": 200,
        "acceptorId": 1,
        "accepted": True,
        "value": {"clientId": "client-1", "data": "value 1", "resource": "R"},
        "timestamp": int(time.time() * 1000)
    }
    
    notification2 = {
        "type": "LEARN",
        "proposalNumber": 42,
        "instanceId": 200,
        "acceptorId": 2,
        "accepted": True,
        "value": {"clientId": "client-1", "data": "value 2", "resource": "R"},
        "timestamp": int(time.time() * 1000)
    }
    
    # Send the notifications
    client.post("/learn", json=notification1)
    client.post("/learn", json=notification2)
    
    # Send more acceptors with value 1 to reach consensus
    for acceptor_id in range(3, 5):
        notification = {
            "type": "LEARN",
            "proposalNumber": 42,
            "instanceId": 200,
            "acceptorId": acceptor_id,
            "accepted": True,
            "value": {"clientId": "client-1", "data": "value 1", "resource": "R"},
            "timestamp": int(time.time() * 1000)
        }
        client.post("/learn", json=notification)
    
    # Wait a moment for async processing
    time.sleep(0.1)
    
    # Check instance state
    response = client.get("/instance/200")
    assert response.status_code == 200
    data = response.json()
    assert data["decided"] == True
    assert data["value"]["data"] == "value 1"  # Majority value won

def test_different_proposal_numbers(setup_integration):
    """Test handling of different proposal numbers."""
    client = setup_integration["client"]
    
    # Create learn notifications with different proposal numbers
    notification1 = {
        "type": "LEARN",
        "proposalNumber": 40,
        "instanceId": 300,
        "acceptorId": 1,
        "accepted": True,
        "value": {"clientId": "client-1", "data": "value from 40", "resource": "R"},
        "timestamp": int(time.time() * 1000)
    }
    
    notification2 = {
        "type": "LEARN",
        "proposalNumber": 50,
        "instanceId": 300,
        "acceptorId": 2,
        "accepted": True,
        "value": {"clientId": "client-1", "data": "value from 50", "resource": "R"},
        "timestamp": int(time.time() * 1000)
    }
    
    # Send first notification with proposal 40
    client.post("/learn", json=notification1)
    
    # Send two more acceptors with proposal 40 to reach consensus
    for acceptor_id in range(3, 5):
        notification = {
            "type": "LEARN",
            "proposalNumber": 40,
            "instanceId": 300,
            "acceptorId": acceptor_id,
            "accepted": True,
            "value": {"clientId": "client-1", "data": "value from 40", "resource": "R"},
            "timestamp": int(time.time() * 1000)
        }
        client.post("/learn", json=notification)
    
    # Send a notification with higher proposal number
    client.post("/learn", json=notification2)
    
    # Wait a moment for async processing
    time.sleep(0.1)
    
    # Check instance state - the first one to reach consensus should win
    response = client.get("/instance/300")
    assert response.status_code == 200
    data = response.json()
    assert data["decided"] == True
    assert data["proposalNumber"] == 40
    assert data["value"]["data"] == "value from 40"

def test_part1_simulation(setup_integration):
    """Test the Part 1 simulation mode (no Cluster Store)."""
    client = setup_integration["client"]
    learner = setup_integration["learner"]
    rowa_manager = setup_integration["rowa_manager"]

    import logging
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger(__name__)
    
    # Ensure we're in Part 1 mode
    learner.use_cluster_store = False
    
    # Verificação adicional para garantir que rowa_manager está configurado corretamente
    logger.debug(f"learner.rowa_manager is rowa_manager: {learner.rowa_manager is rowa_manager}")
    
    # Mock do método do rowa_manager
    original_rowa_simulate = rowa_manager.simulate_resource_access
    rowa_simulation_calls = []
    
    async def mock_rowa_simulate():
        logger.debug("mock_rowa_simulate() called")
        delay = await original_rowa_simulate()
        rowa_simulation_calls.append(delay)
        logger.debug(f"Rowa simulation delay: {delay}")
        return delay
    
    rowa_manager.simulate_resource_access = mock_rowa_simulate
    
    # Mock do método interno do learner - essa é a parte crucial que estava faltando!
    original_learner_simulate = learner._simulate_resource_access
    learner_simulation_calls = []
    
    async def mock_learner_simulate():
        logger.debug("mock_learner_simulate() called")
        delay = await original_learner_simulate()
        learner_simulation_calls.append(delay)
        logger.debug(f"Learner simulation delay: {delay}")
        return delay
    
    learner._simulate_resource_access = mock_learner_simulate
    
    # Create and send learn notifications to reach consensus
    for acceptor_id in range(1, 4):
        notification = {
            "type": "LEARN",
            "proposalNumber": 42,
            "instanceId": 400,
            "acceptorId": acceptor_id,
            "accepted": True,
            "value": {"clientId": "client-1", "data": "test", "resource": "R"},
            "timestamp": int(time.time() * 1000)
        }
        client.post("/learn", json=notification)
    
    # Wait longer for async processing to complete
    time.sleep(2.0)
    
    # Print out additional debug information
    logger.debug(f"Rowa simulation calls: {rowa_simulation_calls}")
    logger.debug(f"Learner simulation calls: {learner_simulation_calls}")
    logger.debug(f"Learner use_cluster_store: {learner.use_cluster_store}")
    logger.debug(f"Learner pending tasks: {len(learner._pending_tasks)}")
    
    # Verificar se algum método de simulação foi chamado
    simulation_calls = rowa_simulation_calls + learner_simulation_calls
    assert len(simulation_calls) > 0, "Nenhum método de simulação foi chamado"
    
    # Restore original methods
    rowa_manager.simulate_resource_access = original_rowa_simulate
    learner._simulate_resource_access = original_learner_simulate

@pytest.mark.skipif(os.environ["USE_CLUSTER_STORE"] != "true", 
                   reason="Requires USE_CLUSTER_STORE=true")
def test_part2_cluster_store(setup_integration):
    """Test the Part 2 Cluster Store integration."""
    client = setup_integration["client"]
    learner = setup_integration["learner"]
    rowa_manager = setup_integration["rowa_manager"]
    http_client_mock = setup_integration["http_client_mock"]
    
    # Ensure we're in Part 2 mode
    learner.use_cluster_store = True
    
    # Reset HTTP client mock
    http_client_mock.post.reset_mock()
    
    # Configure HTTP client mock for prepare and commit success
    http_client_mock.post.side_effect = [
        # Prepare responses for 3 stores
        {"ready": True, "currentVersion": 1},
        {"ready": True, "currentVersion": 1},
        {"ready": True, "currentVersion": 1},
        # Commit responses for 3 stores
        {"success": True, "version": 2},
        {"success": True, "version": 2},
        {"success": True, "version": 2}
    ]
    
    # Create and send learn notifications to reach consensus
    for acceptor_id in range(1, 4):
        notification = {
            "type": "LEARN",
            "proposalNumber": 42,
            "instanceId": 500,
            "acceptorId": acceptor_id,
            "accepted": True,
            "value": {"clientId": "client-1", "data": "test", "resource": "R"},
            "timestamp": int(time.time() * 1000)
        }
        client.post("/learn", json=notification)
    
    # Wait for processing to complete
    time.sleep(0.5)
    
    # Check if HTTP client was called for both prepare and commit phases
    assert http_client_mock.post.call_count >= 6  # 3 prepare + 3 commit

def test_client_notification(setup_integration):
    """Test the client notification mechanism."""
    client = setup_integration["client"]
    learner = setup_integration["learner"]
    http_client_mock = setup_integration["http_client_mock"]
    
    # Reset HTTP client mock
    http_client_mock.post.reset_mock()
    
    # Register a client callback
    learner.register_client_callback(
        instance_id=600,
        client_id="client-1",
        callback_url="http://client-1:8080/callback"
    )
    
    # Mock _should_handle_client_notification to return True
    original_should_handle = learner._should_handle_client_notification
    learner._should_handle_client_notification = lambda client_id: True
    
    # Create and send learn notifications to reach consensus
    for acceptor_id in range(1, 4):
        notification = {
            "type": "LEARN",
            "proposalNumber": 42,
            "instanceId": 600,
            "acceptorId": acceptor_id,
            "accepted": True,
            "value": {"clientId": "client-1", "data": "test", "resource": "R"},
            "timestamp": int(time.time() * 1000)
        }
        client.post("/learn", json=notification)
    
    # Wait for processing to complete
    time.sleep(0.5)
    
    # Check if HTTP client was called for client notification
    notification_calls = [call for call in http_client_mock.post.call_args_list 
                         if "client-1:8080/callback" in str(call)]
    assert len(notification_calls) > 0
    
    # Check notification content
    notification_call = notification_calls[0]
    args, kwargs = notification_call
    assert kwargs.get("json", {}).get("status") == "COMMITTED"
    assert kwargs.get("json", {}).get("instanceId") == 600
    
    # Restore original method
    learner._should_handle_client_notification = original_should_handle

def test_client_notification_on_storage_failure(setup_integration):
    """Test client notification when Cluster Store write fails."""
    client = setup_integration["client"]
    learner = setup_integration["learner"]
    rowa_manager = setup_integration["rowa_manager"]
    http_client_mock = setup_integration["http_client_mock"]
    
    # Ensure we're in Part 2 mode
    learner.use_cluster_store = True
    
    # Reset HTTP client mock
    http_client_mock.post.reset_mock()
    
    # Configure rowa_manager.write_resource to return failure
    original_write = rowa_manager.write_resource
    rowa_manager.write_resource = AsyncMock(return_value=(False, None))
    
    # Register a client callback
    learner.register_client_callback(
        instance_id=650,
        client_id="client-1",
        callback_url="http://client-1:8080/callback"
    )
    
    # Mock _should_handle_client_notification to return True
    original_should_handle = learner._should_handle_client_notification
    learner._should_handle_client_notification = lambda client_id: True
    
    # Create and send learn notifications to reach consensus
    for acceptor_id in range(1, 4):
        notification = {
            "type": "LEARN",
            "proposalNumber": 42,
            "instanceId": 650,
            "acceptorId": acceptor_id,
            "accepted": True,
            "value": {"clientId": "client-1", "data": "test", "resource": "R"},
            "timestamp": int(time.time() * 1000)
        }
        client.post("/learn", json=notification)
    
    # Wait for processing to complete
    time.sleep(0.5)
    
    # Check if HTTP client was called for client notification
    notification_calls = [call for call in http_client_mock.post.call_args_list 
                         if "client-1:8080/callback" in str(call)]
    assert len(notification_calls) > 0
    
    # Check notification content has NOT_COMMITTED status due to storage failure
    notification_call = notification_calls[0]
    args, kwargs = notification_call
    assert kwargs.get("json", {}).get("status") == "NOT_COMMITTED"
    assert kwargs.get("json", {}).get("instanceId") == 650
    
    # Restore original methods
    learner._should_handle_client_notification = original_should_handle
    rowa_manager.write_resource = original_write

def test_full_api_integration(setup_integration):
    """Test all API endpoints together."""
    client = setup_integration["client"]
    
    # First, create decided instances
    for instance_id in [700, 701]:
        for acceptor_id in range(1, 4):
            notification = {
                "type": "LEARN",
                "proposalNumber": 42,
                "instanceId": instance_id,
                "acceptorId": acceptor_id,
                "accepted": True,
                "value": {"clientId": "client-1", "data": f"test {instance_id}", "resource": "R"},
                "timestamp": int(time.time() * 1000)
            }
            client.post("/learn", json=notification)
    
    # Wait for processing
    time.sleep(0.2)
    
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
    data = response.json()
    assert "instances" in data
    assert "total" in data
    assert data["total"] >= 2  # Should have at least our 2 instances
    
    # Test instance endpoint for specific instance
    response = client.get("/instance/700")
    assert response.status_code == 200
    data = response.json()
    assert data["instanceId"] == 700
    assert data["decided"] == True
    assert data["value"]["data"] == "test 700"
    
    # Test logs endpoint
    response = client.get("/logs")
    assert response.status_code == 200
    assert "logs" in response.json()
    
    # Test important logs endpoint
    response = client.get("/logs/important")
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