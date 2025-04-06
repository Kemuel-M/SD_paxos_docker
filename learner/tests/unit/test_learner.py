"""
File: learner/tests/unit/test_learner.py
Unit tests for the Learner implementation.
"""
import os
import json
import time
import pytest
import asyncio
import hashlib
from unittest.mock import MagicMock, patch, AsyncMock

import warnings
warnings.filterwarnings("always", category=RuntimeWarning)

# Configure environment for testing
os.environ["DEBUG"] = "true"
os.environ["NODE_ID"] = "1"

from learner import Learner

@pytest.fixture
def mock_consensus_manager():
    """Fixture that creates a mock ConsensusManager."""
    mock = AsyncMock()
    
    # Mock methods
    mock.process_notification = AsyncMock(return_value=False)
    mock.is_decided = MagicMock(return_value=False)
    mock.get_decision = MagicMock(return_value=None)
    mock.register_decision_callback = MagicMock()
    mock.get_all_instance_ids = MagicMock(return_value=[])
    mock.get_instance_info = MagicMock(return_value={})
    mock.get_stats = MagicMock(return_value={
        "active_instances": 0,
        "decided_instances": 0,
        "notifications_processed": 0,
        "decisions_made": 0
    })
    
    return mock

@pytest.fixture
def mock_rowa_manager():
    """Fixture that creates a mock RowaManager."""
    mock = AsyncMock()
    
    # Mock methods
    mock.read_resource = AsyncMock(return_value={"data": "test", "version": 1})
    mock.write_resource = AsyncMock(return_value=(True, {"version": 2}))
    mock.simulate_resource_access = AsyncMock(return_value=0.5)
    mock.get_stats = MagicMock(return_value={
        "reads_processed": 0,
        "writes_processed": 0,
        "write_successes": 0,
        "write_failures": 0
    })
    
    return mock

@pytest.fixture
def learner(mock_consensus_manager, mock_rowa_manager):
    """Fixture that creates a Learner for tests."""
    acceptors = ["acceptor-1:8080", "acceptor-2:8080", "acceptor-3:8080", "acceptor-4:8080", "acceptor-5:8080"]
    stores = ["store-1:8080", "store-2:8080", "store-3:8080"]
    
    learner = Learner(
        node_id=1,
        acceptors=acceptors,
        stores=stores,
        consensus_manager=mock_consensus_manager,
        rowa_manager=mock_rowa_manager,
        use_cluster_store=True
    )
    
    # Replace HTTP client with a mock
    learner.http_client = AsyncMock()
    learner.http_client.post = AsyncMock()
    
    return learner

@pytest.mark.asyncio
async def test_learner_initialization(learner, mock_consensus_manager, mock_rowa_manager):
    """Test if Learner is initialized correctly."""
    assert learner.node_id == 1
    assert len(learner.acceptors) == 5
    assert len(learner.stores) == 3
    assert learner.consensus_manager == mock_consensus_manager
    assert learner.rowa_manager == mock_rowa_manager
    assert learner.use_cluster_store is True
    assert learner.running is False
    assert learner.notifications_processed == 0
    assert learner.decisions_made == 0
    assert learner.client_notifications_sent == 0
    assert learner.client_notifications_failed == 0

@pytest.mark.asyncio
async def test_start_stop(learner, mock_consensus_manager):
    """Test starting and stopping the learner."""
    # Start the learner
    await learner.start()
    assert learner.running is True
    mock_consensus_manager.register_decision_callback.assert_called()
    
    # Stop the learner
    await learner.stop()
    assert learner.running is False

@pytest.mark.asyncio
async def test_process_learn_notification_valid(learner, mock_consensus_manager):
    """Test processing a valid learn notification."""
    # Create a learn notification
    notification = {
        "type": "LEARN",
        "proposalNumber": 42,
        "instanceId": 1,
        "acceptorId": 1,
        "accepted": True,
        "value": {"data": "test"},
        "timestamp": int(time.time() * 1000)
    }
    
    # Mock consensus_manager.process_notification to return False (no decision yet)
    mock_consensus_manager.process_notification.return_value = False
    
    # Process the notification
    result = await learner.process_learn_notification(notification)
    
    # Check the result
    assert result["accepted"] is True
    assert result["instanceId"] == 1
    assert result["learnerId"] == 1
    
    # Check that consensus_manager.process_notification was called
    mock_consensus_manager.process_notification.assert_called_once_with(notification)
    
    # Check statistics
    assert learner.notifications_processed == 1

@pytest.mark.asyncio
async def test_process_learn_notification_invalid(learner, mock_consensus_manager):
    """Test processing an invalid learn notification."""
    # Create an invalid notification (missing fields)
    invalid_notification = {
        "type": "LEARN",
        "proposalNumber": 42,
        "instanceId": 1,
        # Missing acceptorId
        "accepted": True,
        # Missing value
        "timestamp": int(time.time() * 1000)
    }
    
    # Process the notification
    result = await learner.process_learn_notification(invalid_notification)
    
    # Check the result
    assert result["accepted"] is False
    assert "reason" in result
    
    # Check that consensus_manager.process_notification was not called
    mock_consensus_manager.process_notification.assert_not_called()

@pytest.mark.asyncio
async def test_process_learn_notification_decision(learner, mock_consensus_manager):
    """Test processing a learn notification that leads to a decision."""
    # Create a learn notification
    notification = {
        "type": "LEARN",
        "proposalNumber": 42,
        "instanceId": 1,
        "acceptorId": 3,
        "accepted": True,
        "value": {"data": "test"},
        "timestamp": int(time.time() * 1000)
    }
    
    # Mock consensus_manager.process_notification to return True (decision made)
    mock_consensus_manager.process_notification.return_value = True
    
    # Mock consensus_manager.is_decided to return False (decision just made)
    mock_consensus_manager.is_decided.return_value = False
    
    # Process the notification
    result = await learner.process_learn_notification(notification)
    
    # Check the result
    assert result["accepted"] is True
    
    # Check that consensus_manager.register_decision_callback was called for this instance
    mock_consensus_manager.register_decision_callback.assert_called_with(1, learner._on_decision_made)

@pytest.mark.asyncio
async def test_on_decision_made(learner, mock_consensus_manager, mock_rowa_manager):
    """Test the decision callback."""
    # Start the learner
    await learner.start()
    
    # Create a value
    value = {
        "clientId": "client-1",
        "resource": "R",
        "operation": "WRITE",
        "data": "test data",
        "timestamp": int(time.time() * 1000)
    }
    
    # Mock _should_handle_client_notification to return True
    with patch.object(learner, '_should_handle_client_notification', return_value=True):
        # Call the decision callback
        await learner._on_decision_made(1, 42, value)
        
        # Allow time for the async task to complete
        await asyncio.sleep(0.1)
        
        # Check that the decided value was processed
        mock_rowa_manager.write_resource.assert_called_once()
        call_args = mock_rowa_manager.write_resource.call_args
        assert call_args[1]["resource_id"] == "R"
        assert call_args[1]["data"] == "test data"
        assert call_args[1]["client_id"] == "client-1"
        assert call_args[1]["instance_id"] == 1
        
        # Check statistics
        assert learner.decisions_made == 1

@pytest.mark.asyncio
async def test_process_decided_value_part1(learner, mock_rowa_manager):
    """Test processing a decided value in Part 1 mode (simulation)."""
    # Configure learner for Part 1
    learner.use_cluster_store = False
    
    # Create a value
    value = {
        "clientId": "client-1",
        "resource": "R",
        "operation": "WRITE",
        "data": "test data",
        "timestamp": int(time.time() * 1000)
    }
    
    # Mock _should_handle_client_notification to return True
    with patch.object(learner, '_should_handle_client_notification', return_value=True):
        # Process the decided value
        await learner._process_decided_value(1, 42, value)
        
        # Check that simulate_resource_access was called
        mock_rowa_manager.simulate_resource_access.assert_called_once()
        
        # Check that write_resource was not called
        mock_rowa_manager.write_resource.assert_not_called()

@pytest.mark.asyncio
async def test_process_decided_value_part2_success(learner, mock_rowa_manager):
    """Test processing a decided value in Part 2 mode with successful write."""
    # Configure learner for Part 2
    learner.use_cluster_store = True
    
    # Mock write_resource to return success
    mock_rowa_manager.write_resource.return_value = (True, {"version": 2})
    
    # Create a value
    value = {
        "clientId": "client-1",
        "resource": "R",
        "operation": "WRITE",
        "data": "test data",
        "timestamp": int(time.time() * 1000)
    }
    
    # Register a client callback
    learner.register_client_callback(1, "client-1", "http://client-1:8080/callback")
    
    # Mock _should_handle_client_notification to return True
    with patch.object(learner, '_should_handle_client_notification', return_value=True):
        # Mock _notify_client
        with patch.object(learner, '_notify_client', AsyncMock(return_value=True)) as mock_notify:
            # Process the decided value
            await learner._process_decided_value(1, 42, value)
            
            # Check that write_resource was called
            mock_rowa_manager.write_resource.assert_called_once()
            
            # Check that _notify_client was called with correct status
            mock_notify.assert_called_once()
            call_args = mock_notify.call_args
            assert call_args[0][0] == "http://client-1:8080/callback"  # callback_url
            assert call_args[0][1]["status"] == "COMMITTED"  # result status

@pytest.mark.asyncio
async def test_process_decided_value_part2_failure(learner, mock_rowa_manager):
    """Test processing a decided value in Part 2 mode with failed write."""
    # Configure learner for Part 2
    learner.use_cluster_store = True
    
    # Mock write_resource to return failure
    mock_rowa_manager.write_resource.return_value = (False, None)
    
    # Create a value
    value = {
        "clientId": "client-1",
        "resource": "R",
        "operation": "WRITE",
        "data": "test data",
        "timestamp": int(time.time() * 1000)
    }
    
    # Register a client callback
    learner.register_client_callback(1, "client-1", "http://client-1:8080/callback")
    
    # Mock _should_handle_client_notification to return True
    with patch.object(learner, '_should_handle_client_notification', return_value=True):
        # Mock _notify_client
        with patch.object(learner, '_notify_client', AsyncMock(return_value=True)) as mock_notify:
            # Process the decided value
            await learner._process_decided_value(1, 42, value)
            
            # Check that write_resource was called
            mock_rowa_manager.write_resource.assert_called_once()
            
            # Check that _notify_client was called with failure status
            mock_notify.assert_called_once()
            call_args = mock_notify.call_args
            assert call_args[0][0] == "http://client-1:8080/callback"  # callback_url
            assert call_args[0][1]["status"] == "NOT_COMMITTED"  # result status

@pytest.mark.asyncio
async def test_notify_client_success(learner):
    """Test notifying a client successfully."""
    # Configure HTTP client mock
    learner.http_client.post.reset_mock()
    learner.http_client.post.return_value = {}
    
    # Prepare notification
    callback_url = "http://client-1:8080/callback"
    result = {
        "status": "COMMITTED",
        "instanceId": 1,
        "resource": "R",
        "timestamp": int(time.time() * 1000)
    }
    
    # Send notification
    success = await learner._notify_client(callback_url, result)
    
    # Check result
    assert success is True
    
    # Check HTTP client was called
    learner.http_client.post.assert_called_once_with(
        callback_url, json=result, timeout=5.0)
    
    # Check statistics
    assert learner.client_notifications_sent == 1
    assert learner.client_notifications_failed == 0

@pytest.mark.asyncio
async def test_notify_client_failure(learner):
    """Test notifying a client with failure."""
    # Configure HTTP client mock to raise an exception
    learner.http_client.post.reset_mock()
    learner.http_client.post.side_effect = Exception("Connection error")
    
    # Prepare notification
    callback_url = "http://client-1:8080/callback"
    result = {
        "status": "COMMITTED",
        "instanceId": 1,
        "resource": "R",
        "timestamp": int(time.time() * 1000)
    }
    
    # Send notification
    success = await learner._notify_client(callback_url, result)
    
    # Check result
    assert success is False
    
    # Check HTTP client was called
    learner.http_client.post.assert_called_once()
    
    # Check statistics
    assert learner.client_notifications_sent == 0
    assert learner.client_notifications_failed == 1

def test_should_handle_client_notification(learner):
    """Test determining which learner should handle a client notification."""
    # With node_id=1, we should handle "client-1" (assuming hash leads to 1)
    # Mock hash result directly
    with patch('hashlib.md5') as mock_md5:
        # Mock to return a value that results in 1 after modulo
        mock_md5.return_value.hexdigest.return_value = "aaaaaaaaaaaaaaaa"  # Will be even number when converted to int
        
        # Check client-1
        result = learner._should_handle_client_notification("client-1")
        assert result is True
        
        # Mock to return a value that results in 2 after modulo
        mock_md5.return_value.hexdigest.return_value = "aaaaaaaaaaaaaaab"  # Will be odd number when converted to int
        
        # Check client-2
        result = learner._should_handle_client_notification("client-2")
        assert result is False
    
    # Empty client_id should default to learner 1
    result = learner._should_handle_client_notification("")
    assert result is True

def test_register_client_callback(learner):
    """Test registering a client callback."""
    # Register a callback
    learner.register_client_callback(
        instance_id=1,
        client_id="client-1",
        callback_url="http://client-1:8080/callback"
    )
    
    # Check that it was stored correctly
    assert 1 in learner.pending_clients
    assert learner.pending_clients[1]["client_id"] == "client-1"
    assert learner.pending_clients[1]["callback_url"] == "http://client-1:8080/callback"
    assert "timestamp" in learner.pending_clients[1]

def test_validate_notification(learner):
    """Test validating a notification."""
    # Valid notification
    valid = {
        "instanceId": 1,
        "proposalNumber": 42,
        "acceptorId": 1,
        "accepted": True,
        "value": {"data": "test"},
        "timestamp": int(time.time() * 1000)
    }
    assert learner._validate_notification(valid) is True
    
    # Invalid - missing field
    invalid1 = {
        "instanceId": 1,
        "proposalNumber": 42,
        # Missing acceptorId
        "accepted": True,
        "value": {"data": "test"},
        "timestamp": int(time.time() * 1000)
    }
    assert learner._validate_notification(invalid1) is False
    
    # Invalid - not accepted
    invalid2 = {
        "instanceId": 1,
        "proposalNumber": 42,
        "acceptorId": 1,
        "accepted": False,
        "value": {"data": "test"},
        "timestamp": int(time.time() * 1000)
    }
    assert learner._validate_notification(invalid2) is False

def test_get_status(learner, mock_consensus_manager, mock_rowa_manager):
    """Test getting learner status."""
    # Mock consensus_manager.get_stats
    mock_consensus_manager.get_stats.return_value = {
        "active_instances": 5,
        "decided_instances": 3,
        "notifications_processed": 10,
        "decisions_made": 3
    }
    
    # Mock rowa_manager.get_stats
    mock_rowa_manager.get_stats.return_value = {
        "reads_processed": 2,
        "writes_processed": 3,
        "write_successes": 2,
        "write_failures": 1
    }
    
    # Set learner stats
    learner.notifications_processed = 10
    learner.decisions_made = 3
    learner.client_notifications_sent = 2
    learner.client_notifications_failed = 1
    
    # Get status
    status = learner.get_status()
    
    # Check status
    assert status["node_id"] == 1
    assert status["state"] == "stopped"  # Not started yet
    assert status["active_instances"] == 5
    assert status["decided_instances"] == 3
    assert status["notifications_processed"] == 10
    assert status["decisions_made"] == 3
    assert status["client_notifications_sent"] == 2
    assert status["client_notifications_failed"] == 1
    assert status["use_cluster_store"] is True
    assert "timestamp" in status
    assert "rowa" in status
