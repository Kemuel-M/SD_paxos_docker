"""
File: acceptor/tests/unit/test_acceptor.py
Unit tests for the Acceptor implementation.
"""
import os
import json
import time
import pytest
import asyncio
from unittest.mock import MagicMock, patch, AsyncMock

import warnings
warnings.filterwarnings("always", category=RuntimeWarning)

# Configure environment for testing
os.environ["DEBUG"] = "true"
os.environ["NODE_ID"] = "1"

from acceptor import Acceptor

@pytest.fixture
def mock_persistence():
    """Fixture that creates a mock persistence manager."""
    persistence = MagicMock()
    persistence.load_state.return_value = {
        "promises": {},
        "accepted": {},
        "prepare_requests_processed": 0,
        "accept_requests_processed": 0,
        "promises_made": 0,
        "proposals_accepted": 0
    }
    return persistence

@pytest.fixture
def acceptor(mock_persistence):
    """Fixture that creates an acceptor for tests."""
    mock_learners = ["learner-1:8080", "learner-2:8080"]
    
    acceptor = Acceptor(
        node_id=1,
        learners=mock_learners,
        persistence=mock_persistence
    )
    
    # Replace HTTP client with a mock
    acceptor.http_client = AsyncMock()
    
    return acceptor

@pytest.mark.asyncio
async def test_acceptor_initialization(acceptor, mock_persistence):
    """Test if acceptor is initialized correctly."""
    assert acceptor.node_id == 1
    assert len(acceptor.learners) == 2
    assert acceptor.persistence == mock_persistence
    assert acceptor.promises == {}
    assert acceptor.accepted == {}
    assert acceptor.prepare_requests_processed == 0
    assert acceptor.accept_requests_processed == 0
    assert acceptor.promises_made == 0
    assert acceptor.proposals_accepted == 0

@pytest.mark.asyncio
async def test_start_stop(acceptor, mock_persistence):
    """Test starting and stopping the acceptor."""
    # Start
    await acceptor.start()
    assert acceptor.running == True
    mock_persistence.load_state.assert_called_once()
    
    # Stop
    await acceptor.stop()
    assert acceptor.running == False
    mock_persistence.save_state.assert_called_once()

@pytest.mark.asyncio
async def test_process_prepare_first_promise(acceptor):
    """Test processing a prepare request for a new instance."""
    # Create a prepare message
    prepare_msg = {
        "type": "PREPARE",
        "proposalNumber": 42,
        "instanceId": 1,
        "proposerId": 2
    }
    
    # Process the prepare message
    await acceptor.start()
    response = await acceptor.process_prepare(prepare_msg)
    
    # Check response
    assert response["accepted"] == True
    assert response["highestAccepted"] == -1
    assert "acceptedValue" not in response
    assert response["instanceId"] == 1
    assert response["acceptorId"] == 1
    
    # Check state
    assert 1 in acceptor.promises
    assert acceptor.promises[1] == 42
    assert acceptor.prepare_requests_processed == 1
    assert acceptor.promises_made == 1
    
    # Check persistence
    acceptor.persistence.save_state.assert_called()

@pytest.mark.asyncio
async def test_process_prepare_higher_number(acceptor):
    """Test processing a prepare request with higher proposal number."""
    # Set initial state
    await acceptor.start()
    acceptor.promises = {1: 10}
    
    # Create a prepare message with higher number
    prepare_msg = {
        "type": "PREPARE",
        "proposalNumber": 20,
        "instanceId": 1,
        "proposerId": 2
    }
    
    # Process the prepare message
    response = await acceptor.process_prepare(prepare_msg)
    
    # Check response
    assert response["accepted"] == True
    assert response["highestAccepted"] == -1
    
    # Check state
    assert acceptor.promises[1] == 20
    assert acceptor.prepare_requests_processed == 1
    assert acceptor.promises_made == 1

@pytest.mark.asyncio
async def test_process_prepare_lower_number(acceptor):
    """Test processing a prepare request with lower proposal number."""
    # Set initial state
    await acceptor.start()
    acceptor.promises = {1: 20}
    
    # Create a prepare message with lower number
    prepare_msg = {
        "type": "PREPARE",
        "proposalNumber": 10,
        "instanceId": 1,
        "proposerId": 2
    }
    
    # Process the prepare message
    response = await acceptor.process_prepare(prepare_msg)
    
    # Check response
    assert response["accepted"] == False
    assert response["highestPromised"] == 20
    
    # Check state (shouldn't change)
    assert acceptor.promises[1] == 20
    assert acceptor.prepare_requests_processed == 1
    assert acceptor.promises_made == 0

@pytest.mark.asyncio
async def test_process_prepare_with_accepted_value(acceptor):
    """Test processing a prepare request when a value has been accepted."""
    # Set initial state
    await acceptor.start()
    acceptor.promises = {1: 10}
    acceptor.accepted = {1: (10, {"value": "test"})}
    
    # Create a prepare message with higher number
    prepare_msg = {
        "type": "PREPARE",
        "proposalNumber": 20,
        "instanceId": 1,
        "proposerId": 2
    }
    
    # Process the prepare message
    response = await acceptor.process_prepare(prepare_msg)
    
    # Check response
    assert response["accepted"] == True
    assert response["highestAccepted"] == 10
    assert response["acceptedValue"] == {"value": "test"}
    
    # Check state
    assert acceptor.promises[1] == 20
    assert acceptor.prepare_requests_processed == 1
    assert acceptor.promises_made == 1

@pytest.mark.asyncio
async def test_process_accept_first_time(acceptor):
    """Test processing an accept request for the first time."""
    # Create an accept message
    accept_msg = {
        "type": "ACCEPT",
        "proposalNumber": 42,
        "instanceId": 1,
        "proposerId": 2,
        "value": {"clientId": "client-1", "value": "test"}
    }
    
    # Mock the notify_learners method
    acceptor.notify_learners = AsyncMock()
    
    # Process the accept message
    await acceptor.start()
    response = await acceptor.process_accept(accept_msg)
    
    # Check response
    assert response["accepted"] == True
    assert response["proposalNumber"] == 42
    assert response["instanceId"] == 1
    assert response["acceptorId"] == 1
    
    # Check state
    assert 1 in acceptor.promises
    assert 1 in acceptor.accepted
    assert acceptor.promises[1] == 42
    assert acceptor.accepted[1][0] == 42
    assert acceptor.accepted[1][1] == {"clientId": "client-1", "value": "test"}
    assert acceptor.accept_requests_processed == 1
    assert acceptor.proposals_accepted == 1
    
    # Check notification to learners
    acceptor.notify_learners.assert_called_once_with(
        1, 42, {"clientId": "client-1", "value": "test"}
    )
    
    # Check persistence
    acceptor.persistence.save_state.assert_called()

@pytest.mark.asyncio
async def test_process_accept_higher_promise(acceptor):
    """Test processing an accept request when promised to a higher number."""
    # Set initial state
    await acceptor.start()
    acceptor.promises = {1: 50}
    
    # Create an accept message with lower number
    accept_msg = {
        "type": "ACCEPT",
        "proposalNumber": 42,
        "instanceId": 1,
        "proposerId": 2,
        "value": {"clientId": "client-1", "value": "test"}
    }
    
    # Mock the notify_learners method
    acceptor.notify_learners = AsyncMock()
    
    # Process the accept message
    response = await acceptor.process_accept(accept_msg)
    
    # Check response
    assert response["accepted"] == False
    assert response["highestPromised"] == 50
    
    # Check state (shouldn't change)
    assert 1 not in acceptor.accepted
    assert acceptor.accept_requests_processed == 1
    assert acceptor.proposals_accepted == 0
    
    # Check notification to learners (shouldn't be called)
    acceptor.notify_learners.assert_not_called()

@pytest.mark.asyncio
async def test_process_accept_same_as_promised(acceptor):
    """Test processing an accept request with same number as promised."""
    # Set initial state
    await acceptor.start()
    acceptor.promises = {1: 42}
    
    # Create an accept message with same number
    accept_msg = {
        "type": "ACCEPT",
        "proposalNumber": 42,
        "instanceId": 1,
        "proposerId": 2,
        "value": {"clientId": "client-1", "value": "test"}
    }
    
    # Mock the notify_learners method
    acceptor.notify_learners = AsyncMock()
    
    # Process the accept message
    response = await acceptor.process_accept(accept_msg)
    
    # Check response
    assert response["accepted"] == True
    
    # Check state
    assert 1 in acceptor.accepted
    assert acceptor.accepted[1][0] == 42
    assert acceptor.accepted[1][1] == {"clientId": "client-1", "value": "test"}
    assert acceptor.accept_requests_processed == 1
    assert acceptor.proposals_accepted == 1
    
    # Check notification to learners
    acceptor.notify_learners.assert_called_once()

@pytest.mark.asyncio
async def test_notify_learners(acceptor):
    """Test notifying learners about accepted proposals."""
    # Mock the _notify_learner method
    acceptor._notify_learner = AsyncMock(return_value=True)
    
    # Notify learners
    await acceptor.start()
    await acceptor.notify_learners(1, 42, {"value": "test"})
    
    # Check if _notify_learner was called twice (for 2 learners)
    assert acceptor._notify_learner.call_count == 2
    
    # Check notification content
    for call in acceptor._notify_learner.call_args_list:
        args, kwargs = call
        learner, notification, _ = args
        
        assert learner in ["learner-1:8080", "learner-2:8080"]
        assert notification["type"] == "LEARN"
        assert notification["instanceId"] == 1
        assert notification["proposalNumber"] == 42
        assert notification["acceptorId"] == 1
        assert notification["accepted"] == True
        assert notification["value"] == {"value": "test"}
        assert "timestamp" in notification

@pytest.mark.asyncio
async def test_notify_learner_success(acceptor):
    """Test successful notification to a learner."""
    # Mock the HTTP client
    acceptor.http_client.post = AsyncMock()
    
    # Create mock circuit breaker
    mock_cb = AsyncMock()
    
    # Mock notification
    notification = {
        "type": "LEARN",
        "instanceId": 1,
        "proposalNumber": 42,
        "acceptorId": 1,
        "accepted": True,
        "value": {"value": "test"},
        "timestamp": 123456789
    }
    
    # Send notification
    await acceptor.start()
    result = await acceptor._notify_learner("learner-1:8080", notification, mock_cb)
    
    # Check result
    assert result == True
    
    # Check HTTP client call
    acceptor.http_client.post.assert_called_once_with(
        "http://learner-1:8080/learn",
        json=notification,
        timeout=1.0
    )
    
    # Check circuit breaker
    mock_cb.record_success.assert_called_once()

@pytest.mark.asyncio
async def test_notify_learner_failure(acceptor):
    """Test failed notification to a learner."""
    # Mock the HTTP client to raise an exception
    acceptor.http_client.post = AsyncMock(side_effect=Exception("Connection error"))
    
    # Create mock circuit breaker
    mock_cb = AsyncMock()
    
    # Mock notification
    notification = {
        "type": "LEARN",
        "instanceId": 1,
        "proposalNumber": 42,
        "acceptorId": 1,
        "accepted": True,
        "value": {"value": "test"},
        "timestamp": 123456789
    }
    
    # Send notification
    await acceptor.start()
    result = await acceptor._notify_learner("learner-1:8080", notification, mock_cb)
    
    # Check result
    assert result == False
    
    # Check HTTP client call
    acceptor.http_client.post.assert_called_once()
    
    # Check circuit breaker
    mock_cb.record_failure.assert_called_once()

def test_get_status(acceptor):
    """Test getting acceptor status."""
    # Set some state
    acceptor.promises = {1: 10, 2: 20}
    acceptor.accepted = {1: (10, {"value": "test"})}
    acceptor.prepare_requests_processed = 5
    acceptor.accept_requests_processed = 3
    acceptor.promises_made = 2
    acceptor.proposals_accepted = 1
    acceptor.running = True
    
    # Get status
    status = acceptor.get_status()
    
    # Check status
    assert status["node_id"] == 1
    assert status["state"] == "running"
    assert status["learners"] == 2
    assert status["active_instances"] == 2
    assert status["accepted_instances"] == 1
    assert status["prepare_requests_processed"] == 5
    assert status["accept_requests_processed"] == 3
    assert status["promises_made"] == 2
    assert status["proposals_accepted"] == 1
    assert "timestamp" in status

def test_get_instance_info_existing(acceptor):
    """Test getting information about an existing instance."""
    # Set state for instance
    acceptor.promises = {1: 10}
    acceptor.accepted = {1: (10, {"value": "test"})}
    
    # Get instance info
    info = acceptor.get_instance_info(1)
    
    # Check info
    assert info["instanceId"] == 1
    assert info["highestPromised"] == 10
    assert info["accepted"] == True
    assert info["proposalNumber"] == 10
    assert info["value"] == {"value": "test"}

def test_get_instance_info_promised_only(acceptor):
    """Test getting information about an instance that only has a promise."""
    # Set state for instance
    acceptor.promises = {1: 10}
    
    # Get instance info
    info = acceptor.get_instance_info(1)
    
    # Check info
    assert info["instanceId"] == 1
    assert info["highestPromised"] == 10
    assert info["accepted"] == False

def test_get_instance_info_nonexistent(acceptor):
    """Test getting information about a nonexistent instance."""
    # Get instance info
    info = acceptor.get_instance_info(999)
    
    # Check info (should be empty)
    assert info == {}