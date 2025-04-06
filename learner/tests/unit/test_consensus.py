"""
File: learner/tests/unit/test_consensus.py
Unit tests for the ConsensusManager component.
"""
import os
import json
import time
import pytest
import asyncio
from unittest.mock import MagicMock, patch, AsyncMock
from collections import defaultdict

import warnings
warnings.filterwarnings("always", category=RuntimeWarning)

# Configure environment for testing
os.environ["DEBUG"] = "true"
os.environ["NODE_ID"] = "1"

from consensus import ConsensusManager

@pytest.fixture
def consensus_manager():
    """Fixture that creates a ConsensusManager for tests."""
    return ConsensusManager(node_id=1, quorum_size=3)

@pytest.mark.asyncio
async def test_initialization(consensus_manager):
    """Test if ConsensusManager is initialized correctly."""
    assert consensus_manager.node_id == 1
    assert consensus_manager.quorum_size == 3
    assert isinstance(consensus_manager.votes, defaultdict)
    assert isinstance(consensus_manager.decisions, dict)
    assert consensus_manager.notifications_processed == 0
    assert consensus_manager.decisions_made == 0

@pytest.mark.asyncio
async def test_process_notification_single(consensus_manager):
    """Test processing a single notification."""
    # Create a notification
    notification = {
        "instanceId": 1,
        "proposalNumber": 42,
        "acceptorId": 1,
        "accepted": True,
        "value": {"data": "test"},
        "timestamp": int(time.time() * 1000)
    }
    
    # Process the notification
    result = await consensus_manager.process_notification(notification)
    
    # Check result (should not make a decision with just one acceptor)
    assert result == False
    
    # Check state
    assert 1 in consensus_manager.votes
    assert 42 in consensus_manager.votes[1]
    assert consensus_manager.notifications_processed == 1
    assert consensus_manager.decisions_made == 0
    
    # Check votes structure
    value_hash = consensus_manager._hash_value(notification["value"])
    assert 1 in consensus_manager.votes[1][42][value_hash]

@pytest.mark.asyncio
async def test_process_notification_quorum(consensus_manager):
    """Test processing notifications until quorum is reached."""
    # Create a base notification
    base_notification = {
        "instanceId": 1,
        "proposalNumber": 42,
        "accepted": True,
        "value": {"data": "test"},
        "timestamp": int(time.time() * 1000)
    }
    
    # Add callback mock
    callback_mock = AsyncMock()
    consensus_manager.register_decision_callback(1, callback_mock)
    
    # Process three notifications from different acceptors
    decisions = []
    for acceptor_id in range(1, 4):
        notification = base_notification.copy()
        notification["acceptorId"] = acceptor_id
        
        result = await consensus_manager.process_notification(notification)
        decisions.append(result)
    
    # Check results
    assert decisions == [False, False, True]  # Decision made with the third notification
    
    # Check state
    assert 1 in consensus_manager.decisions
    assert consensus_manager.notifications_processed == 3
    assert consensus_manager.decisions_made == 1
    
    # Check decision value
    assert consensus_manager.decisions[1][0] == 42  # Proposal number
    assert consensus_manager.decisions[1][1]["data"] == "test"  # Value
    
    # Check callback
    await asyncio.sleep(0.1)  # Give time for the callback to be called
    callback_mock.assert_called_once()
    call_args = callback_mock.call_args
    assert call_args[0][0] == 1  # instance_id
    assert call_args[0][1] == 42  # proposal_number
    assert call_args[0][2]["data"] == "test"  # value

@pytest.mark.asyncio
async def test_process_notification_different_values(consensus_manager):
    """Test processing notifications with different values."""
    # Create two notifications with different values
    notification1 = {
        "instanceId": 1,
        "proposalNumber": 42,
        "acceptorId": 1,
        "accepted": True,
        "value": {"data": "value1"},
        "timestamp": int(time.time() * 1000)
    }
    
    notification2 = {
        "instanceId": 1,
        "proposalNumber": 42,
        "acceptorId": 2,
        "accepted": True,
        "value": {"data": "value2"},
        "timestamp": int(time.time() * 1000)
    }
    
    # Process the notifications
    await consensus_manager.process_notification(notification1)
    await consensus_manager.process_notification(notification2)
    
    # Neither value should have reached quorum
    assert 1 not in consensus_manager.decisions
    
    # Now process a third notification with value1 to reach quorum
    notification3 = {
        "instanceId": 1,
        "proposalNumber": 42,
        "acceptorId": 3,
        "accepted": True,
        "value": {"data": "value1"},
        "timestamp": int(time.time() * 1000)
    }
    
    result = await consensus_manager.process_notification(notification3)
    
    # Check result
    assert result == True
    
    # Check decision value
    assert consensus_manager.decisions[1][1]["data"] == "value1"

@pytest.mark.asyncio
async def test_process_notification_higher_proposal(consensus_manager):
    """Test processing notifications with a higher proposal number."""
    # First, decide on a value with proposal 42
    for acceptor_id in range(1, 4):
        notification = {
            "instanceId": 1,
            "proposalNumber": 42,
            "acceptorId": acceptor_id,
            "accepted": True,
            "value": {"data": "test42"},
            "timestamp": int(time.time() * 1000)
        }
        await consensus_manager.process_notification(notification)
    
    # Now process notifications with a higher proposal number
    for acceptor_id in range(1, 4):
        notification = {
            "instanceId": 1,
            "proposalNumber": 50,
            "acceptorId": acceptor_id,
            "accepted": True,
            "value": {"data": "test50"},
            "timestamp": int(time.time() * 1000)
        }
        await consensus_manager.process_notification(notification)
    
    # Decision should still be for the first value decided
    assert consensus_manager.decisions[1][0] == 42
    assert consensus_manager.decisions[1][1]["data"] == "test42"

@pytest.mark.asyncio
async def test_process_invalid_notification(consensus_manager):
    """Test processing an invalid notification."""
    # Create an invalid notification (missing fields)
    invalid_notification = {
        "instanceId": 1,
        "proposalNumber": 42,
        # Missing acceptorId
        "accepted": True,
        # Missing value
        "timestamp": int(time.time() * 1000)
    }
    
    # Process the notification
    result = await consensus_manager.process_notification(invalid_notification)
    
    # Check result
    assert result == False
    
    # Create a rejected notification
    rejected_notification = {
        "instanceId": 1,
        "proposalNumber": 42,
        "acceptorId": 1,
        "accepted": False,  # Rejected
        "value": {"data": "test"},
        "timestamp": int(time.time() * 1000)
    }
    
    # Process the notification
    result = await consensus_manager.process_notification(rejected_notification)
    
    # Check result
    assert result == False

@pytest.mark.asyncio
async def test_register_decision_callback_already_decided(consensus_manager):
    """Test registering a callback for an already decided instance."""
    # First, decide on a value
    for acceptor_id in range(1, 4):
        notification = {
            "instanceId": 1,
            "proposalNumber": 42,
            "acceptorId": acceptor_id,
            "accepted": True,
            "value": {"data": "test"},
            "timestamp": int(time.time() * 1000)
        }
        await consensus_manager.process_notification(notification)
    
    # Register a callback
    callback_mock = AsyncMock()
    consensus_manager.register_decision_callback(1, callback_mock)
    
    # Give time for the callback to be called
    await asyncio.sleep(0.1)
    
    # Check callback
    callback_mock.assert_called_once()
    call_args = callback_mock.call_args
    assert call_args[0][0] == 1  # instance_id
    assert call_args[0][1] == 42  # proposal_number
    assert call_args[0][2]["data"] == "test"  # value

def test_is_decided(consensus_manager):
    """Test checking if an instance is decided."""
    # Initially, nothing is decided
    assert consensus_manager.is_decided(1) == False
    
    # Manually set a decision
    consensus_manager.decisions[1] = (42, {"data": "test"})
    
    # Now it should be decided
    assert consensus_manager.is_decided(1) == True

def test_get_decision(consensus_manager):
    """Test getting a decision."""
    # Initially, no decisions
    assert consensus_manager.get_decision(1) is None
    
    # Manually set a decision
    consensus_manager.decisions[1] = (42, {"data": "test"})
    
    # Now we should get the decision
    decision = consensus_manager.get_decision(1)
    assert decision[0] == 42
    assert decision[1]["data"] == "test"

def test_get_instance_info(consensus_manager):
    """Test getting instance information."""
    # Initially, no instance info
    assert consensus_manager.get_instance_info(1) == {}
    
    # Add some votes
    consensus_manager.votes[1][42][consensus_manager._hash_value({"data": "test"})].add(1)
    
    # Now we should get the instance info
    info = consensus_manager.get_instance_info(1)
    assert info["instanceId"] == 1
    assert info["decided"] == False
    assert "votes" in info
    
    # Add a decision
    consensus_manager.decisions[1] = (42, {"data": "test"})
    
    # Now the info should include the decision
    info = consensus_manager.get_instance_info(1)
    assert info["decided"] == True
    assert info["proposalNumber"] == 42
    assert info["value"]["data"] == "test"

def test_get_all_instance_ids(consensus_manager):
    """Test getting all instance IDs."""
    # Initially, no instances
    assert consensus_manager.get_all_instance_ids() == []
    
    # Add some votes and decisions
    consensus_manager.votes[1][42][consensus_manager._hash_value({"data": "test"})].add(1)
    consensus_manager.decisions[2] = (42, {"data": "test"})
    
    # Now we should get both instance IDs
    assert sorted(consensus_manager.get_all_instance_ids()) == [1, 2]
    
    # With decided_only=True, we should only get instance 2
    assert consensus_manager.get_all_instance_ids(decided_only=True) == [2]

def test_get_stats(consensus_manager):
    """Test getting statistics."""
    # Initially, all stats are zero
    stats = consensus_manager.get_stats()
    assert stats["active_instances"] == 0
    assert stats["decided_instances"] == 0
    assert stats["notifications_processed"] == 0
    assert stats["decisions_made"] == 0
    
    # Add some data
    consensus_manager.votes[1][42][consensus_manager._hash_value({"data": "test"})].add(1)
    consensus_manager.decisions[2] = (42, {"data": "test"})
    consensus_manager.notifications_processed = 5
    consensus_manager.decisions_made = 1
    
    # Check stats again
    stats = consensus_manager.get_stats()
    assert stats["active_instances"] == 1
    assert stats["decided_instances"] == 1
    assert stats["notifications_processed"] == 5
    assert stats["decisions_made"] == 1

def test_hash_value(consensus_manager):
    """Test the value hashing function."""
    # Simple values
    assert consensus_manager._hash_value("test") == "test"
    assert consensus_manager._hash_value(42) == "42"
    assert consensus_manager._hash_value(True) == "True"
    
    # Dict values
    dict1 = {"a": 1, "b": 2}
    dict2 = {"b": 2, "a": 1}  # Same content, different order
    assert consensus_manager._hash_value(dict1) == consensus_manager._hash_value(dict2)
    
    # List values
    list1 = [1, 2, 3]
    list2 = [1, 2, 3]
    assert consensus_manager._hash_value(list1) == consensus_manager._hash_value(list2)