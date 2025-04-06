"""
File: learner/tests/unit/test_two_phase.py
Unit tests for the TwoPhaseCommitManager component.
"""
import os
import json
import time
import pytest
import asyncio
from unittest.mock import MagicMock, patch, AsyncMock, call

import warnings
warnings.filterwarnings("always", category=RuntimeWarning)

# Configure environment for testing
os.environ["DEBUG"] = "true"
os.environ["NODE_ID"] = "1"

from two_phase import TwoPhaseCommitManager

@pytest.fixture
def two_phase_manager():
    """Fixture that creates a TwoPhaseCommitManager for tests."""
    stores = ["store-1:8080", "store-2:8080", "store-3:8080"]
    manager = TwoPhaseCommitManager(node_id=1, stores=stores)
    
    # Mock HTTP client
    manager.http_client = AsyncMock()
    manager.http_client.post = AsyncMock()
    
    return manager

@pytest.mark.asyncio
async def test_initialization(two_phase_manager):
    """Test if TwoPhaseCommitManager is initialized correctly."""
    assert two_phase_manager.node_id == 1
    assert len(two_phase_manager.stores) == 3
    assert isinstance(two_phase_manager.active_transactions, dict)
    assert two_phase_manager.transactions_started == 0
    assert two_phase_manager.transactions_committed == 0
    assert two_phase_manager.transactions_aborted == 0

@pytest.mark.asyncio
async def test_execute_transaction_success(two_phase_manager):
    """Test executing a transaction successfully."""
    # Mock prepare responses
    prepare_responses = [
        {"ready": True, "currentVersion": 1},
        {"ready": True, "currentVersion": 1},
        {"ready": True, "currentVersion": 1}
    ]
    
    # Mock commit responses
    commit_responses = [
        {"success": True, "version": 2},
        {"success": True, "version": 2},
        {"success": True, "version": 2}
    ]
    
    # Set up HTTP client mock
    two_phase_manager.http_client.post.side_effect = prepare_responses + commit_responses
    
    # Execute transaction
    value = {
        "data": "test data",
        "instanceId": 42,
        "clientId": "client-1",
        "timestamp": int(time.time() * 1000)
    }
    
    success, result = await two_phase_manager.execute_transaction("R", value)
    
    # Check result
    assert success is True
    assert result == {"success": True, "version": 2}
    
    # Check HTTP client was called correctly
    assert two_phase_manager.http_client.post.call_count == 6  # 3 prepare + 3 commit
    
    # Check statistics
    assert two_phase_manager.transactions_started == 1
    assert two_phase_manager.transactions_committed == 1
    assert two_phase_manager.transactions_aborted == 0

@pytest.mark.asyncio
async def test_execute_transaction_prepare_failure(two_phase_manager):
    """Test executing a transaction with prepare phase failure."""
    # Mock prepare responses - one store not ready
    prepare_responses = [
        {"ready": True, "currentVersion": 1},
        {"ready": False, "reason": "Busy"},
        {"ready": True, "currentVersion": 1}
    ]
    
    # Set up HTTP client mock
    two_phase_manager.http_client.post.side_effect = prepare_responses
    
    # Execute transaction
    value = {
        "data": "test data",
        "instanceId": 42,
        "clientId": "client-1",
        "timestamp": int(time.time() * 1000)
    }
    
    # Should fail after one attempt
    success, result = await two_phase_manager.execute_transaction("R", value, max_retries=1)
    
    # Check result
    assert success is False
    assert result is None
    
    # Verificar chamadas à fase de preparação
    prepare_calls = [call for call in two_phase_manager.http_client.post.call_args_list 
                    if '/prepare' in str(call)]
    assert len(prepare_calls) == 3, f"Expected 3 prepare calls, got {len(prepare_calls)}"
    
    # Verificar chamadas à fase de aborto
    abort_calls = [call for call in two_phase_manager.http_client.post.call_args_list 
                  if '/abort' in str(call)]
    assert len(abort_calls) == 2, f"Expected 2 abort calls, got {len(abort_calls)}"
    
    # Verificar número total de chamadas
    assert two_phase_manager.http_client.post.call_count == 5, f"Expected 5 total calls, got {two_phase_manager.http_client.post.call_count}"
    
    # Verificar estatísticas
    assert two_phase_manager.transactions_started == 1
    assert two_phase_manager.transactions_committed == 0
    assert two_phase_manager.transactions_aborted == 1

@pytest.mark.asyncio
async def test_execute_transaction_commit_failure(two_phase_manager):
    """Test executing a transaction with commit phase failure."""
    # Mock prepare responses - all ready
    prepare_responses = [
        {"ready": True, "currentVersion": 1},
        {"ready": True, "currentVersion": 1},
        {"ready": True, "currentVersion": 1}
    ]
    
    # Mock commit responses - one fails
    commit_responses = [
        {"success": True, "version": 2},
        {"success": False, "reason": "Error writing"},
        {"success": True, "version": 2}
    ]
    
    # Set up HTTP client mock
    two_phase_manager.http_client.post.side_effect = prepare_responses + commit_responses
    
    # Execute transaction
    value = {
        "data": "test data",
        "instanceId": 42,
        "clientId": "client-1",
        "timestamp": int(time.time() * 1000)
    }
    
    # Should fail after one attempt
    success, result = await two_phase_manager.execute_transaction("R", value, max_retries=1)
    
    # Check result
    assert success is False
    assert result is None
    
    # Check HTTP client was called for both phases
    assert two_phase_manager.http_client.post.call_count == 6  # 3 prepare + 3 commit
    
    # Check statistics
    assert two_phase_manager.transactions_started == 1
    assert two_phase_manager.transactions_committed == 0
    assert two_phase_manager.transactions_aborted == 1

@pytest.mark.asyncio
async def test_execute_transaction_communication_error(two_phase_manager):
    """Test executing a transaction with communication error."""
    # Mock HTTP client to raise exception
    two_phase_manager.http_client.post.side_effect = Exception("Connection error")
    
    # Execute transaction
    value = {
        "data": "test data",
        "instanceId": 42,
        "clientId": "client-1",
        "timestamp": int(time.time() * 1000)
    }
    
    # Should fail after one attempt
    success, result = await two_phase_manager.execute_transaction("R", value, max_retries=1)
    
    # Check result
    assert success is False
    assert result is None
    
    # Check statistics
    assert two_phase_manager.transactions_started == 1
    assert two_phase_manager.transactions_committed == 0
    assert two_phase_manager.transactions_aborted == 1

@pytest.mark.asyncio
async def test_execute_transaction_retry_success(two_phase_manager):
    """Test executing a transaction with retry success."""
    # First attempt: prepare phase failure
    first_attempt = [
        {"ready": True, "currentVersion": 1},
        {"ready": False, "reason": "Busy"},
        {"ready": True, "currentVersion": 1}
    ]
    
    # Second attempt: all succeed
    second_prepare = [
        {"ready": True, "currentVersion": 1},
        {"ready": True, "currentVersion": 1},
        {"ready": True, "currentVersion": 1}
    ]
    
    second_commit = [
        {"success": True, "version": 2},
        {"success": True, "version": 2},
        {"success": True, "version": 2}
    ]
    
    # Set up HTTP client mock
    two_phase_manager.http_client.post.side_effect = first_attempt + second_prepare + second_commit
    
    # Execute transaction with retry
    value = {
        "data": "test data",
        "instanceId": 42,
        "clientId": "client-1",
        "timestamp": int(time.time() * 1000)
    }
    
    # Should succeed on second attempt
    success, result = await two_phase_manager.execute_transaction("R", value, max_retries=2)
    
    # Check result
    assert success is True
    assert result == {"success": True, "version": 2}
    
    # Check HTTP client was called for all attempts
    # 3 (first prepare) + 3 (second prepare) + 3 (second commit) = 9
    assert two_phase_manager.http_client.post.call_count == 9
    
    # Check statistics
    assert two_phase_manager.transactions_started == 1
    assert two_phase_manager.transactions_committed == 1
    assert two_phase_manager.transactions_aborted == 0

@pytest.mark.asyncio
async def test_prepare_phase(two_phase_manager):
    """Test the prepare phase directly."""
    # Mock prepare responses
    prepare_responses = [
        {"ready": True, "currentVersion": 1},
        {"ready": True, "currentVersion": 1},
        {"ready": True, "currentVersion": 1}
    ]
    
    # Set up HTTP client mock
    two_phase_manager.http_client.post.side_effect = prepare_responses
    
    # Create a transaction ID
    transaction_id = f"42_{int(time.time() * 1000)}"
    
    # Add transaction to active_transactions
    two_phase_manager.active_transactions[transaction_id] = {
        "status": "started",
        "resource_id": "R",
        "value": {"data": "test"},
        "ready_participants": set(),
        "start_time": int(time.time() * 1000)
    }
    
    # Execute prepare phase
    success, result = await two_phase_manager._prepare_phase(
        transaction_id, "R", {"data": "test"})
    
    # Check result
    assert success is True
    assert len(result) == 3
    assert all(store in result for store in two_phase_manager.stores)
    
    # Check transaction state
    assert len(two_phase_manager.active_transactions[transaction_id]["ready_participants"]) == 3

@pytest.mark.asyncio
async def test_commit_phase(two_phase_manager):
    """Test the commit phase directly."""
    # Mock commit responses
    commit_responses = [
        {"success": True, "version": 2},
        {"success": True, "version": 2},
        {"success": True, "version": 2}
    ]
    
    # Set up HTTP client mock
    two_phase_manager.http_client.post.side_effect = commit_responses
    
    # Create a transaction ID
    transaction_id = f"42_{int(time.time() * 1000)}"
    
    # Add transaction to active_transactions with ready participants
    two_phase_manager.active_transactions[transaction_id] = {
        "status": "prepared",
        "resource_id": "R",
        "value": {"data": "test"},
        "ready_participants": set(two_phase_manager.stores),
        "start_time": int(time.time() * 1000)
    }
    
    # Execute commit phase
    success, result = await two_phase_manager._commit_phase(
        transaction_id, "R", {"data": "test"}, 2)
    
    # Check result
    assert success is True
    assert result == {"success": True, "version": 2}

@pytest.mark.asyncio
async def test_abort_phase(two_phase_manager):
    """Test the abort phase directly."""
    # Mock abort responses
    abort_responses = [
        {"success": True},
        {"success": True},
        {"success": True}
    ]
    
    # Set up HTTP client mock
    two_phase_manager.http_client.post.side_effect = abort_responses
    
    # Create a transaction ID
    transaction_id = f"42_{int(time.time() * 1000)}"
    
    # Add transaction to active_transactions with ready participants
    two_phase_manager.active_transactions[transaction_id] = {
        "status": "prepared",
        "resource_id": "R",
        "value": {"data": "test"},
        "ready_participants": set(two_phase_manager.stores),
        "start_time": int(time.time() * 1000)
    }
    
    # Execute abort phase
    success = await two_phase_manager._abort_phase(
        transaction_id, "R", {"data": "test"})
    
    # Check result
    assert success is True
    
    # Check transaction state
    assert two_phase_manager.active_transactions[transaction_id]["status"] == "aborted"
    
    # Check statistics
    assert two_phase_manager.transactions_aborted == 1

def test_get_transaction_status(two_phase_manager):
    """Test getting transaction status."""
    # Create a transaction ID
    transaction_id = f"42_{int(time.time() * 1000)}"
    
    # Initially, transaction doesn't exist
    assert two_phase_manager.get_transaction_status(transaction_id) is None
    
    # Add transaction to active_transactions
    transaction_data = {
        "status": "started",
        "resource_id": "R",
        "value": {"data": "test"},
        "ready_participants": set(),
        "start_time": int(time.time() * 1000)
    }
    two_phase_manager.active_transactions[transaction_id] = transaction_data
    
    # Now should return the transaction
    assert two_phase_manager.get_transaction_status(transaction_id) == transaction_data

def test_get_stats(two_phase_manager):
    """Test getting statistics."""
    # Update statistics
    two_phase_manager.transactions_started = 10
    two_phase_manager.transactions_committed = 7
    two_phase_manager.transactions_aborted = 3
    
    # Add some active transactions
    two_phase_manager.active_transactions["tx1"] = {"status": "started"}
    two_phase_manager.active_transactions["tx2"] = {"status": "preparing"}
    two_phase_manager.active_transactions["tx3"] = {"status": "aborted"}
    
    # Get stats
    stats = two_phase_manager.get_stats()
    
    # Check stats
    assert stats["transactions_started"] == 10
    assert stats["transactions_committed"] == 7
    assert stats["transactions_aborted"] == 3
    assert stats["active_transactions"] == 2  # Only started and preparing count as active
