"""
File: learner/tests/unit/test_rowa.py
Unit tests for the RowaManager component.
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

from rowa import RowaManager

@pytest.fixture
def two_phase_manager():
    """Fixture that creates a mock TwoPhaseCommitManager."""
    mock = AsyncMock()
    mock.execute_transaction = AsyncMock(return_value=(True, {"version": 1, "timestamp": int(time.time() * 1000)}))
    return mock

@pytest.fixture
def rowa_manager(two_phase_manager):
    """Fixture that creates a RowaManager for tests."""
    stores = ["store-1:8080", "store-2:8080", "store-3:8080"]
    manager = RowaManager(node_id=1, stores=stores, two_phase_manager=two_phase_manager)
    
    # Mock HTTP client
    manager.http_client = AsyncMock()
    manager.http_client.get = AsyncMock(return_value={"data": "test", "version": 1})
    
    return manager

@pytest.mark.asyncio
async def test_initialization(rowa_manager, two_phase_manager):
    """Test if RowaManager is initialized correctly."""
    assert rowa_manager.node_id == 1
    assert len(rowa_manager.stores) == 3
    assert rowa_manager.two_phase_manager == two_phase_manager
    assert rowa_manager.current_read_index == 0
    assert rowa_manager.reads_processed == 0
    assert rowa_manager.writes_processed == 0
    assert rowa_manager.write_successes == 0
    assert rowa_manager.write_failures == 0

@pytest.mark.asyncio
async def test_read_resource_success(rowa_manager):
    """Test reading a resource successfully."""
    # Mock HTTP client response
    expected_response = {"data": "test", "version": 1}
    rowa_manager.http_client.get.reset_mock()
    rowa_manager.http_client.get.return_value = expected_response
    
    # Read the resource
    result = await rowa_manager.read_resource("R")
    
    # Check result
    assert result == expected_response
    
    # Check HTTP client was called correctly
    rowa_manager.http_client.get.assert_called_once()
    call_args = rowa_manager.http_client.get.call_args
    assert "http://store-1:8080/resource/R" in call_args[0]
    
    # Check statistics
    assert rowa_manager.reads_processed == 1
    
    # Check round-robin index was incremented
    assert rowa_manager.current_read_index == 1

@pytest.mark.asyncio
async def test_read_resource_failure_retry(rowa_manager):
    """Test reading a resource with failure and retry."""
    # Make first store fail, second succeed
    rowa_manager.http_client.get.reset_mock()
    rowa_manager.http_client.get.side_effect = [
        Exception("Connection error"),
        {"data": "test", "version": 1}
    ]
    
    # Read the resource
    result = await rowa_manager.read_resource("R")
    
    # Check result
    assert result == {"data": "test", "version": 1}
    
    # Check HTTP client was called twice
    assert rowa_manager.http_client.get.call_count == 2
    
    # First call should be to store-1, second to store-2
    first_call = rowa_manager.http_client.get.call_args_list[0]
    second_call = rowa_manager.http_client.get.call_args_list[1]
    assert "http://store-1:8080/resource/R" in first_call[0]
    assert "http://store-2:8080/resource/R" in second_call[0]
    
    # Check statistics
    assert rowa_manager.reads_processed == 1
    
    # Check round-robin index was incremented twice
    assert rowa_manager.current_read_index == 2

@pytest.mark.asyncio
async def test_read_resource_all_failures(rowa_manager):
    """Test reading a resource with all stores failing."""
    # Make all stores fail
    rowa_manager.http_client.get.reset_mock()
    rowa_manager.http_client.get.side_effect = Exception("Connection error")
    
    # Read the resource
    result = await rowa_manager.read_resource("R")
    
    # Check result
    assert result is None
    
    # Check HTTP client was called for each store
    assert rowa_manager.http_client.get.call_count == 3
    
    # Check statistics
    assert rowa_manager.reads_processed == 1

@pytest.mark.asyncio
async def test_write_resource_success(rowa_manager, two_phase_manager):
    """Test writing a resource successfully."""
    # Mock two_phase_manager.execute_transaction
    two_phase_manager.execute_transaction.reset_mock()
    two_phase_manager.execute_transaction.return_value = (True, {"version": 2})
    
    # Write the resource
    success, result = await rowa_manager.write_resource(
        resource_id="R",
        data="new data",
        client_id="client-1",
        instance_id=42,
        client_timestamp=int(time.time() * 1000)
    )
    
    # Check result
    assert success is True
    assert result == {"version": 2}
    
    # Check two_phase_manager.execute_transaction was called correctly
    two_phase_manager.execute_transaction.assert_called_once()
    call_args = two_phase_manager.execute_transaction.call_args
    assert call_args[0][0] == "R"  # resource_id
    assert call_args[0][1]["data"] == "new data"  # value.data
    assert call_args[0][1]["clientId"] == "client-1"  # value.clientId
    assert call_args[0][1]["instanceId"] == 42  # value.instanceId
    
    # Check statistics
    assert rowa_manager.writes_processed == 1
    assert rowa_manager.write_successes == 1
    assert rowa_manager.write_failures == 0

@pytest.mark.asyncio
async def test_write_resource_failure(rowa_manager, two_phase_manager):
    """Test writing a resource with failure."""
    # Mock two_phase_manager.execute_transaction to fail
    two_phase_manager.execute_transaction.reset_mock()
    two_phase_manager.execute_transaction.return_value = (False, None)
    
    # Write the resource
    success, result = await rowa_manager.write_resource(
        resource_id="R",
        data="new data",
        client_id="client-1",
        instance_id=42,
        client_timestamp=int(time.time() * 1000)
    )
    
    # Check result
    assert success is False
    assert result is None
    
    # Check statistics
    assert rowa_manager.writes_processed == 1
    assert rowa_manager.write_successes == 0
    assert rowa_manager.write_failures == 1

@pytest.mark.asyncio
async def test_simulate_resource_access(rowa_manager):
    """Test simulating resource access."""
    # Simulate access
    start_time = time.time()
    delay = await rowa_manager.simulate_resource_access()
    end_time = time.time()
    
    # Check delay
    assert 0.2 <= delay <= 1.0
    assert start_time + delay <= end_time + 0.1  # Allow small error

def test_get_stats(rowa_manager):
    """Test getting statistics."""
    # Update statistics
    rowa_manager.reads_processed = 5
    rowa_manager.writes_processed = 10
    rowa_manager.write_successes = 8
    rowa_manager.write_failures = 2
    
    # Get stats
    stats = rowa_manager.get_stats()
    
    # Check stats
    assert stats["reads_processed"] == 5
    assert stats["writes_processed"] == 10
    assert stats["write_successes"] == 8
    assert stats["write_failures"] == 2
    assert stats["success_rate"] == 0.8
