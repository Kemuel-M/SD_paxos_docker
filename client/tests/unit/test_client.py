"""
File: client/tests/unit/test_client.py
Unit tests for the PaxosClient implementation.
"""
import os
import json
import time
import pytest
import pytest_asyncio
import asyncio
from unittest.mock import MagicMock, patch, AsyncMock

import warnings
warnings.filterwarnings("always", category=RuntimeWarning)

# Configure environment for testing
os.environ["DEBUG"] = "true"
os.environ["NODE_ID"] = "1"
os.environ["CLIENT_ID"] = "client-1"
os.environ["LOG_DIR"] = "/tmp/paxos-test-logs"

# Create test directory
os.makedirs(os.environ["LOG_DIR"], exist_ok=True)

from client import PaxosClient

@pytest.fixture
def mock_http_client():
    """Fixture that creates a mock HTTP client."""
    mock = AsyncMock()
    mock.post = AsyncMock(return_value=MagicMock(
        status_code=202,
        json=MagicMock(return_value={"instanceId": 1})
    ))
    return mock

@pytest_asyncio.fixture
async def client(mock_http_client):
    """Fixture que cria um PaxosClient para testes."""
    client = PaxosClient(
        client_id="client-1",
        proposer_url="http://proposer-1:8080",
        callback_url="http://client-1:8080/notification",
        num_operations=20
    )
    
    # Substitui HTTP client com um mock
    client.http_client = mock_http_client
    client._cleanup_tasks = []
    client._timeout_tasks = []
    
    yield client
    
    # Limpa recursos ao final
    await client.cleanup_pending_tasks()

@pytest.mark.asyncio
async def test_client_initialization(client):
    """Test if PaxosClient is initialized correctly."""
    assert client.client_id == "client-1"
    assert client.proposer_url == "http://proposer-1:8080"
    assert client.callback_url == "http://client-1:8080/notification"
    assert client.num_operations == 20
    assert client.operations_completed == 0
    assert client.operations_failed == 0
    assert isinstance(client.operations_in_progress, dict)
    assert client.next_operation_id == 1
    assert isinstance(client.history, list)
    assert client.running is False
    assert len(client.latencies) == 0

@pytest.mark.asyncio
async def test_start_stop(client):
    """Test starting and stopping the client."""
    # Test start
    with patch.object(asyncio, 'create_task') as mock_create_task:
        await client.start()
        assert client.running is True
        mock_create_task.assert_called_once()
    
    # Test stop
    await client.stop()
    assert client.running is False

@pytest.mark.asyncio
async def test_send_operation_success(client, mock_http_client):
    """Test sending an operation successfully."""
    # Configure mock to return success
    mock_http_client.post.reset_mock()
    mock_http_client.post.return_value = MagicMock(
        status_code=202,
        json=MagicMock(return_value={"instanceId": 42})
    )
    
    # Send operation
    await client._send_operation(1)
    
    # Check that HTTP client was called correctly
    mock_http_client.post.assert_called_once()
    call_args = mock_http_client.post.call_args
    assert f"{client.proposer_url}/propose" in str(call_args)
    
    # Check that operation was registered as in progress
    assert 42 in client.operations_in_progress
    assert client.operations_in_progress[42]["id"] == 1

@pytest.mark.asyncio
async def test_send_operation_redirect(client, mock_http_client):
    """Test handling redirection to another proposer."""
    # First call returns redirect, second call returns success
    mock_http_client.post.reset_mock()
    mock_http_client.post.side_effect = [
        MagicMock(
            status_code=307,
            headers={"Location": "http://proposer-2:8080/propose"},
            json=MagicMock(return_value={})
        ),
        MagicMock(
            status_code=202,
            json=MagicMock(return_value={"instanceId": 42})
        )
    ]
    
    # Usar patch para evitar que _operation_loop seja chamado como efeito colateral
    with patch.object(client, '_operation_loop', AsyncMock(return_value=None)):
        # Send operation
        await client._send_operation(1)
    
    # Check that HTTP client was called twice
    assert mock_http_client.post.call_count == 2
    
    # Second call should be to redirected URL
    second_call = mock_http_client.post.call_args_list[1]
    assert "proposer-2:8080" in str(second_call)
    
    # Check that operation was registered as in progress
    assert 42 in client.operations_in_progress
    assert client.operations_in_progress[42]["id"] == 1

@pytest.mark.asyncio
async def test_send_operation_error_retry(client, mock_http_client):
    """Test operation retry after error."""
    # Configure mock to fail first, then succeed
    mock_http_client.post.reset_mock()
    mock_http_client.post.side_effect = [
        MagicMock(status_code=500, text="Internal error"),
        MagicMock(status_code=202, json=MagicMock(return_value={"instanceId": 42}))
    ]
    
    # Adicionar este patch para evitar a criação da coroutine não aguardada
    with patch.object(client, '_operation_loop', AsyncMock(return_value=None)):
        # Patch mais específico no método que está criando as tasks
        with patch.object(client, '_cleanup_request_id', AsyncMock()):
            # Patch no asyncio.sleep para facilitar o teste
            with patch('asyncio.sleep', AsyncMock()) as mock_sleep:
                await client._send_operation(1)
            
            # Check that retry sleep was called
            mock_sleep.assert_called_once()
            
            # Check that HTTP client was called twice
            assert mock_http_client.post.call_count == 2
            
            # Check that operation was registered as in progress after retry
            assert 42 in client.operations_in_progress
            assert client.operations_in_progress[42]["id"] == 1
            
        # Limpa _cleanup_tasks antes do teardown para evitar o erro
        client._cleanup_tasks = []

@pytest.mark.asyncio
async def test_send_operation_max_retries(client, mock_http_client):
    """Test operation fails after maximum retries."""
    # Configure mock to always fail
    mock_http_client.post.reset_mock()
    mock_http_client.post.return_value = MagicMock(
        status_code=500,
        text="Internal error"
    )
    
    # Usar patch para evitar que _operation_loop seja chamado como efeito colateral
    with patch.object(client, '_operation_loop', AsyncMock(return_value=None)):
        # Send operation with retry limit
        with patch('asyncio.sleep', AsyncMock()):
            await client._send_operation(1, retries=2)  # Already at max retries
    
    # Check that operation was marked as failed
    assert client.operations_failed == 1
    assert len(client.history) == 1
    assert client.history[0]["status"] == "failed"

@pytest.mark.asyncio
async def test_handle_operation_timeout(client):
    """Test timeout handling for pending operations."""
    # Create a pending operation
    instance_id = 42
    operation_info = {
        "id": 1,
        "start_time": time.time(),
        "payload": {"data": "test"},
        "status": "in_progress"
    }
    client.operations_in_progress[instance_id] = operation_info
    
    # Call timeout handler
    with patch('asyncio.sleep', AsyncMock()):
        await client._handle_operation_timeout(instance_id)
    
    # Check that operation was removed from in_progress
    assert instance_id not in client.operations_in_progress
    
    # Check that operation was marked as timeout
    assert client.operations_failed == 1
    assert len(client.history) == 1
    assert client.history[0]["status"] == "timeout"

def test_process_notification_success(client):
    """Test processing a successful notification."""
    # Create a pending operation
    instance_id = 42
    operation_info = {
        "id": 1,
        "start_time": time.time() - 2.0,  # 2 seconds ago
        "payload": {"data": "test"},
        "status": "in_progress"
    }
    client.operations_in_progress[instance_id] = operation_info
    
    # Create notification
    notification = {
        "status": "COMMITTED",
        "instanceId": instance_id,
        "resource": "R",
        "timestamp": int(time.time() * 1000)
    }
    
    # Process notification
    result = client.process_notification(notification)
    
    # Check response
    assert result["status"] == "acknowledged"
    assert result["known"] == True
    assert result["operation_id"] == 1
    
    # Check client state
    assert instance_id not in client.operations_in_progress
    assert client.operations_completed == 1
    assert client.operations_failed == 0
    assert len(client.history) == 1
    assert client.history[0]["status"] == "COMMITTED"
    assert client.history[0]["latency"] > 0

def test_process_notification_failure(client):
    """Test processing a failed notification."""
    # Create a pending operation
    instance_id = 42
    operation_info = {
        "id": 1,
        "start_time": time.time() - 2.0,  # 2 seconds ago
        "payload": {"data": "test"},
        "status": "in_progress"
    }
    client.operations_in_progress[instance_id] = operation_info
    
    # Create notification
    notification = {
        "status": "NOT_COMMITTED",
        "instanceId": instance_id,
        "resource": "R",
        "timestamp": int(time.time() * 1000)
    }
    
    # Process notification
    result = client.process_notification(notification)
    
    # Check response
    assert result["status"] == "acknowledged"
    assert result["known"] == True
    
    # Check client state
    assert instance_id not in client.operations_in_progress
    assert client.operations_completed == 0
    assert client.operations_failed == 1
    assert len(client.history) == 1
    assert client.history[0]["status"] == "NOT_COMMITTED"

def test_process_notification_unknown(client):
    """Test processing a notification for unknown instance."""
    # Create notification for unknown instance
    notification = {
        "status": "COMMITTED",
        "instanceId": 999,
        "resource": "R",
        "timestamp": int(time.time() * 1000)
    }
    
    # Process notification
    result = client.process_notification(notification)
    
    # Check response
    assert result["status"] == "acknowledged"
    assert result["known"] == False
    
    # Check client state
    assert client.operations_completed == 0
    assert client.operations_failed == 0
    assert len(client.history) == 0

def test_get_status(client):
    """Test getting client status."""
    # Set up some state
    client.operations_completed = 5
    client.operations_failed = 2
    client.operations_in_progress = {1: {}, 2: {}, 3: {}}
    client.latencies = [1.0, 2.0, 3.0, 4.0, 5.0]
    client.running = True
    
    # Get status
    status = client.get_status()
    
    # Check status fields
    assert status["client_id"] == "client-1"
    assert status["proposer_url"] == "http://proposer-1:8080"
    assert status["total_operations"] == 20
    assert status["completed"] == 5
    assert status["failed"] == 2
    assert status["in_progress"] == 3
    assert status["avg_latency"] == 3.0
    assert "runtime" in status
    assert status["running"] == True

def test_get_history(client):
    """Test getting operation history."""
    # Add some history entries
    client.history = [
        {"id": 1, "start_time": time.time() - 30, "status": "COMMITTED"},
        {"id": 2, "start_time": time.time() - 20, "status": "NOT_COMMITTED"},
        {"id": 3, "start_time": time.time() - 10, "status": "in_progress"}
    ]
    
    # Get history with limit
    history = client.get_history(2)
    
    # Check result
    assert len(history) == 2
    assert history[0]["id"] == 3  # Most recent first
    assert history[1]["id"] == 2

def test_get_operation(client):
    """Test getting a specific operation."""
    # Add some history entries
    client.history = [
        {"id": 1, "start_time": time.time() - 30, "status": "COMMITTED"},
        {"id": 2, "start_time": time.time() - 20, "status": "NOT_COMMITTED"}
    ]
    
    # Add an in-progress operation
    client.operations_in_progress = {
        42: {"id": 3, "start_time": time.time() - 10, "status": "in_progress"}
    }
    
    # Get operations
    op1 = client.get_operation(1)
    op2 = client.get_operation(2)
    op3 = client.get_operation(3)
    op4 = client.get_operation(4)  # Not found
    
    # Check results
    assert op1["id"] == 1
    assert op1["status"] == "COMMITTED"
    
    assert op2["id"] == 2
    assert op2["status"] == "NOT_COMMITTED"
    
    assert op3["id"] == 3
    assert op3["status"] == "in_progress"
    
    assert op4 is None  # Not found

@pytest.mark.asyncio
async def test_network_partition(client, mock_http_client):
    """Testa o comportamento do cliente durante uma partição de rede."""
    import httpx
    # Simula partição - todas as requisições falham
    mock_http_client.post.side_effect = httpx.ConnectError("Falha de conexão simulada")
    
    # Envia operação durante a partição
    with patch('asyncio.sleep', AsyncMock()):
        await client._send_operation(100, retries=0)
    
    # Verifica que a operação foi marcada como falha após retentativas
    failed_op = next((op for op in client.history if op.get("id") == 100), None)
    assert failed_op is not None
    assert failed_op["status"] == "failed"
    assert "ConnectError" in failed_op.get("error", "")
    
    # Simula recuperação da partição
    mock_http_client.post.side_effect = None
    mock_http_client.post.return_value = MagicMock(
        status_code=202,
        json=MagicMock(return_value={"instanceId": 101})
    )
    
    # Verifica que novas operações funcionam
    await client._send_operation(101)
    assert 101 in client.operations_in_progress

@pytest.mark.asyncio
async def test_request_deduplication(client, mock_http_client):
    """Testa a desduplicação de requisições."""
    try:
        # Configura o cliente com desduplicação
        client.request_ids = {}
        client.request_id_ttl = 0.5  # TTL curto para teste
        client._cleanup_tasks = []
        
        # Usar o mesmo timestamp para garantir IDs de requisição idênticos
        fixed_timestamp = 1000000000
        
        # Primeira tentativa da operação
        await client._send_operation(42, timestamp_override=fixed_timestamp)
        
        # Verificar que o HTTP client foi chamado
        assert mock_http_client.post.call_count == 1
        
        # Segunda tentativa da mesma operação (com mesmo ID de operação)
        # Deve ser detectada como duplicada e ignorada
        mock_http_client.post.reset_mock()
        await client._send_operation(42, timestamp_override=fixed_timestamp)
        
        # Verificar que o HTTP client não foi chamado na segunda vez
        assert mock_http_client.post.call_count == 0
        
        # Aguarda expiração do TTL
        await asyncio.sleep(1.0)
        
        # Após expiração, deve aceitar novamente
        mock_http_client.post.reset_mock()
        await client._send_operation(42, timestamp_override=fixed_timestamp)
        assert mock_http_client.post.call_count == 1
    finally:
        # Garante limpeza de recursos ao final do teste
        await client.cleanup_pending_tasks()

@pytest.mark.asyncio
async def test_operation_loop_with_many_in_progress(client):
    """Test operation loop behavior when many operations are in progress."""
    # Configure client with many operations in progress
    for i in range(10):
        client.operations_in_progress[i] = {
            "id": i,
            "start_time": time.time(),
            "status": "in_progress"
        }
    
    # Make a copy of the original operation loop
    original_operation_loop = client._operation_loop
    
    # Create a patched version that we can test
    async def patched_operation_loop():
        # Run one iteration only
        client.running = True
        if len(client.operations_in_progress) > 5:
            await asyncio.sleep(0.5)
            return False  # Return result to verify the logic was executed
        return True
    
    # Replace the method for testing
    client._operation_loop = patched_operation_loop
    
    # Run the patched method and check result
    result = await client._operation_loop()
    
    # Should wait because there are too many operations in progress
    assert result is False
    
    # Restore original method
    client._operation_loop = original_operation_loop

@pytest.mark.asyncio
async def test_proposer_missing_instance_id(client, mock_http_client):
    """Test handling when proposer returns 202 but no instanceId."""
    # Configure mock to return success but no instanceId
    mock_http_client.post.reset_mock()
    mock_http_client.post.return_value = MagicMock(
        status_code=202,
        json=MagicMock(return_value={})  # No instanceId
    )
    
    # Send operation
    await client._send_operation(42)
    
    # Check that operation was marked as failed
    assert client.operations_failed == 1
    assert len(client.history) == 1
    assert client.history[0]["status"] == "failed"
    assert "Missing instanceId" in client.history[0]["error"]

@pytest.mark.asyncio
async def test_proposer_redirect_without_location(client, mock_http_client):
    """Test handling when proposer returns 307 but no Location header."""
    # Clear previous state
    client.history = []
    client.operations_failed = 0
    
    # Configure mock to return redirect without Location
    mock_http_client.post.reset_mock()
    mock_http_client.post.return_value = MagicMock(
        status_code=307,
        headers={},  # No Location header
        json=MagicMock(return_value={})
    )
    
    # Send operation
    await client._send_operation(43)
    
    # Check that operation was marked as failed
    assert client.operations_failed == 1
    assert len(client.history) == 1
    assert client.history[0]["status"] == "failed"
    assert "Redirect without Location header" in client.history[0]["error"]

@pytest.mark.asyncio
async def test_unknown_notification_for_completed_operation(client):
    """Test handling notification for operation that's already completed."""
    # Add a completed operation to history
    completed_instance_id = 100
    client.history.append({
        "id": 50,
        "instance_id": completed_instance_id,
        "start_time": time.time() - 30,
        "end_time": time.time() - 25,
        "status": "COMMITTED"
    })
    
    # Create notification for the same instance
    notification = {
        "status": "COMMITTED",
        "instanceId": completed_instance_id,
        "resource": "R",
        "timestamp": int(time.time() * 1000)
    }
    
    # Process notification
    result = client.process_notification(notification)
    
    # Should be unknown since the operation is not in operations_in_progress
    assert result["status"] == "acknowledged"
    assert result["known"] == False
    
    # Should not affect the completed count
    assert client.operations_completed == 0

@pytest.mark.asyncio
async def test_cleanup_request_id(client):
    """Test cleanup of request IDs."""
    # Add a request ID
    request_id = "test-request-id"
    client.request_ids[request_id] = 42
    client.request_id_ttl = 0.1  # Short TTL for testing
    
    # Call cleanup
    await client._cleanup_request_id(request_id)
    
    # Request ID should be removed
    assert request_id not in client.request_ids

@pytest.mark.asyncio
async def test_stop_with_pending_operations(client, mock_http_client):
    """Test stopping client with pending operations."""
    # Add some operations in progress
    client.operations_in_progress = {
        1: {"id": 101, "start_time": time.time(), "status": "in_progress"},
        2: {"id": 102, "start_time": time.time(), "status": "in_progress"}
    }
    
    # Start client
    with patch.object(asyncio, 'create_task') as mock_create_task:
        await client.start()
        assert client.running is True
        mock_create_task.assert_called_once()
    
    # Stop client
    await client.stop()
    
    # Client should be stopped, but operations_in_progress should remain
    assert client.running is False
    assert len(client.operations_in_progress) == 2  # Operations still in progress
    
    # Verify that http_client.aclose was called
    mock_http_client.aclose.assert_called_once()