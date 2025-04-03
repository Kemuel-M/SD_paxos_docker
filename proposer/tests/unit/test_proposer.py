"""
Testes unitários para o componente Proposer.
"""
import os
import json
import time
import pytest
import asyncio
from unittest.mock import MagicMock, patch, AsyncMock

# Configura variáveis de ambiente para teste
os.environ["DEBUG"] = "true"
os.environ["NODE_ID"] = "1"

from proposer import Proposer

@pytest.fixture
def proposer():
    """Fixture que cria um proposer para testes."""
    mock_acceptors = ["acceptor-1:8080", "acceptor-2:8080", "acceptor-3:8080", "acceptor-4:8080", "acceptor-5:8080"]
    mock_learners = ["learner-1:8080", "learner-2:8080"]
    mock_stores = ["cluster-store-1:8080", "cluster-store-2:8080", "cluster-store-3:8080"]
    
    prop = Proposer(
        node_id=1,
        acceptors=mock_acceptors,
        learners=mock_learners,
        stores=mock_stores,
        proposal_counter=0,
        last_instance_id=0
    )
    
    # Substitui o http_client por um mock
    prop.http_client = AsyncMock()
    
    return prop

@pytest.mark.asyncio
async def test_proposer_initialization(proposer):
    """Testa se o proposer é inicializado corretamente."""
    assert proposer.node_id == 1
    assert len(proposer.acceptors) == 5
    assert len(proposer.learners) == 2
    assert len(proposer.stores) == 3
    assert proposer.proposal_counter == 0
    assert proposer.last_instance_id == 0
    assert proposer.state == "initialized"

@pytest.mark.asyncio
async def test_propose_adds_to_queue(proposer):
    """Testa se requisições do cliente são adicionadas à fila corretamente."""
    # Mock da fila para verificar a adição
    proposer.pending_queue = AsyncMock()
    
    client_request = {
        "clientId": "client-1",
        "timestamp": int(time.time() * 1000),
        "operation": "WRITE",
        "resource": "R",
        "data": "test_data"
    }
    
    await proposer.propose(client_request)
    
    # Verifica se foi adicionado à fila
    proposer.pending_queue.put.assert_called_once()
    
    # Verifica se o instanceId foi incrementado
    assert proposer.last_instance_id == 1
    
    # Verifica se a proposta foi armazenada no cache
    assert 1 in proposer.proposals

@pytest.mark.asyncio
async def test_run_prepare_phase(proposer):
    """Testa a fase Prepare do algoritmo Paxos."""
    # Mock para respostas dos acceptors
    mock_responses = [
        {"accepted": True, "highestAccepted": -1},  # Acceptor 1
        {"accepted": True, "highestAccepted": 10, "acceptedValue": {"value": "old_value"}},  # Acceptor 2
        {"accepted": True, "highestAccepted": 5},  # Acceptor 3
        {"accepted": False},  # Acceptor 4
        None  # Acceptor 5 (falhou)
    ]
    
    # Mock do método _send_to_acceptors
    proposer._send_to_acceptors = AsyncMock(return_value=mock_responses)
    
    # Executa fase Prepare
    proposal = {"clientRequest": {"data": "test_data"}}
    result = await proposer._run_prepare_phase(1, 42, proposal)
    
    # Verifica resultados
    assert result["success"] == True  # 3 promises > maioria
    assert result["highest_value"] == {"value": "old_value"}  # Valor do acceptor 2
    assert result["promises"] == 3
    assert result["highest_proposal"] == 10

@pytest.mark.asyncio
async def test_run_accept_phase(proposer):
    """Testa a fase Accept do algoritmo Paxos."""
    # Mock para respostas dos acceptors
    mock_responses = [
        {"accepted": True},  # Acceptor 1
        {"accepted": True},  # Acceptor 2
        {"accepted": True},  # Acceptor 3
        {"accepted": False},  # Acceptor 4
        None  # Acceptor 5 (falhou)
    ]
    
    # Mock do método _send_to_acceptors
    proposer._send_to_acceptors = AsyncMock(return_value=mock_responses)
    
    # Executa fase Accept
    proposal = {"clientRequest": {"data": "test_data"}}
    result = await proposer._run_accept_phase(1, proposal, proposal_number=42)
    
    # Verifica resultados
    assert result == True  # 3 aceites > maioria

@pytest.mark.asyncio
async def test_run_paxos_success(proposer):
    """Testa o fluxo completo do Paxos quando bem-sucedido."""
    # Mock das fases Prepare e Accept para retornar sucesso
    proposer._run_prepare_phase = AsyncMock(return_value={"success": True, "highest_value": None})
    proposer._run_accept_phase = AsyncMock(return_value=True)
    
    # Executa Paxos
    proposal = {"clientRequest": {"data": "test_data"}}
    result = await proposer._run_paxos(1, proposal)
    
    # Verifica resultado
    assert result == True
    
    # Verifica se as fases foram chamadas
    proposer._run_prepare_phase.assert_called_once()
    proposer._run_accept_phase.assert_called_once()

@pytest.mark.asyncio
async def test_run_paxos_prepare_failure(proposer):
    """Testa o fluxo do Paxos quando a fase Prepare falha."""
    # Mock da fase Prepare para retornar falha
    proposer._run_prepare_phase = AsyncMock(return_value={"success": False})
    
    # Executa Paxos
    proposal = {"clientRequest": {"data": "test_data"}}
    result = await proposer._run_paxos(1, proposal)
    
    # Verifica resultado
    assert result == False
    
    # Verifica se a fase Accept não foi chamada
    assert proposer._run_prepare_phase.call_count == 3  # Deve tentar 3 vezes

@pytest.mark.asyncio
async def test_send_to_acceptors(proposer):
    """Testa o envio de requisições para acceptors."""
    # Mock das respostas para cada acceptor
    async def mock_send(*args, **kwargs):
        url = args[0]
        if "acceptor-1" in url:
            return {"result": "ok", "acceptorId": 1}
        elif "acceptor-2" in url:
            return {"result": "ok", "acceptorId": 2}
        elif "acceptor-3" in url:
            # Simula timeout
            raise asyncio.TimeoutError()
        elif "acceptor-4" in url:
            # Simula erro HTTP
            raise Exception("HTTP Error")
        else:
            return {"result": "ok", "acceptorId": 5}
    
    # Substitui o método _send_with_retry
    proposer._send_with_retry = mock_send
    
    # Executa envio para todos os acceptors
    results = await proposer._send_to_acceptors("/test-endpoint", {"test": "data"})
    
    # Verifica resultados
    assert len(results) == 5
    assert results[0]["acceptorId"] == 1
    assert results[1]["acceptorId"] == 2
    assert results[2] is None  # Timeout
    assert results[3] is None  # HTTP Error
    assert results[4]["acceptorId"] == 5

@pytest.mark.asyncio
async def test_simulate_resource_access(proposer):
    """Testa a simulação de acesso ao recurso."""
    # Mede o tempo de execução
    start_time = time.time()
    await proposer._simulate_resource_access()
    elapsed = time.time() - start_time
    
    # Verifica se o tempo de execução está dentro do intervalo esperado
    assert 0.2 <= elapsed <= 1.0