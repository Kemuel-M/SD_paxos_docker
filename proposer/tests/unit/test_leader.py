"""
Testes unitários para o módulo de eleição de líder.
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

from leader import LeaderElection

@pytest.fixture
def mock_proposer():
    """Fixture que cria um mock do proposer."""
    proposer = AsyncMock()
    proposer.node_id = 1
    proposer.acceptors = ["acceptor-1:8080", "acceptor-2:8080", "acceptor-3:8080", "acceptor-4:8080", "acceptor-5:8080"]
    proposer.last_instance_id = 0
    proposer.proposal_counter = 0
    proposer.set_leader = AsyncMock()
    return proposer

@pytest.fixture
def leader_election(mock_proposer):
    """Fixture que cria um gerenciador de eleição para testes."""
    proposers = ["proposer-1:8080", "proposer-2:8080", "proposer-3:8080", "proposer-4:8080", "proposer-5:8080"]
    
    election = LeaderElection(
        node_id=1,
        proposers=proposers,
        proposer=mock_proposer,
        current_leader=None,
        current_term=0
    )
    
    # Mock do envio de heartbeats
    election._send_heartbeat = AsyncMock()
    
    # Mock do envio para acceptors
    election._send_to_acceptors = AsyncMock()
    
    return election

@pytest.mark.asyncio
async def test_leader_election_initialization(leader_election, mock_proposer):
    """Testa se o gerenciador de eleição é inicializado corretamente."""
    assert leader_election.node_id == 1
    assert len(leader_election.proposers) == 5
    assert leader_election.proposer == mock_proposer
    assert leader_election.current_leader is None
    assert leader_election.current_term == 0
    assert leader_election.running == False

@pytest.mark.asyncio
async def test_start_stop(leader_election):
    """Testa inicialização e parada do gerenciador de eleição."""
    # Start
    await leader_election.start()
    assert leader_election.running == True
    
    # Stop
    await leader_election.stop()
    assert leader_election.running == False

@pytest.mark.asyncio
async def test_is_leader(leader_election):
    """Testa verificação de liderança."""
    # Inicialmente não é líder
    assert leader_election.is_leader() == False
    
    # Define como líder
    leader_election.current_leader = leader_election.node_id
    assert leader_election.is_leader() == True
    
    # Define outro nó como líder
    leader_election.current_leader = 2
    assert leader_election.is_leader() == False

@pytest.mark.asyncio
async def test_receive_heartbeat(leader_election, mock_proposer):
    """Testa recebimento de heartbeat do líder."""
    # Simula heartbeat de outro líder
    data = {
        "leaderId": 2,
        "term": 5,
        "lastInstanceId": 100
    }
    
    # Mock do monitor de heartbeat
    leader_election.leader_monitor.record_heartbeat = MagicMock()
    leader_election.leader_monitor.set_target = MagicMock()
    leader_election.leader_monitor.start = MagicMock()
    
    # Recebe heartbeat
    await leader_election.receive_heartbeat(data)
    
    # Verifica se alterou o líder e termo
    assert leader_election.current_leader == 2
    assert leader_election.current_term == 5
    
    # Verifica se atualizou o lastInstanceId no proposer
    assert mock_proposer.last_instance_id == 100
    
    # Verifica se o heartbeat foi registrado
    leader_election.leader_monitor.record_heartbeat.assert_called_once()

@pytest.mark.asyncio
async def test_start_election_success(leader_election, mock_proposer):
    """Testa processo de eleição com sucesso."""
    # Mock das respostas da fase Prepare
    prepare_responses = [
        {"accepted": True},  # Acceptor 1
        {"accepted": True},  # Acceptor 2
        {"accepted": True},  # Acceptor 3
        {"accepted": False}, # Acceptor 4
        None                # Acceptor 5 (falhou)
    ]
    
    # Mock das respostas da fase Accept
    accept_responses = [
        {"accepted": True},  # Acceptor 1
        {"accepted": True},  # Acceptor 2
        {"accepted": True},  # Acceptor 3
        {"accepted": False}, # Acceptor 4
        None                # Acceptor 5 (falhou)
    ]
    
    # Configura o mock para retornar as respostas corretas
    leader_election._send_to_acceptors.side_effect = [
        prepare_responses,  # Para a fase Prepare
        accept_responses    # Para a fase Accept
    ]
    
    # Executa a eleição
    await leader_election._start_election()
    
    # Verifica se o termo foi incrementado
    assert leader_election.current_term == 1
    
    # Verifica se este nó foi eleito líder
    assert leader_election.current_leader == 1
    
    # Verifica se o proposer foi notificado
    mock_proposer.set_leader.assert_called_with(True)

@pytest.mark.asyncio
async def test_start_election_failure(leader_election, mock_proposer):
    """Testa processo de eleição com falha na fase Prepare."""
    # Mock das respostas da fase Prepare (menos que maioria)
    prepare_responses = [
        {"accepted": True},  # Acceptor 1
        {"accepted": False}, # Acceptor 2
        {"accepted": False}, # Acceptor 3
        {"accepted": False}, # Acceptor 4
        None                # Acceptor 5 (falhou)
    ]
    
    # Configura o mock para retornar as respostas
    leader_election._send_to_acceptors.side_effect = [prepare_responses]
    
    # Executa a eleição
    await leader_election._start_election()
    
    # Verifica se o termo foi incrementado
    assert leader_election.current_term == 1
    
    # Verifica se o líder não mudou
    assert leader_election.current_leader is None
    
    # Verifica se o proposer não foi notificado
    mock_proposer.set_leader.assert_not_called()

@pytest.mark.asyncio
async def test_become_leader(leader_election, mock_proposer):
    """Testa processo de se tornar líder."""
    # Mock do monitor de heartbeat
    leader_election.leader_monitor.stop = MagicMock()
    
    # Executa
    await leader_election._become_leader()
    
    # Verifica se o proposer foi notificado
    mock_proposer.set_leader.assert_called_with(True)
    
    # Verifica se o monitor de heartbeat foi parado
    leader_election.leader_monitor.stop.assert_called_once()
    
    # Verifica se a task de heartbeat foi iniciada
    assert leader_election.heartbeat_task is not None

@pytest.mark.asyncio
async def test_stop_being_leader(leader_election, mock_proposer):
    """Testa processo de deixar de ser líder."""
    # Cria uma task simulada
    leader_election.heartbeat_task = asyncio.create_task(asyncio.sleep(0.1))
    
    # Executa
    await leader_election._stop_being_leader()
    
    # Verifica se o proposer foi notificado
    mock_proposer.set_leader.assert_called_with(False)
    
    # Verifica se a task de heartbeat foi cancelada
    assert leader_election.heartbeat_task is None

@pytest.mark.asyncio
async def test_handle_leader_failure(leader_election):
    """Testa o manipulador de falha do líder."""
    # Configura o estado inicial
    leader_election.current_leader = 2
    leader_election.running = True
    leader_election.last_election_time = 0
    
    # Mock do método de eleição
    leader_election._start_election = AsyncMock()
    
    # Executa o manipulador
    await leader_election._handle_leader_failure()
    
    # Verifica se o líder foi limpo
    assert leader_election.current_leader is None
    
    # Verifica se a eleição foi iniciada
    leader_election._start_election.assert_called_once()