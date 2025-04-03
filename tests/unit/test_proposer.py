"""
Testes unitários para o componente Proposer (Cluster Sync)
"""
import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
import json
import time

# Importações diretas para testes unitários
# Usamos patch para substituir funções que dependem de IO ou serviços externos
with patch("proposer.main.load_config"), \
     patch("proposer.main.TinyDB"):
    from proposer.proposer.main import ProposerState, generate_latest, app

@pytest.mark.asyncio
async def test_generate_proposal_number():
    """Teste da geração de números de proposta únicos."""
    # Arrange
    state = ProposerState(1)
    state.proposal_counter = 0
    
    # Act
    proposal1 = state.generate_proposal_number()
    proposal2 = state.generate_proposal_number()
    
    # Assert
    assert proposal1 < proposal2, "Números de proposta devem ser crescentes"
    assert proposal1 == (1 << 8) | 1, "Formato do número de proposta deve ser (contador << 8) | node_id"
    assert state.proposal_counter == 2, "Contador de propostas deve ser incrementado"

@pytest.mark.asyncio
async def test_get_next_instance_id():
    """Teste da geração de IDs de instância sequenciais."""
    # Arrange
    state = ProposerState(1)
    state.instance_counter = 0
    
    # Act
    instance1 = state.get_next_instance_id()
    instance2 = state.get_next_instance_id()
    
    # Assert
    assert instance1 == 1, "Primeira instância deve ser 1"
    assert instance2 == 2, "Segunda instância deve ser 2"
    assert instance2 > instance1, "IDs de instância devem ser sequenciais"

@pytest.mark.asyncio
async def test_leader_election(mock_proposer_state):
    """Teste do processo de eleição de líder."""
    # Arrange
    state = mock_proposer_state
    state.node_id = 1
    state.leader_id = None
    state.is_leader = False
    
    # Mock para run_paxos para simular eleição bem-sucedida
    state.run_paxos = AsyncMock(return_value=True)
    state.prepare_instances_for_multi_paxos = AsyncMock(return_value=True)
    
    # Act
    await state.initiate_leader_election()
    
    # Assert
    assert state.is_leader == True, "Proposer deve se tornar líder após eleição bem-sucedida"
    assert state.leader_id == 1, "ID do líder deve ser atualizado para o ID do proposer"
    state.run_paxos.assert_called_once(), "Método run_paxos deve ser chamado uma vez"
    state.prepare_instances_for_multi_paxos.assert_called_once(), "Método prepare_instances_for_multi_paxos deve ser chamado após eleição"

@pytest.mark.asyncio
async def test_leader_election_failure(mock_proposer_state):
    """Teste de falha na eleição de líder."""
    # Arrange
    state = mock_proposer_state
    state.node_id = 1
    state.leader_id = None
    state.is_leader = False
    
    # Mock para run_paxos para simular eleição mal-sucedida
    state.run_paxos = AsyncMock(return_value=False)
    
    # Act
    await state.initiate_leader_election()
    
    # Assert
    assert state.is_leader == False, "Proposer não deve se tornar líder após eleição mal-sucedida"
    state.run_paxos.assert_called_once(), "Método run_paxos deve ser chamado uma vez"
    state.prepare_instances_for_multi_paxos.assert_not_called(), "Método prepare_instances_for_multi_paxos não deve ser chamado após falha na eleição"

@pytest.mark.asyncio
async def test_prepare_phase(mock_proposer_state):
    """Teste da fase de preparação do Paxos."""
    # Arrange
    state = mock_proposer_state
    instance_id = 1
    proposal_number = (1 << 8) | 1
    
    # Mock para send_prepare_to_acceptors
    mock_responses = [
        {"accepted": True, "highest_accepted": None, "accepted_value": None},
        {"accepted": True, "highest_accepted": None, "accepted_value": None},
        {"accepted": True, "highest_accepted": None, "accepted_value": None},
        {"accepted": False, "highestPromised": (2 << 8) | 2}
    ]
    state.send_prepare_to_acceptors = AsyncMock(return_value=mock_responses)
    
    # Act
    result, highest_accepted, accepted_value = await state.prepare_phase(instance_id, proposal_number)
    
    # Assert
    assert result == True, "Prepare phase deve ter sucesso com quórum de aceitações"
    assert highest_accepted is None, "Não deve haver valor aceito anteriormente"
    assert accepted_value is None, "Não deve haver valor aceito anteriormente"
    state.send_prepare_to_acceptors.assert_called_once(), "Método send_prepare_to_acceptors deve ser chamado uma vez"

@pytest.mark.asyncio
async def test_accept_phase(mock_proposer_state):
    """Teste da fase de aceitação do Paxos."""
    # Arrange
    state = mock_proposer_state
    instance_id = 1
    proposal_number = (1 << 8) | 1
    value = {"operation": "TEST", "path": "/test", "content": "test content"}
    
    # Mock para send_accept_to_acceptors
    mock_responses = [
        {"accepted": True},
        {"accepted": True},
        {"accepted": True},
        {"accepted": False, "highestPromised": (2 << 8) | 2}
    ]
    state.send_accept_to_acceptors = AsyncMock(return_value=mock_responses)
    
    # Act
    result = await state.accept_phase(instance_id, proposal_number, value)
    
    # Assert
    assert result == True, "Accept phase deve ter sucesso com quórum de aceitações"
    state.send_accept_to_acceptors.assert_called_once(), "Método send_accept_to_acceptors deve ser chamado uma vez"

@pytest.mark.asyncio
async def test_process_batch(mock_proposer_state):
    """Teste do processamento de batch de propostas."""
    # Arrange
    state = mock_proposer_state
    state.is_leader = True
    state.leader_id = state.node_id
    state.batch_queue = [
        {"operation": "CREATE", "path": "/test1", "content": "content1"},
        {"operation": "CREATE", "path": "/test2", "content": "content2"}
    ]
    
    # Mocks para os métodos chamados em process_batch
    state.get_next_instance_id = MagicMock(side_effect=[1, 2])
    state.generate_proposal_number = MagicMock(side_effect=[(1 << 8) | 1, (2 << 8) | 1])
    state.run_paxos = AsyncMock(side_effect=[True, False])  # Primeira proposta sucesso, segunda falha
    
    # Act
    await state.process_batch()
    
    # Assert
    assert len(state.active_proposals) == 0, "Propostas ativas devem ser removidas após processamento"
    assert state.run_paxos.call_count == 2, "run_paxos deve ser chamado para cada proposta no batch"
    state.save_state.assert_called(), "Estado deve ser salvo após processamento do batch"

@pytest.mark.asyncio
async def test_forward_batch_to_leader(mock_proposer_state):
    """Teste de encaminhamento de batch para o líder."""
    # Arrange
    state = mock_proposer_state
    state.is_leader = False
    state.leader_id = 2  # outro proposer é o líder
    state.node_id = 1
    state.batch_queue = [
        {"operation": "CREATE", "path": "/test1", "content": "content1"},
        {"operation": "CREATE", "path": "/test2", "content": "content2"}
    ]
    
    # Mock para aiohttp.ClientSession
    mock_session = AsyncMock()
    mock_response = AsyncMock()
    mock_response.status = 200
    
    mock_session.__aenter__.return_value.post.return_value.__aenter__.return_value = mock_response
    
    # Act
    with patch("aiohttp.ClientSession", return_value=mock_session):
        await state.forward_batch_to_leader()
    
    # Assert
    assert mock_session.__aenter__.return_value.post.call_count == 2, "Deve fazer uma requisição POST para cada proposta no batch"
    assert len(state.batch_queue) == 0, "O batch_queue deve ser esvaziado após encaminhamento bem-sucedido"

def test_app_initialization():
    """Teste da inicialização da aplicação FastAPI."""
    # Assert
    assert app.title == "Paxos Proposer", "Título da aplicação deve ser 'Paxos Proposer'"
    assert "/propose" in [route.path for route in app.routes], "A aplicação deve ter uma rota /propose"
    assert "/status" in [route.path for route in app.routes], "A aplicação deve ter uma rota /status"
    assert "/heartbeat" in [route.path for route in app.routes], "A aplicação deve ter uma rota /heartbeat"
    assert "/metrics" in [route.path for route in app.routes], "A aplicação deve ter uma rota /metrics"
    assert "/ws" in [route.path for route in app.routes], "A aplicação deve ter uma rota WebSocket /ws"