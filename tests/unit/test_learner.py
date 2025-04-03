"""
Testes unitários para o componente Learner do sistema Paxos.
"""
import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
import json
import time
import hashlib

# Importações diretas para testes unitários
# Usamos patch para substituir funções que dependem de IO ou serviços externos
with patch("learner.main.load_config"), \
     patch("learner.main.TinyDB"):
    from learner.learner.main import LearnerState, DecisionApplier, app, generate_latest

@pytest.mark.asyncio
async def test_learner_state_initialization():
    """Teste de inicialização do estado do Learner."""
    # Arrange & Act
    state = LearnerState(1)
    
    # Assert
    assert state.node_id == 1, "ID do nó deve ser inicializado corretamente"
    assert state.decisions == {}, "Decisions deve ser inicializado como um dicionário vazio"
    assert state.acceptances == {}, "Acceptances deve ser inicializado como um dicionário vazio"
    assert state.last_applied_instance == 0, "last_applied_instance deve ser inicializado como 0"
    assert state.sync_in_progress == False, "sync_in_progress deve ser inicializado como False"

@pytest.mark.asyncio
async def test_record_acceptance_no_quorum():
    """Teste de registro de aceitação sem atingir quórum."""
    # Arrange
    state = LearnerState(1)
    instance_id = 1
    proposal_number = 100
    acceptor_id = 2
    
    # Act
    result = state.record_acceptance(instance_id, proposal_number, acceptor_id)
    
    # Assert
    assert result == False, "Não deve atingir quórum com apenas uma aceitação"
    assert instance_id in state.acceptances, "A instância deve ser adicionada ao dicionário de aceitações"
    assert proposal_number in state.acceptances[instance_id], "O número da proposta deve ser adicionado ao dicionário de aceitações"
    assert acceptor_id in state.acceptances[instance_id][proposal_number]["acceptor_ids"], "O ID do acceptor deve ser registrado"
    assert len(state.acceptances[instance_id][proposal_number]["acceptor_ids"]) == 1, "Deve haver apenas uma aceitação registrada"

@pytest.mark.asyncio
async def test_record_acceptance_with_quorum():
    """Teste de registro de aceitação atingindo quórum."""
    # Arrange
    state = LearnerState(1)
    instance_id = 1
    proposal_number = 100
    
    # Act - Registrar 3 aceitações (quórum)
    result1 = state.record_acceptance(instance_id, proposal_number, 1)
    result2 = state.record_acceptance(instance_id, proposal_number, 2)
    result3 = state.record_acceptance(instance_id, proposal_number, 3)
    
    # Assert
    assert result1 == False, "Primeira aceitação não deve atingir quórum"
    assert result2 == False, "Segunda aceitação não deve atingir quórum"
    assert result3 == True, "Terceira aceitação deve atingir quórum (3 de 5 acceptors)"
    assert len(state.acceptances[instance_id][proposal_number]["acceptor_ids"]) == 3, "Deve haver 3 aceitações registradas"

@pytest.mark.asyncio
async def test_is_decided():
    """Teste da verificação se uma instância já foi decidida."""
    # Arrange
    state = LearnerState(1)
    instance_id = 1
    
    # Act & Assert - Antes de decidir
    assert state.is_decided(instance_id) == False, "Instância não deve estar decidida inicialmente"
    
    # Registrar uma decisão
    state.decisions[instance_id] = {
        "value": {"operation": "TEST"},
        "proposal_number": 100,
        "decided_at": time.time()
    }
    
    # Act & Assert - Depois de decidir
    assert state.is_decided(instance_id) == True, "Instância deve estar decidida após registro"

@pytest.mark.asyncio
async def test_get_responsible_learner():
    """Teste da determinação do learner responsável por notificar o cliente."""
    # Arrange
    state = LearnerState(1)
    client_id1 = "client-123"
    client_id2 = "client-456"
    
    # Garantir comportamento determinístico usando hash fixo
    # Simulando a implementação da função para tornar previsível nos testes
    with patch.object(hashlib, 'md5') as mock_md5:
        # Configurar para que client-123 vá para learner-1
        mock_hash1 = MagicMock()
        mock_hash1.hexdigest.return_value = "0000000000000000000000000000000"  # hex que mod 2 = 0
        # Configurar para que client-456 vá para learner-2
        mock_hash2 = MagicMock()
        mock_hash2.hexdigest.return_value = "0000000000000000000000000000001"  # hex que mod 2 = 1
        
        mock_md5.side_effect = [mock_hash1, mock_hash2]
        
        # Act
        responsible1 = state.get_responsible_learner(client_id1)
        responsible2 = state.get_responsible_learner(client_id2)
    
    # Assert
    assert responsible1 == 1, f"client-123 deve ser atribuído ao learner-1, foi {responsible1}"
    assert responsible2 == 2, f"client-456 deve ser atribuído ao learner-2, foi {responsible2}"

@pytest.mark.asyncio
async def test_should_notify_client():
    """Teste da verificação se este learner deve notificar o cliente."""
    # Arrange
    state = LearnerState(1)  # Este é o learner-1
    client_id = "client-123"
    
    # Act & Assert - Com determinação que learner-1 é responsável
    with patch.object(state, 'get_responsible_learner', return_value=1):
        assert state.should_notify_client(client_id) == True, "Deve retornar True quando este learner é responsável"
    
    # Act & Assert - Com determinação que learner-2 é responsável
    with patch.object(state, 'get_responsible_learner', return_value=2):
        assert state.should_notify_client(client_id) == False, "Deve retornar False quando outro learner é responsável"
    
    # Act & Assert - Com client_id None
    assert state.should_notify_client(None) == False, "Deve retornar False quando client_id é None"

@pytest.mark.asyncio
async def test_cleanup_old_acceptances():
    """Teste da limpeza de registros de aceitação antigos."""
    # Arrange
    state = LearnerState(1)
    
    # Configurar aceitações para instâncias já decididas e não decididas
    state.acceptances = {
        1: {100: {"acceptor_ids": {1, 2, 3}, "first_seen_at": time.time()}},  # Será decidida
        2: {200: {"acceptor_ids": {1, 2}, "first_seen_at": time.time()}},     # Não será decidida
    }
    
    # Configurar decisões
    state.decisions = {
        1: {"value": {"operation": "TEST"}, "proposal_number": 100, "decided_at": time.time()},
        # Instância 2 não está decidida
    }
    
    # Act
    await state.cleanup_old_acceptances()
    
    # Assert
    assert 1 not in state.acceptances, "Aceitações para instância decidida devem ser removidas"
    assert 2 in state.acceptances, "Aceitações para instância não decidida devem ser mantidas"

@pytest.mark.asyncio
async def test_learn_request_new_decision(mock_learner_state):
    """Teste de recebimento de requisição de aprendizado para nova decisão."""
    # Arrange
    state = mock_learner_state
    
    # Mock para o endpoint learn
    with patch.object(state, 'record_acceptance', return_value=True) as mock_record, \
         patch.object(state, 'is_decided', return_value=False) as mock_is_decided, \
         patch("learner.main.decision_applier.process_decision") as mock_process:
        # Configuração do app
        app.state.state = state
        app.state.manager = MagicMock()
        app.state.manager.broadcast = AsyncMock()
        
        # Mock request data
        request_data = {
            "instance_id": 1,
            "proposal_number": 100,
            "acceptor_id": 2,
            "value": {"operation": "TEST", "path": "/test", "content": "test content"}
        }
        
        # Act
        response = await app.state.state.learn(request_data)
    
    # Assert
    assert response["success"] == True, "A resposta deve indicar sucesso"
    assert response["decision_reached"] == True, "A resposta deve indicar que uma decisão foi alcançada"
    mock_record.assert_called_once_with(1, 100, 2), "O método record_acceptance deve ser chamado com os parâmetros corretos"
    mock_is_decided.assert_called_once_with(1), "O método is_decided deve ser chamado"
    assert 1 in state.decisions, "A instância deve ser adicionada ao dicionário de decisões"
    assert state.decisions[1]["value"] == request_data["value"], "O valor da decisão deve ser armazenado"
    state.save_decision.assert_called_once(), "O método save_decision deve ser chamado"
    app.state.manager.broadcast.assert_called_once(), "O método broadcast deve ser chamado para notificar clientes"
    mock_process.assert_called_once(), "O método process_decision deve ser chamado para aplicar a decisão"

@pytest.mark.asyncio
async def test_learn_request_already_decided(mock_learner_state):
    """Teste de recebimento de requisição de aprendizado para decisão já conhecida."""
    # Arrange
    state = mock_learner_state
    
    # Mock para o endpoint learn
    with patch.object(state, 'is_decided', return_value=True) as mock_is_decided:
        # Configuração do app
        app.state.state = state
        
        # Mock request data
        request_data = {
            "instance_id": 1,
            "proposal_number": 100,
            "acceptor_id": 2,
            "value": {"operation": "TEST", "path": "/test", "content": "test content"}
        }
        
        # Act
        response = await app.state.state.learn(request_data)
    
    # Assert
    assert response["success"] == True, "A resposta deve indicar sucesso"
    assert response["already_decided"] == True, "A resposta deve indicar que a decisão já era conhecida"
    mock_is_decided.assert_called_once_with(1), "O método is_decided deve ser chamado"

@pytest.mark.asyncio
async def test_decision_applier_process_decision_next_instance(mock_learner_state):
    """Teste do processamento de decisão quando é a próxima instância a ser aplicada."""
    # Arrange
    state = mock_learner_state
    state.last_applied_instance = 0
    
    applier = DecisionApplier(state, MagicMock())
    instance_id = 1
    value = {"operation": "TEST", "path": "/test", "content": "test content"}
    
    # Mock para o método apply_decision
    applier.apply_decision = AsyncMock()
    applier.apply_pending_decisions = AsyncMock()
    
    # Act
    result = await applier.process_decision(instance_id, value)
    
    # Assert
    assert result == True, "O resultado deve ser True quando a instância é aplicada diretamente"
    applier.apply_decision.assert_called_once_with(instance_id, value), "O método apply_decision deve ser chamado"
    assert state.last_applied_instance == 1, "last_applied_instance deve ser atualizado"
    applier.apply_pending_decisions.assert_called_once(), "O método apply_pending_decisions deve ser chamado"

@pytest.mark.asyncio
async def test_decision_applier_process_decision_future_instance(mock_learner_state):
    """Teste do processamento de decisão quando é uma instância futura."""
    # Arrange
    state = mock_learner_state
    state.last_applied_instance = 0
    
    applier = DecisionApplier(state, MagicMock())
    instance_id = 2  # Instância 2 (futuro)
    value = {"operation": "TEST", "path": "/test", "content": "test content"}
    
    # Mock para o método add_pending_decision
    applier.add_pending_decision = AsyncMock()
    applier.apply_decision = AsyncMock()
    
    # Act
    result = await applier.process_decision(instance_id, value)
    
    # Assert
    assert result == False, "O resultado deve ser False quando a instância é adicionada como pendente"
    applier.add_pending_decision.assert_called_once_with(instance_id, value), "O método add_pending_decision deve ser chamado"
    applier.apply_decision.assert_not_called(), "O método apply_decision não deve ser chamado"
    assert state.last_applied_instance == 0, "last_applied_instance não deve ser alterado"

@pytest.mark.asyncio
async def test_decision_applier_apply_pending_decisions(mock_learner_state):
    """Teste da aplicação de decisões pendentes em ordem."""
    # Arrange
    state = mock_learner_state
    state.last_applied_instance = 1
    
    applier = DecisionApplier(state, MagicMock())
    applier.pending_decisions = {
        2: {"operation": "TEST", "path": "/test2"},
        3: {"operation": "TEST", "path": "/test3"},
        5: {"operation": "TEST", "path": "/test5"}  # Lacuna: instância 4 está faltando
    }
    
    # Mock para o método apply_decision
    applier.apply_decision = AsyncMock()
    
    # Act
    applied_count = await applier.apply_pending_decisions()
    
    # Assert
    assert applied_count == 2, "Deve aplicar 2 decisões (instâncias 2 e 3)"
    assert applier.apply_decision.call_count == 2, "O método apply_decision deve ser chamado 2 vezes"
    assert state.last_applied_instance == 3, "last_applied_instance deve ser atualizado para 3"
    assert 2 not in applier.pending_decisions, "Instância 2 deve ser removida das pendências"
    assert 3 not in applier.pending_decisions, "Instância 3 deve ser removida das pendências"
    assert 5 in applier.pending_decisions, "Instância 5 deve permanecer pendente (lacuna)"

def test_app_initialization():
    """Teste da inicialização da aplicação FastAPI."""
    # Assert
    assert app.title == "Paxos Learner", "Título da aplicação deve ser 'Paxos Learner'"
    assert "/learn" in [route.path for route in app.routes], "A aplicação deve ter uma rota /learn"
    assert "/files" in [route.path for route in app.routes], "A aplicação deve ter uma rota /files"
    assert "/status" in [route.path for route in app.routes], "A aplicação deve ter uma rota /status"
    assert "/sync" in [route.path for route in app.routes], "A aplicação deve ter uma rota /sync"
    assert "/ws" in [route.path for route in app.routes], "A aplicação deve ter uma rota WebSocket /ws"