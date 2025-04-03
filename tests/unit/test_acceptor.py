"""
Testes unitários para o componente Acceptor do sistema Paxos.
"""
import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
import json
import time

# Importações diretas para testes unitários
# Usamos patch para substituir funções que dependem de IO ou serviços externos
with patch("acceptor.main.load_config"), \
     patch("acceptor.main.TinyDB"):
    from acceptor.acceptor.main import AcceptorState, app, generate_latest

@pytest.mark.asyncio
async def test_acceptor_state_initialization():
    """Teste de inicialização do estado do Acceptor."""
    # Arrange & Act
    state = AcceptorState(1)
    
    # Assert
    assert state.node_id == 1, "ID do nó deve ser inicializado corretamente"
    assert state.promises == {}, "Promises deve ser inicializado como um dicionário vazio"
    assert state.accepted_values == {}, "accepted_values deve ser inicializado como um dicionário vazio"

@pytest.mark.asyncio
async def test_prepare_request_first_time():
    """Teste de mensagem prepare quando é a primeira vez para a instância."""
    # Arrange
    state = AcceptorState(1)
    instance_id = 1
    proposal_number = 100
    proposer_id = 2
    
    # Act
    with patch.object(state, 'save_promise', new_callable=AsyncMock) as mock_save:
        response = await app.state.state.prepare(instance_id, proposal_number, proposer_id)
    
    # Assert
    assert response["accepted"] == True, "Primeira promessa deve ser aceita"
    assert response["highest_accepted"] is None, "Não deve haver proposta aceita anteriormente"
    assert response["accepted_value"] is None, "Não deve haver valor aceito anteriormente"
    assert instance_id in state.promises, "A instância deve ser adicionada ao dicionário de promessas"
    assert state.promises[instance_id]["highestPromised"] == proposal_number, "O número da proposta prometida deve ser atualizado"
    mock_save.assert_called_once_with(instance_id, proposal_number), "O método save_promise deve ser chamado"

@pytest.mark.asyncio
async def test_prepare_request_higher_proposal():
    """Teste de mensagem prepare com número de proposta maior que o prometido anteriormente."""
    # Arrange
    state = AcceptorState(1)
    instance_id = 1
    lower_proposal = 100
    higher_proposal = 200
    proposer_id = 2
    
    # Simular promessa anterior
    state.promises[instance_id] = {
        "highestPromised": lower_proposal,
        "lastPromiseTimestamp": time.time()
    }
    
    # Act
    with patch.object(state, 'save_promise', new_callable=AsyncMock) as mock_save:
        response = await app.state.state.prepare(instance_id, higher_proposal, proposer_id)
    
    # Assert
    assert response["accepted"] == True, "Proposta maior deve ser aceita"
    assert state.promises[instance_id]["highestPromised"] == higher_proposal, "O número da proposta prometida deve ser atualizado"
    mock_save.assert_called_once_with(instance_id, higher_proposal), "O método save_promise deve ser chamado"

@pytest.mark.asyncio
async def test_prepare_request_lower_proposal():
    """Teste de mensagem prepare com número de proposta menor que o prometido anteriormente."""
    # Arrange
    state = AcceptorState(1)
    instance_id = 1
    higher_proposal = 200
    lower_proposal = 100
    proposer_id = 2
    
    # Simular promessa anterior
    state.promises[instance_id] = {
        "highestPromised": higher_proposal,
        "lastPromiseTimestamp": time.time()
    }
    
    # Act
    response = await app.state.state.prepare(instance_id, lower_proposal, proposer_id)
    
    # Assert
    assert response["accepted"] == False, "Proposta menor deve ser rejeitada"
    assert response["highestPromised"] == higher_proposal, "A resposta deve conter o número da proposta prometida mais alto"
    assert state.promises[instance_id]["highestPromised"] == higher_proposal, "O número da proposta prometida não deve ser alterado"

@pytest.mark.asyncio
async def test_prepare_with_previous_accepted_value():
    """Teste de mensagem prepare quando já existe um valor aceito para a instância."""
    # Arrange
    state = AcceptorState(1)
    instance_id = 1
    proposal_number = 200
    previous_proposal = 100
    proposer_id = 2
    previous_value = {"operation": "TEST", "path": "/test", "content": "test content"}
    
    # Simular proposta aceita anteriormente
    state.accepted_values[instance_id] = {
        "acceptedProposalNumber": previous_proposal,
        "acceptedValue": previous_value,
        "acceptTimestamp": time.time()
    }
    
    # Act
    with patch.object(state, 'save_promise', new_callable=AsyncMock) as mock_save:
        response = await app.state.state.prepare(instance_id, proposal_number, proposer_id)
    
    # Assert
    assert response["accepted"] == True, "Proposta deve ser aceita"
    assert response["highest_accepted"] == previous_proposal, "A resposta deve conter o número da proposta aceita anteriormente"
    assert response["accepted_value"] == previous_value, "A resposta deve conter o valor aceito anteriormente"
    mock_save.assert_called_once_with(instance_id, proposal_number), "O método save_promise deve ser chamado"

@pytest.mark.asyncio
async def test_accept_request_first_time():
    """Teste de mensagem accept quando é a primeira vez para a instância."""
    # Arrange
    state = AcceptorState(1)
    instance_id = 1
    proposal_number = 100
    proposer_id = 2
    value = {"operation": "TEST", "path": "/test", "content": "test content"}
    
    # Act
    with patch.object(state, 'save_accepted', new_callable=AsyncMock) as mock_save, \
         patch("asyncio.create_task") as mock_task:
        response = await app.state.state.accept(instance_id, proposal_number, proposer_id, value)
    
    # Assert
    assert response["accepted"] == True, "Accept request deve ser aceito"
    assert instance_id in state.accepted_values, "A instância deve ser adicionada ao dicionário de valores aceitos"
    assert state.accepted_values[instance_id]["acceptedProposalNumber"] == proposal_number, "O número da proposta aceita deve ser atualizado"
    assert state.accepted_values[instance_id]["acceptedValue"] == value, "O valor aceito deve ser atualizado"
    mock_save.assert_called_once_with(instance_id, proposal_number, value), "O método save_accepted deve ser chamado"
    mock_task.assert_called_once(), "Uma tarefa para notificar learners deve ser criada"

@pytest.mark.asyncio
async def test_accept_request_with_promise():
    """Teste de mensagem accept quando já existe uma promessa para a instância."""
    # Arrange
    state = AcceptorState(1)
    instance_id = 1
    promised_proposal = 200
    lower_proposal = 100
    proposer_id = 2
    value = {"operation": "TEST", "path": "/test", "content": "test content"}
    
    # Simular promessa anterior
    state.promises[instance_id] = {
        "highestPromised": promised_proposal,
        "lastPromiseTimestamp": time.time()
    }
    
    # Act - Tentar com proposta inferior à prometida
    response_lower = await app.state.state.accept(instance_id, lower_proposal, proposer_id, value)
    
    # Assert
    assert response_lower["accepted"] == False, "Accept request com proposta inferior deve ser rejeitado"
    assert response_lower["highestPromised"] == promised_proposal, "A resposta deve conter o número da proposta prometida mais alto"
    assert instance_id not in state.accepted_values, "A instância não deve ser adicionada ao dicionário de valores aceitos"
    
    # Act - Tentar com proposta igual à prometida
    with patch.object(state, 'save_accepted', new_callable=AsyncMock) as mock_save, \
         patch("asyncio.create_task") as mock_task:
        response_equal = await app.state.state.accept(instance_id, promised_proposal, proposer_id, value)
    
    # Assert
    assert response_equal["accepted"] == True, "Accept request com proposta igual à prometida deve ser aceito"
    assert instance_id in state.accepted_values, "A instância deve ser adicionada ao dicionário de valores aceitos"
    mock_save.assert_called_once_with(instance_id, promised_proposal, value), "O método save_accepted deve ser chamado"
    mock_task.assert_called_once(), "Uma tarefa para notificar learners deve ser criada"

@pytest.mark.asyncio
async def test_notify_learners():
    """Teste da notificação dos learners sobre um valor aceito."""
    # Arrange
    state = AcceptorState(1)
    instance_id = 1
    proposal_number = 100
    value = {"operation": "TEST", "path": "/test", "content": "test content"}
    
    # Mock para aiohttp.ClientSession
    mock_session = AsyncMock()
    mock_response = AsyncMock()
    mock_response.status = 200
    
    mock_session.__aenter__.return_value.post.return_value.__aenter__.return_value = mock_response
    
    # Configure o state das notificações
    app.state.state = state
    app.state.config = {
        "networking": {
            "learners": ["learner-1:8080", "learner-2:8080"]
        }
    }
    
    # Act
    with patch("aiohttp.ClientSession", return_value=mock_session):
        await app.state.state.notify_learners(instance_id, proposal_number, value)
    
    # Assert
    assert mock_session.__aenter__.return_value.post.call_count == 2, "Deve fazer uma requisição POST para cada learner"
    # Verificar se os argumentos da primeira chamada estão corretos
    call_args = mock_session.__aenter__.return_value.post.call_args_list[0][0]
    assert "learner-1:8080/learn" in call_args[0], "A URL deve conter o endpoint /learn do learner"
    # Verificar se os argumentos da segunda chamada estão corretos
    call_args = mock_session.__aenter__.return_value.post.call_args_list[1][0]
    assert "learner-2:8080/learn" in call_args[0], "A URL deve conter o endpoint /learn do learner"

def test_app_initialization():
    """Teste da inicialização da aplicação FastAPI."""
    # Assert
    assert app.title == "Paxos Acceptor", "Título da aplicação deve ser 'Paxos Acceptor'"
    assert "/prepare" in [route.path for route in app.routes], "A aplicação deve ter uma rota /prepare"
    assert "/accept" in [route.path for route in app.routes], "A aplicação deve ter uma rota /accept"
    assert "/status" in [route.path for route in app.routes], "A aplicação deve ter uma rota /status"
    assert "/metrics" in [route.path for route in app.routes], "A aplicação deve ter uma rota /metrics"