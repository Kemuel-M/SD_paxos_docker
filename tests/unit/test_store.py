"""
Testes unitários para o componente Store (Cluster Store) do sistema Paxos.
"""
import pytest
import asyncio
import os
from unittest.mock import AsyncMock, MagicMock, patch
import json
import time

# Importações diretas para testes unitários
# Usamos patch para substituir funções que dependem de IO ou serviços externos
with patch("store.store.main.load_config"), \
     patch("store.store.main.TinyDB"):
    from store.store.main import StoreState, ResourceManager, TransactionManager, app, generate_latest

@pytest.mark.asyncio
async def test_store_state_initialization(mock_config):
    """Teste de inicialização do estado do Store."""
    # Arrange
    node_id = 1
    resource_path = "/tmp/test-resources"
    
    # Garantir que o diretório existe
    os.makedirs(resource_path, exist_ok=True)
    
    # Act
    state = StoreState(node_id, resource_path)
    
    # Assert
    assert state.node_id == 1, "ID do nó deve ser inicializado corretamente"
    assert state.resource_path == resource_path, "Caminho de recursos deve ser inicializado corretamente"
    assert state.resources == {}, "Resources deve ser inicializado como um dicionário vazio"
    assert state.transactions == {}, "Transactions deve ser inicializado como um dicionário vazio"
    assert state.is_recovering == False, "is_recovering deve ser inicializado como False"
    
    # Limpar
    import shutil
    shutil.rmtree(resource_path, ignore_errors=True)

@pytest.mark.asyncio
async def test_calculate_resources_checksum(mock_store_state):
    """Teste do cálculo de checksum para verificação de consistência."""
    # Arrange
    state = mock_store_state
    # Adicionar alguns recursos para testar
    state.resources = {
        "/test1": {"content": "content1", "version": 1, "timestamp": 100, "node_id": 1},
        "/test2": {"content": "content2", "version": 2, "timestamp": 200, "node_id": 1}
    }
    
    # Act
    checksum = state.calculate_resources_checksum()
    
    # Assert
    assert isinstance(checksum, str), "Checksum deve ser uma string"
    assert len(checksum) > 0, "Checksum não deve ser vazia"
    
    # Verificar se o checksum muda quando os recursos mudam
    state.resources["/test1"]["version"] = 3
    new_checksum = state.calculate_resources_checksum()
    assert checksum != new_checksum, "Checksum deve mudar quando os recursos mudam"

@pytest.mark.asyncio
async def test_resource_manager_read_resource(mock_store_state):
    """Teste da leitura de um recurso do sistema."""
    # Arrange
    state = mock_store_state
    manager = ResourceManager(state)
    
    # Caso 1: Recurso existe
    state.resources = {
        "/test": {"content": "test content", "version": 1, "timestamp": 100, "node_id": 1}
    }
    
    # Act
    result = await manager.read_resource("/test")
    
    # Assert
    assert result["success"] == True, "Leitura de recurso existente deve ter sucesso"
    assert result["path"] == "/test", "O campo path deve corresponder ao solicitado"
    assert result["content"] == "test content", "O conteúdo deve corresponder ao armazenado"
    assert result["version"] == 1, "A versão deve corresponder à armazenada"
    
    # Caso 2: Recurso não existe
    result_not_found = await manager.read_resource("/nonexistent")
    
    # Assert
    assert result_not_found["success"] == False, "Leitura de recurso inexistente deve falhar"
    assert result_not_found["error"] == "Resource not found", "A mensagem de erro deve ser adequada"

@pytest.mark.asyncio
async def test_resource_manager_write_resource(mock_store_state):
    """Teste da escrita de um recurso no sistema."""
    # Arrange
    state = mock_store_state
    manager = ResourceManager(state)
    
    # Act - Criar novo recurso
    result = await manager.write_resource("/newtest", "new content", "tx-123")
    
    # Assert
    assert result["success"] == True, "Escrita de novo recurso deve ter sucesso"
    assert result["path"] == "/newtest", "O campo path deve corresponder ao solicitado"
    assert result["version"] == 1, "Versão inicial deve ser 1"
    assert "/newtest" in state.resources, "O recurso deve ser adicionado ao dicionário de recursos"
    assert state.resources["/newtest"]["content"] == "new content", "O conteúdo deve ser armazenado corretamente"
    state.save_resource.assert_called_once(), "O método save_resource deve ser chamado"
    
    # Act - Atualizar recurso existente
    result_update = await manager.write_resource("/newtest", "updated content", "tx-456")
    
    # Assert
    assert result_update["success"] == True, "Atualização de recurso deve ter sucesso"
    assert result_update["version"] == 2, "Versão deve ser incrementada"
    assert state.resources["/newtest"]["content"] == "updated content", "O conteúdo deve ser atualizado"
    assert state.save_resource.call_count == 2, "O método save_resource deve ser chamado novamente"

@pytest.mark.asyncio
async def test_resource_manager_delete_resource(mock_store_state):
    """Teste da remoção de um recurso do sistema."""
    # Arrange
    state = mock_store_state
    manager = ResourceManager(state)
    
    # Configurar um recurso para teste
    state.resources = {
        "/test": {"content": "test content", "version": 1, "timestamp": 100, "node_id": 1}
    }
    
    # Act
    result = await manager.delete_resource("/test", "tx-123")
    
    # Assert
    assert result["success"] == True, "Remoção de recurso deve ter sucesso"
    assert result["path"] == "/test", "O campo path deve corresponder ao solicitado"
    assert "/test" not in state.resources, "O recurso deve ser removido do dicionário"
    state.delete_resource.assert_called_once_with("/test"), "O método delete_resource deve ser chamado"
    
    # Caso 2: Recurso não existe
    result_not_found = await manager.delete_resource("/nonexistent", "tx-456")
    
    # Assert
    assert result_not_found["success"] == False, "Remoção de recurso inexistente deve falhar"
    assert result_not_found["error"] == "Resource not found", "A mensagem de erro deve ser adequada"

@pytest.mark.asyncio
async def test_transaction_manager_prepare_success(mock_store_state):
    """Teste da fase de preparação (2PC) com sucesso."""
    # Arrange
    state = mock_store_state
    resource_manager = ResourceManager(state)
    manager = TransactionManager(state, resource_manager)
    
    # Mock para a request
    request = MagicMock()
    request.transaction_id = "tx-123"
    request.operations = [
        {"path": "/test", "operation": "WRITE", "content": "test content"}
    ]
    request.learner_id = 1
    
    # Act
    response = await manager.prepare(request)
    
    # Assert
    assert response.ready == True, "O resultado deve indicar que está pronto para commit"
    assert response.transaction_id == "tx-123", "O transaction_id deve ser o mesmo da request"
    assert response.store_id == state.node_id, "O store_id deve ser o ID do nó"
    assert "tx-123" in state.transactions, "A transação deve ser registrada"
    assert state.transactions["tx-123"]["status"] == "PREPARED", "O status da transação deve ser PREPARED"
    state.save_transaction.assert_called_once(), "O método save_transaction deve ser chamado"

@pytest.mark.asyncio
async def test_transaction_manager_prepare_resource_locked(mock_store_state):
    """Teste da fase de preparação (2PC) com recurso bloqueado."""
    # Arrange
    state = mock_store_state
    resource_manager = ResourceManager(state)
    manager = TransactionManager(state, resource_manager)
    
    # Simular recurso bloqueado
    manager.locks.add("/test")
    
    # Mock para a request
    request = MagicMock()
    request.transaction_id = "tx-123"
    request.operations = [
        {"path": "/test", "operation": "WRITE", "content": "test content"}
    ]
    request.learner_id = 1
    
    # Act
    response = await manager.prepare(request)
    
    # Assert
    assert response.ready == False, "O resultado deve indicar que não está pronto (recurso bloqueado)"
    assert "Resource /test is locked" in response.reason, "A razão deve mencionar o recurso bloqueado"

@pytest.mark.asyncio
async def test_transaction_manager_commit_success(mock_store_state):
    """Teste da fase de commit (2PC) com sucesso."""
    # Arrange
    state = mock_store_state
    resource_manager = ResourceManager(state)
    manager = TransactionManager(state, resource_manager)
    
    # Simular transação preparada
    state.transactions["tx-123"] = {
        "status": "PREPARED",
        "operations": [
            {"operation": "WRITE", "path": "/test", "content": "test content"}
        ],
        "timestamp": time.time(),
        "learner_id": 1
    }
    
    # Adicionar lock para o recurso
    manager.locks.add("/test")
    
    # Mock para resource_manager.write_resource
    resource_manager.write_resource = AsyncMock(return_value={"success": True})
    
    # Mock para a request
    request = MagicMock()
    request.transaction_id = "tx-123"
    request.commit = True
    request.learner_id = 1
    
    # Act
    response = await manager.commit(request)
    
    # Assert
    assert response.success == True, "O resultado deve indicar sucesso"
    assert response.transaction_id == "tx-123", "O transaction_id deve ser o mesmo da request"
    assert state.transactions["tx-123"]["status"] == "COMMITTED", "O status da transação deve ser COMMITTED"
    assert "/test" not in manager.locks, "O lock do recurso deve ser liberado"
    resource_manager.write_resource.assert_called_once(), "O método write_resource deve ser chamado"
    assert state.save_transaction.call_count == 2, "O método save_transaction deve ser chamado 2 vezes"

@pytest.mark.asyncio
async def test_transaction_manager_commit_abort(mock_store_state):
    """Teste da fase de commit (2PC) com instrução para abortar."""
    # Arrange
    state = mock_store_state
    resource_manager = ResourceManager(state)
    manager = TransactionManager(state, resource_manager)
    
    # Simular transação preparada
    state.transactions["tx-123"] = {
        "status": "PREPARED",
        "operations": [
            {"operation": "WRITE", "path": "/test", "content": "test content"}
        ],
        "timestamp": time.time(),
        "learner_id": 1
    }
    
    # Adicionar lock para o recurso
    manager.locks.add("/test")
    
    # Mock para a request
    request = MagicMock()
    request.transaction_id = "tx-123"
    request.commit = False  # Abort
    request.learner_id = 1
    
    # Act
    response = await manager.commit(request)
    
    # Assert
    assert response.success == True, "O resultado deve indicar sucesso (abort bem-sucedido)"
    assert response.transaction_id == "tx-123", "O transaction_id deve ser o mesmo da request"
    assert state.transactions["tx-123"]["status"] == "ABORTED", "O status da transação deve ser ABORTED"
    assert "/test" not in manager.locks, "O lock do recurso deve ser liberado"
    assert state.save_transaction.call_count == 1, "O método save_transaction deve ser chamado 1 vez"

@pytest.mark.asyncio
async def test_transaction_manager_commit_transaction_not_found(mock_store_state):
    """Teste da fase de commit (2PC) com transação não encontrada."""
    # Arrange
    state = mock_store_state
    resource_manager = ResourceManager(state)
    manager = TransactionManager(state, resource_manager)
    
    # Não registrar nenhuma transação
    
    # Mock para a request
    request = MagicMock()
    request.transaction_id = "tx-nonexistent"
    request.commit = True
    request.learner_id = 1
    
    # Act
    response = await manager.commit(request)
    
    # Assert
    assert response.success == False, "O resultado deve indicar falha"
    assert response.transaction_id == "tx-nonexistent", "O transaction_id deve ser o mesmo da request"
    assert "Transaction not found" in response.reason, "A razão deve mencionar que a transação não foi encontrada"

@pytest.mark.asyncio
async def test_transaction_manager_commit_invalid_state(mock_store_state):
    """Teste da fase de commit (2PC) com transação em estado inválido."""
    # Arrange
    state = mock_store_state
    resource_manager = ResourceManager(state)
    manager = TransactionManager(state, resource_manager)
    
    # Simular transação em estado inválido (já commitada)
    state.transactions["tx-123"] = {
        "status": "COMMITTED",  # Já está commitada
        "operations": [
            {"operation": "WRITE", "path": "/test", "content": "test content"}
        ],
        "timestamp": time.time(),
        "learner_id": 1
    }
    
    # Mock para a request
    request = MagicMock()
    request.transaction_id = "tx-123"
    request.commit = True
    request.learner_id = 1
    
    # Act
    response = await manager.commit(request)
    
    # Assert
    assert response.success == False, "O resultado deve indicar falha"
    assert "Transaction not in PREPARED state" in response.reason, "A razão deve mencionar o estado inválido"

@pytest.mark.asyncio
async def test_sync_handle_sync_request(mock_store_state):
    """Teste do processamento de requisição de sincronização de recurso."""
    # Arrange
    state = mock_store_state
    
    # Configurar um recurso existente com versão mais antiga
    state.resources["/test"] = {
        "content": "old content",
        "version": 1,
        "timestamp": 100,
        "node_id": 1
    }
    
    # Mock para a requisição de sincronização
    resource_data = MagicMock()
    resource_data.path = "/test"
    resource_data.content = "new content"
    resource_data.version = 2
    resource_data.timestamp = 200
    
    # Act
    with patch("store.store.main.SyncManager.handle_sync_request", 
               new_callable=lambda: app.state.sync_manager.handle_sync_request):
        result = await app.state.sync_manager.handle_sync_request(resource_data)
    
    # Assert
    assert result["success"] == True, "A sincronização deve ter sucesso"
    assert state.resources["/test"]["content"] == "new content", "O conteúdo deve ser atualizado"
    assert state.resources["/test"]["version"] == 2, "A versão deve ser atualizada"
    state.save_resource.assert_called_once(), "O método save_resource deve ser chamado"

@pytest.mark.asyncio
async def test_sync_handle_sync_request_reject_older_version(mock_store_state):
    """Teste de rejeição de sincronização com versão mais antiga."""
    # Arrange
    state = mock_store_state
    
    # Configurar um recurso existente com versão mais recente
    state.resources["/test"] = {
        "content": "newer content",
        "version": 3,
        "timestamp": 300,
        "node_id": 1
    }
    
    # Mock para a requisição de sincronização
    resource_data = MagicMock()
    resource_data.path = "/test"
    resource_data.content = "old content"
    resource_data.version = 2
    resource_data.timestamp = 200
    
    # Act
    with patch("store.store.main.SyncManager.handle_sync_request", 
               new_callable=lambda: app.state.sync_manager.handle_sync_request):
        result = await app.state.sync_manager.handle_sync_request(resource_data)
    
    # Assert
    assert result["success"] == False, "A sincronização deve falhar"
    assert "Local version is newer" in result["reason"], "A razão deve mencionar que a versão local é mais recente"
    assert state.resources["/test"]["content"] == "newer content", "O conteúdo não deve ser alterado"
    assert state.resources["/test"]["version"] == 3, "A versão não deve ser alterada"
    state.save_resource.assert_not_called(), "O método save_resource não deve ser chamado"

def test_app_initialization():
    """Teste da inicialização da aplicação FastAPI."""
    # Assert
    assert app.title == "Paxos Store", "Título da aplicação deve ser 'Paxos Store'"
    assert "/prepare" in [route.path for route in app.routes], "A aplicação deve ter uma rota /prepare"
    assert "/commit" in [route.path for route in app.routes], "A aplicação deve ter uma rota /commit"
    assert "/resources/" in [route.path for route in app.routes][0], "A aplicação deve ter rotas para recursos"
    assert "/heartbeat" in [route.path for route in app.routes], "A aplicação deve ter uma rota /heartbeat"
    assert "/status" in [route.path for route in app.routes], "A aplicação deve ter uma rota /status"
    assert "/metrics" in [route.path for route in app.routes], "A aplicação deve ter uma rota /metrics"