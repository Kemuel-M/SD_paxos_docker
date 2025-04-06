"""
File: learner/tests/integration/test_integration.py
Integration tests for the Learner component.
"""
import os
import json
import time
import pytest
import asyncio
import tempfile
import shutil
from unittest.mock import MagicMock, patch, AsyncMock
from fastapi.testclient import TestClient
from functools import partial
import threading

import warnings
warnings.filterwarnings("always", category=RuntimeWarning)

# Configure environment for testing
os.environ["DEBUG"] = "true"
os.environ["DEBUG_LEVEL"] = "basic"
os.environ["NODE_ID"] = "1"
os.environ["ACCEPTORS"] = "localhost:8091,localhost:8092,localhost:8093,localhost:8094,localhost:8095"
os.environ["STORES"] = "localhost:8081,localhost:8082,localhost:8083"
os.environ["LOG_DIR"] = "/tmp/paxos-test-logs"

# Definir o valor padrão, que pode ser sobrescrito pelos parâmetros do pytest ou variável de ambiente
DEFAULT_USE_CLUSTER_STORE = "false"

# Função auxiliar para determinar se os testes devem usar Cluster Store
def should_use_cluster_store():
    """Determine if tests should use cluster store based on environment variable or pytest configuration."""
    # Verificar se foi passado via parâmetro do comando pytest
    if hasattr(pytest, "use_cluster_store"):
        return pytest.use_cluster_store == "true"
    
    # Verificar variável de ambiente
    return os.environ.get("USE_CLUSTER_STORE", DEFAULT_USE_CLUSTER_STORE) == "true"

# Create directories
os.makedirs(os.environ["LOG_DIR"], exist_ok=True)

# Fixture para testes que devem ser executados com Cluster Store
@pytest.fixture
def require_cluster_store():
    """
    Fixture para pular testes que requerem Cluster Store se não estiver habilitado.
    """
    if not should_use_cluster_store():
        # Configurar temporariamente o ambiente para usar cluster store apenas para este teste
        original = os.environ.get("USE_CLUSTER_STORE", None)
        os.environ["USE_CLUSTER_STORE"] = "true"
        yield
        # Restaurar a configuração original
        if original is not None:
            os.environ["USE_CLUSTER_STORE"] = original
        else:
            del os.environ["USE_CLUSTER_STORE"]
    else:
        yield

# Fixture for setup and teardown of test environment
@pytest.fixture(scope="module")
def setup_integration():
    """Setup function executed once before all integration tests."""
    # Verificar a configuração
    use_cluster_store = should_use_cluster_store()
    print(f"Running integration tests with USE_CLUSTER_STORE={use_cluster_store}")
    
    # Create temporary data directory
    temp_dir = tempfile.mkdtemp()
    
    # Configure mock for HTTP client
    http_client_mock = AsyncMock()
    http_client_mock.post.return_value = {"status": "ok"}
    http_client_mock.get.return_value = {"data": "test", "version": 1}
    
    # Import necessary modules
    from learner import Learner
    from consensus import ConsensusManager
    from rowa import RowaManager
    from two_phase import TwoPhaseCommitManager
    from api import create_api
    from common.logging import setup_logging
    from common.communication import HttpClient
    
    # Patch HTTP client
    with patch.object(HttpClient, 'post', new=http_client_mock.post), \
         patch.object(HttpClient, 'get', new=http_client_mock.get):
        # Setup logging
        setup_logging("learner-test", debug=True, debug_level="basic", log_dir=os.environ["LOG_DIR"])
        
        # Create components
        consensus_manager = ConsensusManager(node_id=1)
        two_phase_manager = TwoPhaseCommitManager(node_id=1, stores=os.environ["STORES"].split(","))
        rowa_manager = RowaManager(node_id=1, stores=os.environ["STORES"].split(","), two_phase_manager=two_phase_manager)
        
        # Create learner (usando o valor calculado para use_cluster_store)
        learner = Learner(
            node_id=1,
            acceptors=os.environ["ACCEPTORS"].split(","),
            stores=os.environ["STORES"].split(","),
            consensus_manager=consensus_manager,
            rowa_manager=rowa_manager,
            use_cluster_store=use_cluster_store
        )
        
        # Create API
        app = create_api(learner, consensus_manager, rowa_manager)
        client = TestClient(app)
        
        # Start learner
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(learner.start())
        
        # Return components for tests
        yield {
            "temp_dir": temp_dir,
            "learner": learner,
            "consensus_manager": consensus_manager,
            "rowa_manager": rowa_manager,
            "two_phase_manager": two_phase_manager,
            "client": client,
            "http_client_mock": http_client_mock,
            "loop": loop,  # Passar o loop para os testes
            "use_cluster_store": use_cluster_store  # Para que os testes saibam a configuração
        }
        
        # Cleanup
        try:
            if not loop.is_closed():
                loop.run_until_complete(learner.stop())
        except RuntimeError as e:
            if "Event loop is closed" in str(e):
                # Criar um novo loop se o anterior estiver fechado
                new_loop = asyncio.new_event_loop()
                asyncio.set_event_loop(new_loop)
                new_loop.run_until_complete(learner.stop())
                new_loop.close()
            else:
                raise
        
        # Fechar o loop original se ainda estiver aberto
        if not loop.is_closed():
            loop.close()

        try:
            shutil.rmtree(temp_dir)
            shutil.rmtree(os.environ["LOG_DIR"])
        except:
            pass

def test_process_learn_notification_flow(setup_integration):
    """Test the complete flow of processing learn notifications."""
    client = setup_integration["client"]
    consensus_manager = setup_integration["consensus_manager"]
    
    # Create learn notifications for the same instance but different acceptors
    notifications = []
    for acceptor_id in range(1, 6):
        notifications.append({
            "type": "LEARN",
            "proposalNumber": 42,
            "instanceId": 100,
            "acceptorId": acceptor_id,
            "accepted": True,
            "value": {"clientId": "client-1", "data": "test data", "resource": "R"},
            "timestamp": int(time.time() * 1000)
        })
    
    # Send first notification (should not reach consensus yet)
    response1 = client.post("/learn", json=notifications[0])
    assert response1.status_code == 200
    assert response1.json()["accepted"] == True
    
    # Check instance state
    response_instance = client.get("/instance/100")
    assert response_instance.status_code == 200
    data = response_instance.json()
    assert data["decided"] == False
    
    # Send second notification (still not enough for consensus)
    response2 = client.post("/learn", json=notifications[1])
    assert response2.status_code == 200

    # Check instance state again
    response_instance = client.get("/instance/100")
    assert response_instance.status_code == 200
    data = response_instance.json()
    assert data["decided"] == False
    
    # Send third notification (should reach consensus)
    response3 = client.post("/learn", json=notifications[2])
    assert response3.status_code == 200
    
    # Wait a moment for async processing
    time.sleep(0.1)
    
    # Check instance state again - should be decided now
    response_instance = client.get("/instance/100")
    assert response_instance.status_code == 200
    data = response_instance.json()
    assert data["decided"] == True
    assert data["proposalNumber"] == 42
    assert data["value"]["clientId"] == "client-1"
    assert data["value"]["data"] == "test data"

def test_conflicting_values(setup_integration):
    """Test handling of conflicting values from different acceptors."""
    client = setup_integration["client"]
    
    # Create learn notifications for the same instance but different values
    notification1 = {
        "type": "LEARN",
        "proposalNumber": 42,
        "instanceId": 200,
        "acceptorId": 1,
        "accepted": True,
        "value": {"clientId": "client-1", "data": "value 1", "resource": "R"},
        "timestamp": int(time.time() * 1000)
    }
    
    notification2 = {
        "type": "LEARN",
        "proposalNumber": 42,
        "instanceId": 200,
        "acceptorId": 2,
        "accepted": True,
        "value": {"clientId": "client-1", "data": "value 2", "resource": "R"},
        "timestamp": int(time.time() * 1000)
    }
    
    # Send the notifications
    client.post("/learn", json=notification1)
    client.post("/learn", json=notification2)
    
    # Send more acceptors with value 1 to reach consensus
    for acceptor_id in range(3, 5):
        notification = {
            "type": "LEARN",
            "proposalNumber": 42,
            "instanceId": 200,
            "acceptorId": acceptor_id,
            "accepted": True,
            "value": {"clientId": "client-1", "data": "value 1", "resource": "R"},
            "timestamp": int(time.time() * 1000)
        }
        client.post("/learn", json=notification)
    
    # Wait a moment for async processing
    time.sleep(0.1)
    
    # Check instance state
    response = client.get("/instance/200")
    assert response.status_code == 200
    data = response.json()
    assert data["decided"] == True
    assert data["value"]["data"] == "value 1"  # Majority value won

def test_different_proposal_numbers(setup_integration):
    """Test handling of different proposal numbers."""
    client = setup_integration["client"]
    
    # Create learn notifications with different proposal numbers
    notification1 = {
        "type": "LEARN",
        "proposalNumber": 40,
        "instanceId": 300,
        "acceptorId": 1,
        "accepted": True,
        "value": {"clientId": "client-1", "data": "value from 40", "resource": "R"},
        "timestamp": int(time.time() * 1000)
    }
    
    notification2 = {
        "type": "LEARN",
        "proposalNumber": 50,
        "instanceId": 300,
        "acceptorId": 2,
        "accepted": True,
        "value": {"clientId": "client-1", "data": "value from 50", "resource": "R"},
        "timestamp": int(time.time() * 1000)
    }
    
    # Send first notification with proposal 40
    client.post("/learn", json=notification1)
    
    # Send two more acceptors with proposal 40 to reach consensus
    for acceptor_id in range(3, 5):
        notification = {
            "type": "LEARN",
            "proposalNumber": 40,
            "instanceId": 300,
            "acceptorId": acceptor_id,
            "accepted": True,
            "value": {"clientId": "client-1", "data": "value from 40", "resource": "R"},
            "timestamp": int(time.time() * 1000)
        }
        client.post("/learn", json=notification)
    
    # Send a notification with higher proposal number
    client.post("/learn", json=notification2)
    
    # Wait a moment for async processing
    time.sleep(0.1)
    
    # Check instance state - the first one to reach consensus should win
    response = client.get("/instance/300")
    assert response.status_code == 200
    data = response.json()
    assert data["decided"] == True
    assert data["proposalNumber"] == 40
    assert data["value"]["data"] == "value from 40"

# Função auxiliar para executar código assíncrono em um evento sincrônico
def run_async(coroutine):
    """
    Executa uma coroutine de forma síncrona e retorna seu resultado.
    Útil para testar código assíncrono em contextos síncronos.
    """
    try:
        # Tentar usar o loop existente primeiro
        loop = asyncio.get_event_loop()
        if loop.is_closed():
            raise RuntimeError("Loop is closed")
        return loop.run_until_complete(coroutine)
    except (RuntimeError, asyncio.InvalidStateError):
        # Se o loop estiver fechado ou em estado inválido, criar um novo
        new_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(new_loop)
        try:
            return new_loop.run_until_complete(coroutine)
        finally:
            new_loop.close()

# Versão modificada do teste - agora sempre executa independentemente da configuração
def test_part1_simulation(setup_integration):
    """Test the Part 1 simulation mode (no Cluster Store)."""
    client = setup_integration["client"]
    learner = setup_integration["learner"]
    rowa_manager = setup_integration["rowa_manager"]
    
    # Salvar a configuração original
    original_use_cluster_store = learner.use_cluster_store
    
    # Configurar temporariamente para modo de simulação
    learner.use_cluster_store = False

    import logging
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger(__name__)
    
    # Armazenar resultados em uma estrutura thread-safe
    # (já que estamos em um ambiente de teste que mistura código síncrono e assíncrono)
    class SimulationResults:
        def __init__(self):
            self.results = []
            self.lock = threading.Lock()
        
        def add(self, value):
            with self.lock:
                self.results.append(value)
                logger.debug(f"Adicionado {value} aos resultados. Atual: {self.results}")
        
        def get(self):
            with self.lock:
                return self.results.copy()
    
    results = SimulationResults()
    
    # Versão sincronizada do mock
    async def mock_rowa_simulate():
        logger.debug("mock_rowa_simulate() chamado")
        delay = 0.5
        logger.debug(f"Usando delay fixo: {delay}")
        
        # Em vez de simplesmente esperar, criamos um evento para sinalizar conclusão
        done_event = asyncio.Event()
        
        # Esta função será chamada quando o sleep terminar
        def on_sleep_done():
            logger.debug(f"Sleep concluído após {delay}s")
            results.add(delay)
            done_event.set()
            logger.debug("Evento de conclusão definido")
        
        # Criar e iniciar uma tarefa que espera e depois sinaliza conclusão
        async def wait_and_signal():
            await asyncio.sleep(delay)
            on_sleep_done()
        
        # Certifique-se de que a tarefa seja executada no mesmo loop de eventos
        asyncio.create_task(wait_and_signal())
        
        # Para testes, não esperamos pelo evento, apenas retornamos o valor
        # Em um ambiente real, faríamos: await done_event.wait()
        return delay
    
    # MÉTODO DIRETO: Em vez de enviar notificações via REST, injetamos diretamente
    async def direct_test():
        # Aplicar mock
        original_simulate = rowa_manager.simulate_resource_access
        rowa_manager.simulate_resource_access = mock_rowa_simulate
        
        try:
            logger.debug("Método de teste direto iniciado")
            
            # Criar notificações diretamente
            notifications = []
            for acceptor_id in range(1, 4):
                notifications.append({
                    "instanceId": 400,
                    "proposalNumber": 42,
                    "acceptorId": acceptor_id,
                    "accepted": True,
                    "value": {"clientId": "client-1", "data": "test", "resource": "R"},
                    "timestamp": int(time.time() * 1000)
                })
            
            # Processar notificações diretamente no consensus_manager
            for notification in notifications:
                logger.debug(f"Processando notificação para aceitador {notification['acceptorId']}")
                await learner.consensus_manager.process_notification(notification)
            
            # Forçar o processo de simulação de forma direta e independente
            instance_id = 400
            proposal_number = 42
            value = {"clientId": "client-1", "data": "test", "resource": "R"}
            
            # Chamar o callback de decisão diretamente
            logger.debug("Chamando o callback de decisão diretamente")
            await learner._on_decision_made(instance_id, proposal_number, value)
            
            # Esperar para dar tempo às tarefas assíncronas de concluírem
            logger.debug("Esperando tarefas assíncronas concluírem")
            for _ in range(10):  # 10 iterações de 0.2s = 2s total
                if results.get():
                    logger.debug(f"Resultados encontrados: {results.get()}")
                    break
                await asyncio.sleep(0.2)
            
            # Verificar resultados
            return results.get()
            
        finally:
            # Restaurar o método original
            rowa_manager.simulate_resource_access = original_simulate
    
    try:
        # Executar o teste direto de forma síncrona
        direct_results = run_async(direct_test())
        
        # Verificação
        logger.debug(f"Resultados finais: {direct_results}")
        assert len(direct_results) > 0, "Método de simulação não foi chamado ou não registrou resultados"
    finally:
        # Restaurar a configuração original
        learner.use_cluster_store = original_use_cluster_store

# Usando a fixture require_cluster_store para forçar a disponibilidade deste teste
def test_part2_cluster_store(setup_integration, require_cluster_store):
    """Test the Part 2 Cluster Store integration."""
    client = setup_integration["client"]
    learner = setup_integration["learner"]
    rowa_manager = setup_integration["rowa_manager"]
    http_client_mock = setup_integration["http_client_mock"]
    
    # Salvar a configuração original
    original_use_cluster_store = learner.use_cluster_store
    
    # Forçar ativação do Cluster Store para este teste
    learner.use_cluster_store = True
    
    try:
        # Reset HTTP client mock
        http_client_mock.post.reset_mock()
        
        # Configure HTTP client mock for prepare and commit success
        http_client_mock.post.side_effect = [
            # Prepare responses for 3 stores
            {"ready": True, "currentVersion": 1},
            {"ready": True, "currentVersion": 1},
            {"ready": True, "currentVersion": 1},
            # Commit responses for 3 stores
            {"success": True, "version": 2},
            {"success": True, "version": 2},
            {"success": True, "version": 2}
        ]
        
        # Create and send learn notifications to reach consensus
        for acceptor_id in range(1, 4):
            notification = {
                "type": "LEARN",
                "proposalNumber": 42,
                "instanceId": 500,
                "acceptorId": acceptor_id,
                "accepted": True,
                "value": {"clientId": "client-1", "data": "test", "resource": "R"},
                "timestamp": int(time.time() * 1000)
            }
            client.post("/learn", json=notification)
        
        # Wait for processing to complete
        time.sleep(0.5)
        
        # Check if HTTP client was called for both prepare and commit phases
        assert http_client_mock.post.call_count >= 6  # 3 prepare + 3 commit
    finally:
        # Restaurar a configuração original
        learner.use_cluster_store = original_use_cluster_store

def test_client_notification(setup_integration):
    """Test the client notification mechanism."""
    client = setup_integration["client"]
    learner = setup_integration["learner"]
    rowa_manager = setup_integration["rowa_manager"]  # Adicionado rowa_manager
    http_client_mock = setup_integration["http_client_mock"]
    
    # Reset HTTP client mock
    http_client_mock.post.reset_mock()
    
    # Mock rowa_manager.write_resource para retornar sucesso controlado
    original_write = rowa_manager.write_resource
    rowa_manager.write_resource = AsyncMock(return_value=(True, {"version": 2}))
    
    # Register a client callback
    learner.register_client_callback(
        instance_id=600,
        client_id="client-1",
        callback_url="http://client-1:8080/callback"
    )
    
    # Mock _should_handle_client_notification to return True
    original_should_handle = learner._should_handle_client_notification
    learner._should_handle_client_notification = lambda client_id: True
    
    try:
        # Create and send learn notifications to reach consensus
        for acceptor_id in range(1, 4):
            notification = {
                "type": "LEARN",
                "proposalNumber": 42,
                "instanceId": 600,
                "acceptorId": acceptor_id,
                "accepted": True,
                "value": {"clientId": "client-1", "data": "test", "resource": "R"},
                "timestamp": int(time.time() * 1000)
            }
            client.post("/learn", json=notification)
        
        # Implementar polling para aguardar a notificação
        notification_found = False
        for _ in range(10):  # Tentar até 10 vezes (5 segundos total)
            notification_calls = [call for call in http_client_mock.post.call_args_list 
                                if "client-1:8080/callback" in str(call)]
            if notification_calls:
                notification_found = True
                break
            time.sleep(0.5)
        
        # Verificar se o cliente HTTP foi chamado para notificação
        assert notification_found, "Notificação do cliente não foi enviada após espera"
        
        # Verificar conteúdo da notificação
        notification_calls = [call for call in http_client_mock.post.call_args_list 
                            if "client-1:8080/callback" in str(call)]
        notification_call = notification_calls[0]
        args, kwargs = notification_call
        assert kwargs.get("json", {}).get("status") == "COMMITTED"
        assert kwargs.get("json", {}).get("instanceId") == 600
        
    finally:
        # Restaurar métodos originais
        learner._should_handle_client_notification = original_should_handle
        rowa_manager.write_resource = original_write

def test_client_notification_failure(setup_integration):
    """Test client notification when write operation fails."""
    client = setup_integration["client"]
    learner = setup_integration["learner"]
    rowa_manager = setup_integration["rowa_manager"]
    http_client_mock = setup_integration["http_client_mock"]
    
    # Reset HTTP client mock
    http_client_mock.post.reset_mock()
    
    # Mock rowa_manager.write_resource para simular falha
    original_write = rowa_manager.write_resource
    rowa_manager.write_resource = AsyncMock(return_value=(False, None))
    
    # Register a client callback
    learner.register_client_callback(
        instance_id=650,  # Usar ID diferente para evitar conflito
        client_id="client-1",
        callback_url="http://client-1:8080/callback"
    )
    
    # Mock _should_handle_client_notification to return True
    original_should_handle = learner._should_handle_client_notification
    learner._should_handle_client_notification = lambda client_id: True
    
    try:
        # Create and send learn notifications to reach consensus
        for acceptor_id in range(1, 4):
            notification = {
                "type": "LEARN",
                "proposalNumber": 42,
                "instanceId": 650,
                "acceptorId": acceptor_id,
                "accepted": True,
                "value": {"clientId": "client-1", "data": "test", "resource": "R"},
                "timestamp": int(time.time() * 1000)
            }
            client.post("/learn", json=notification)
        
        # Implementar polling para aguardar a notificação
        notification_found = False
        for _ in range(10):  # Tentar até 10 vezes (5 segundos total)
            notification_calls = [call for call in http_client_mock.post.call_args_list 
                                if "client-1:8080/callback" in str(call)]
            if notification_calls:
                notification_found = True
                break
            time.sleep(0.5)
        
        # Verificar se o cliente HTTP foi chamado para notificação
        assert notification_found, "Notificação do cliente não foi enviada após espera"
        
        # Verificar conteúdo da notificação - deve ser NOT_COMMITTED devido à falha
        notification_calls = [call for call in http_client_mock.post.call_args_list 
                            if "client-1:8080/callback" in str(call)]
        notification_call = notification_calls[0]
        args, kwargs = notification_call
        assert kwargs.get("json", {}).get("status") == "NOT_COMMITTED"
        assert kwargs.get("json", {}).get("instanceId") == 650
        
    finally:
        # Restaurar métodos originais
        learner._should_handle_client_notification = original_should_handle
        rowa_manager.write_resource = original_write

# Este teste agora usa a fixture require_cluster_store
def test_client_notification_on_storage_failure(setup_integration, require_cluster_store):
    """Test client notification when Cluster Store write fails."""
    client = setup_integration["client"]
    learner = setup_integration["learner"]
    rowa_manager = setup_integration["rowa_manager"]
    http_client_mock = setup_integration["http_client_mock"]
    
    # Salvar configuração original
    original_use_cluster_store = learner.use_cluster_store
    
    # Forçar ativação do Cluster Store para este teste
    learner.use_cluster_store = True
    
    try:
        # Reset HTTP client mock
        http_client_mock.post.reset_mock()
        
        # Configure rowa_manager.write_resource to return failure
        original_write = rowa_manager.write_resource
        rowa_manager.write_resource = AsyncMock(return_value=(False, None))
        
        # Register a client callback
        learner.register_client_callback(
            instance_id=650,
            client_id="client-1",
            callback_url="http://client-1:8080/callback"
        )
        
        # Mock _should_handle_client_notification to return True
        original_should_handle = learner._should_handle_client_notification
        learner._should_handle_client_notification = lambda client_id: True
        
        # Create and send learn notifications to reach consensus
        for acceptor_id in range(1, 4):
            notification = {
                "type": "LEARN",
                "proposalNumber": 42,
                "instanceId": 650,
                "acceptorId": acceptor_id,
                "accepted": True,
                "value": {"clientId": "client-1", "data": "test", "resource": "R"},
                "timestamp": int(time.time() * 1000)
            }
            client.post("/learn", json=notification)
        
        # Wait for processing to complete
        time.sleep(0.5)
        
        # Check if HTTP client was called for client notification
        notification_calls = [call for call in http_client_mock.post.call_args_list 
                             if "client-1:8080/callback" in str(call)]
        assert len(notification_calls) > 0
        
        # Check notification content has NOT_COMMITTED status due to storage failure
        notification_call = notification_calls[0]
        args, kwargs = notification_call
        assert kwargs.get("json", {}).get("status") == "NOT_COMMITTED"
        assert kwargs.get("json", {}).get("instanceId") == 650
        
        # Restore original methods
        learner._should_handle_client_notification = original_should_handle
        rowa_manager.write_resource = original_write
    finally:
        # Restaurar configuração original
        learner.use_cluster_store = original_use_cluster_store

def test_full_api_integration(setup_integration):
    """Test all API endpoints together."""
    client = setup_integration["client"]
    
    # First, create decided instances
    for instance_id in [700, 701]:
        for acceptor_id in range(1, 4):
            notification = {
                "type": "LEARN",
                "proposalNumber": 42,
                "instanceId": instance_id,
                "acceptorId": acceptor_id,
                "accepted": True,
                "value": {"clientId": "client-1", "data": f"test {instance_id}", "resource": "R"},
                "timestamp": int(time.time() * 1000)
            }
            client.post("/learn", json=notification)
    
    # Wait for processing
    time.sleep(0.2)
    
    # Test health endpoint
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "healthy"
    
    # Test status endpoint
    response = client.get("/status")
    assert response.status_code == 200
    assert response.json()["node_id"] == 1
    
    # Test instances endpoint
    response = client.get("/instances")
    assert response.status_code == 200
    data = response.json()
    assert "instances" in data
    assert "total" in data
    assert data["total"] >= 2  # Should have at least our 2 instances
    
    # Test instance endpoint for specific instance
    response = client.get("/instance/700")
    assert response.status_code == 200
    data = response.json()
    assert data["instanceId"] == 700
    assert data["decided"] == True
    assert data["value"]["data"] == "test 700"
    
    # Test logs endpoint
    response = client.get("/logs")
    assert response.status_code == 200
    assert "logs" in response.json()
    
    # Test important logs endpoint
    response = client.get("/logs/important")
    assert response.status_code == 200
    assert "logs" in response.json()
    
    # Test stats endpoint
    response = client.get("/stats")
    assert response.status_code == 200
    assert "stats" in response.json()
    
    # Test debug config endpoint
    debug_config = {"enabled": True, "level": "advanced"}
    response = client.post("/debug/config", json=debug_config)
    assert response.status_code == 200
    assert response.json()["debug"]["level"] == "advanced"