"""
File: client/tests/integration/test_integration.py
Testes de integração para o cliente Paxos.
"""
import os
import sys
import json
import time
import logging
import asyncio
import pytest
import pytest_asyncio
from typing import Dict, Any, List, Optional
from unittest.mock import MagicMock, patch, AsyncMock

# Configuração de logs para debugging
logging.basicConfig(level=logging.DEBUG, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("integration_tests")

# Configure environment for testing
os.environ["DEBUG"] = "true"
os.environ["DEBUG_LEVEL"] = "trace"  # Usar trace para máximo debugging
os.environ["NODE_ID"] = "1"
os.environ["CLIENT_ID"] = "client-1"
os.environ["LOG_DIR"] = "/tmp/paxos-test-logs"
os.environ["PROPOSER"] = "mock-proposer:8080"  # Será substituído pelo mock nos testes

# Create test directory
os.makedirs(os.environ["LOG_DIR"], exist_ok=True)

# Importante: Ajusta PYTHONPATH para encontrar os módulos
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))

# Importamos os módulos necessários
from client.client import PaxosClient
from client.api import create_api
from client.web_server import WebServer
from common.logging import setup_logging

# Fixture para configuração e limpeza do ambiente de teste
@pytest.fixture(scope="module", autouse=True)
def setup_module():
    """Setup function executed once before all tests."""
    logger.info("Inicializando ambiente para testes de integração")
    
    # Initialize logging
    setup_logging("client-test-integration", debug=True, debug_level="trace", log_dir=os.environ["LOG_DIR"])
    
    # Aguarda um momento para certeza que logs estão configurados
    time.sleep(0.1)
    
    logger.info("Ambiente de testes inicializado com sucesso")
    
    yield
    
    # Cleanup after tests
    logger.info("Limpando ambiente após testes de integração")
    import shutil
    try:
        shutil.rmtree(os.environ["LOG_DIR"])
        logger.info("Diretório de logs removido com sucesso")
    except Exception as e:
        logger.error(f"Erro ao limpar diretório de logs: {e}")

# Mock Server para simular o Proposer
class MockProposerServer:
    """Servidor mock para simular o proposer nos testes de integração"""
    
    def __init__(self):
        self.requests = []
        self.responses = {}  # endpoint -> response
        self.should_fail = False
        self.delay = 0.0
        self.instance_counter = 100
        logger.info("MockProposerServer inicializado")
    
    async def handle_request(self, method, url, **kwargs):
        """Manipula requisições simulando um proposer"""
        logger.debug(f"MockProposerServer recebeu requisição: {method} {url}")
        logger.debug(f"Kwargs: {kwargs}")
        
        # Registra a requisição
        self.requests.append({
            "method": method,
            "url": url,
            "kwargs": kwargs
        })
        
        # Simula delay de rede
        if self.delay > 0:
            logger.debug(f"Simulando delay de rede: {self.delay}s")
            await asyncio.sleep(self.delay)
        
        # Simula falha se configurado
        if self.should_fail:
            logger.debug("Simulando falha do proposer")
            import httpx
            raise httpx.ConnectError("Falha simulada do proposer")
        
        # Extrai endpoint da URL
        endpoint = url.split("/")[-1]
        
        # Se houver resposta específica configurada, use-a
        if endpoint in self.responses:
            logger.debug(f"Usando resposta pré-configurada para endpoint {endpoint}")
            return self.responses[endpoint]
        
        # Resposta padrão para /propose
        if endpoint == "propose":
            logger.debug("Gerando resposta para /propose")
            self.instance_counter += 1
            return MagicMock(
                status_code=202,
                json=MagicMock(return_value={"instanceId": self.instance_counter})
            )
        
        # Resposta padrão para /health
        if endpoint == "health":
            logger.debug("Gerando resposta para /health")
            return MagicMock(
                status_code=200,
                json=MagicMock(return_value={"status": "healthy"})
            )
        
        # Resposta genérica para outros endpoints
        logger.debug(f"Gerando resposta genérica para endpoint desconhecido: {endpoint}")
        return MagicMock(
            status_code=200,
            json=MagicMock(return_value={})
        )
    
    def set_response(self, endpoint, status_code, data):
        """Configura resposta específica para um endpoint"""
        logger.info(f"Configurando resposta para endpoint {endpoint}: status={status_code}, data={data}")
        self.responses[endpoint] = MagicMock(
            status_code=status_code,
            json=MagicMock(return_value=data)
        )
    
    def reset(self):
        """Reinicia o estado do servidor mock"""
        logger.info("Reiniciando estado do MockProposerServer")
        self.requests = []
        self.responses = {}
        self.should_fail = False
        self.delay = 0.0

# Mock Server para simular o Learner
class MockLearnerServer:
    """Servidor mock para simular o learner nos testes de integração"""
    
    def __init__(self):
        self.notifications = []
        self.should_fail = False
        self.delay = 0.0
        logger.info("MockLearnerServer inicializado")
    
    async def send_notification(self, client_url, notification):
        """Envia notificação simulando um learner"""
        logger.debug(f"MockLearnerServer enviando notificação para {client_url}")
        logger.debug(f"Notificação: {notification}")
        
        # Registra a notificação
        self.notifications.append({
            "url": client_url,
            "notification": notification
        })
        
        # Simula delay de rede
        if self.delay > 0:
            logger.debug(f"Simulando delay de rede: {self.delay}s")
            await asyncio.sleep(self.delay)
        
        # Simula falha se configurado
        if self.should_fail:
            logger.debug("Simulando falha do learner")
            import httpx
            raise httpx.ConnectError("Falha simulada do learner")
        
        try:
            # Envia notificação para o cliente
            async with httpx.AsyncClient() as client:
                response = await client.post(client_url, json=notification, timeout=5.0)
                logger.debug(f"Resposta do cliente: {response.status_code}")
                logger.debug(f"Conteúdo da resposta: {response.text}")
                return response
        except Exception as e:
            logger.error(f"Erro ao enviar notificação: {e}")
            raise
    
    def reset(self):
        """Reinicia o estado do servidor mock"""
        logger.info("Reiniciando estado do MockLearnerServer")
        self.notifications = []
        self.should_fail = False
        self.delay = 0.0

# Fixture para criar cliente Paxos com mocks
@pytest_asyncio.fixture
async def integration_client():
    """Fixture que cria um PaxosClient integrado com mocks para testes"""
    logger.info("Criando cliente Paxos para testes de integração")
    
    # Criar mock servers
    proposer_mock = MockProposerServer()
    learner_mock = MockLearnerServer()
    
    # Criar cliente com URLs para mocks
    client = PaxosClient(
        client_id="client-1",
        proposer_url="http://mock-proposer:8080",
        callback_url="http://client-1:8080/notification",
        num_operations=10
    )
    
    # Substituir o cliente HTTP por um que use nossos mocks
    async def mock_post(url, **kwargs):
        logger.debug(f"Mock HTTP POST: {url}")
        if "mock-proposer" in url:
            return await proposer_mock.handle_request("POST", url, **kwargs)
        else:
            logger.warning(f"URL desconhecida no mock HTTP POST: {url}")
            return MagicMock(status_code=404)
    
    async def mock_get(url, **kwargs):
        logger.debug(f"Mock HTTP GET: {url}")
        if "mock-proposer" in url:
            return await proposer_mock.handle_request("GET", url, **kwargs)
        else:
            logger.warning(f"URL desconhecida no mock HTTP GET: {url}")
            return MagicMock(status_code=404)
    
    # Substituir métodos do cliente HTTP
    client.http_client.post = AsyncMock(side_effect=mock_post)
    client.http_client.get = AsyncMock(side_effect=mock_get)
    
    # Inicializar listas de tasks para limpeza
    client._cleanup_tasks = []
    client._timeout_tasks = []
    
    logger.info("Cliente Paxos criado com sucesso")
    
    # Retornar cliente e mocks para testes
    yield client, proposer_mock, learner_mock
    
    # Limpar recursos após testes
    logger.info("Limpando recursos do cliente após testes")
    try:
        await client.stop()
        await client.cleanup_pending_tasks()
        logger.info("Cliente parado e tasks limpas com sucesso")
    except Exception as e:
        logger.error(f"Erro ao limpar recursos do cliente: {e}")
    
    # Pequeno delay para permitir que tasks interrompidas terminem
    await asyncio.sleep(0.1)
    logger.info("Limpeza concluída")

# Fixture para criar API e Web Server
@pytest_asyncio.fixture
async def integration_servers(integration_client):
    """Fixture que cria API e Web Server integrados com o cliente para testes"""
    logger.info("Criando API e Web Server para testes de integração")
    
    client, proposer_mock, learner_mock = integration_client
    
    # Criar API
    api_app = create_api(client)
    
    # Criar Web Server
    web_server = WebServer(client, host="127.0.0.1", port=8081)
    
    # Mockear HTTP client do Web Server
    web_server.http_client = AsyncMock()
    web_server.http_client.get = AsyncMock(return_value=MagicMock(
        status_code=200,
        json=MagicMock(return_value={
            "data": "Test resource data",
            "version": 1,
            "timestamp": int(time.time() * 1000),
            "node_id": 1
        })
    ))
    
    logger.info("API e Web Server criados com sucesso")
    
    # Retornar para testes
    yield client, api_app, web_server, proposer_mock, learner_mock
    
    # Nenhuma limpeza adicional necessária aqui - o fixture do cliente já faz isso

# Agora os testes de integração propriamente ditos

@pytest.mark.asyncio
async def test_client_startup_and_shutdown(integration_client):
    """Teste de inicialização e desligamento do cliente"""
    logger.info("Iniciando teste de inicialização e desligamento do cliente")
    
    client, proposer_mock, learner_mock = integration_client
    
    # Verificar estado inicial
    assert client.running is False
    assert client.operations_completed == 0
    assert client.operations_failed == 0
    logger.debug("Estado inicial do cliente verificado")
    
    # Iniciar cliente
    with patch.object(asyncio, 'create_task') as mock_create_task:
        await client.start()
        logger.debug("Cliente iniciado")
        
        # Verificar que está em execução
        assert client.running is True
        mock_create_task.assert_called_once()
        logger.debug("Estado após inicialização verificado")
    
    # Parar cliente
    await client.stop()
    logger.debug("Cliente parado")
    
    # Verificar que está parado
    assert client.running is False
    logger.debug("Estado após parada verificado")
    
    logger.info("Teste de inicialização e desligamento concluído com sucesso")

@pytest.mark.asyncio
async def test_send_single_operation(integration_client):
    """Teste de envio de uma única operação"""
    logger.info("Iniciando teste de envio de uma única operação")
    
    client, proposer_mock, learner_mock = integration_client
    
    # Resetar estado do mock
    proposer_mock.reset()
    logger.debug("Mock do proposer resetado")
    
    # Configurar resposta específica para teste
    proposer_mock.set_response("propose", 202, {"instanceId": 42})
    logger.debug("Resposta do mock configurada")
    
    # Enviar operação
    logger.debug("Enviando operação")
    operation_id = 1
    await client._send_operation(operation_id)
    
    # Verificar que foi enviada para o proposer
    assert len(proposer_mock.requests) > 0
    logger.debug(f"Proposer recebeu {len(proposer_mock.requests)} requisições")
    
    # Verificar detalhes da requisição
    req = proposer_mock.requests[0]
    assert "propose" in req["url"]
    assert req["method"] == "POST"
    logger.debug(f"Detalhes da requisição: {req}")
    
    # Verificar que operação está em andamento
    assert 42 in client.operations_in_progress
    assert client.operations_in_progress[42]["id"] == operation_id
    logger.debug("Operação está registrada corretamente como em andamento")
    
    logger.info("Teste de envio de uma única operação concluído com sucesso")

@pytest.mark.asyncio
async def test_operation_notification(integration_client):
    """Teste de recebimento de notificação para uma operação"""
    logger.info("Iniciando teste de recebimento de notificação")
    
    client, proposer_mock, learner_mock = integration_client
    
    # Resetar estado dos mocks
    proposer_mock.reset()
    learner_mock.reset()
    logger.debug("Mocks resetados")
    
    # Configurar resposta específica para teste
    proposer_mock.set_response("propose", 202, {"instanceId": 43})
    logger.debug("Resposta do mock configurada")
    
    # Enviar operação
    logger.debug("Enviando operação")
    operation_id = 2
    await client._send_operation(operation_id)
    
    # Verificar que operação está em andamento
    assert 43 in client.operations_in_progress
    logger.debug("Operação está registrada como em andamento")
    
    # Simular notificação do learner
    logger.debug("Simulando notificação do learner")
    notification = {
        "status": "COMMITTED",
        "instanceId": 43,
        "resource": "R",
        "timestamp": int(time.time() * 1000)
    }
    
    # Processar notificação
    result = client.process_notification(notification)
    logger.debug(f"Resultado do processamento da notificação: {result}")
    
    # Verificar resposta
    assert result["status"] == "acknowledged"
    assert result["known"] == True
    assert result["operation_id"] == operation_id
    logger.debug("Resposta à notificação verificada")
    
    # Verificar que operação foi concluída
    assert 43 not in client.operations_in_progress
    assert client.operations_completed == 1
    assert len(client.history) == 1
    assert client.history[0]["status"] == "COMMITTED"
    assert client.history[0]["id"] == operation_id
    logger.debug("Estado da operação após notificação verificado")
    
    logger.info("Teste de recebimento de notificação concluído com sucesso")

@pytest.mark.asyncio
async def test_operation_with_redirection(integration_client):
    """Teste de operação com redirecionamento para outro proposer"""
    logger.info("Iniciando teste de operação com redirecionamento")
    
    client, proposer_mock, learner_mock = integration_client
    
    # Resetar estado dos mocks
    proposer_mock.reset()
    logger.debug("Mocks resetados")
    
    # Primeiro, configurar resposta de redirecionamento
    redirect_response = MagicMock(
        status_code=307,
        headers={"Location": "http://proposer-leader:8080/propose"},
        json=MagicMock(return_value={})
    )
    
    # Segundo, configurar resposta de sucesso após redirecionamento
    success_after_redirect = MagicMock(
        status_code=202,
        json=MagicMock(return_value={"instanceId": 44})
    )
    
    # Configurar sequência de respostas
    client.http_client.post.side_effect = [redirect_response, success_after_redirect]
    logger.debug("Respostas do mock configuradas para simular redirecionamento")
    
    # Enviar operação
    logger.debug("Enviando operação")
    operation_id = 3
    await client._send_operation(operation_id)
    
    # Verificar que houve duas chamadas ao proposer (original + redirecionamento)
    assert client.http_client.post.call_count >= 2
    logger.debug(f"Número de chamadas POST: {client.http_client.post.call_count}")
    
    # Verificar que operação está em andamento
    assert 44 in client.operations_in_progress
    assert client.operations_in_progress[44]["id"] == operation_id
    logger.debug("Operação está registrada corretamente como em andamento")
    
    logger.info("Teste de operação com redirecionamento concluído com sucesso")

@pytest.mark.asyncio
async def test_operation_failure_retry(integration_client):
    """Teste de falha e retry de operação"""
    logger.info("Iniciando teste de falha e retry de operação")
    
    client, proposer_mock, learner_mock = integration_client
    
    # Resetar estado dos mocks
    proposer_mock.reset()
    logger.debug("Mocks resetados")
    
    # Configurar sequência: falha seguida de sucesso
    error_response = MagicMock(status_code=500, text="Internal Server Error")
    success_response = MagicMock(
        status_code=202,
        json=MagicMock(return_value={"instanceId": 45})
    )
    
    client.http_client.post.side_effect = [error_response, success_response]
    logger.debug("Respostas do mock configuradas para simular falha seguida de sucesso")
    
    # Patch em asyncio.sleep para acelerar o teste
    with patch('asyncio.sleep', AsyncMock()):
        # Enviar operação
        logger.debug("Enviando operação")
        operation_id = 4
        await client._send_operation(operation_id)
        
        # Verificar que houve duas chamadas (original + retry)
        assert client.http_client.post.call_count >= 2
        logger.debug(f"Número de chamadas POST: {client.http_client.post.call_count}")
        
        # Verificar que operação está em andamento após retry
        assert 45 in client.operations_in_progress
        assert client.operations_in_progress[45]["id"] == operation_id
        logger.debug("Operação está registrada corretamente como em andamento após retry")
    
    logger.info("Teste de falha e retry de operação concluído com sucesso")

@pytest.mark.asyncio
async def test_operation_cycle(integration_client):
    """Teste de ciclo completo: envio + notificação + confirmação"""
    logger.info("Iniciando teste de ciclo completo de operação")
    
    client, proposer_mock, learner_mock = integration_client
    
    # Resetar cliente e mocks
    client.operations_completed = 0
    client.operations_failed = 0
    client.history = []
    proposer_mock.reset()
    learner_mock.reset()
    logger.debug("Estado do cliente e mocks resetados")
    
    # Configurar resposta do proposer
    proposer_mock.set_response("propose", 202, {"instanceId": 46})
    logger.debug("Resposta do mock configurada")
    
    # Enviar operação
    logger.debug("Enviando operação")
    operation_id = 5
    await client._send_operation(operation_id)
    
    # Verificar que operação está em andamento
    assert 46 in client.operations_in_progress
    logger.debug("Operação está registrada como em andamento")
    
    # Simular notificação do learner
    logger.debug("Simulando notificação do learner")
    notification = {
        "status": "COMMITTED",
        "instanceId": 46,
        "resource": "R",
        "timestamp": int(time.time() * 1000)
    }
    
    # Verificar estado inicial antes da notificação
    assert client.operations_completed == 0
    
    # Processar notificação
    result = client.process_notification(notification)
    logger.debug(f"Resultado do processamento da notificação: {result}")
    
    # Verificar que operação foi concluída
    assert 46 not in client.operations_in_progress
    assert client.operations_completed == 1
    assert len(client.history) == 1
    assert client.history[0]["status"] == "COMMITTED"
    logger.debug("Estado da operação após notificação verificado")
    
    # Verificar que resposta da notificação é correta
    assert result["status"] == "acknowledged"
    assert result["known"] == True
    assert result["operation_id"] == operation_id
    logger.debug("Resposta à notificação verificada")
    
    logger.info("Teste de ciclo completo de operação concluído com sucesso")

@pytest.mark.asyncio
async def test_multiple_operations(integration_client):
    """Teste de envio de múltiplas operações"""
    logger.info("Iniciando teste de múltiplas operações")
    
    client, proposer_mock, learner_mock = integration_client
    
    # Resetar cliente e mocks
    client.operations_completed = 0
    client.operations_failed = 0
    client.history = []
    client.operations_in_progress = {}
    proposer_mock.reset()
    learner_mock.reset()
    logger.debug("Estado do cliente e mocks resetados")
    
    # Enviar múltiplas operações
    num_operations = 5
    logger.debug(f"Enviando {num_operations} operações")
    
    # Lista para acompanhar instance IDs
    instance_ids = []
    
    for i in range(num_operations):
        # Configurar resposta do proposer
        instance_id = 100 + i
        proposer_mock.set_response("propose", 202, {"instanceId": instance_id})
        
        # Enviar operação
        operation_id = 10 + i
        logger.debug(f"Enviando operação {operation_id} (instance_id: {instance_id})")
        await client._send_operation(operation_id)
        
        # Salvar instance_id para verificação
        instance_ids.append(instance_id)
    
    # Verificar que todas as operações estão em andamento
    for instance_id in instance_ids:
        assert instance_id in client.operations_in_progress
    logger.debug(f"Todas as {num_operations} operações estão registradas como em andamento")
    
    # Simular notificações para metade das operações como sucesso
    logger.debug("Simulando notificações de sucesso para metade das operações")
    for i in range(num_operations // 2):
        notification = {
            "status": "COMMITTED",
            "instanceId": instance_ids[i],
            "resource": "R",
            "timestamp": int(time.time() * 1000)
        }
        client.process_notification(notification)
    
    # Simular notificações para outra metade como falha
    logger.debug("Simulando notificações de falha para outra metade das operações")
    for i in range(num_operations // 2, num_operations):
        notification = {
            "status": "NOT_COMMITTED",
            "instanceId": instance_ids[i],
            "resource": "R",
            "timestamp": int(time.time() * 1000)
        }
        client.process_notification(notification)
    
    # Verificar estado final
    assert client.operations_completed == num_operations // 2
    assert client.operations_failed == num_operations - (num_operations // 2)
    assert len(client.history) == num_operations
    # Todas as operações em progresso devem ter sido processadas
    assert len(client.operations_in_progress) == 0
    logger.debug(f"Estado final: {client.operations_completed} completadas, {client.operations_failed} falhas")
    
    logger.info("Teste de múltiplas operações concluído com sucesso")

@pytest.mark.asyncio
async def test_operation_timeout(integration_client):
    """Teste de timeout de operação"""
    logger.info("Iniciando teste de timeout de operação")
    
    client, proposer_mock, learner_mock = integration_client
    
    # Resetar cliente e mocks
    client.operations_completed = 0
    client.operations_failed = 0
    client.history = []
    client.operations_in_progress = {}
    proposer_mock.reset()
    learner_mock.reset()
    logger.debug("Estado do cliente e mocks resetados")
    
    # Enviar operação
    logger.debug("Enviando operação")
    operation_id = 6
    instance_id = 47
    proposer_mock.set_response("propose", 202, {"instanceId": instance_id})
    await client._send_operation(operation_id)
    
    # Verificar que operação está em andamento
    assert instance_id in client.operations_in_progress
    logger.debug("Operação está registrada como em andamento")
    
    # Simular timeout
    logger.debug("Simulando timeout")
    with patch('asyncio.sleep', AsyncMock()):
        await client._handle_operation_timeout(instance_id)
    
    # Verificar que operação foi marcada como timeout
    assert instance_id not in client.operations_in_progress
    assert client.operations_failed == 1
    assert len(client.history) == 1
    assert client.history[0]["status"] == "timeout"
    logger.debug("Operação marcada corretamente como timeout")
    
    logger.info("Teste de timeout de operação concluído com sucesso")

@pytest.mark.asyncio
async def test_network_partition(integration_client):
    """Teste de comportamento durante partição de rede"""
    logger.info("Iniciando teste de partição de rede")
    
    client, proposer_mock, learner_mock = integration_client
    
    # Resetar cliente e mocks
    client.operations_completed = 0
    client.operations_failed = 0
    client.history = []
    client.operations_in_progress = {}
    proposer_mock.reset()
    logger.debug("Estado do cliente e mocks resetados")
    
    # Configurar proposer para falhar (simular partição)
    proposer_mock.should_fail = True
    logger.debug("Proposer configurado para falhar (simulando partição)")
    
    # Tentar enviar operação durante a partição
    logger.debug("Tentando enviar operação durante a partição")
    operation_id = 7
    
    # Patch em asyncio.sleep para acelerar o teste
    with patch('asyncio.sleep', AsyncMock()):
        # Substituir side_effect do client.http_client.post para simular erro de conexão
        import httpx
        client.http_client.post.side_effect = httpx.ConnectError("Falha simulada do proposer")
        
        # Simular tentativas esgotadas (retries=2)
        await client._send_operation(operation_id, retries=2)
        
        # Verificar que operação foi marcada como falha
        assert client.operations_failed >= 1
        assert len(client.history) >= 1
        failed_op = next((op for op in client.history if op.get("id") == operation_id), None)
        assert failed_op is not None
        assert failed_op["status"] == "failed"
        logger.debug("Operação marcada corretamente como falha após tentativas")
    
    # Simular recuperação da partição
    proposer_mock.should_fail = False
    proposer_mock.set_response("propose", 202, {"instanceId": 48})
    logger.debug("Proposer configurado para sucesso (simulando recuperação)")
    
    # Restaurar o comportamento normal do post
    client.http_client.post = AsyncMock(side_effect=lambda url, **kwargs: 
        proposer_mock.handle_request("POST", url, **kwargs)
    )
    
    # Enviar nova operação após recuperação
    logger.debug("Enviando operação após recuperação")
    operation_id = 8
    await client._send_operation(operation_id)
    
    # Verificar que operação está em andamento
    assert 48 in client.operations_in_progress
    logger.debug("Operação está registrada como em andamento após recuperação")
    
    logger.info("Teste de partição de rede concluído com sucesso")

@pytest.mark.asyncio
async def test_request_deduplication(integration_client):
    """Teste de desduplicação de requisições"""
    logger.info("Iniciando teste de desduplicação de requisições")
    
    client, proposer_mock, learner_mock = integration_client
    
    # Resetar cliente e mocks
    client.operations_completed = 0
    client.operations_failed = 0
    client.history = []
    client.operations_in_progress = {}
    client.request_ids = {}
    proposer_mock.reset()
    logger.debug("Estado do cliente e mocks resetados")
    
    # Configurar TTL curto para teste
    client.request_id_ttl = 1.0
    logger.debug(f"TTL configurado para {client.request_id_ttl}s")
    
    # Enviar operação (primeiro envio)
    logger.debug("Enviando operação pela primeira vez")
    operation_id = 9
    timestamp = 1000000000  # Timestamp fixo para garantir mesmo ID
    proposer_mock.set_response("propose", 202, {"instanceId": 49})
    await client._send_operation(operation_id, timestamp_override=timestamp)
    
    # Verificar que foi enviada
    assert len(proposer_mock.requests) == 1
    assert 49 in client.operations_in_progress
    logger.debug("Primeira requisição enviada corretamente")
    
    # Resetar contagem de requisições do mock
    proposer_mock.requests = []
    logger.debug("Contador de requisições do mock resetado")
    
    # Tentar enviar a mesma operação novamente
    logger.debug("Tentando enviar mesma operação novamente (deve ser desduplicada)")
    await client._send_operation(operation_id, timestamp_override=timestamp)
    
    # Verificar que foi desduplicada (não enviada)
    assert len(proposer_mock.requests) == 0
    logger.debug("Segunda requisição desduplicada corretamente")
    
    # Aguardar expiração do TTL
    logger.debug(f"Aguardando expiração do TTL ({client.request_id_ttl}s)")
    await asyncio.sleep(client.request_id_ttl + 0.1)
    
    # Tentar novamente após expiração
    logger.debug("Tentando novamente após expiração do TTL")
    proposer_mock.set_response("propose", 202, {"instanceId": 50})
    await client._send_operation(operation_id, timestamp_override=timestamp)
    
    # Verificar que foi enviada novamente
    assert len(proposer_mock.requests) == 1
    assert 50 in client.operations_in_progress
    logger.debug("Terceira requisição enviada corretamente após expiração")
    
    logger.info("Teste de desduplicação de requisições concluído com sucesso")

@pytest.mark.asyncio
async def test_api_notification_endpoint(integration_servers):
    """Teste do endpoint de notificação da API"""
    logger.info("Iniciando teste do endpoint de notificação da API")
    
    client, api_app, web_server, proposer_mock, learner_mock = integration_servers
    
    # Resetar cliente
    client.operations_completed = 0
    client.operations_failed = 0
    client.history = []
    logger.debug("Estado do cliente resetado")
    
    # Criar operação em andamento
    instance_id = 51
    operation_id = 10
    client.operations_in_progress[instance_id] = {
        "id": operation_id,
        "start_time": time.time(),
        "status": "in_progress",
        "payload": {"data": "test"}
    }
    logger.debug(f"Operação em andamento criada: instance_id={instance_id}, operation_id={operation_id}")
    
    # Criar cliente de teste para a API
    from fastapi.testclient import TestClient
    api_client = TestClient(api_app)
    logger.debug("Cliente de teste da API criado")
    
    # Enviar notificação
    logger.debug("Enviando notificação via API")
    notification = {
        "status": "COMMITTED",
        "instanceId": instance_id,
        "resource": "R",
        "timestamp": int(time.time() * 1000)
    }
    response = api_client.post("/notification", json=notification)
    
    # Verificar resposta
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "acknowledged"
    assert data["known"] == True
    assert data["operation_id"] == operation_id
    logger.debug(f"Resposta da API verificada: {data}")
    
    # Verificar que operação foi processada
    assert instance_id not in client.operations_in_progress
    assert client.operations_completed == 1
    assert len(client.history) == 1
    assert client.history[0]["status"] == "COMMITTED"
    logger.debug("Estado do cliente após notificação verificado")
    
    logger.info("Teste do endpoint de notificação da API concluído com sucesso")

@pytest.mark.asyncio
async def test_web_server_routes(integration_servers):
    """Teste das rotas do servidor web"""
    logger.info("Iniciando teste das rotas do servidor web")
    
    client, api_app, web_server, proposer_mock, learner_mock = integration_servers
    
    # Criar cliente de teste para o web server
    from fastapi.testclient import TestClient
    web_client = TestClient(web_server.app)
    logger.debug("Cliente de teste do web server criado")
    
    # Testar rota index
    logger.debug("Testando rota index")
    response = web_client.get("/")
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    logger.debug("Rota index testada com sucesso")
    
    # Testar rota de recursos
    logger.debug("Testando rota de recursos")
    response = web_client.get("/resources")
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    logger.debug("Rota de recursos testada com sucesso")
    
    # Testar rota de histórico
    logger.debug("Testando rota de histórico")
    response = web_client.get("/history")
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    logger.debug("Rota de histórico testada com sucesso")
    
    # Testar rota de logs
    logger.debug("Testando rota de logs")
    response = web_client.get("/logs")
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    logger.debug("Rota de logs testada com sucesso")
    
    # Testar API para status
    logger.debug("Testando API de status")
    response = web_client.get("/api/status")
    assert response.status_code == 200
    data = response.json()
    assert "client_id" in data
    assert "total_operations" in data
    logger.debug("API de status testada com sucesso")
    
    logger.info("Teste das rotas do servidor web concluído com sucesso")

@pytest.mark.asyncio
async def test_full_client_operation_flow(integration_servers):
    """Teste do fluxo completo: iniciar cliente, enviar operação, receber notificação, parar cliente"""
    logger.info("Iniciando teste do fluxo completo de operação do cliente")
    
    client, api_app, web_server, proposer_mock, learner_mock = integration_servers
    
    # Resetar cliente e mocks
    client.operations_completed = 0
    client.operations_failed = 0
    client.history = []
    client.operations_in_progress = {}
    client.next_operation_id = 100
    proposer_mock.reset()
    learner_mock.reset()
    logger.debug("Estado do cliente e mocks resetados")
    
    # Configurar mock do proposer para resposta de sucesso
    proposer_mock.set_response("propose", 202, {"instanceId": 60})
    logger.debug("Mock do proposer configurado")
    
    # Iniciar cliente com patch para não executar o loop de operações real
    with patch.object(client, '_operation_loop', AsyncMock()):
        # Iniciar cliente
        logger.debug("Iniciando cliente")
        await client.start()
        assert client.running == True
        
        # Criar cliente de teste para a API
        from fastapi.testclient import TestClient
        api_client = TestClient(api_app)
        logger.debug("Cliente de teste da API criado")
        
        # Enviar operação manual via API
        logger.debug("Enviando operação manual via API")
        with patch('asyncio.create_task') as mock_create_task:
            response = api_client.post("/api/operation", params={"data": "test data"})
            assert response.status_code == 200
            data = response.json()
            assert "operation_id" in data
            assert data["status"] == "initiated"
            logger.debug(f"Resposta da API para operação manual: {data}")
            
            # Verificar que task foi criada
            mock_create_task.assert_called_once()
        
        # Simular envio direto da operação
        operation_id = client.next_operation_id - 1  # ID gerado pela operação manual
        logger.debug(f"Simulando envio direto da operação {operation_id}")
        await client._send_operation(operation_id)
        
        # Verificar que operação está em andamento
        assert 60 in client.operations_in_progress
        logger.debug("Operação está registrada como em andamento")
        
        # Criar notificação
        notification = {
            "status": "COMMITTED",
            "instanceId": 60,
            "resource": "R",
            "timestamp": int(time.time() * 1000)
        }
        
        # Enviar notificação via API
        logger.debug("Enviando notificação via API")
        response = api_client.post("/notification", json=notification)
        assert response.status_code == 200
        
        # Verificar que operação foi processada
        assert 60 not in client.operations_in_progress
        assert client.operations_completed == 1
        assert len(client.history) == 1
        logger.debug("Operação processada corretamente")
        
        # Parar cliente
        logger.debug("Parando cliente")
        response = api_client.post("/api/stop")
        assert response.status_code == 200
        assert response.json()["status"] == "stopped"
        
        # Verificar que cliente está parado
        assert client.running == False
        logger.debug("Cliente parado corretamente")
    
    logger.info("Teste do fluxo completo de operação do cliente concluído com sucesso")
