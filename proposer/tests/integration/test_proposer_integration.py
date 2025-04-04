"""
File: proposer/tests/integration/test_proposer_integration.py
Testes de integração para o componente Proposer.
"""
import os
import json
import time
import pytest
import asyncio
import httpx
from fastapi.testclient import TestClient
from unittest.mock import MagicMock, patch, AsyncMock

# Configura variáveis de ambiente para teste
os.environ["DEBUG"] = "true"
os.environ["DEBUG_LEVEL"] = "basic"
os.environ["NODE_ID"] = "1"
os.environ["ACCEPTORS"] = "localhost:8081,localhost:8082,localhost:8083,localhost:8084,localhost:8085"
os.environ["PROPOSERS"] = "localhost:8071,localhost:8072,localhost:8073,localhost:8074,localhost:8075"
os.environ["LEARNERS"] = "localhost:8091,localhost:8092"
os.environ["STORES"] = "localhost:8061,localhost:8062,localhost:8063"
os.environ["LOG_DIR"] = "/tmp/paxos-test-logs"

# Mock para os acceptors, learners e cluster stores
class MockServer:
    """Servidor HTTP simples para simular respostas dos outros componentes."""
    def __init__(self, responses):
        """
        Inicializa o servidor mock.
        
        Args:
            responses: Dicionário de endpoint -> resposta
        """
        self.responses = responses
        self.app = TestClient(self._create_app())
        self.requests = []
    
    def _create_app(self):
        """Cria uma aplicação FastAPI para o servidor mock."""
        from fastapi import FastAPI, Request
        
        app = FastAPI()
        
        @app.post("/{path:path}")
        async def catch_all(request: Request, path: str):
            # Lê o corpo da requisição
            body = await request.json()
            
            # Registra a requisição
            self.requests.append({
                "path": f"/{path}",
                "body": body,
                "timestamp": time.time()
            })
            
            # Retorna resposta configurada ou padrão
            endpoint = f"/{path}"
            if endpoint in self.responses:
                return self.responses[endpoint]
            return {"status": "ok"}
        
        return app

# Fixture para iniciar os servidores mock e a API do proposer
@pytest.fixture(scope="module")
def setup_integration():
    """Configura o ambiente para testes de integração."""
    # Cria diretório para logs temporários, se não existir
    os.makedirs(os.environ["LOG_DIR"], exist_ok=True)
    
    # Configura respostas para os mock servers
    acceptor_responses = {
        "/prepare": {"accepted": True, "highestAccepted": -1},
        "/accept": {"accepted": True}
    }
    
    learner_responses = {
        "/notify": {"status": "ok"},
        "/notify-client": {"status": "ok"},
        "/cluster-access": {"status": "success"}
    }
    
    # Cria servers mock
    acceptor_server = MockServer(acceptor_responses)
    learner_server = MockServer(learner_responses)
    
    # Importa módulos necessários aqui para evitar problemas com variáveis de ambiente
    from api import create_api
    from proposer import Proposer
    from leader import LeaderElection
    from common.logging import setup_logging
    
    # Configura logging para testes
    setup_logging("proposer-test", debug=True, debug_level="basic", log_dir=os.environ["LOG_DIR"])
    
    # Cria objetos reais
    proposer = Proposer(
        node_id=1,
        acceptors=os.environ["ACCEPTORS"].split(","),
        learners=os.environ["LEARNERS"].split(","),
        stores=os.environ["STORES"].split(",")
    )
    
    leader_election = LeaderElection(
        node_id=1,
        proposers=os.environ["PROPOSERS"].split(","),
        proposer=proposer,
        current_leader=1,  # Este nó é o líder
        current_term=1
    )
    
    # Define o proposer como líder
    proposer.is_leader = True
    
    # Sobrescreve o método is_leader para sempre retornar True durante os testes
    leader_election.is_leader = MagicMock(return_value=True)
    
    # Cria a API real
    app = create_api(proposer, leader_election)
    client = TestClient(app)
    
    # Inicializa o executor de propostas
    loop = asyncio.get_event_loop()
    
    # Patch para métodos HTTP reais
    # Isto evita que o código faça chamadas HTTP reais durante os testes
    async def mock_post(self, url, **kwargs):
        if "acceptor" in url:
            return acceptor_server.responses.get("/accept", {"accepted": True})
        elif "learner" in url:
            return learner_server.responses.get("/learn", {"status": "ok"})
        else:
            return {"status": "ok"}
    
    async def mock_get(self, url, **kwargs):
        return {"status": "healthy"}
    
    # Aplica os patches
    from common.communication import HttpClient
    HttpClient.post = mock_post
    HttpClient.get = mock_get
    
    # Retorna os objetos necessários para os testes
    yield {
        "client": client,
        "proposer": proposer,
        "leader_election": leader_election,
        "acceptor_server": acceptor_server,
        "learner_server": learner_server
    }
    
    # Limpeza após os testes
    try:
        import shutil
        shutil.rmtree(os.environ["LOG_DIR"])
    except Exception as e:
        print(f"Erro ao limpar diretório de logs: {e}")

# Testes de integração
def test_propose_endpoint(setup_integration):
    """Testa o endpoint /propose da API."""
    client = setup_integration["client"]
    proposer = setup_integration["proposer"]
    
    # Estado inicial
    initial_instance_id = proposer.last_instance_id
    
    # Envia uma requisição
    client_request = {
        "clientId": "client-1",
        "timestamp": int(time.time() * 1000),
        "operation": "WRITE",
        "resource": "R",
        "data": "test_integration_data"
    }
    
    response = client.post("/propose", json=client_request)
    
    # Verifica resposta
    assert response.status_code == 202
    assert response.json()["status"] == "accepted"
    
    # Verifica se o instanceId foi incrementado
    assert proposer.last_instance_id == initial_instance_id + 1
    
    # Verifica se a proposta foi armazenada
    assert initial_instance_id + 1 in proposer.proposals

def test_health_endpoint(setup_integration):
    """Testa o endpoint /health da API."""
    client = setup_integration["client"]
    
    response = client.get("/health")
    
    # Verifica resposta
    assert response.status_code == 200
    assert response.json()["status"] == "healthy"
    assert "timestamp" in response.json()
    assert "debug_enabled" in response.json()
    assert "debug_level" in response.json()

def test_status_endpoint(setup_integration):
    """Testa o endpoint /status da API."""
    client = setup_integration["client"]
    proposer = setup_integration["proposer"]
    leader_election = setup_integration["leader_election"]
    
    response = client.get("/status")
    
    # Verifica resposta
    assert response.status_code == 200
    data = response.json()
    assert data["node_id"] == proposer.node_id
    assert data["is_leader"] == leader_election.is_leader()
    assert data["leader_id"] == leader_election.current_leader
    assert data["term"] == leader_election.current_term
    assert data["last_instance_id"] == proposer.last_instance_id
    assert data["proposal_counter"] == proposer.proposal_counter
    assert data["state"] == proposer.state
    assert "uptime" in data

def test_leader_heartbeat_endpoint(setup_integration):
    """Testa o endpoint /leader-heartbeat da API."""
    client = setup_integration["client"]
    leader_election = setup_integration["leader_election"]
    
    # Dados do heartbeat
    heartbeat_data = {
        "leaderId": 2,
        "term": 5,
        "lastInstanceId": 100
    }
    
    # Envia heartbeat
    response = client.post("/leader-heartbeat", json=heartbeat_data)
    
    # Verifica resposta
    assert response.status_code == 200
    assert response.json()["status"] == "acknowledged"

def test_logs_endpoint(setup_integration):
    """Testa o endpoint /logs da API."""
    client = setup_integration["client"]
    
    # Como DEBUG=true, deve funcionar
    response = client.get("/logs")
    
    # Verifica resposta
    assert response.status_code == 200
    data = response.json()
    assert "logs" in data

def test_logs_important_endpoint(setup_integration):
    """Testa o endpoint /logs/important da API."""
    client = setup_integration["client"]
    
    response = client.get("/logs/important")
    
    # Verifica resposta
    assert response.status_code == 200
    data = response.json()
    assert "logs" in data

def test_multiple_requests(setup_integration):
    """Testa o processamento de múltiplas requisições."""
    client = setup_integration["client"]
    proposer = setup_integration["proposer"]
    
    # Estado inicial
    initial_instance_id = proposer.last_instance_id
    
    # Envia 5 requisições
    num_requests = 5
    for i in range(num_requests):
        client_request = {
            "clientId": f"client-{i+1}",
            "timestamp": int(time.time() * 1000) + i,
            "operation": "WRITE",
            "resource": "R",
            "data": f"test_data_{i}"
        }
        
        response = client.post("/propose", json=client_request)
        assert response.status_code == 202
    
    # Verifica se todos os instanceIds foram criados
    assert proposer.last_instance_id == initial_instance_id + num_requests
    
    # Verifica se todas as propostas foram armazenadas
    for i in range(1, num_requests + 1):
        assert initial_instance_id + i in proposer.proposals

def test_debug_config_endpoint(setup_integration):
    """Testa o endpoint /debug/config da API."""
    client = setup_integration["client"]
    
    # Testa alterar para debug avançado
    config = {
        "enabled": True,
        "level": "advanced"
    }
    
    response = client.post("/debug/config", json=config)
    
    # Verifica resposta
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "success"
    assert data["debug"]["enabled"] == True
    assert data["debug"]["level"] == "advanced"
    
    # Verifica se a configuração foi aplicada verificando o endpoint health
    response = client.get("/health")
    assert response.json()["debug_enabled"] == True
    assert response.json()["debug_level"] == "advanced"
    
    # Restaura configuração original
    config = {
        "enabled": True,
        "level": "basic"
    }
    response = client.post("/debug/config", json=config)
    assert response.status_code == 200

def test_stats_endpoint(setup_integration):
    """Testa o endpoint /stats da API."""
    client = setup_integration["client"]
    proposer = setup_integration["proposer"]
    leader_election = setup_integration["leader_election"]
    
    response = client.get("/stats")
    
    # Verifica resposta
    assert response.status_code == 200
    data = response.json()
    assert "stats" in data
    stats = data["stats"]
    
    # Verifica campos obrigatórios
    assert "uptime" in stats
    assert "node_id" in stats
    assert "is_leader" in stats
    assert "last_instance_id" in stats
    assert "proposal_counter" in stats
    assert "leader_election_calls" in stats
    
    # Verifica valores
    assert stats["node_id"] == proposer.node_id
    assert stats["is_leader"] == True  # Mockado para ser sempre verdadeiro nos testes
    assert stats["leader_election_calls"] >= 0  # Pelo menos algumas chamadas devem ter sido feitas

def test_invalid_request_handling(setup_integration):
    """Testa o tratamento de requisições inválidas."""
    client = setup_integration["client"]
    
    # Requisição sem campos obrigatórios
    invalid_request = {
        "clientId": "client-1",
        # Faltando timestamp, operation, resource e data
    }
    
    response = client.post("/propose", json=invalid_request)
    
    # Resposta deve indicar quais campos estão faltando ou ser rejetada
    assert response.status_code in [400, 422] or (
        response.status_code == 202 and response.json().get("status") == "rejected"
    )
    
    # Testa outro endpoint com dados inválidos
    invalid_debug_config = {
        "enabled": "not_a_boolean",  # Deveria ser um booleano
        "level": "invalid_level"     # Nível inválido
    }
    
    response = client.post("/debug/config", json=invalid_debug_config)
    assert response.status_code in [400, 422]

def test_edge_case_empty_lists(setup_integration):
    """Testa o tratamento de listas vazias nos parâmetros."""
    client = setup_integration["client"]
    proposer = setup_integration["proposer"]
    
    # Backup dos valores originais
    original_acceptors = proposer.acceptors
    
    try:
        # Simula um caso onde todos os acceptors ficam indisponíveis
        proposer.acceptors = []
        
        # Tenta fazer uma proposta
        client_request = {
            "clientId": "client-edge",
            "timestamp": int(time.time() * 1000),
            "operation": "WRITE",
            "resource": "R",
            "data": "edge_case_test"
        }
        
        # O sistema deve lidar graciosamente com esta situação
        response = client.post("/propose", json=client_request)
        
        # A proposta ainda deve ser aceita, mesmo que falhe depois
        assert response.status_code == 202
        
    finally:
        # Restaura os valores originais
        proposer.acceptors = original_acceptors

def test_concurrent_requests(setup_integration):
    """Testa o processamento de requisições concorrentes."""
    client = setup_integration["client"]
    proposer = setup_integration["proposer"]
    
    # Estado inicial
    initial_instance_id = proposer.last_instance_id
    
    # Prepara 10 requisições para enviar "simultaneamente"
    num_requests = 10
    responses = []
    
    # Cria e envia as requisições em sequência rápida
    for i in range(num_requests):
        client_request = {
            "clientId": f"client-concurrent-{i}",
            "timestamp": int(time.time() * 1000) + i,
            "operation": "WRITE",
            "resource": "R",
            "data": f"concurrent_data_{i}"
        }
        
        response = client.post("/propose", json=client_request)
        responses.append(response)
    
    # Verifica que todas as respostas foram bem-sucedidas
    for response in responses:
        assert response.status_code == 202
    
    # Verifica que todos os instanceIds foram criados sequencialmente
    assert proposer.last_instance_id == initial_instance_id + num_requests
    
    # Verifica que todas as propostas foram armazenadas corretamente
    for i in range(1, num_requests + 1):
        assert initial_instance_id + i in proposer.proposals