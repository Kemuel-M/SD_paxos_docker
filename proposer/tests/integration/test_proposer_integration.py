"""
Testes de integração para o componente Proposer.
"""
import os
import json
import time
import pytest
import asyncio
import httpx
from fastapi.testclient import TestClient

# Configura variáveis de ambiente para teste
os.environ["DEBUG"] = "true"
os.environ["NODE_ID"] = "1"
os.environ["ACCEPTORS"] = "localhost:8081,localhost:8082,localhost:8083,localhost:8084,localhost:8085"
os.environ["PROPOSERS"] = "localhost:8071,localhost:8072,localhost:8073,localhost:8074,localhost:8075"
os.environ["LEARNERS"] = "localhost:8091,localhost:8092"
os.environ["STORES"] = "localhost:8061,localhost:8062,localhost:8063"

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
    # Configura respostas para os mock servers
    acceptor_responses = {
        "/prepare": {"accepted": True, "highestAccepted": -1},
        "/accept": {"accepted": True}
    }
    
    learner_responses = {
        "/notify": {"status": "ok"}
    }
    
    # Cria servers mock
    acceptor_server = MockServer(acceptor_responses)
    learner_server = MockServer(learner_responses)
    
    # Importa módulos necessários aqui para evitar problemas com variáveis de ambiente
    from api import create_api
    from proposer import Proposer
    from leader import LeaderElection
    
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
    proposer.set_leader(True)
    
    # Cria a API real
    app = create_api(proposer, leader_election)
    client = TestClient(app)
    
    # Inicializa o executor de propostas
    loop = asyncio.get_event_loop()
    loop.run_until_complete(proposer.start())
    
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
    loop.run_until_complete(proposer.stop())

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
    
    # Espera um pouco para o processamento assíncrono
    time.sleep(0.5)
    
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
    
    # Verifica se o líder foi atualizado
    assert leader_election.current_leader == 2
    assert leader_election.current_term == 5

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
    
    # Espera um pouco para o processamento assíncrono
    time.sleep(1.0)
    
    # Verifica se todos os instanceIds foram criados
    assert proposer.last_instance_id == initial_instance_id + num_requests
    
    # Verifica se todas as propostas foram armazenadas
    for i in range(1, num_requests + 1):
        assert initial_instance_id + i in proposer.proposals