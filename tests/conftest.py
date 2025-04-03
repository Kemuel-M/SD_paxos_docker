"""
Configuração global para testes.
Contém fixtures compartilhadas entre testes unitários e de integração.
"""
def pytest_addoption(parser):
    """Adicionar opções específicas para testes de integração."""
    parser.addoption(
        "--runintegration", action="store_true", default=False, help="Executar testes de integração"
    )

import os
import pytest
import asyncio
import json
import logging
from unittest.mock import AsyncMock, MagicMock, patch

# Configurar logging para testes
logging.basicConfig(level=logging.INFO)

# Garantir que fixtures assíncronas funcionem corretamente
@pytest.fixture(scope="session")
def event_loop():
    """Criar um event loop por sessão de teste."""
    try:
        loop = asyncio.get_event_loop_policy().new_event_loop()
        yield loop
        loop.close()
    except RuntimeError:
        # Fallback para caso o loop já esteja fechado
        yield asyncio.new_event_loop()

@pytest.fixture
def mock_config():
    """
    Configuração simulada para testes.
    Usado para isolar componentes e evitar dependência de arquivos.
    """
    return {
        "node": {
            "id": 1,
            "role": "test"
        },
        "networking": {
            "port": 8080,
            "acceptors": ["acceptor-1:8080", "acceptor-2:8080", "acceptor-3:8080", "acceptor-4:8080", "acceptor-5:8080"],
            "proposers": ["proposer-1:8080", "proposer-2:8080", "proposer-3:8080", "proposer-4:8080", "proposer-5:8080"],
            "learners": ["learner-1:8080", "learner-2:8080"],
            "stores": ["store-1:8080", "store-2:8080", "store-3:8080"]
        },
        "paxos": {
            "quorumSize": 3,
            "batchSize": 10,
            "batchDelayMs": 50,
            "proposalTimeout": 2000
        },
        "store": {
            "readQuorum": 1,
            "writeQuorum": 3
        },
        "storage": {
            "path": ":memory:",  # Usar TinyDB em memória para testes
            "resourcePath": "/tmp/test-resource",
            "syncIntervalSeconds": 1
        },
        "protocol": {
            "preparationTimeout": 500,
            "commitTimeout": 1000,
            "heartbeatInterval": 1000,
            "recoveryTimeout": 2000,
            "maxRetries": 2,
            "retryBackoffMs": 100
        }
    }

@pytest.fixture
async def mock_proposer_state(mock_config):
    """Cria um estado simulado de Proposer para testes unitários."""
    # Este é um mock parcial - poderia ser expandido conforme necessário para testes específicos
    with patch("proposer.main.load_config", return_value=mock_config):
        from proposer.proposer.main import ProposerState
        state = ProposerState(mock_config["node"]["id"])
        # Substituir métodos que acessam o disco
        state.save_state = AsyncMock()
        state.load_state = MagicMock()
        yield state

@pytest.fixture
async def mock_acceptor_state(mock_config):
    """Cria um estado simulado de Acceptor para testes unitários."""
    with patch("acceptor.main.load_config", return_value=mock_config):
        from acceptor.acceptor.main import AcceptorState
        state = AcceptorState(mock_config["node"]["id"])
        # Substituir métodos que acessam o disco
        state.save_promise = AsyncMock()
        state.save_accepted = AsyncMock()
        yield state

@pytest.fixture
async def mock_learner_state(mock_config):
    """Cria um estado simulado de Learner para testes unitários."""
    with patch("learner.main.load_config", return_value=mock_config):
        from learner.learner.main import LearnerState
        state = LearnerState(mock_config["node"]["id"])
        # Substituir métodos que acessam o disco
        state.save_decision = AsyncMock()
        state.mark_as_applied = AsyncMock()
        state.save_transaction = AsyncMock()
        state.save_directory = AsyncMock()
        yield state

@pytest.fixture
async def mock_store_state(mock_config):
    """Cria um estado simulado de Store para testes unitários."""
    with patch("store.main.load_config", return_value=mock_config):
        from store.store.main import StoreState
        # Garantir que o diretório de recursos existe para testes
        os.makedirs(mock_config["storage"]["resourcePath"], exist_ok=True)
        state = StoreState(mock_config["node"]["id"], mock_config["storage"]["resourcePath"])
        # Substituir métodos que acessam o disco
        state.save_resource = AsyncMock()
        state.delete_resource = AsyncMock()
        state.save_transaction = AsyncMock()
        yield state
        # Limpar arquivos temporários após o teste
        import shutil
        shutil.rmtree(mock_config["storage"]["resourcePath"], ignore_errors=True)

# Fixtures para integração
@pytest.fixture(scope="module")
def docker_compose_file(pytestconfig):
    """Localização do arquivo docker-compose para testes de integração."""
    return os.path.join(os.path.dirname(__file__), "integration", "docker-compose.test.yml")