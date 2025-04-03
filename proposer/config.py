"""
Configurações para o componente Proposer.
"""
import os
from common.utils import get_env_int, get_env_str, get_env_float


# Informações do nó
NODE_ID = get_env_int("NODE_ID", 1)
NODE_ROLE = get_env_str("NODE_ROLE", "proposer")

# Configurações do servidor
HOST = get_env_str("HOST", "0.0.0.0")
PORT = get_env_int("PORT", 8080)

# Configurações do Paxos
QUORUM_SIZE = get_env_int("QUORUM_SIZE", 3)
MAX_INSTANCE_ID = 2**31 - 1  # Máximo valor para instanceId

# Configurações de liderança
LEADER_HEARTBEAT_INTERVAL = get_env_float("LEADER_HEARTBEAT_INTERVAL", 1.0)  # 1 segundo
LEADER_TIMEOUT = get_env_float("LEADER_TIMEOUT", 5.0)  # 5 segundos

# Configurações de acesso ao recurso
RESOURCE_ACCESS_MIN_TIME = get_env_float("RESOURCE_ACCESS_MIN_TIME", 0.2)  # 0.2 segundos
RESOURCE_ACCESS_MAX_TIME = get_env_float("RESOURCE_ACCESS_MAX_TIME", 1.0)  # 1.0 segundos

# Configurações de heartbeat
HEARTBEAT_INTERVAL = get_env_float("HEARTBEAT_INTERVAL", 2.0)  # 2 segundos
HEARTBEAT_TIMEOUT = get_env_float("HEARTBEAT_TIMEOUT", 0.5)  # 500ms
HEARTBEAT_FAILURE_THRESHOLD = get_env_int("HEARTBEAT_FAILURE_THRESHOLD", 3)  # 3 falhas consecutivas

# Caminhos de persistência
DATA_DIR = get_env_str("DATA_DIR", "/app/data")
PROPOSAL_COUNTER_FILE = f"{DATA_DIR}/proposal_counter.json"
INSTANCE_COUNTER_FILE = f"{DATA_DIR}/instance_counter.json"
LEADERSHIP_FILE = f"{DATA_DIR}/leadership.json"

# Endpoints para outros componentes
ACCEPTOR_ENDPOINTS = [
    f"http://acceptor-{i}:8080" for i in range(1, 6)
]

LEARNER_ENDPOINTS = [
    f"http://learner-{i}:8080" for i in range(1, 3)
]

PROPOSER_ENDPOINTS = [
    f"http://proposer-{i}:8080" for i in range(1, 6)
]

# Excluir o próprio endpoint da lista de proposers
PROPOSER_ENDPOINTS.remove(f"http://proposer-{NODE_ID}:8080")