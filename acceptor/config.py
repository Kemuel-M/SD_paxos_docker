"""
Configurações para o componente Acceptor.
"""
import os
from common.utils import get_env_int, get_env_str, get_env_float


# Informações do nó
NODE_ID = get_env_int("NODE_ID", 1)
NODE_ROLE = get_env_str("NODE_ROLE", "acceptor")

# Configurações do servidor
HOST = get_env_str("HOST", "0.0.0.0")
PORT = get_env_int("PORT", 8080)

# Configurações de heartbeat
HEARTBEAT_INTERVAL = get_env_float("HEARTBEAT_INTERVAL", 2.0)  # 2 segundos
HEARTBEAT_TIMEOUT = get_env_float("HEARTBEAT_TIMEOUT", 0.5)  # 500ms

# Caminhos de persistência
DATA_DIR = get_env_str("DATA_DIR", "/app/data")
PROMISES_FILE = f"{DATA_DIR}/promises.json"
ACCEPTED_FILE = f"{DATA_DIR}/accepted.json"

# Endpoints para outros componentes
LEARNER_ENDPOINTS = [
    f"http://learner-{i}:8080" for i in range(1, 3)
]