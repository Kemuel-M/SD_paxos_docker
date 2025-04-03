"""
Configurações para o componente Learner.
"""
import os
from common.utils import get_env_int, get_env_str, get_env_float


# Informações do nó
NODE_ID = get_env_int("NODE_ID", 1)
NODE_ROLE = get_env_str("NODE_ROLE", "learner")

# Configurações do servidor
HOST = get_env_str("HOST", "0.0.0.0")
PORT = get_env_int("PORT", 8080)

# Configuração de quórum
QUORUM_SIZE = get_env_int("QUORUM_SIZE", 3)  # Maioria dos acceptors (3 de 5)

# Configurações de heartbeat
HEARTBEAT_INTERVAL = get_env_float("HEARTBEAT_INTERVAL", 2.0)  # 2 segundos
HEARTBEAT_TIMEOUT = get_env_float("HEARTBEAT_TIMEOUT", 0.5)  # 500ms

# Configurações do Cluster Store (Parte 2)
STORE_ENDPOINTS = [
    f"http://store-{i}:8080" for i in range(1, 4)
]

# Caminhos de persistência
DATA_DIR = get_env_str("DATA_DIR", "/app/data")
LEARNED_VALUES_FILE = f"{DATA_DIR}/learned_values.json"

# Tempos de timeout para operações
CLIENT_NOTIFICATION_TIMEOUT = get_env_float("CLIENT_NOTIFICATION_TIMEOUT", 5.0)  # 5 segundos
BACKUP_LEARNER_DELAY = get_env_float("BACKUP_LEARNER_DELAY", 3.0)  # 3 segundos

# Tempos de backoff para retentativas
RETRY_BACKOFF_BASE = get_env_float("RETRY_BACKOFF_BASE", 1.0)  # 1 segundo
RETRY_BACKOFF_FACTOR = get_env_float("RETRY_BACKOFF_FACTOR", 2.0)  # Fator de backoff exponencial
RETRY_MAX_ATTEMPTS = get_env_int("RETRY_MAX_ATTEMPTS", 3)  # Máximo de 3 tentativas