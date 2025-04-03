"""
Configurações para o componente Cluster Store.
"""
import os
from common.utils import get_env_int, get_env_str, get_env_float


# Informações do nó
NODE_ID = get_env_int("NODE_ID", 1)
NODE_ROLE = get_env_str("NODE_ROLE", "store")

# Configurações do servidor
HOST = get_env_str("HOST", "0.0.0.0")
PORT = get_env_int("PORT", 8080)

# Configurações de heartbeat
HEARTBEAT_INTERVAL = get_env_float("HEARTBEAT_INTERVAL", 2.0)  # 2 segundos
HEARTBEAT_TIMEOUT = get_env_float("HEARTBEAT_TIMEOUT", 0.5)  # 500ms

# Caminhos de persistência
DATA_DIR = get_env_str("DATA_DIR", "/app/data")
RESOURCE_FILE = f"{DATA_DIR}/resource_r.json"
PENDING_TXN_FILE = f"{DATA_DIR}/pending_transactions.json"

# Endpoints para outros componentes do Cluster Store
STORE_ENDPOINTS = [
    f"http://store-{i}:8080" for i in range(1, 4) if i != NODE_ID
]

# Configurações de sincronização
SYNC_INTERVAL = get_env_float("SYNC_INTERVAL", 10.0)  # 10 segundos
LOCK_TIMEOUT = get_env_float("LOCK_TIMEOUT", 10.0)  # 10 segundos