"""
Configurações para o componente Cliente.
"""
import os
import random
from common.utils import get_env_int, get_env_str, get_env_float


# Informações do cliente
CLIENT_ID = get_env_str("CLIENT_ID", "client-1")
CLIENT_NUM = int(CLIENT_ID.split("-")[1]) if "-" in CLIENT_ID else 1

# Proposer designado
PROPOSER_HOST = get_env_str("PROPOSER_HOST", f"proposer-{CLIENT_NUM}")
PROPOSER_PORT = get_env_int("PROPOSER_PORT", 8080)
PROPOSER_ENDPOINT = f"http://{PROPOSER_HOST}:{PROPOSER_PORT}"

# Configurações de operações
MIN_REQUESTS = get_env_int("MIN_REQUESTS", 10)
MAX_REQUESTS = get_env_int("MAX_REQUESTS", 50)
TOTAL_REQUESTS = random.randint(MIN_REQUESTS, MAX_REQUESTS)

MIN_WAIT_TIME = get_env_float("MIN_WAIT_TIME", 1.0)  # 1.0 segundo
MAX_WAIT_TIME = get_env_float("MAX_WAIT_TIME", 5.0)  # 5.0 segundos

# Configurações de retentativa
MAX_RETRIES = get_env_int("MAX_RETRIES", 3)
REQUEST_TIMEOUT = get_env_float("REQUEST_TIMEOUT", 10.0)  # 10.0 segundos

# Dados de teste para geração de conteúdo
LOREM_IPSUM = [
    "Lorem ipsum dolor sit amet, consectetur adipiscing elit.",
    "Nullam euismod ipsum eget ligula tincidunt, vel luctus justo tincidunt.",
    "Fusce vehicula nunc at dui posuere, nec tincidunt urna fermentum.",
    "Praesent varius risus vel elementum ultricies.",
    "Cras nec turpis vel ante vestibulum ultricies id vitae ligula.",
    "Morbi id risus id quam efficitur finibus.",
    "Vivamus vitae mi sit amet lectus posuere sagittis.",
    "Mauris vel risus vel dolor tincidunt ultricies in id eros.",
    "Etiam auctor nisi at leo ultricies, vel tincidunt justo finibus.",
    "Sed vel mauris vel eros tincidunt ultricies.",
    "Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas.",
    "Nullam eget nisi eget nunc ultricies ultricies."
]