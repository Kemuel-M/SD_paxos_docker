"""
Funções de utilidade geral compartilhadas entre componentes.
"""
import os
import time
import uuid
import json
import random
import asyncio
from typing import Dict, Any, List, Optional, Tuple, Union

def generate_unique_id() -> str:
    """
    Gera um ID único.
    
    Returns:
        str: ID único baseado em UUID
    """
    return str(uuid.uuid4())

def current_timestamp() -> int:
    """
    Obtém o timestamp atual em milissegundos.
    
    Returns:
        int: Timestamp atual em milissegundos
    """
    return int(time.time() * 1000)

def calculate_backoff(attempt: int, base: float = 0.5, jitter: float = 0.2) -> float:
    """
    Calcula tempo de espera com backoff exponencial e jitter.
    
    Args:
        attempt: Número da tentativa (0-based)
        base: Tempo base em segundos
        jitter: Fator de jitter (0.0-1.0)
    
    Returns:
        float: Tempo de espera em segundos
    """
    # Calcula backoff exponencial
    backoff_time = base * (2 ** attempt)
    
    # Adiciona jitter (±jitter%)
    jitter_amount = backoff_time * jitter * (2 * random.random() - 1)
    
    return max(0, backoff_time + jitter_amount)

async def wait_with_backoff(attempt: int, base: float = 0.5, jitter: float = 0.2):
    """
    Espera um tempo com backoff exponencial e jitter.
    
    Args:
        attempt: Número da tentativa (0-based)
        base: Tempo base em segundos
        jitter: Fator de jitter (0.0-1.0)
    """
    wait_time = calculate_backoff(attempt, base, jitter)
    await asyncio.sleep(wait_time)

def safe_json_loads(data: str, default: Any = None) -> Any:
    """
    Carrega JSON de forma segura.
    
    Args:
        data: String JSON
        default: Valor padrão se falhar
    
    Returns:
        Any: Objeto Python convertido do JSON ou default
    """
    try:
        return json.loads(data)
    except Exception:
        return default

def safe_json_dumps(obj: Any, default: str = "{}") -> str:
    """
    Converte objeto para JSON de forma segura.
    
    Args:
        obj: Objeto Python
        default: String padrão se falhar
    
    Returns:
        str: String JSON
    """
    try:
        return json.dumps(obj)
    except Exception:
        return default

def hash_consistent(key: str, num_buckets: int) -> int:
    """
    Implementa hashing consistente para distribuição de carga.
    
    Args:
        key: Chave a ser hasheada
        num_buckets: Número de buckets
    
    Returns:
        int: Índice do bucket (0 a num_buckets-1)
    """
    # Usa hash interno do Python, que é diferente em cada execução
    # Para consistência, precisamos usar um método fixo
    hash_value = 0
    for char in key:
        hash_value = ((hash_value * 31) + ord(char)) & 0xFFFFFFFF
    
    return hash_value % num_buckets

def parse_id_from_address(address: str) -> Optional[int]:
    """
    Extrai ID de um endereço no formato 'component-X:port'.
    
    Args:
        address: Endereço no formato 'component-X:port'
    
    Returns:
        Optional[int]: ID extraído ou None se não for possível extrair
    """
    try:
        # Exemplo: 'proposer-3:8080' -> 3
        parts = address.split(':')[0].split('-')
        if len(parts) >= 2:
            return int(parts[-1])
        return None
    except Exception:
        return None

def get_round_robin_item(items: List[Any], current_index: int) -> Tuple[Any, int]:
    """
    Seleciona próximo item em esquema round-robin.
    
    Args:
        items: Lista de itens
        current_index: Índice atual
    
    Returns:
        Tuple[Any, int]: Próximo item e novo índice
    """
    if not items:
        raise ValueError("Lista vazia")
    
    next_index = (current_index + 1) % len(items)
    return items[next_index], next_index

def deep_merge(dict1: Dict[str, Any], dict2: Dict[str, Any]) -> Dict[str, Any]:
    """
    Mescla dois dicionários profundamente.
    
    Args:
        dict1: Primeiro dicionário
        dict2: Segundo dicionário (prevalece em caso de conflito)
    
    Returns:
        Dict[str, Any]: Dicionário mesclado
    """
    result = dict1.copy()
    
    for key, value in dict2.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value
    
    return result

def parse_range(range_str: str) -> Tuple[int, int]:
    """
    Converte uma string de faixa (ex: "1-5") para tupla de inteiros.
    
    Args:
        range_str: String no formato "min-max"
    
    Returns:
        Tuple[int, int]: Tupla (min, max)
    """
    try:
        parts = range_str.split('-')
        if len(parts) == 2:
            return int(parts[0]), int(parts[1])
        return int(parts[0]), int(parts[0])
    except Exception:
        raise ValueError(f"Formato de faixa inválido: {range_str}")

def random_delay(min_delay: float, max_delay: float) -> float:
    """
    Gera um tempo de espera aleatório dentro de uma faixa.
    
    Args:
        min_delay: Tempo mínimo em segundos
        max_delay: Tempo máximo em segundos
    
    Returns:
        float: Tempo de espera em segundos
    """
    return min_delay + (random.random() * (max_delay - min_delay))

async def wait_random_delay(min_delay: float, max_delay: float):
    """
    Espera um tempo aleatório dentro de uma faixa.
    
    Args:
        min_delay: Tempo mínimo em segundos
        max_delay: Tempo máximo em segundos
    """
    delay = random_delay(min_delay, max_delay)
    await asyncio.sleep(delay)
    return delay