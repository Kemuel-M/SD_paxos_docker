"""
Utilidades para o componente Cliente.
"""
import random
import json
import time
from typing import Dict, Any, Optional

from client.config import LOREM_IPSUM


def generate_random_content() -> str:
    """
    Gera conteúdo aleatório para o recurso.
    
    Returns:
        Conteúdo aleatório.
    """
    # Selecionar um número aleatório de frases
    num_sentences = random.randint(1, 5)
    
    # Selecionar frases aleatórias
    sentences = random.sample(LOREM_IPSUM, num_sentences)
    
    # Combinar frases em um único texto
    return " ".join(sentences)


def get_current_timestamp() -> int:
    """
    Obtém o timestamp atual em milissegundos.
    
    Returns:
        Timestamp atual em milissegundos.
    """
    return int(time.time() * 1000)


def get_random_wait_time(min_seconds: float, max_seconds: float) -> float:
    """
    Gera um tempo de espera aleatório entre min_seconds e max_seconds.
    
    Args:
        min_seconds: Tempo mínimo em segundos.
        max_seconds: Tempo máximo em segundos.
        
    Returns:
        Tempo de espera aleatório em segundos.
    """
    return round(random.uniform(min_seconds, max_seconds), 3)


def log_message(message: str, data: Optional[Dict[str, Any]] = None):
    """
    Loga uma mensagem no formato JSON.
    
    Args:
        message: Mensagem a ser logada.
        data: Dados adicionais.
    """
    log_object = {
        "timestamp": get_current_timestamp(),
        "message": message
    }
    
    if data:
        log_object["data"] = data
        
    print(json.dumps(log_object))