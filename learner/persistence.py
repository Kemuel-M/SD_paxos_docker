"""
Persistência de dados para o componente Learner.
"""
import os
import json
import aiofiles
import logging
from typing import Dict, Any, Optional

from learner.config import (
    DATA_DIR,
    LEARNED_VALUES_FILE
)


async def ensure_data_dir():
    """
    Garante que o diretório de dados exista.
    """
    os.makedirs(DATA_DIR, exist_ok=True)


async def load_learned_values() -> Dict[str, Dict[str, Any]]:
    """
    Carrega os valores aprendidos do disco.
    
    Returns:
        Dicionário de valores aprendidos na forma {instance_id: {proposal_number, value, timestamp}}.
    """
    await ensure_data_dir()
    
    try:
        if os.path.exists(LEARNED_VALUES_FILE):
            async with aiofiles.open(LEARNED_VALUES_FILE, 'r') as f:
                data = await f.read()
                return json.loads(data)
    except Exception as e:
        logging.error(f"Erro ao carregar valores aprendidos: {e}")
    
    return {}


async def save_learned_values(learned_values: Dict[str, Dict[str, Any]]):
    """
    Salva os valores aprendidos no disco.
    
    Args:
        learned_values: Dicionário de valores aprendidos.
    """
    await ensure_data_dir()
    
    try:
        async with aiofiles.open(LEARNED_VALUES_FILE, 'w') as f:
            await f.write(json.dumps(learned_values))
    except Exception as e:
        logging.error(f"Erro ao salvar valores aprendidos: {e}")