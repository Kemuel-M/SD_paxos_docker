"""
Persistência de dados para o componente Proposer.
"""
import os
import json
import aiofiles
import logging
from typing import Dict, Any, Optional

from proposer.config import (
    DATA_DIR,
    PROPOSAL_COUNTER_FILE,
    INSTANCE_COUNTER_FILE,
    LEADERSHIP_FILE
)


async def ensure_data_dir():
    """
    Garante que o diretório de dados exista.
    """
    os.makedirs(DATA_DIR, exist_ok=True)
    

async def load_proposal_counter() -> int:
    """
    Carrega o contador de propostas do disco.
    
    Returns:
        Contador de propostas.
    """
    await ensure_data_dir()
    
    try:
        if os.path.exists(PROPOSAL_COUNTER_FILE):
            async with aiofiles.open(PROPOSAL_COUNTER_FILE, 'r') as f:
                data = await f.read()
                return json.loads(data).get("counter", 0)
    except Exception as e:
        logging.error(f"Erro ao carregar contador de propostas: {e}")
    
    return 0


async def save_proposal_counter(counter: int):
    """
    Salva o contador de propostas no disco.
    
    Args:
        counter: Contador de propostas.
    """
    await ensure_data_dir()
    
    try:
        async with aiofiles.open(PROPOSAL_COUNTER_FILE, 'w') as f:
            await f.write(json.dumps({"counter": counter}))
    except Exception as e:
        logging.error(f"Erro ao salvar contador de propostas: {e}")


async def load_instance_counter() -> int:
    """
    Carrega o contador de instâncias do disco.
    
    Returns:
        Contador de instâncias.
    """
    await ensure_data_dir()
    
    try:
        if os.path.exists(INSTANCE_COUNTER_FILE):
            async with aiofiles.open(INSTANCE_COUNTER_FILE, 'r') as f:
                data = await f.read()
                return json.loads(data).get("counter", 0)
    except Exception as e:
        logging.error(f"Erro ao carregar contador de instâncias: {e}")
    
    return 0


async def save_instance_counter(counter: int):
    """
    Salva o contador de instâncias no disco.
    
    Args:
        counter: Contador de instâncias.
    """
    await ensure_data_dir()
    
    try:
        async with aiofiles.open(INSTANCE_COUNTER_FILE, 'w') as f:
            await f.write(json.dumps({"counter": counter}))
    except Exception as e:
        logging.error(f"Erro ao salvar contador de instâncias: {e}")


async def load_leadership_state() -> Dict[str, Any]:
    """
    Carrega o estado de liderança do disco.
    
    Returns:
        Estado de liderança.
    """
    await ensure_data_dir()
    
    default_state = {
        "is_leader": False,
        "current_leader_id": None,
        "term": 0,
        "last_heartbeat": 0
    }
    
    try:
        if os.path.exists(LEADERSHIP_FILE):
            async with aiofiles.open(LEADERSHIP_FILE, 'r') as f:
                data = await f.read()
                return json.loads(data)
    except Exception as e:
        logging.error(f"Erro ao carregar estado de liderança: {e}")
    
    return default_state


async def save_leadership_state(state: Dict[str, Any]):
    """
    Salva o estado de liderança no disco.
    
    Args:
        state: Estado de liderança.
    """
    await ensure_data_dir()
    
    try:
        async with aiofiles.open(LEADERSHIP_FILE, 'w') as f:
            await f.write(json.dumps(state))
    except Exception as e:
        logging.error(f"Erro ao salvar estado de liderança: {e}")