"""
Persistência de dados para o componente Acceptor.
"""
import os
import json
import aiofiles
import logging
from typing import Dict, Any, Optional

from acceptor.config import (
    DATA_DIR,
    PROMISES_FILE,
    ACCEPTED_FILE
)


async def ensure_data_dir():
    """
    Garante que o diretório de dados exista.
    """
    os.makedirs(DATA_DIR, exist_ok=True)


async def load_promises() -> Dict[str, Dict[str, Any]]:
    """
    Carrega as promessas do disco.
    
    Returns:
        Dicionário de promessas na forma {instance_id: {highest_promised, last_promise_timestamp}}.
    """
    await ensure_data_dir()
    
    try:
        if os.path.exists(PROMISES_FILE):
            async with aiofiles.open(PROMISES_FILE, 'r') as f:
                data = await f.read()
                return json.loads(data)
    except Exception as e:
        logging.error(f"Erro ao carregar promessas: {e}")
    
    return {}


async def save_promises(promises: Dict[str, Dict[str, Any]]):
    """
    Salva as promessas no disco.
    
    Args:
        promises: Dicionário de promessas.
    """
    await ensure_data_dir()
    
    try:
        async with aiofiles.open(PROMISES_FILE, 'w') as f:
            await f.write(json.dumps(promises))
    except Exception as e:
        logging.error(f"Erro ao salvar promessas: {e}")


async def load_accepted() -> Dict[str, Dict[str, Any]]:
    """
    Carrega os valores aceitos do disco.
    
    Returns:
        Dicionário de valores aceitos na forma {instance_id: {accepted_proposal_number, accepted_value, accept_timestamp}}.
    """
    await ensure_data_dir()
    
    try:
        if os.path.exists(ACCEPTED_FILE):
            async with aiofiles.open(ACCEPTED_FILE, 'r') as f:
                data = await f.read()
                return json.loads(data)
    except Exception as e:
        logging.error(f"Erro ao carregar valores aceitos: {e}")
    
    return {}


async def save_accepted(accepted: Dict[str, Dict[str, Any]]):
    """
    Salva os valores aceitos no disco.
    
    Args:
        accepted: Dicionário de valores aceitos.
    """
    await ensure_data_dir()
    
    try:
        async with aiofiles.open(ACCEPTED_FILE, 'w') as f:
            await f.write(json.dumps(accepted))
    except Exception as e:
        logging.error(f"Erro ao salvar valores aceitos: {e}")