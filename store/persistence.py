"""
Persistência de dados para o componente Cluster Store.
"""
import os
import json
import aiofiles
import logging
from typing import Dict, Any, Optional

from common.models import ResourceData
from store.config import (
    DATA_DIR,
    RESOURCE_FILE,
    PENDING_TXN_FILE,
    NODE_ID
)


async def ensure_data_dir():
    """
    Garante que o diretório de dados exista.
    """
    os.makedirs(DATA_DIR, exist_ok=True)


async def load_resource(resource_id: str = "R") -> ResourceData:
    """
    Carrega os dados do recurso do disco.
    
    Args:
        resource_id: ID do recurso.
        
    Returns:
        Dados do recurso.
    """
    await ensure_data_dir()
    
    # Formato padrão para um novo recurso
    default_resource = ResourceData(
        data="",
        version=0,
        timestamp=0,
        node_id=NODE_ID
    )
    
    try:
        resource_file = RESOURCE_FILE.replace("resource_r", f"resource_{resource_id.lower()}")
        
        if os.path.exists(resource_file):
            async with aiofiles.open(resource_file, 'r') as f:
                data = await f.read()
                return ResourceData(**json.loads(data))
    except Exception as e:
        logging.error(f"Erro ao carregar recurso {resource_id}: {e}")
    
    return default_resource


async def save_resource(resource: ResourceData, resource_id: str = "R"):
    """
    Salva os dados do recurso no disco.
    
    Args:
        resource: Dados do recurso.
        resource_id: ID do recurso.
    """
    await ensure_data_dir()
    
    try:
        resource_file = RESOURCE_FILE.replace("resource_r", f"resource_{resource_id.lower()}")
        
        async with aiofiles.open(resource_file, 'w') as f:
            await f.write(json.dumps(resource.dict()))
    except Exception as e:
        logging.error(f"Erro ao salvar recurso {resource_id}: {e}")


async def load_pending_transactions() -> Dict[str, Dict[str, Any]]:
    """
    Carrega as transações pendentes do disco.
    
    Returns:
        Dicionário de transações pendentes.
    """
    await ensure_data_dir()
    
    try:
        if os.path.exists(PENDING_TXN_FILE):
            async with aiofiles.open(PENDING_TXN_FILE, 'r') as f:
                data = await f.read()
                return json.loads(data)
    except Exception as e:
        logging.error(f"Erro ao carregar transações pendentes: {e}")
    
    return {}


async def save_pending_transactions(transactions: Dict[str, Dict[str, Any]]):
    """
    Salva as transações pendentes no disco.
    
    Args:
        transactions: Dicionário de transações pendentes.
    """
    await ensure_data_dir()
    
    try:
        async with aiofiles.open(PENDING_TXN_FILE, 'w') as f:
            await f.write(json.dumps(transactions))
    except Exception as e:
        logging.error(f"Erro ao salvar transações pendentes: {e}")