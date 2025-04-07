"""
File: cluster_store/main.py
Ponto de entrada principal para o componente Cluster Store.
"""
import os
import sys
import json
import asyncio
import logging
import signal
import uvicorn
from typing import List, Dict, Any

# Adiciona diretório atual e parent ao path para imports relativos
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.logging import setup_logging
from common.heartbeat import HeartbeatMonitor, HeartbeatSender
from common.communication import HttpClient

from persistence import ResourceStorage
from store import ResourceManager
from synchronization import SynchronizationManager
from api import create_api

# Configurações a partir de variáveis de ambiente
NODE_ID = int(os.getenv("NODE_ID", "1"))
PORT = int(os.getenv("PORT", "8080"))
DEBUG = os.getenv("DEBUG", "false").lower() in ("true", "1", "yes")
DEBUG_LEVEL = os.getenv("DEBUG_LEVEL", "basic").lower()
LOG_DIR = os.getenv("LOG_DIR", "/data/logs")
DATA_DIR = os.getenv("DATA_DIR", "/data/resources")

# Obtém lista de outros nós do Cluster Store
STORES = os.getenv("STORES", "").split(",")

# Configurar logger
logger = None

async def shutdown(app, loop):
    """
    Desliga graciosamente o serviço.
    
    Args:
        app: Aplicação FastAPI
        loop: Event loop do asyncio
    """
    logger.important(f"Cluster Store #{NODE_ID} está sendo desligado...")
    
    # Para o SynchronizationManager
    if hasattr(app, "state") and hasattr(app.state, "sync_manager"):
        logger.info("Parando gerenciador de sincronização...")
        await app.state.sync_manager.stop()
    
    # Para o HeartbeatSender
    if hasattr(app, "state") and hasattr(app.state, "heartbeat_sender"):
        logger.info("Parando emissor de heartbeat...")
        app.state.heartbeat_sender.stop()
    
    # Para o HeartbeatMonitor
    if hasattr(app, "state") and hasattr(app.state, "heartbeat_monitor"):
        logger.info("Parando monitor de heartbeat...")
        app.state.heartbeat_monitor.stop()
    
    # Fecha conexões HTTP
    if hasattr(app, "state") and hasattr(app.state, "http_client"):
        logger.info("Fechando conexões HTTP...")
        await app.state.http_client.close()
    
    logger.important(f"Cluster Store #{NODE_ID} desligado com sucesso.")

def main():
    # Configuração global do logging
    global logger
    logger = setup_logging(f"store-{NODE_ID}", debug=DEBUG, debug_level=DEBUG_LEVEL, log_dir=LOG_DIR)
    
    # Inicializa diretório de dados
    os.makedirs(DATA_DIR, exist_ok=True)
    
    logger.important(f"Iniciando Cluster Store #{NODE_ID}...")
    logger.info(f"Configurações: PORT={PORT}, DEBUG={DEBUG}, DEBUG_LEVEL={DEBUG_LEVEL}")
    logger.info(f"Diretórios: LOG_DIR={LOG_DIR}, DATA_DIR={DATA_DIR}")
    
    # Inicializa componentes
    storage = ResourceStorage(NODE_ID, data_dir=DATA_DIR)
    resource_manager = ResourceManager(NODE_ID, storage)
    
    # Inicializa cliente HTTP
    http_client = HttpClient(timeout=5.0, max_connections=100)
    
    # Cria a aplicação FastAPI
    from api import create_api
    app = create_api(resource_manager, None, NODE_ID)  # SyncManager será adicionado depois
    
    # Armazena componentes no estado da aplicação
    app.state.storage = storage
    app.state.resource_manager = resource_manager
    app.state.http_client = http_client
    
    # Manipulador para shutdown gracioso
    async def on_shutdown():
        await shutdown(app, asyncio.get_event_loop())
    
    # Adiciona evento de shutdown
    app.add_event_handler("shutdown", on_shutdown)
    
    # Inicializa lista completa de nós do Cluster Store
    all_stores = []
    for store in STORES:
        if store:  # Ignora entradas vazias
            all_stores.append(store)
    
    logger.info(f"Nós do Cluster Store: {all_stores}")
    
    # Inicializa SynchronizationManager
    sync_manager = SynchronizationManager(NODE_ID, all_stores, resource_manager)
    app.state.sync_manager = sync_manager
    
    # Atualiza a aplicação com o SynchronizationManager
    app = create_api(resource_manager, sync_manager, NODE_ID)
    
    # Inicializa HeartbeatSender
    heartbeat_sender = HeartbeatSender(f"store-{NODE_ID}")
    app.state.heartbeat_sender = heartbeat_sender
    
    # Adiciona outros nós do Cluster Store como alvos do HeartbeatSender
    for store in STORES:
        if store and not store.startswith(f"cluster-store-{NODE_ID}:"):
            async def send_heartbeat(store=store):
                try:
                    url = f"http://{store}/health"
                    await http_client.get(url, timeout=0.5)
                except Exception as e:
                    logger.debug(f"Falha ao enviar heartbeat para {store}: {e}")
            
            heartbeat_sender.add_target(store, send_heartbeat)
    
    # Inicializa HeartbeatMonitor para detectar falhas de outros nós
    heartbeat_monitor = HeartbeatMonitor(f"Cluster Store #{NODE_ID}")
    app.state.heartbeat_monitor = heartbeat_monitor
    
    # Handler de sinal para shutdown gracioso
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda sig, frame: asyncio.create_task(shutdown(app, asyncio.get_event_loop())))
    
    # Função para inicializar componentes assíncronos
    async def startup():
        logger.info("Iniciando componentes assíncronos...")
        
        # Inicia HeartbeatSender
        heartbeat_sender.start()
        
        # Inicia HeartbeatMonitor
        heartbeat_monitor.start()
        
        # Inicia SynchronizationManager
        await sync_manager.start()
        
        logger.info("Componentes assíncronos iniciados com sucesso.")
    
    # Executa inicialização assíncrona
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(startup())
    
    # Inicia o servidor HTTP
    logger.important(f"Cluster Store #{NODE_ID} iniciado e escutando na porta {PORT}")
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")

if __name__ == "__main__":
    main()