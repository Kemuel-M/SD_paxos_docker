#!/usr/bin/env python3
"""
File: client/main.py
Ponto de entrada da aplicação Cliente.
Responsável por inicializar o cliente Paxos e o servidor web.
"""
import os
import sys
import logging
import asyncio
import signal
import threading
import uvicorn
from fastapi import FastAPI

# Adiciona diretório atual ao PYTHONPATH
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # Para common

from api import create_api
from client import PaxosClient
from web_server import WebServer
from common.logging import setup_logging

# Carrega configurações
CLIENT_ID = os.getenv("CLIENT_ID", "client-1")
NODE_ID = int(os.getenv("NODE_ID", CLIENT_ID.split("-")[1]))
API_PORT = int(os.getenv("API_PORT", 8080))
WEB_PORT = int(os.getenv("WEB_PORT", 8081))
DEBUG = os.getenv("DEBUG", "false").lower() in ("true", "1", "yes")
DEBUG_LEVEL = os.getenv("DEBUG_LEVEL", "basic").lower()  # Níveis: basic, advanced, trace
PROPOSER = os.getenv("PROPOSER", f"proposer-{NODE_ID}:8080")
NUM_OPERATIONS = int(os.getenv("NUM_OPERATIONS", "0"))  # 0 para aleatório
AUTO_START = os.getenv("AUTO_START", "false").lower() in ("true", "1", "yes")
LOG_DIR = os.getenv("LOG_DIR", "/data/logs")

logger = logging.getLogger("client")

# Variáveis globais
client = None
api_app = None
web_server = None
api_server = None
should_exit = threading.Event()

async def shutdown():
    """Função para desligar graciosamente o serviço"""
    logger.important(f"Cliente {CLIENT_ID} está sendo desligado...")
    
    global client, api_server
    
    # Para o cliente
    if client:
        await client.stop()
        logger.info("Cliente parado")
    
    # Para o servidor web (via signal para o uvicorn)
    if api_server:
        api_server.should_exit = True
        logger.info("Servidor API sinalizado para parar")
    
    logger.important(f"Cliente {CLIENT_ID} desligado com sucesso.")

def start_api_server():
    """Inicia o servidor da API em uma thread separada."""
    global api_app, api_server
    
    # Configura o servidor uvicorn
    config = uvicorn.Config(api_app, host="0.0.0.0", port=API_PORT)
    api_server = uvicorn.Server(config)
    
    # Inicia o servidor (bloqueante)
    api_server.run()
    
    logger.info(f"Servidor API encerrado")

def main():
    # Configura o logger com suporte a níveis de debug
    setup_logging(f"client-{NODE_ID}", debug=DEBUG, debug_level=DEBUG_LEVEL, log_dir=LOG_DIR)
    logger.important(f"Iniciando Cliente {CLIENT_ID}...")
    
    if DEBUG:
        logger.info(f"Modo DEBUG ativado com nível: {DEBUG_LEVEL}")
        logger.info(f"Configuração: API_PORT={API_PORT}, WEB_PORT={WEB_PORT}, PROPOSER={PROPOSER}")
    
    # Determina URL do proposer baseado na variável de ambiente
    proposer_url = f"http://{PROPOSER}"
    
    # Determina URL de callback baseado no ID do cliente
    callback_url = f"http://{CLIENT_ID}:{API_PORT}/notification"
    
    # Cria o cliente
    global client
    client = PaxosClient(
        client_id=CLIENT_ID,
        proposer_url=proposer_url,
        callback_url=callback_url,
        num_operations=NUM_OPERATIONS if NUM_OPERATIONS > 0 else None
    )
    
    # Cria a API
    global api_app
    api_app = create_api(client)
    
    # Configura callback para shutdown gracioso
    @api_app.on_event("shutdown")
    async def on_api_shutdown():
        logger.info("Evento de shutdown da API detectado")
    
    # Configura manipuladores de sinal para shutdown gracioso
    loop = asyncio.get_event_loop()
    signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
    for s in signals:
        loop.add_signal_handler(
            s, lambda s=s: asyncio.create_task(shutdown())
        )
    
    # Inicia o servidor API em uma thread separada
    api_thread = threading.Thread(target=start_api_server, daemon=True)
    api_thread.start()
    logger.info(f"Servidor API iniciado em 0.0.0.0:{API_PORT}")
    
    # Inicia o servidor web
    web_server = WebServer(client, port=WEB_PORT)
    
    # Inicia cliente automaticamente se configurado
    if AUTO_START:
        asyncio.run_coroutine_threadsafe(client.start(), loop)
        logger.info("Cliente iniciado automaticamente")
    
    # Inicia o servidor web (bloqueante)
    try:
        logger.important(f"Cliente {CLIENT_ID} iniciado e servidores escutando nas portas API:{API_PORT}, Web:{WEB_PORT}")
        web_server.start()
    except KeyboardInterrupt:
        logger.info("Interrupção de teclado detectada")
    finally:
        # Executa shutdown antes de encerrar
        asyncio.run(shutdown())

if __name__ == "__main__":
    main()