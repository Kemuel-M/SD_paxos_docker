#!/usr/bin/env python3
"""
File: proposer/main.py
Ponto de entrada da aplicação Proposer (Cluster Sync).
Responsável por inicializar o servidor HTTP e carregar as configurações.
Com suporte a múltiplos níveis de debug.
"""
import os
import sys
import logging
import asyncio
import signal
import uvicorn
from fastapi import FastAPI

# Adiciona diretório atual ao PYTHONPATH
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from api import create_api
from proposer import Proposer
from leader import LeaderElection
from persistence import ProposerPersistence
from common.logging import setup_logging

# Carrega configurações
NODE_ID = int(os.getenv("NODE_ID", 1))
PORT = int(os.getenv("PORT", 8080))
DEBUG = os.getenv("DEBUG", "false").lower() in ("true", "1", "yes")
DEBUG_LEVEL = os.getenv("DEBUG_LEVEL", "basic").lower()  # Níveis: basic, advanced, trace
ACCEPTORS = os.getenv("ACCEPTORS", "acceptor-1:8080,acceptor-2:8080,acceptor-3:8080,acceptor-4:8080,acceptor-5:8080").split(",")
PROPOSERS = os.getenv("PROPOSERS", "proposer-1:8080,proposer-2:8080,proposer-3:8080,proposer-4:8080,proposer-5:8080").split(",")
LEARNERS = os.getenv("LEARNERS", "learner-1:8080,learner-2:8080").split(",")
STORES = os.getenv("STORES", "cluster-store-1:8080,cluster-store-2:8080,cluster-store-3:8080").split(",")
LOG_DIR = os.getenv("LOG_DIR", "/data/logs")

logger = logging.getLogger("proposer")

async def shutdown(app, persistence, leader_election, proposer):
    """Função para desligar graciosamente o serviço"""
    logger.important(f"Proposer {NODE_ID} está sendo desligado...")
    
    # Salva estado persistente
    await persistence.save_state()
    logger.info("Estado persistente salvo")
    
    # Encerra o detector de líder
    await leader_election.stop()
    logger.info("Detector de líder encerrado")
    
    # Encerra o proposer
    await proposer.stop()
    logger.info("Processador de propostas encerrado")
    
    logger.important(f"Proposer {NODE_ID} desligado com sucesso.")

def main():
    # Configura o logger com suporte a níveis de debug
    setup_logging(f"proposer-{NODE_ID}", debug=DEBUG, debug_level=DEBUG_LEVEL, log_dir=LOG_DIR)
    logger.important(f"Iniciando Proposer {NODE_ID}...")
    
    if DEBUG:
        logger.info(f"Modo DEBUG ativado com nível: {DEBUG_LEVEL}")
        logger.info(f"Configuração: PORT={PORT}, ACCEPTORS={len(ACCEPTORS)}, PROPOSERS={len(PROPOSERS)}, LEARNERS={len(LEARNERS)}, STORES={len(STORES)}")
    
    # Carrega estado persistente
    persistence = ProposerPersistence(NODE_ID)
    initial_state = persistence.load_state()
    
    if DEBUG and DEBUG_LEVEL in ("advanced", "trace"):
        logger.debug(f"Estado inicial carregado: {initial_state}")
    
    # Cria instância do proposer
    proposer = Proposer(
        node_id=NODE_ID,
        acceptors=ACCEPTORS,
        learners=LEARNERS,
        stores=STORES,
        proposal_counter=initial_state.get("proposal_counter", 0),
        last_instance_id=initial_state.get("last_instance_id", 0)
    )
    
    # Cria gerenciador de eleição de líder
    leader_election = LeaderElection(
        node_id=NODE_ID,
        proposers=PROPOSERS,
        proposer=proposer,
        current_leader=initial_state.get("current_leader", None),
        current_term=initial_state.get("current_term", 0)
    )
    
    # Cria API
    app = create_api(proposer, leader_election)
    
    # Configura callback para shutdown gracioso
    @app.on_event("shutdown")
    async def on_shutdown():
        await shutdown(app, persistence, leader_election, proposer)
    
    # Inicia verificação periódica para persistência
    @app.on_event("startup")
    async def on_startup():
        # Inicia o processador de propostas
        await proposer.start()
        logger.info("Processador de propostas iniciado")
        
        # Inicia a eleição de líder
        asyncio.create_task(leader_election.start())
        logger.info("Detector de líder iniciado")
        
        # Inicia o loop de persistência
        asyncio.create_task(persistence_loop(persistence, proposer, leader_election))
        logger.info("Loop de persistência iniciado")
    
    async def persistence_loop(persistence, proposer, leader_election):
        """Loop para salvar estado periodicamente"""
        while True:
            try:
                await asyncio.sleep(10)  # Salva a cada 10 segundos
                
                # Prepara estado para salvamento
                current_state = {
                    "proposal_counter": proposer.proposal_counter,
                    "last_instance_id": proposer.last_instance_id,
                    "current_leader": leader_election.current_leader,
                    "current_term": leader_election.current_term
                }
                
                if DEBUG and DEBUG_LEVEL == "trace":
                    logger.debug(f"Salvando estado: {current_state}")
                
                # Salva estado
                await persistence.save_state(current_state)
                
            except Exception as e:
                logger.error(f"Erro ao salvar estado persistente: {e}", exc_info=True)
    
    # Configura manipuladores de sinal para shutdown gracioso
    loop = asyncio.get_event_loop()
    signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
    for s in signals:
        loop.add_signal_handler(
            s, lambda s=s: asyncio.create_task(shutdown(app, persistence, leader_election, proposer))
        )
    
    # Inicia o servidor
    logger.important(f"Proposer {NODE_ID} iniciado e escutando na porta {PORT}")
    uvicorn.run(app, host="0.0.0.0", port=PORT)

if __name__ == "__main__":
    main()