"""
Ponto de entrada da aplicação FastAPI para o componente Learner.
"""
import logging
from typing import Dict, Any

from fastapi import FastAPI
from prometheus_client import start_http_server
import uvicorn

from common.logging import configure_logger
from common.models import LearnMessage, HealthResponse
from learner.config import NODE_ID, HOST, PORT
from learner.learner import Learner


# Configurar logger
logger = configure_logger("learner", NODE_ID)

# Criar aplicação FastAPI
app = FastAPI(title=f"Learner {NODE_ID}", description="Learner do protocolo Paxos")

# Instância global
learner = None


@app.on_event("startup")
async def startup_event():
    """
    Inicializa componentes na inicialização da aplicação.
    """
    global learner
    
    # Iniciar servidor Prometheus em porta separada
    start_http_server(9090 + NODE_ID)
    
    # Inicializar learner
    learner = Learner(logger)
    await learner.initialize()
    
    logger.info(f"Learner {NODE_ID} iniciado")


@app.on_event("shutdown")
async def shutdown_event():
    """
    Finaliza componentes no encerramento da aplicação.
    """
    global learner
    
    # Finalizar learner
    if learner:
        await learner.shutdown()
        
    logger.info(f"Learner {NODE_ID} encerrado")


@app.post("/learn", status_code=200)
async def learn(message: LearnMessage):
    """
    Endpoint para receber mensagens learn dos acceptors.
    """
    global learner
    
    logger.info(f"Mensagem learn recebida: proposal_number={message.proposalNumber}, "
              f"instance_id={message.instanceId}, acceptor_id={message.acceptorId}")
    
    # Processar mensagem learn
    await learner.learn(message)
    
    return {"acknowledged": True}


@app.get("/status", status_code=200)
async def get_status():
    """
    Endpoint para verificar o status do learner.
    """
    global learner
    
    return learner.get_status()


@app.get("/health", status_code=200, response_model=HealthResponse)
async def health_check():
    """
    Endpoint para verificação de saúde (heartbeat).
    """
    return {"status": "healthy"}


if __name__ == "__main__":
    uvicorn.run("main:app", host=HOST, port=PORT)