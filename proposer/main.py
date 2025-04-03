"""
Ponto de entrada da aplicação FastAPI para o componente Proposer.
"""
import asyncio
import logging
from typing import Dict, Any, Optional

from fastapi import FastAPI, HTTPException
from prometheus_client import start_http_server
import uvicorn

from common.logging import configure_logger
from common.models import ClientRequest, HealthResponse
from proposer.config import NODE_ID, HOST, PORT
from proposer.proposer import Proposer
from proposer.leader import LeaderElection


# Configurar logger
logger = configure_logger("proposer", NODE_ID)

# Criar aplicação FastAPI
app = FastAPI(title=f"Proposer {NODE_ID}", description="Proposer do protocolo Paxos")

# Instâncias globais
leader_election = None
proposer = None


@app.on_event("startup")
async def startup_event():
    """
    Inicializa componentes na inicialização da aplicação.
    """
    global leader_election, proposer
    
    # Iniciar servidor Prometheus em porta separada
    start_http_server(9090 + NODE_ID)
    
    # Inicializar componentes
    leader_election = LeaderElection(logger, None)
    proposer = Proposer(logger, leader_election)
    
    # Resolver referência circular
    leader_election.proposer = proposer
    
    # Inicializar componentes
    await proposer.initialize()
    await leader_election.initialize()
    
    # Iniciar tarefas de background
    await leader_election.start_background_tasks()
    
    logger.info(f"Proposer {NODE_ID} iniciado")


@app.on_event("shutdown")
async def shutdown_event():
    """
    Finaliza componentes no encerramento da aplicação.
    """
    global leader_election, proposer
    
    # Finalizar componentes
    if leader_election:
        await leader_election.shutdown()
        
    if proposer:
        await proposer.shutdown()
        
    logger.info(f"Proposer {NODE_ID} encerrado")


@app.post("/propose", status_code=200)
async def propose(request: ClientRequest):
    """
    Endpoint para receber requisições de clientes.
    """
    global proposer
    
    client_id = request.clientId
    timestamp = request.timestamp
    
    logger.info(f"Requisição recebida: client_id={client_id}, timestamp={timestamp}")
    
    # Propor a requisição
    success, instance_id = await proposer.propose(request)
    
    if not success:
        logger.warning(f"Falha na proposta: client_id={client_id}, timestamp={timestamp}")
        raise HTTPException(status_code=500, detail="Falha na proposta")
        
    return {
        "success": success,
        "instanceId": instance_id,
        "message": "Proposta aceita e em processamento"
    }


@app.post("/leader-status", status_code=200)
async def leader_status(data: Dict[str, Any]):
    """
    Endpoint para receber heartbeats do líder.
    """
    global leader_election
    
    leader_id = data.get("leaderId")
    term = data.get("term")
    last_instance_id = data.get("lastInstanceId")
    
    await leader_election.receive_leader_heartbeat(leader_id, term, last_instance_id)
    
    return {"acknowledged": True}


@app.get("/status", status_code=200)
async def get_status():
    """
    Endpoint para verificar o status do proposer.
    """
    global proposer
    
    return proposer.get_status()


@app.get("/health", status_code=200, response_model=HealthResponse)
async def health_check():
    """
    Endpoint para verificação de saúde (heartbeat).
    """
    return {"status": "healthy"}


if __name__ == "__main__":
    uvicorn.run("main:app", host=HOST, port=PORT)