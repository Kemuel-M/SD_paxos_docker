"""
Implementação dos endpoints da API REST do Proposer.
"""
import os
import time
import logging
import asyncio
from typing import Dict, Any, List, Optional
from fastapi import FastAPI, HTTPException, Body, BackgroundTasks, Query
from pydantic import BaseModel, Field

logger = logging.getLogger("proposer")
DEBUG = os.getenv("DEBUG", "false").lower() in ("true", "1", "yes")

# Modelos de dados para a API
class ClientRequest(BaseModel):
    clientId: str = Field(..., description="ID do cliente no formato client-X")
    timestamp: int = Field(..., description="Unix timestamp em milissegundos")
    operation: str = Field(..., description="Tipo de operação (WRITE)")
    resource: str = Field(..., description="Identificador do recurso")
    data: str = Field(..., description="Dados a serem escritos no recurso")

class StatusResponse(BaseModel):
    node_id: int = Field(..., description="ID deste proposer")
    is_leader: bool = Field(..., description="Indica se este proposer é o líder atual")
    leader_id: Optional[int] = Field(None, description="ID do líder atual, se conhecido")
    term: int = Field(..., description="Termo atual da eleição")
    last_instance_id: int = Field(..., description="ID da última instância processada")
    proposal_counter: int = Field(..., description="Contador atual de propostas")
    state: str = Field(..., description="Estado atual do proposer")
    uptime: float = Field(..., description="Tempo de execução em segundos")

class HealthResponse(BaseModel):
    status: str = Field(..., description="Estado de saúde do serviço")
    timestamp: int = Field(..., description="Timestamp atual")

def create_api(proposer, leader_election):
    """
    Cria a API FastAPI para o Proposer.
    
    Args:
        proposer: Instância do Proposer
        leader_election: Instância do gerenciador de eleição de líder
    
    Returns:
        FastAPI: Aplicação FastAPI configurada
    """
    app = FastAPI(title="Proposer API", description="API para o componente Proposer do sistema Paxos")
    start_time = time.time()
    
    # Endpoint para receber requisições de clientes
    @app.post("/propose", status_code=202)
    async def propose(request: ClientRequest, background_tasks: BackgroundTasks):
        logger.info(f"Recebida requisição do cliente {request.clientId}: {request.operation} em {request.resource}")
        
        if DEBUG:
            logger.debug(f"Detalhes da requisição: {request.dict()}")
        
        # Verifica se somos o líder ou se devemos redirecionar
        if not leader_election.is_leader() and leader_election.current_leader is not None:
            logger.info(f"Redirecionando requisição para o líder atual: {leader_election.current_leader}")
            raise HTTPException(
                status_code=307,  # Temporary Redirect
                detail=f"Este proposer não é o líder atual. Redirecionando para o líder: proposer-{leader_election.current_leader}",
                headers={"Location": f"http://proposer-{leader_election.current_leader}:8080/propose"}
            )
        
        # Processa a requisição em background para não bloquear
        background_tasks.add_task(proposer.propose, request.dict())
        
        return {"status": "accepted", "message": "Requisição aceita e será processada"}
    
    # Endpoint para obter status do proposer
    @app.get("/status", response_model=StatusResponse)
    async def get_status():
        return {
            "node_id": proposer.node_id,
            "is_leader": leader_election.is_leader(),
            "leader_id": leader_election.current_leader,
            "term": leader_election.current_term,
            "last_instance_id": proposer.last_instance_id,
            "proposal_counter": proposer.proposal_counter,
            "state": proposer.state,
            "uptime": time.time() - start_time
        }
    
    # Endpoint para verificação de saúde (heartbeat)
    @app.get("/health", response_model=HealthResponse)
    async def health_check():
        return {
            "status": "healthy",
            "timestamp": int(time.time() * 1000)
        }
    
    # Endpoint para comunicação entre líderes
    @app.get("/leader-status")
    async def leader_status():
        if not leader_election.is_leader():
            raise HTTPException(status_code=403, detail="Este nó não é o líder atual")
        
        return {
            "leaderId": proposer.node_id,
            "term": leader_election.current_term,
            "lastInstanceId": proposer.last_instance_id
        }
    
    # Endpoint para receber heartbeat do líder
    @app.post("/leader-heartbeat")
    async def leader_heartbeat(data: Dict[str, Any] = Body(...)):
        await leader_election.receive_heartbeat(data)
        return {"status": "acknowledged"}
    
    # Endpoints para logs
    @app.get("/logs")
    async def get_logs(limit: int = Query(100, ge=1, le=1000)):
        """Retorna os logs do proposer. Disponível apenas quando DEBUG=true."""
        if not DEBUG:
            raise HTTPException(status_code=403, detail="DEBUG mode not enabled")
        
        from common.logging import get_log_entries
        return {"logs": get_log_entries("proposer", limit)}
    
    @app.get("/logs/important")
    async def get_important_logs(limit: int = Query(100, ge=1, le=1000)):
        """Retorna logs importantes do proposer."""
        from common.logging import get_important_log_entries
        return {"logs": get_important_log_entries("proposer", limit)}
    
    return app