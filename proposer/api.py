"""
File: proposer/api.py
Implementação dos endpoints da API REST do Proposer.
Com melhorias para logging e controle de debug.
"""
import os
import time
import logging
import asyncio
from typing import Dict, Any, List, Optional
from fastapi import FastAPI, HTTPException, Body, BackgroundTasks, Query, Depends
from pydantic import BaseModel, Field

# Configuração aprimorada de debug
DEBUG = os.getenv("DEBUG", "false").lower() in ("true", "1", "yes")
DEBUG_LEVEL = os.getenv("DEBUG_LEVEL", "basic").lower()  # Níveis: basic, advanced, trace

logger = logging.getLogger("proposer")

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
    debug_enabled: bool = Field(..., description="Estado do modo debug")
    debug_level: str = Field(..., description="Nível de debug atual")
    
class DebugConfigRequest(BaseModel):
    enabled: bool = Field(..., description="Habilitar ou desabilitar debug")
    level: str = Field("basic", description="Nível de debug (basic, advanced, trace)")

def get_current_debug_state():
    """
    Retorna o estado atual do debug para uso em endpoints.
    """
    return {
        "enabled": DEBUG,
        "level": DEBUG_LEVEL
    }

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
        if DEBUG and DEBUG_LEVEL in ("advanced", "trace"):
            logger.debug(f"Recebida requisição de propose: {request.dict()}")
            
        logger.info(f"Recebida requisição do cliente {request.clientId}: {request.operation} em {request.resource}")
        
        # Verifica se somos o líder ou se devemos redirecionar
        if not leader_election.is_leader() and leader_election.current_leader is not None:
            logger.info(f"Redirecionando requisição para o líder atual: {leader_election.current_leader}")
            raise HTTPException(
                status_code=307,  # Temporary Redirect
                detail=f"Este proposer não é o líder atual. Redirecionando para o líder: proposer-{leader_election.current_leader}",
                headers={"Location": f"http://proposer-{leader_election.current_leader}:8080/propose"}
            )
        
        # Processa a requisição em background para não bloquear
        result = await proposer.propose(request.dict())
        background_tasks.add_task(lambda: None)  # Dummy task para satisfazer o sistema
        
        return result or {"status": "accepted", "message": "Requisição aceita e será processada"}
    
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
    async def health_check(debug_state: Dict = Depends(get_current_debug_state)):
        return {
            "status": "healthy",
            "timestamp": int(time.time() * 1000),
            "debug_enabled": debug_state["enabled"],
            "debug_level": debug_state["level"]
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
        if DEBUG and DEBUG_LEVEL == "trace":
            logger.debug(f"Recebido heartbeat: {data}")
            
        await leader_election.receive_heartbeat(data)
        return {"status": "acknowledged"}
    
    # Endpoint para configurar debug em tempo de execução
    @app.post("/debug/config")
    async def configure_debug(config: DebugConfigRequest):
        """Configura o modo debug em tempo de execução."""
        if DEBUG and DEBUG_LEVEL in ("advanced", "trace"):
            logger.debug(f"Alterando configuração de debug: {config.dict()}")
            
        # Importa e usa o módulo de logging para configurar debug
        from common.logging import set_debug_level
        set_debug_level(config.enabled, config.level)
        
        logger.important(f"Configuração de debug alterada: enabled={config.enabled}, level={config.level}")
        return {"status": "success", "debug": {"enabled": config.enabled, "level": config.level}}
    
    # Endpoints para logs
    @app.get("/logs")
    async def get_logs(limit: int = Query(100, ge=1, le=1000)):
        """Retorna os logs do proposer. Disponível apenas quando DEBUG=true."""
        from common.logging import get_log_entries
        
        if not DEBUG:
            raise HTTPException(status_code=403, detail="DEBUG mode not enabled")
        
        return {"logs": get_log_entries("proposer", limit=limit)}
    
    @app.get("/logs/important")
    async def get_important_logs(limit: int = Query(100, ge=1, le=1000)):
        """Retorna logs importantes do proposer."""
        from common.logging import get_important_log_entries
        return {"logs": get_important_log_entries("proposer", limit=limit)}
    
    # Endpoint para estatísticas do sistema
    @app.get("/stats")
    async def get_stats():
        """Retorna estatísticas do proposer."""
        stats = {
            "uptime": time.time() - start_time,
            "node_id": proposer.node_id,
            "is_leader": leader_election.is_leader(),
            "leader_election_calls": leader_election.is_leader_call_count,
            "last_instance_id": proposer.last_instance_id,
            "proposal_counter": proposer.proposal_counter,
            "pending_proposals": proposer.pending_queue.qsize() if hasattr(proposer.pending_queue, "qsize") else "unknown",
            "active_proposals": len(proposer.proposals),
        }
        
        return {"stats": stats}
    
    return app