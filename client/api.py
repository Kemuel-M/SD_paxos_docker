"""
API REST do cliente para receber notificações do learner.
"""
import os
import time
import logging
import asyncio
from typing import Dict, Any, List, Optional
from fastapi import FastAPI, HTTPException, Body, Depends, Query
from pydantic import BaseModel, Field

# Configuração de debug
DEBUG = os.getenv("DEBUG", "false").lower() in ("true", "1", "yes")
DEBUG_LEVEL = os.getenv("DEBUG_LEVEL", "basic").lower()  # Níveis: basic, advanced, trace

logger = logging.getLogger("client")

# Modelos de dados para a API
class NotificationRequest(BaseModel):
    status: str = Field(..., description="Status da operação (COMMITTED/NOT_COMMITTED)")
    instanceId: int = Field(..., description="ID da instância Paxos")
    resource: str = Field(..., description="Identificador do recurso")
    timestamp: int = Field(..., description="Timestamp da operação em milissegundos")

class OperationRequest(BaseModel):
    data: str = Field(..., description="Dados para escrever no recurso")

class StatusResponse(BaseModel):
    client_id: str = Field(..., description="ID do cliente")
    proposer_url: str = Field(..., description="URL do proposer designado")
    total_operations: int = Field(..., description="Total de operações planejadas")
    completed: int = Field(..., description="Operações concluídas com sucesso")
    failed: int = Field(..., description="Operações falhas")
    in_progress: int = Field(..., description="Operações em andamento")
    avg_latency: float = Field(..., description="Latência média em segundos")
    runtime: float = Field(..., description="Tempo de execução em segundos")
    running: bool = Field(..., description="Se o cliente está em execução")

def create_api(client):
    """
    Cria a API FastAPI para o cliente.
    
    Args:
        client: Instância do PaxosClient
    
    Returns:
        FastAPI: Aplicação FastAPI configurada
    """
    app = FastAPI(title="Client API", description="API para o componente Client do sistema Paxos")
    
    # Endpoint para receber notificações dos learners
    @app.post("/notification")
    async def notification(notification: NotificationRequest):
        """Recebe notificação do learner sobre resultado da operação."""
        if DEBUG:
            logger.debug(f"Recebida notificação: {notification.dict()}")
            
        logger.info(f"Recebida notificação para instância {notification.instanceId}: {notification.status}")
        
        # Processa notificação no cliente
        result = client.process_notification(notification.dict())
        
        return result
    
    # Endpoint para obter status do cliente
    @app.get("/status", response_model=StatusResponse)
    async def get_status():
        """Retorna status atual do cliente."""
        return client.get_status()
    
    # Endpoint para verificação de saúde (heartbeat)
    @app.get("/health")
    async def health_check():
        """Endpoint de verificação de saúde."""
        return {
            "status": "healthy",
            "timestamp": int(time.time() * 1000)
        }
    
    # Endpoint para obter histórico de operações
    @app.get("/history")
    async def get_history(limit: int = Query(100, ge=1, le=1000)):
        """Retorna histórico de operações do cliente."""
        return {"operations": client.get_history(limit)}
    
    # Endpoint para obter detalhes de uma operação específica
    @app.get("/operation/{operation_id}")
    async def get_operation(operation_id: int):
        """Retorna detalhes de uma operação específica."""
        operation = client.get_operation(operation_id)
        if not operation:
            raise HTTPException(status_code=404, detail=f"Operação {operation_id} não encontrada")
        return operation
    
    # Endpoint para iniciar operação manual
    @app.post("/operation")
    async def create_operation(operation: OperationRequest):
        """Inicia uma operação manual."""
        if not client.running:
            raise HTTPException(status_code=400, detail="Cliente não está em execução")
        
        # Cria ID para a operação manual
        operation_id = client.next_operation_id
        client.next_operation_id += 1
        
        # Cria task para executar a operação
        asyncio.create_task(client._send_operation(operation_id))
        
        return {
            "status": "initiated",
            "operation_id": operation_id
        }
    
    # Endpoint para iniciar o cliente
    @app.post("/start")
    async def start_client():
        """Inicia o ciclo de operações do cliente."""
        await client.start()
        return {"status": "started"}
    
    # Endpoint para parar o cliente
    @app.post("/stop")
    async def stop_client():
        """Para o ciclo de operações do cliente."""
        await client.stop()
        return {"status": "stopped"}
    
    # Endpoint para obter logs
    @app.get("/logs")
    async def get_logs(limit: int = Query(100, ge=1, le=1000)):
        """Retorna logs do cliente."""
        from common.logging import get_log_entries
        return {"logs": get_log_entries("client", limit=limit)}
    
    return app