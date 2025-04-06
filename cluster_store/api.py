"""
File: cluster_store/api.py
Implementação da API REST para o Cluster Store.
"""
import os
import time
import json
import logging
from typing import Dict, Any, List, Optional

from fastapi import FastAPI, HTTPException, Depends, Query, Path, Body
from pydantic import BaseModel, Field
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

logger = logging.getLogger("api")

# Modelos de dados para a API
class PrepareRequest(BaseModel):
    type: str = Field("PREPARE", description="Tipo da requisição (sempre PREPARE)")
    transactionId: str = Field(..., description="ID da transação")
    instanceId: int = Field(..., description="ID da instância Paxos")
    resource: str = Field(..., description="ID do recurso")
    data: Any = Field(..., description="Dados para escrita no recurso")
    clientId: str = Field(..., description="ID do cliente")
    timestamp: int = Field(..., description="Timestamp em milissegundos")

class CommitRequest(BaseModel):
    type: str = Field("COMMIT", description="Tipo da requisição (sempre COMMIT)")
    transactionId: str = Field(..., description="ID da transação")
    instanceId: int = Field(..., description="ID da instância Paxos")
    resource: str = Field(..., description="ID do recurso")
    data: Any = Field(..., description="Dados para escrita no recurso")
    version: int = Field(..., description="Nova versão do recurso")
    clientId: str = Field(..., description="ID do cliente")
    timestamp: int = Field(..., description="Timestamp em milissegundos")

class AbortRequest(BaseModel):
    type: str = Field("ABORT", description="Tipo da requisição (sempre ABORT)")
    transactionId: str = Field(..., description="ID da transação")
    resource: str = Field(..., description="ID do recurso")
    reason: Optional[str] = Field(None, description="Razão do abort")

class ResourceStatusResponse(BaseModel):
    data: Any = Field(..., description="Conteúdo do recurso")
    version: int = Field(..., description="Número de versão")
    timestamp: int = Field(..., description="Timestamp da última modificação")
    node_id: int = Field(..., description="ID do nó que realizou a última modificação")

def create_api(resource_manager, sync_manager, node_id: int):
    """
    Cria a API FastAPI para o Cluster Store.
    
    Args:
        resource_manager: Instância do ResourceManager
        sync_manager: Instância do SynchronizationManager
        node_id: ID do nó no cluster
    
    Returns:
        FastAPI: Aplicação FastAPI configurada
    """
    app = FastAPI(title="Cluster Store API", 
                 description="API para o componente Cluster Store do sistema Paxos")
    
    # Configura CORS para permitir acesso de qualquer origem
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    # Endpoint de verificação de saúde (heartbeat)
    @app.get("/health")
    async def health_check():
        """Endpoint para verificação de saúde (heartbeat)."""
        from common.logging import DEBUG, DEBUG_LEVEL
        
        return {
            "status": "healthy",
            "node_id": node_id,
            "timestamp": int(time.time() * 1000),
            "debug_enabled": DEBUG,
            "debug_level": DEBUG_LEVEL
        }
    
    # Endpoint para obter status do nó
    @app.get("/status")
    async def get_status():
        """Retorna o status atual do nó."""
        # Obtém informações de status dos componentes
        resource_status = resource_manager.get_status()
        sync_status = sync_manager.get_status()
        
        # Combina informações
        status = {
            "node_id": node_id,
            "status": "online",
            "timestamp": int(time.time() * 1000),
            "uptime": os.times().elapsed,  # Tempo desde inicialização
            "resources": resource_status,
            "synchronization": sync_status
        }
        
        return status
    
    # Endpoint para preparar transação (fase 1 do 2PC)
    @app.post("/prepare")
    async def prepare_transaction(request: PrepareRequest):
        """
        Prepara para executar uma operação (fase 1 do 2PC).
        Verifica se o recurso está disponível para escrita.
        """
        transaction_id = request.transactionId
        resource_id = request.resource
        data = request.data
        client_id = request.clientId
        
        logger.info(f"Recebida requisição PREPARE para transação {transaction_id}, recurso {resource_id}")
        
        # Verifica se pode executar a operação
        ready, info = resource_manager.prepare_transaction(transaction_id, resource_id, data, client_id)
        
        # Constrói resposta
        response = {
            "ready": ready,
            "currentVersion": info.get("currentVersion", 0),
        }
        
        # Adiciona razão se houver falha
        if not ready and "reason" in info:
            response["reason"] = info["reason"]
        
        logger.info(f"Resposta PREPARE para transação {transaction_id}: ready={ready}")
        return response
    
    # Endpoint para confirmar transação (fase 2 do 2PC)
    @app.post("/commit")
    async def commit_transaction(request: CommitRequest):
        """
        Executa uma operação previamente preparada (fase 2 do 2PC).
        Atualiza o recurso com os novos dados.
        """
        transaction_id = request.transactionId
        resource_id = request.resource
        data = request.data
        version = request.version
        client_id = request.clientId
        timestamp = request.timestamp
        
        logger.info(f"Recebida requisição COMMIT para transação {transaction_id}, recurso {resource_id}, versão {version}")
        
        # Executa a operação
        success, info = resource_manager.commit_transaction(
            transaction_id, resource_id, data, version, client_id, timestamp)
        
        # Constrói resposta
        response = {
            "success": success,
        }
        
        # Adiciona informações adicionais
        if success:
            response.update({
                "resource": info.get("resource", resource_id),
                "version": info.get("version", version),
                "timestamp": info.get("timestamp", timestamp)
            })
        else:
            response["reason"] = info.get("reason", "Erro desconhecido")
        
        logger.info(f"Resposta COMMIT para transação {transaction_id}: success={success}")
        return response
    
    # Endpoint para abortar transação
    @app.post("/abort")
    async def abort_transaction(request: AbortRequest):
        """
        Aborta uma transação previamente preparada.
        Libera recursos alocados durante a fase prepare.
        """
        transaction_id = request.transactionId
        resource_id = request.resource
        reason = request.reason
        
        logger.info(f"Recebida requisição ABORT para transação {transaction_id}, recurso {resource_id}, razão: {reason}")
        
        # Aborta a transação
        success = resource_manager.abort_transaction(transaction_id, resource_id)
        
        # Constrói resposta
        response = {
            "success": success,
            "transactionId": transaction_id,
            "resource": resource_id
        }
        
        logger.info(f"Resposta ABORT para transação {transaction_id}: success={success}")
        return response
    
    # Endpoint para obter um recurso
    @app.get("/resource/{resource_id}", response_model=ResourceStatusResponse)
    async def get_resource(resource_id: str = Path(..., description="ID do recurso")):
        """
        Obtém o estado atual de um recurso.
        """
        logger.info(f"Recebida requisição GET para recurso {resource_id}")
        
        # Obtém o recurso
        resource = resource_manager.get_resource(resource_id)
        
        # Se não existir, retorna erro 404
        if resource is None:
            logger.warning(f"Recurso {resource_id} não encontrado")
            raise HTTPException(status_code=404, detail=f"Recurso {resource_id} não encontrado")
        
        logger.debug(f"Recurso {resource_id} obtido: versão {resource['version']}")
        return resource
    
    # Endpoint para obter metadados de todos os recursos
    @app.get("/resources/metadata")
    async def get_resources_metadata():
        """
        Obtém metadados de todos os recursos disponíveis.
        Usado para sincronização entre nós.
        """
        logger.debug("Recebida requisição para metadados de recursos")
        
        # Lista todos os recursos
        resources_list = resource_manager.storage.list_resources()
        
        # Obtém metadados de cada recurso
        metadata = {}
        for resource_id in resources_list:
            resource_metadata = resource_manager.storage.get_resource_metadata(resource_id)
            if resource_metadata:
                metadata[resource_id] = resource_metadata
        
        return {"resources": metadata, "node_id": node_id}
    
    # Endpoint para forçar sincronização
    @app.post("/synchronize")
    async def force_synchronize(resource_id: Optional[str] = None):
        """
        Força sincronização imediata de recursos.
        
        Args:
            resource_id: ID específico do recurso para sincronizar (opcional)
        """
        logger.important(f"Sincronização forçada iniciada para recurso: {resource_id or 'todos'}")
        
        # Inicia sincronização
        success = await sync_manager.force_synchronize(resource_id)
        
        return {
            "success": success,
            "resource": resource_id,
            "timestamp": int(time.time() * 1000)
        }
    
    # Endpoint para acessar logs
    @app.get("/logs")
    async def get_logs(limit: int = Query(100, ge=1, le=1000)):
        """
        Retorna logs recentes do componente.
        
        Args:
            limit: Número máximo de logs a retornar
        """
        from common.logging import get_log_entries
        
        logs = get_log_entries(f"store-{node_id}", limit=limit)
        
        return {"logs": logs}
    
    # Endpoint para acessar logs importantes
    @app.get("/logs/important")
    async def get_important_logs(limit: int = Query(100, ge=1, le=1000)):
        """
        Retorna logs importantes do componente.
        
        Args:
            limit: Número máximo de logs a retornar
        """
        from common.logging import get_important_log_entries
        
        logs = get_important_log_entries(f"store-{node_id}", limit=limit)
        
        return {"logs": logs}
    
    # Endpoint para obter status de transação
    @app.get("/transaction/{transaction_id}")
    async def get_transaction_status(transaction_id: str = Path(..., description="ID da transação")):
        """
        Obtém o status de uma transação.
        
        Args:
            transaction_id: ID da transação
        """
        logger.debug(f"Recebida requisição para status da transação {transaction_id}")
        
        # Obtém status da transação
        transaction = resource_manager.get_transaction_status(transaction_id)
        
        # Se não existir, retorna erro 404
        if transaction is None:
            logger.warning(f"Transação {transaction_id} não encontrada")
            raise HTTPException(status_code=404, detail=f"Transação {transaction_id} não encontrada")
        
        # Remove dados sensíveis
        if "data" in transaction:
            transaction["data_size"] = len(str(transaction["data"]))
            del transaction["data"]
        
        return {
            "transaction_id": transaction_id,
            "status": transaction
        }
    
    # Endpoint para configuração de debug
    @app.post("/debug/config")
    async def configure_debug(config: Dict[str, Any] = Body(...)):
        """
        Configura o modo de debug em tempo de execução.
        
        Args:
            config: Configuração com enabled e level
        """
        from common.logging import set_debug_level
        
        enabled = config.get("enabled", False)
        level = config.get("level", "basic")
        
        # Configura novo nível de debug
        set_debug_level(enabled, level)
        
        logger.important(f"Nível de debug alterado: enabled={enabled}, level={level}")
        
        return {
            "status": "success",
            "debug": {
                "enabled": enabled,
                "level": level
            }
        }
    
    # Endpoint para limpeza de manutenção
    @app.post("/maintenance/cleanup")
    async def maintenance_cleanup():
        """
        Executa tarefas de manutenção (limpeza de transações expiradas, etc).
        """
        logger.info("Iniciando limpeza de manutenção")
        
        # Limpa transações e locks expirados
        resource_manager.clean_expired_transactions()
        
        return {
            "status": "success",
            "timestamp": int(time.time() * 1000)
        }
    
    return app