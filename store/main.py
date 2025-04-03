"""
Ponto de entrada da aplicação FastAPI para o componente Cluster Store.
"""
import logging
from typing import Dict, Any, Optional

from fastapi import FastAPI, HTTPException
from prometheus_client import start_http_server
import uvicorn

from common.logging import configure_logger
from common.models import (
    TwoPhaseCommitPrepareRequest,
    TwoPhaseCommitCommitRequest,
    ResourceData,
    HealthResponse
)
from store.config import NODE_ID, HOST, PORT
from store.store import ClusterStore


# Configurar logger
logger = configure_logger("store", NODE_ID)

# Criar aplicação FastAPI
app = FastAPI(title=f"Store {NODE_ID}", description="Nó do Cluster Store")

# Instância global
store = None


@app.on_event("startup")
async def startup_event():
    """
    Inicializa componentes na inicialização da aplicação.
    """
    global store
    
    # Iniciar servidor Prometheus em porta separada
    start_http_server(9090 + NODE_ID)
    
    # Inicializar store
    store = ClusterStore(logger)
    await store.initialize()
    
    logger.info(f"Store {NODE_ID} iniciado")


@app.on_event("shutdown")
async def shutdown_event():
    """
    Finaliza componentes no encerramento da aplicação.
    """
    global store
    
    # Finalizar store
    if store:
        await store.shutdown()
        
    logger.info(f"Store {NODE_ID} encerrado")


@app.post("/prepare", status_code=200)
async def prepare(request: TwoPhaseCommitPrepareRequest):
    """
    Endpoint para receber requisições prepare do protocolo 2PC.
    """
    global store
    
    logger.info(f"Requisição prepare recebida: instance_id={request.instanceId}, "
              f"resource={request.resource}, client_id={request.clientId}")
    
    # Processar prepare
    response = await store.prepare(request)
    
    return response.dict()


@app.post("/commit", status_code=200)
async def commit(request: TwoPhaseCommitCommitRequest):
    """
    Endpoint para receber requisições commit do protocolo 2PC.
    """
    global store
    
    logger.info(f"Requisição commit recebida: instance_id={request.instanceId}, "
              f"resource={request.resource}, version={request.version}")
    
    # Processar commit
    response = await store.commit(request)
    
    return response.dict()


@app.post("/abort", status_code=200)
async def abort(data: Dict[str, Any]):
    """
    Endpoint para receber requisições abort do protocolo 2PC.
    """
    global store
    
    instance_id = data.get("instanceId")
    resource_id = data.get("resource", "R")
    
    logger.info(f"Requisição abort recebida: instance_id={instance_id}, resource={resource_id}")
    
    # Processar abort
    await store.abort(instance_id, resource_id)
    
    return {"acknowledged": True}


@app.get("/resource/{resource_id}", status_code=200, response_model=ResourceData)
async def get_resource(resource_id: str):
    """
    Endpoint para ler um recurso.
    """
    global store
    
    logger.info(f"Requisição de leitura recebida: resource={resource_id}")
    
    # Ler recurso
    resource = await store.read_resource(resource_id)
    
    if resource is None:
        raise HTTPException(status_code=404, detail=f"Recurso {resource_id} não encontrado")
        
    return resource


@app.get("/status", status_code=200)
async def get_status():
    """
    Endpoint para verificar o status do store.
    """
    global store
    
    return store.get_status()


@app.get("/health", status_code=200, response_model=HealthResponse)
async def health_check():
    """
    Endpoint para verificação de saúde (heartbeat).
    """
    return {"status": "healthy"}


if __name__ == "__main__":
    uvicorn.run("main:app", host=HOST, port=PORT)