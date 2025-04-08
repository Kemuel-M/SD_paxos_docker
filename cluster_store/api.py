import asyncio
import json
import logging
import time
from typing import Dict, List, Optional

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from cluster_store.persistence import PersistenceManager
from cluster_store.store import Store
from common.utils import get_debug_mode, setup_logging

# Configura o logger
logger = logging.getLogger(__name__)

# Modelos de dados para as requisições e respostas
class PrepareRequest(BaseModel):
    proposal_id: str
    tid: int

class PrepareResponse(BaseModel):
    success: bool
    tid: int
    message: str
    version: Optional[int] = None
    resource_data: Optional[Dict] = None
    lock_holder: Optional[str] = None
    lock_expiry: Optional[float] = None

class CommitRequest(BaseModel):
    proposal_id: str
    tid: int
    resource_data: Dict
    learner_id: str
    timestamp: float
    protocol: Optional[str] = None

class CommitResponse(BaseModel):
    success: bool
    tid: int
    version: Optional[int] = None
    message: str

class ResourceResponse(BaseModel):
    data: Dict
    version: int
    tid: int
    timestamp: float

class StatusResponse(BaseModel):
    id: str
    version: int
    last_tid: int
    locked: bool
    lock_holder: Optional[str] = None
    lock_expiry: Optional[float] = None
    updates_count: int
    persistence_enabled: bool

class HealthResponse(BaseModel):
    status: str
    timestamp: float

# Variáveis globais para instâncias
store: Optional[Store] = None
persistence_manager: Optional[PersistenceManager] = None

# Cria a aplicação FastAPI
app = FastAPI(title="Cluster Store API")

# Middleware para logging de requisições
@app.middleware("http")
async def log_requests(request: Request, call_next):
    # Obtém o corpo da requisição
    body = b""
    async for chunk in request.body():
        body += chunk
    
    # Reconstrói o stream de requisição para ser consumido novamente
    request._body = body
    
    # Log da requisição
    method = request.method
    url = request.url
    logger.debug(f"Requisição recebida: {method} {url}")
    
    if get_debug_mode() and body:
        try:
            body_str = body.decode('utf-8')
            if body_str:
                body_json = json.loads(body_str)
                logger.debug(f"Corpo da requisição: {json.dumps(body_json, indent=2)}")
        except Exception as e:
            logger.debug(f"Corpo da requisição não é JSON válido: {body}")
    
    # Processa a requisição
    response = await call_next(request)
    
    # Log da resposta
    status_code = response.status_code
    logger.debug(f"Resposta enviada: {status_code}")
    
    return response

# Rotas da API
@app.post("/prepare", response_model=PrepareResponse)
async def prepare_endpoint(request: PrepareRequest):
    """
    Endpoint para processar mensagens PREPARE do protocolo de acesso ao recurso.
    """
    if not store:
        raise HTTPException(status_code=500, detail="Store não inicializado")
    
    # Processa a mensagem PREPARE
    result = await store.prepare(request.proposal_id, request.tid)
    
    # Retorna o resultado
    return result

@app.post("/commit", response_model=CommitResponse)
async def commit_endpoint(request: CommitRequest):
    """
    Endpoint para processar mensagens COMMIT do protocolo de acesso ao recurso.
    """
    if not store:
        raise HTTPException(status_code=500, detail="Store não inicializado")
    
    # Log adicional se estivermos usando ROWA
    if request.protocol == "ROWA":
        logger.info(f"Processando COMMIT via protocolo ROWA para proposta {request.proposal_id}")
    
    # Processa a mensagem COMMIT
    result = await store.commit(
        request.proposal_id, request.tid, request.resource_data,
        request.learner_id, request.timestamp
    )
    
    # Retorna o resultado
    return result

@app.get("/resource", response_model=ResourceResponse)
async def resource_endpoint():
    """
    Endpoint para obter o estado atual do recurso R.
    """
    if not store:
        raise HTTPException(status_code=500, detail="Store não inicializado")
    
    # Obtém o recurso
    resource = await store.get_resource()
    
    # Retorna o recurso
    return resource

@app.get("/status", response_model=StatusResponse)
async def status_endpoint():
    """
    Endpoint para obter o status do store.
    """
    if not store:
        raise HTTPException(status_code=500, detail="Store não inicializado")
    
    # Obtém o status do store
    status = store.get_status()
    
    # Retorna o status
    return status

@app.get("/health", response_model=HealthResponse)
async def health_endpoint():
    """
    Endpoint para verificar a saúde do store.
    """
    # Retorna sempre saudável com o timestamp atual
    return {
        "status": "healthy",
        "timestamp": time.time()
    }

@app.get("/logs", response_class=Response)
async def logs_endpoint(lines: int = 100):
    """
    Endpoint para obter os logs do store.
    """
    try:
        # Obtém os N últimos logs do arquivo
        log_file = "logs/store.log"
        with open(log_file, "r") as f:
            logs = f.readlines()
            last_logs = logs[-lines:] if len(logs) >= lines else logs
        
        # Formata os logs
        formatted_logs = "".join(last_logs)
        
        # Retorna os logs como texto
        return Response(content=formatted_logs, media_type="text/plain")
    except Exception as e:
        logger.error(f"Erro ao obter logs: {str(e)}")
        return Response(content=f"Erro ao obter logs: {str(e)}", media_type="text/plain", status_code=500)

# Funções para inicialização
def initialize(store_id: str, data_dir: str = "data", debug: bool = None):
    """
    Inicializa o store e o gerenciador de persistência.
    
    Args:
        store_id: ID do store
        data_dir: Diretório para armazenar os dados
        debug: Flag para ativar modo de depuração
    """
    global store, persistence_manager
    
    # Configura o logging
    if debug is None:
        debug = get_debug_mode()
    
    setup_logging(f"store_{store_id}", debug)
    
    # Cria o gerenciador de persistência
    persistence_manager = PersistenceManager(store_id, data_dir, debug)
    
    # Cria o store
    store = Store(store_id, persistence_manager, debug)
    
    logger.info(f"Store {store_id} inicializado (debug={debug})")