import asyncio
import json
import logging
import time
from typing import Dict, List, Optional

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from common.utils import get_debug_mode, setup_logging
from learner.learner import Learner
from learner.rowa import ROWAManager

# Configura o logger
logger = logging.getLogger(__name__)

# Modelos de dados para as requisições e respostas
class LearnRequest(BaseModel):
    proposal_id: str
    acceptor_id: str
    proposer_id: str
    tid: int
    client_id: str
    resource_data: Dict
    timestamp: int

class LearnResponse(BaseModel):
    success: bool
    proposal_id: str
    learner_id: str

class StatusResponse(BaseModel):
    id: str
    acceptor_count: int
    quorum_size: int
    stores_count: int
    clients_count: int
    proposals_committed: int
    proposals_pending: int
    total_proposals: int
    rowa_enabled: bool

class HealthResponse(BaseModel):
    status: str
    timestamp: float

# Variáveis globais para instâncias de Learner e ROWAManager
learner: Optional[Learner] = None
rowa_manager: Optional[ROWAManager] = None

# Cria a aplicação FastAPI
app = FastAPI(title="Learner API")

# Middleware para logging de requisições
@app.middleware("http")
async def log_requests(request: Request, call_next):
    # Obtém o corpo da requisição
    body = await request.body()
    
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
@app.post("/learn", response_model=LearnResponse)
async def learn_endpoint(request: LearnRequest):
    """
    Endpoint para aprender sobre uma proposta aceita.
    """
    if not learner:
        raise HTTPException(status_code=500, detail="Learner não inicializado")
    
    # Processa a mensagem LEARN
    if rowa_manager and rowa_manager.is_enabled():
        # Usa o protocolo ROWA se estiver habilitado
        result = await rowa_manager.process_learn(
            request.proposal_id, request.acceptor_id, request.proposer_id,
            request.tid, request.client_id, request.resource_data, request.timestamp
        )
    else:
        # Usa o learner padrão
        result = await learner.learn(
            request.proposal_id, request.acceptor_id, request.proposer_id,
            request.tid, request.client_id, request.resource_data, request.timestamp
        )
    
    # Retorna o resultado
    return result

@app.get("/status", response_model=StatusResponse)
async def status_endpoint():
    """
    Endpoint para obter o status do learner.
    """
    if not learner:
        raise HTTPException(status_code=500, detail="Learner não inicializado")
    
    # Obtém o status do learner
    status = learner.get_status()
    
    # Adiciona informações do ROWA
    status["rowa_enabled"] = rowa_manager.is_enabled() if rowa_manager else False
    
    # Retorna o status
    return status

@app.get("/health", response_model=HealthResponse)
async def health_endpoint():
    """
    Endpoint para verificar a saúde do learner.
    """
    # Retorna sempre saudável com o timestamp atual
    return {
        "status": "healthy",
        "timestamp": time.time()
    }

@app.get("/logs", response_class=Response)
async def logs_endpoint(lines: int = 100):
    """
    Endpoint para obter os logs do learner.
    """
    try:
        # Obtém os N últimos logs do arquivo
        log_file = "logs/learner.log"
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

@app.get("/proposals/{proposal_id}")
async def get_proposal_endpoint(proposal_id: str):
    """
    Endpoint para obter o status de uma proposta específica.
    """
    if not learner:
        raise HTTPException(status_code=500, detail="Learner não inicializado")
    
    # Obtém o status da proposta
    proposal_status = learner.get_proposal_status(proposal_id)
    
    # Se a proposta não foi encontrada, retorna um erro
    if "error" in proposal_status:
        raise HTTPException(status_code=404, detail=proposal_status["error"])
    
    # Retorna o status da proposta
    return proposal_status

@app.post("/toggle_rowa")
async def toggle_rowa_endpoint(enable: bool):
    """
    Endpoint para ativar ou desativar o protocolo ROWA.
    """
    if not rowa_manager:
        raise HTTPException(status_code=500, detail="ROWA Manager não inicializado")
    
    # Ativa ou desativa o ROWA
    rowa_manager.set_enabled(enable)
    
    # Retorna o novo estado
    return {
        "rowa_enabled": rowa_manager.is_enabled(),
        "message": f"Protocolo ROWA {'ativado' if enable else 'desativado'}"
    }

# Funções para inicialização
def initialize(learner_id: str, debug: bool = None):
    """
    Inicializa o learner e o ROWA manager.
    
    Args:
        learner_id: ID do learner
        debug: Flag para ativar modo de depuração
    """
    global learner, rowa_manager
    
    # Configura o logging
    if debug is None:
        debug = get_debug_mode()
    
    setup_logging(f"learner_{learner_id}", debug)
    
    # Cria as instâncias
    learner = Learner(learner_id, debug)
    rowa_manager = ROWAManager(learner_id, debug)
    
    logger.info(f"Learner {learner_id} inicializado (debug={debug})")

def set_acceptor_count(count: int):
    """
    Define o número total de acceptors.
    
    Args:
        count: Número total de acceptors
    """
    if learner:
        learner.set_acceptor_count(count)
    
    if rowa_manager:
        rowa_manager.set_acceptor_count(count)

def add_store(store_id: str, url: str):
    """
    Adiciona um store ao learner e ao ROWA manager.
    
    Args:
        store_id: ID do store
        url: URL base do store
    """
    if learner:
        learner.add_store(store_id, url)
    
    if rowa_manager:
        rowa_manager.add_store(store_id, url)

def add_client(client_id: str, url: str):
    """
    Adiciona um cliente ao learner e ao ROWA manager.
    
    Args:
        client_id: ID do cliente
        url: URL base do cliente
    """
    if learner:
        learner.add_client(client_id, url)
    
    if rowa_manager:
        rowa_manager.add_client(client_id, url)