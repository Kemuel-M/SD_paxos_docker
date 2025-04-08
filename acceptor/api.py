import asyncio
import json
import logging
import time
from typing import Dict, List, Optional

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from acceptor.acceptor import Acceptor
from common.utils import get_debug_mode, setup_logging

# Configura o logger
logger = logging.getLogger(__name__)

# Modelos de dados para as requisições e respostas
class PrepareRequest(BaseModel):
    proposal_id: str
    proposer_id: str
    tid: int

class PrepareResponse(BaseModel):
    success: bool
    tid: int
    message: str
    reason: Optional[str] = None
    accepted_tid: Optional[int] = None
    accepted_value: Optional[Dict] = None
    accepted_proposal_id: Optional[str] = None

class AcceptRequest(BaseModel):
    proposal_id: str
    proposer_id: str
    tid: int
    client_id: str
    resource_data: Dict
    timestamp: int

class AcceptResponse(BaseModel):
    success: bool
    tid: int
    message: str
    reason: Optional[str] = None

class StatusResponse(BaseModel):
    id: str
    highest_promised_tid: int
    accepted_tid: int
    accepted_proposal_id: Optional[str]
    active_promises: int
    total_accepted: int
    learners_count: int

class HealthResponse(BaseModel):
    status: str
    timestamp: float

# Variável global para a instância do Acceptor
acceptor: Optional[Acceptor] = None

# Cria a aplicação FastAPI
app = FastAPI(title="Acceptor API")

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
    Endpoint para processar mensagens PREPARE do protocolo Paxos.
    """
    if not acceptor:
        raise HTTPException(status_code=500, detail="Acceptor não inicializado")
    
    # Processa a mensagem PREPARE
    result = await acceptor.prepare(
        request.proposal_id, request.proposer_id, request.tid
    )
    
    # Retorna o resultado
    return result

@app.post("/accept", response_model=AcceptResponse)
async def accept_endpoint(request: AcceptRequest):
    """
    Endpoint para processar mensagens ACCEPT do protocolo Paxos.
    """
    if not acceptor:
        raise HTTPException(status_code=500, detail="Acceptor não inicializado")
    
    # Processa a mensagem ACCEPT
    result = await acceptor.accept(
        request.proposal_id, request.proposer_id, request.tid,
        request.client_id, request.resource_data, request.timestamp
    )
    
    # Retorna o resultado
    return result

@app.get("/status", response_model=StatusResponse)
async def status_endpoint():
    """
    Endpoint para obter o status do acceptor.
    """
    if not acceptor:
        raise HTTPException(status_code=500, detail="Acceptor não inicializado")
    
    # Obtém o status do acceptor
    status = acceptor.get_status()
    
    # Retorna o status
    return status

@app.get("/health", response_model=HealthResponse)
async def health_endpoint():
    """
    Endpoint para verificar a saúde do acceptor.
    """
    # Retorna sempre saudável com o timestamp atual
    return {
        "status": "healthy",
        "timestamp": time.time()
    }

@app.get("/logs", response_class=Response)
async def logs_endpoint(lines: int = 100):
    """
    Endpoint para obter os logs do acceptor.
    """
    try:
        # Obtém os N últimos logs do arquivo
        log_file = "logs/acceptor.log"
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
    if not acceptor:
        raise HTTPException(status_code=500, detail="Acceptor não inicializado")
    
    # Obtém o status da proposta
    proposal_status = acceptor.get_proposal_status(proposal_id)
    
    # Retorna o status da proposta
    return proposal_status

# Funções para inicialização
def initialize(acceptor_id: str, debug: bool = None):
    """
    Inicializa o acceptor.
    
    Args:
        acceptor_id: ID do acceptor
        debug: Flag para ativar modo de depuração
    """
    global acceptor
    
    # Configura o logging
    if debug is None:
        debug = get_debug_mode()
    
    setup_logging(f"acceptor_{acceptor_id}", debug)
    
    # Cria a instância do acceptor
    acceptor = Acceptor(acceptor_id, debug)
    
    logger.info(f"Acceptor {acceptor_id} inicializado (debug={debug})")

def add_learner(learner_id: str, url: str):
    """
    Adiciona um learner ao acceptor.
    
    Args:
        learner_id: ID do learner
        url: URL base do learner
    """
    if acceptor:
        acceptor.add_learner(learner_id, url)