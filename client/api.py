import asyncio
import json
import logging
import time
from typing import Dict, List, Optional

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from client.client import Client
from common.utils import get_debug_mode, setup_logging

# Configura o logger
logger = logging.getLogger(__name__)

# Modelos de dados para as requisições e respostas
class NotifyRequest(BaseModel):
    proposal_id: str
    tid: int
    status: str
    resource_data: Dict
    learner_id: str
    timestamp: float
    protocol: Optional[str] = None

class NotifyResponse(BaseModel):
    success: bool
    proposal_id: str
    client_id: str

class RequestData(BaseModel):
    resource_data: Optional[Dict] = None

class RequestResponse(BaseModel):
    success: bool
    proposal_id: Optional[str] = None
    error: Optional[str] = None

class StartRandomRequestsRequest(BaseModel):
    num_requests: Optional[int] = None

class StatusResponse(BaseModel):
    id: str
    running: bool
    total_requests: int
    successful_requests: int
    failed_requests: int
    proposer_url: Optional[str]
    min_wait: float
    max_wait: float
    min_requests: int
    max_requests: int

class HealthResponse(BaseModel):
    status: str
    timestamp: float

# Variável global para a instância do Client
client: Optional[Client] = None

# Cria a aplicação FastAPI
app = FastAPI(title="Client API")

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
@app.post("/notify", response_model=NotifyResponse)
async def notify_endpoint(request: NotifyRequest):
    """
    Endpoint para receber notificações dos learners.
    """
    if not client:
        raise HTTPException(status_code=500, detail="Cliente não inicializado")
    
    # Processa a notificação
    result = await client.handle_notification(
        request.proposal_id, request.tid, request.status,
        request.resource_data, request.learner_id, request.timestamp,
        request.protocol
    )
    
    # Retorna o resultado
    return result

@app.post("/request", response_model=RequestResponse)
async def request_endpoint(request: RequestData = None):
    """
    Endpoint para enviar um pedido de acesso ao recurso R.
    """
    if not client:
        raise HTTPException(status_code=500, detail="Cliente não inicializado")
    
    # Envia o pedido
    result = await client.send_request(
        resource_data=request.resource_data if request else None
    )
    
    # Retorna o resultado
    return result

@app.post("/start")
async def start_endpoint(request: StartRandomRequestsRequest = None):
    """
    Endpoint para iniciar o loop de envio de pedidos aleatórios.
    """
    if not client:
        raise HTTPException(status_code=500, detail="Cliente não inicializado")
    
    # Inicia o loop de pedidos
    num_requests = request.num_requests if request else None
    await client.start_random_requests(num_requests)
    
    # Retorna o status
    return {
        "success": True,
        "message": f"Iniciado envio de {num_requests or 'N'} pedidos aleatórios"
    }

@app.post("/stop")
async def stop_endpoint():
    """
    Endpoint para parar o loop de envio de pedidos.
    """
    if not client:
        raise HTTPException(status_code=500, detail="Cliente não inicializado")
    
    # Para o loop de pedidos
    client.stop_requests()
    
    # Retorna o status
    return {
        "success": True,
        "message": "Envio de pedidos interrompido"
    }

@app.get("/status", response_model=StatusResponse)
async def status_endpoint():
    """
    Endpoint para obter o status do cliente.
    """
    if not client:
        raise HTTPException(status_code=500, detail="Cliente não inicializado")
    
    # Obtém o status do cliente
    status = client.get_status()
    
    # Retorna o status
    return status

@app.get("/health", response_model=HealthResponse)
async def health_endpoint():
    """
    Endpoint para verificar a saúde do cliente.
    """
    # Retorna sempre saudável com o timestamp atual
    return {
        "status": "healthy",
        "timestamp": time.time()
    }

@app.get("/logs", response_class=Response)
async def logs_endpoint(lines: int = 100):
    """
    Endpoint para obter os logs do cliente.
    """
    try:
        # Obtém os N últimos logs do arquivo
        log_file = "logs/client.log"
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

@app.get("/history")
async def history_endpoint(limit: int = 10):
    """
    Endpoint para obter o histórico de pedidos.
    """
    if not client:
        raise HTTPException(status_code=500, detail="Cliente não inicializado")
    
    # Obtém os N últimos pedidos do histórico
    history = client.requests[-limit:] if len(client.requests) > limit else client.requests
    
    # Retorna o histórico
    return {
        "history": history,
        "total": len(client.requests)
    }

@app.put("/config")
async def config_endpoint(min_wait: float = None, max_wait: float = None, 
                      min_requests: int = None, max_requests: int = None):
    """
    Endpoint para configurar os parâmetros do cliente.
    """
    if not client:
        raise HTTPException(status_code=500, detail="Cliente não inicializado")
    
    # Atualiza os parâmetros
    if min_wait is not None:
        client.min_wait = min_wait
    
    if max_wait is not None:
        client.max_wait = max_wait
    
    if min_requests is not None:
        client.min_requests = min_requests
    
    if max_requests is not None:
        client.max_requests = max_requests
    
    # Retorna os novos parâmetros
    return {
        "min_wait": client.min_wait,
        "max_wait": client.max_wait,
        "min_requests": client.min_requests,
        "max_requests": client.max_requests
    }

# Funções para inicialização
def initialize(client_id: str, proposer_url: str = None, debug: bool = None):
    """
    Inicializa o cliente.
    
    Args:
        client_id: ID do cliente
        proposer_url: URL base do proposer
        debug: Flag para ativar modo de depuração
    """
    global client
    
    # Configura o logging
    if debug is None:
        debug = get_debug_mode()
    
    setup_logging(f"client_{client_id}", debug)
    
    # Cria a instância do cliente
    client = Client(client_id, debug)
    
    # Define o proposer, se fornecido
    if proposer_url:
        client.set_proposer(proposer_url)
    
    logger.info(f"Cliente {client_id} inicializado (debug={debug})")

def set_proposer(url: str):
    """
    Define o proposer que este cliente conhece.
    
    Args:
        url: URL base do proposer
    """
    if client:
        client.set_proposer(url)