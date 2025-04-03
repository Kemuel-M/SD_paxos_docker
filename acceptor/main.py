"""
Ponto de entrada da aplicação FastAPI para o componente Acceptor.
"""
import logging
from typing import Dict, Any

from fastapi import FastAPI, HTTPException, Response, Request
from fastapi.middleware.gzip import GZipMiddleware
from prometheus_client import start_http_server
import uvicorn
import json
import time
import asyncio

from common.logging import configure_logger, log_with_context
from common.models import PrepareRequest, AcceptRequest, HealthResponse
from acceptor.config import NODE_ID, HOST, PORT, HEARTBEAT_INTERVAL
from acceptor.acceptor import Acceptor


# Configurar logger
logger = configure_logger("acceptor", NODE_ID)

# Criar aplicação FastAPI
app = FastAPI(
    title=f"Acceptor {NODE_ID}", 
    description="Acceptor do protocolo Paxos",
    version="1.0.0"
)

# Adicionar middleware para compressão gzip
app.add_middleware(GZipMiddleware, minimum_size=1024)

# Instância global
acceptor = None
heartbeat_task = None


@app.on_event("startup")
async def startup_event():
    """
    Inicializa componentes na inicialização da aplicação.
    """
    global acceptor, heartbeat_task
    
    # Iniciar servidor Prometheus em porta separada
    start_http_server(9090 + NODE_ID)
    
    # Inicializar acceptor
    acceptor = Acceptor(logger)
    await acceptor.initialize()
    
    # Iniciar tarefa de heartbeat
    heartbeat_task = asyncio.create_task(send_heartbeats())
    
    logger.info(f"Acceptor {NODE_ID} iniciado")


@app.on_event("shutdown")
async def shutdown_event():
    """
    Finaliza componentes no encerramento da aplicação.
    """
    global acceptor, heartbeat_task
    
    # Cancelar tarefa de heartbeat
    if heartbeat_task:
        heartbeat_task.cancel()
        try:
            await heartbeat_task
        except asyncio.CancelledError:
            pass
    
    # Finalizar acceptor
    if acceptor:
        await acceptor.shutdown()
        
    logger.info(f"Acceptor {NODE_ID} encerrado")


async def send_heartbeats():
    """
    Tarefa de background para enviar heartbeats para outros componentes.
    """
    while True:
        try:
            logger.debug("Enviando heartbeats para monitoramento")
            # Se fosse necessário enviar heartbeats para outros componentes,
            # este seria o lugar para implementar a lógica
            
            # Aguardar o intervalo de heartbeat
            await asyncio.sleep(HEARTBEAT_INTERVAL)
        except asyncio.CancelledError:
            logger.info("Tarefa de heartbeat cancelada")
            break
        except Exception as e:
            logger.error(f"Erro na tarefa de heartbeat: {e}")
            await asyncio.sleep(1)  # Evitar loop muito rápido em caso de erro


@app.middleware("http")
async def log_requests(request: Request, call_next):
    """
    Middleware para logging de requisições HTTP.
    """
    start_time = time.time()
    
    # Registrar início da requisição
    log_with_context(
        logger,
        logging.INFO,
        f"Requisição recebida: {request.method} {request.url.path}",
        {
            "method": request.method,
            "path": request.url.path,
            "query_params": dict(request.query_params),
            "client_host": request.client.host if request.client else None,
        }
    )
    
    # Processar a requisição
    try:
        response = await call_next(request)
        
        # Registrar resposta
        duration = time.time() - start_time
        log_with_context(
            logger,
            logging.INFO,
            f"Resposta enviada: {response.status_code} em {duration:.3f}s",
            {
                "status_code": response.status_code,
                "duration": duration,
                "content_length": response.headers.get("content-length", 0),
            }
        )
        
        return response
    except Exception as e:
        # Registrar erro
        duration = time.time() - start_time
        log_with_context(
            logger,
            logging.ERROR,
            f"Erro ao processar requisição: {str(e)}",
            {
                "duration": duration,
                "error": str(e),
                "error_type": type(e).__name__,
            }
        )
        
        # Retornar erro 500 para o cliente
        return Response(
            content=json.dumps({"detail": "Internal Server Error"}),
            status_code=500,
            media_type="application/json"
        )


@app.post("/prepare", status_code=200)
async def prepare(request: PrepareRequest):
    """
    Endpoint para receber requisições prepare.
    
    Args:
        request: Requisição prepare.
        
    Returns:
        Resposta promise ou not promise.
    """
    global acceptor
    
    logger.info(f"Requisição prepare recebida: proposal_number={request.proposalNumber}, "
              f"instance_id={request.instanceId}, proposer_id={request.proposerId}")
    
    # Processar prepare
    response = await acceptor.prepare(request)
    
    return response.dict()


@app.post("/accept", status_code=200)
async def accept(request: AcceptRequest):
    """
    Endpoint para receber requisições accept.
    
    Args:
        request: Requisição accept.
        
    Returns:
        Resposta accepted ou not accepted.
    """
    global acceptor
    
    logger.info(f"Requisição accept recebida: proposal_number={request.proposalNumber}, "
              f"instance_id={request.instanceId}, proposer_id={request.proposerId}")
    
    # Processar accept
    response = await acceptor.accept(request)
    
    return response.dict()


@app.get("/status", status_code=200)
async def get_status():
    """
    Endpoint para verificar o status do acceptor.
    
    Returns:
        Status do acceptor.
    """
    global acceptor
    
    if not acceptor:
        raise HTTPException(status_code=503, detail="Acceptor not initialized")
    
    return acceptor.get_status()


@app.get("/health", status_code=200, response_model=HealthResponse)
async def health_check():
    """
    Endpoint para verificação de saúde (heartbeat).
    
    Returns:
        Status "healthy" se o acceptor estiver saudável.
    """
    global acceptor
    
    if not acceptor:
        raise HTTPException(status_code=503, detail="Acceptor not initialized")
    
    return {"status": "healthy"}


if __name__ == "__main__":
    uvicorn.run("main:app", host=HOST, port=PORT)