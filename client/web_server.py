import asyncio
import json
import logging
import os
from typing import Dict, List, Optional

from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from client.client import Client
from common.utils import get_debug_mode, http_request, setup_logging

# Configura o logger
logger = logging.getLogger(__name__)

# Variáveis globais
client: Optional[Client] = None
connected_websockets: List[WebSocket] = []

# Cria a aplicação FastAPI
web_app = FastAPI(title="Client Web Server")

# Configura os diretórios de templates e arquivos estáticos
templates_dir = os.path.join(os.path.dirname(__file__), "templates")
static_dir = os.path.join(os.path.dirname(__file__), "static")

templates = Jinja2Templates(directory=templates_dir)
web_app.mount("/static", StaticFiles(directory=static_dir), name="static")

# Rotas da interface web
@web_app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """
    Página inicial da interface web.
    """
    if not client:
        raise HTTPException(status_code=500, detail="Cliente não inicializado")
    
    # Obtém o status do cliente
    status = client.get_status()
    
    # Renderiza o template
    return templates.TemplateResponse(
        "index.html",
        {"request": request, "client": status}
    )

@web_app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    """
    Dashboard da interface web.
    """
    if not client:
        raise HTTPException(status_code=500, detail="Cliente não inicializado")
    
    # Obtém o status do cliente
    status = client.get_status()
    
    # Obtém o histórico de pedidos
    history = client.requests[-20:] if len(client.requests) > 20 else client.requests
    
    # Renderiza o template
    return templates.TemplateResponse(
        "dashboard.html",
        {"request": request, "client": status, "history": history}
    )

@web_app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """
    Endpoint WebSocket para comunicação em tempo real.
    """
    await websocket.accept()
    
    # Adiciona o websocket à lista de conexões
    connected_websockets.append(websocket)
    
    try:
        # Envia o status inicial
        if client:
            status = client.get_status()
            await websocket.send_json({
                "type": "status",
                "data": status
            })
        
        # Loop para receber mensagens do cliente
        while True:
            data = await websocket.receive_text()
            message = json.loads(data)
            
            # Processa a mensagem recebida
            if message.get("type") == "request":
                # Envia um pedido único
                if client:
                    resource_data = message.get("resource_data")
                    result = await client.send_request(resource_data)
                    await websocket.send_json({
                        "type": "request_result",
                        "data": result
                    })
            
            elif message.get("type") == "start":
                # Inicia o loop de pedidos
                if client:
                    num_requests = message.get("num_requests")
                    await client.start_random_requests(num_requests)
                    await websocket.send_json({
                        "type": "started",
                        "data": {
                            "num_requests": num_requests or "aleatório"
                        }
                    })
            
            elif message.get("type") == "stop":
                # Para o loop de pedidos
                if client:
                    client.stop_requests()
                    await websocket.send_json({
                        "type": "stopped",
                        "data": {}
                    })
            
            elif message.get("type") == "get_status":
                # Obtém o status atual
                if client:
                    status = client.get_status()
                    await websocket.send_json({
                        "type": "status",
                        "data": status
                    })
            
            elif message.get("type") == "get_history":
                # Obtém o histórico de pedidos
                if client:
                    limit = message.get("limit", 10)
                    history = client.requests[-limit:] if len(client.requests) > limit else client.requests
                    await websocket.send_json({
                        "type": "history",
                        "data": {
                            "history": history,
                            "total": len(client.requests)
                        }
                    })
    
    except WebSocketDisconnect:
        # Remove o websocket da lista de conexões
        if websocket in connected_websockets:
            connected_websockets.remove(websocket)
    
    except Exception as e:
        logger.error(f"Erro no WebSocket: {str(e)}")
        if websocket in connected_websockets:
            connected_websockets.remove(websocket)

async def broadcast_event(event_type: str, data: Dict):
    """
    Envia um evento para todos os websockets conectados.
    
    Args:
        event_type: Tipo de evento
        data: Dados do evento
    """
    if not connected_websockets:
        return
    
    message = {
        "type": event_type,
        "data": data
    }
    
    # Envia a mensagem para todos os websockets
    for websocket in connected_websockets:
        try:
            await websocket.send_json(message)
        except Exception as e:
            logger.error(f"Erro ao enviar mensagem para websocket: {str(e)}")
            # Remove o websocket da lista de conexões
            if websocket in connected_websockets:
                connected_websockets.remove(websocket)

def client_event_handler(event: str, data: Dict):
    """
    Handler para eventos do cliente.
    
    Args:
        event: Tipo de evento
        data: Dados do evento
    """
    # Cria uma task para broadcast do evento
    asyncio.create_task(broadcast_event(event, data))

def initialize(client_instance: Client = None):
    """
    Inicializa o servidor web.
    
    Args:
        client_instance: Instância do cliente
    """
    global client
    
    # Usa a instância fornecida ou usa a global
    client = client_instance
    
    if client:
        # Registra o handler de eventos
        client.add_listener(client_event_handler)
        
        logger.info(f"Servidor web inicializado para cliente {client.id}")
    else:
        logger.warning("Servidor web inicializado sem cliente")