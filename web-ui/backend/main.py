import os
import json
import asyncio
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
import aiohttp
from typing import Dict, List, Optional, Union, Any

# Inicializa a aplicação FastAPI
app = FastAPI(title="Paxos Web UI")

# Configurar CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Montar arquivos estáticos do React
app.mount("/static", StaticFiles(directory="static"), name="static")

# Lista de endereços dos proposers
PROPOSERS = [f"proposer-{i}:8080" for i in range(1, 6)]
# Lista de endereços dos learners
LEARNERS = [f"learner-{i}:8080" for i in range(1, 4)]
# Lista de endereços dos acceptors
ACCEPTORS = [f"acceptor-{i}:8080" for i in range(1, 6)]

# Gerenciador de conexões WebSocket
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception as e:
                print(f"Error broadcasting message: {e}")

manager = ConnectionManager()

# Função para escolher um proposer ativo - round robin simples
current_proposer_index = 0
async def get_active_proposer():
    global current_proposer_index
    try:
        # Atualiza o índice de forma circular
        current_proposer_index = (current_proposer_index + 1) % len(PROPOSERS)
        return PROPOSERS[current_proposer_index]
    except Exception as e:
        print(f"Error selecting proposer: {e}")
        # Fallback para o primeiro proposer
        return PROPOSERS[0]

# Função para escolher um learner ativo - round robin simples
current_learner_index = 0
async def get_active_learner():
    global current_learner_index
    try:
        # Atualiza o índice de forma circular
        current_learner_index = (current_learner_index + 1) % len(LEARNERS)
        return LEARNERS[current_learner_index]
    except Exception as e:
        print(f"Error selecting learner: {e}")
        # Fallback para o primeiro learner
        return LEARNERS[0]

# Rota principal - serve o frontend React
@app.get("/{full_path:path}")
async def serve_frontend(full_path: str):
    # Retorna o index.html para todas as rotas, deixando o React lidar com o roteamento no cliente
    return FileResponse("static/index.html")

# API para propor operações de arquivo (via proposers)
@app.post("/api/files/propose")
async def propose_file_operation(request: Request):
    operation = await request.json()
    
    # Seleciona um proposer ativo
    proposer_url = await get_active_proposer()
    
    # Encaminha a operação para o proposer
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"http://{proposer_url}/propose",
                json=operation,
                timeout=5
            ) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    return JSONResponse(
                        status_code=response.status,
                        content={"error": f"Proposer error: {response.status}"}
                    )
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": f"Failed to communicate with proposer: {str(e)}"}
        )

# API para listar arquivos (via learners)
@app.get("/api/files")
async def list_files(directory: str = "/"):
    # Seleciona um learner ativo
    learner_url = await get_active_learner()
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"http://{learner_url}/files?directory={directory}",
                timeout=5
            ) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    return JSONResponse(
                        status_code=response.status,
                        content={"error": f"Learner error: {response.status}"}
                    )
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": f"Failed to communicate with learner: {str(e)}"}
        )

# API para ler conteúdo de arquivo (via learners)
@app.get("/api/files/{file_path:path}")
async def read_file(file_path: str):
    # Normalizar o caminho
    if not file_path.startswith("/"):
        file_path = "/" + file_path
    
    # Seleciona um learner ativo
    learner_url = await get_active_learner()
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"http://{learner_url}/files{file_path}",
                timeout=5
            ) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    return JSONResponse(
                        status_code=response.status,
                        content={"error": f"Learner error: {response.status}"}
                    )
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": f"Failed to communicate with learner: {str(e)}"}
        )

# API para obter status do sistema
@app.get("/api/system/status")
async def system_status():
    results = {
        "proposers": {},
        "acceptors": {},
        "learners": {}
    }
    
    # Verificar status dos proposers
    for i in range(1, 6):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"http://proposer-{i}:8080/status",
                    timeout=2
                ) as response:
                    if response.status == 200:
                        results["proposers"][i] = await response.json()
                    else:
                        results["proposers"][i] = {
                            "status": "error",
                            "code": response.status
                        }
        except Exception as e:
            results["proposers"][i] = {
                "status": "error",
                "message": str(e)
            }
    
    # Verificar status dos acceptors
    for i in range(1, 6):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"http://acceptor-{i}:8080/status",
                    timeout=2
                ) as response:
                    if response.status == 200:
                        results["acceptors"][i] = await response.json()
                    else:
                        results["acceptors"][i] = {
                            "status": "error",
                            "code": response.status
                        }
        except Exception as e:
            results["acceptors"][i] = {
                "status": "error",
                "message": str(e)
            }
    
    # Verificar status dos learners
    for i in range(1, 4):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"http://learner-{i}:8080/status",
                    timeout=2
                ) as response:
                    if response.status == 200:
                        results["learners"][i] = await response.json()
                    else:
                        results["learners"][i] = {
                            "status": "error",
                            "code": response.status
                        }
        except Exception as e:
            results["learners"][i] = {
                "status": "error",
                "message": str(e)
            }
    
    return results

# WebSocket para atualizações em tempo real
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    
    # Conectar aos WebSockets dos learners para receber atualizações
    learner_connections = []
    
    # Inicializar a lista de tarefas antes do bloco try para evitar referências não definidas
    tasks = []
    
    try:
        # Conectar aos WebSockets de todos os learners
        async def connect_to_learner(learner_url):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(f"ws://{learner_url}/ws") as ws:
                        while True:
                            msg = await ws.receive_json()
                            # Repassar mensagem para o cliente
                            await websocket.send_json(msg)
            except Exception as e:
                print(f"Error in learner websocket connection: {e}")
        
        # Criar tarefas para conectar a cada learner
        for learner_url in LEARNERS:
            task = asyncio.create_task(connect_to_learner(learner_url))
            tasks.append(task)
        
        # Aguardar mensagens do cliente
        while True:
            try:
                data = await websocket.receive_text()
                # Processar comandos do cliente, se necessário
                try:
                    message = json.loads(data)
                    # Aqui você pode adicionar lógica para processar comandos do cliente
                    print(f"Received message from client: {message}")
                except json.JSONDecodeError:
                    print(f"Received invalid JSON from client: {data}")
            except WebSocketDisconnect:
                break
    
    except WebSocketDisconnect:
        manager.disconnect(websocket)
        # Cancelar tarefas de conexão com learners
        for task in tasks:
            task.cancel()
    
    except Exception as e:
        print(f"Error in WebSocket connection: {e}")
        manager.disconnect(websocket)
        # Cancelar tarefas de conexão com learners
        for task in tasks:
            task.cancel()

# Rota para criar um novo arquivo
@app.post("/api/files/create")
async def create_file(request: Request):
    data = await request.json()
    path = data.get("path")
    content = data.get("content", "")
    
    # Verifica se o caminho foi fornecido
    if not path:
        return JSONResponse(
            status_code=400,
            content={"error": "Path is required"}
        )
    
    # Prepara a operação para o proposer
    operation = {
        "operation": "CREATE",
        "path": path,
        "content": content
    }
    
    # Encaminha para a rota de proposta
    return await propose_file_operation(Request(scope={"type": "http"}, receive=None, send=None))

# Rota para modificar um arquivo existente
@app.post("/api/files/modify")
async def modify_file(request: Request):
    data = await request.json()
    path = data.get("path")
    content = data.get("content", "")
    
    # Verifica se o caminho foi fornecido
    if not path:
        return JSONResponse(
            status_code=400,
            content={"error": "Path is required"}
        )
    
    # Prepara a operação para o proposer
    operation = {
        "operation": "MODIFY",
        "path": path,
        "content": content
    }
    
    # Encaminha para a rota de proposta
    return await propose_file_operation(Request(scope={"type": "http"}, receive=None, send=None))

# Rota para excluir um arquivo ou diretório
@app.delete("/api/files/{file_path:path}")
async def delete_file(file_path: str):
    # Normalizar o caminho
    if not file_path.startswith("/"):
        file_path = "/" + file_path
    
    # Prepara a operação para o proposer
    operation = {
        "operation": "DELETE",
        "path": file_path
    }
    
    # Encaminha para a rota de proposta
    return await propose_file_operation(Request(scope={"type": "http"}, receive=None, send=None))

# Iniciado apenas se executado diretamente (não como módulo)
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=80)