"""
Servidor web para interface do cliente Paxos.
"""
import os
import time
import logging
import asyncio
import httpx
from typing import Dict, Any, List, Optional
from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

logger = logging.getLogger("client")

class WebServer:
    """
    Servidor web para interface do cliente Paxos.
    """
    def __init__(self, client, host: str = "0.0.0.0", port: int = 8081):
        """
        Inicializa o servidor web.
        
        Args:
            client: Instância do PaxosClient
            host: Host para o servidor web
            port: Porta para o servidor web
        """
        self.client = client
        self.host = host
        self.port = port
        self.http_client = httpx.AsyncClient(timeout=5.0)
        
        # Inicializa o servidor FastAPI
        self.app = FastAPI(title="Client Web Interface", 
                          description="Interface web para o cliente Paxos")
        
        # Configura middlewares
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
        
        # Configura arquivos estáticos
        self.app.mount("/static", StaticFiles(directory="static"), name="static")
        
        # Configura templates
        self.templates = Jinja2Templates(directory="templates")
        
        # Configura rotas
        self._setup_routes()
        
        # Cache para logs do sistema
        self.system_logs_cache = {}
        self.system_logs_timestamp = 0
        
        logger.info(f"Servidor web inicializado em {host}:{port}")
    
    def _setup_routes(self):
        """Configura as rotas da aplicação web."""
        # Página inicial (dashboard)
        @self.app.get("/", response_class=HTMLResponse)
        async def index(request: Request):
            status = self.client.get_status()
            recent_operations = self.client.get_history(10)
            return self.templates.TemplateResponse(
                "index.html", 
                {
                    "request": request, 
                    "status": status, 
                    "recent_operations": recent_operations,
                    "client_id": self.client.client_id
                }
            )
        
        # Listar recursos
        @self.app.get("/resources", response_class=HTMLResponse)
        async def resources(request: Request):
            resource_data = await self._fetch_resource_data()
            return self.templates.TemplateResponse(
                "resources.html",
                {
                    "request": request,
                    "resources": [resource_data],
                    "client_id": self.client.client_id
                }
            )
        
        # Visualizar/editar recurso
        @self.app.get("/resource/{resource_id}", response_class=HTMLResponse)
        async def resource(request: Request, resource_id: str):
            resource_data = await self._fetch_resource_data(resource_id)
            return self.templates.TemplateResponse(
                "resource.html",
                {
                    "request": request,
                    "resource": resource_data,
                    "client_id": self.client.client_id
                }
            )
        
        # Visualizar histórico de operações
        @self.app.get("/history", response_class=HTMLResponse)
        async def history(request: Request, limit: int = 100):
            operations = self.client.get_history(limit)
            return self.templates.TemplateResponse(
                "history.html",
                {
                    "request": request,
                    "operations": operations,
                    "client_id": self.client.client_id
                }
            )
        
        # Visualizar logs do cliente
        @self.app.get("/logs", response_class=HTMLResponse)
        async def logs(request: Request, limit: int = 100):
            from common.logging import get_log_entries
            logs = get_log_entries("client", limit=limit)
            return self.templates.TemplateResponse(
                "logs.html",
                {
                    "request": request,
                    "logs": logs,
                    "client_id": self.client.client_id
                }
            )
        
        # Visualizar logs do sistema
        @self.app.get("/system-logs", response_class=HTMLResponse)
        async def system_logs(request: Request, limit: int = 100):
            logs = await self._fetch_system_logs()
            return self.templates.TemplateResponse(
                "system_logs.html",
                {
                    "request": request,
                    "logs": logs,
                    "client_id": self.client.client_id
                }
            )
        
        # Visualizar logs importantes do sistema
        @self.app.get("/system-logs/important", response_class=HTMLResponse)
        async def system_logs_important(request: Request, limit: int = 100):
            logs = await self._fetch_system_logs_important()
            return self.templates.TemplateResponse(
                "system_logs_important.html",
                {
                    "request": request,
                    "logs": logs,
                    "client_id": self.client.client_id
                }
            )
        
        # API para obter status do cliente
        @self.app.get("/api/status")
        async def api_status():
            return self.client.get_status()
        
        # API para obter histórico de operações
        @self.app.get("/api/history")
        async def api_history(limit: int = 100):
            return {"operations": self.client.get_history(limit)}
        
        # API para iniciar o cliente
        @self.app.post("/api/start")
        async def api_start():
            await self.client.start()
            return {"status": "started"}
        
        # API para parar o cliente
        @self.app.post("/api/stop")
        async def api_stop():
            await self.client.stop()
            return {"status": "stopped"}
        
        # API para iniciar operação manual
        @self.app.post("/api/operation")
        async def api_operation(data: str):
            if not self.client.running:
                raise HTTPException(status_code=400, detail="Cliente não está em execução")
            
            # Cria ID para a operação manual
            operation_id = self.client.next_operation_id
            self.client.next_operation_id += 1
            
            # Cria payload para operação
            timestamp = int(time.time() * 1000)
            payload = {
                "clientId": self.client.client_id,
                "timestamp": timestamp,
                "operation": "WRITE",
                "resource": "R",
                "data": data
            }
            
            # Registra operação
            operation_info = {
                "id": operation_id,
                "start_time": time.time(),
                "payload": payload,
                "retries": 0,
                "status": "in_progress"
            }
            
            # Cria task para enviar operação
            asyncio.create_task(self.client._send_operation(operation_id))
            
            return {
                "status": "initiated",
                "operation_id": operation_id
            }
    
    async def _fetch_resource_data(self, resource_id: str = "R") -> Dict[str, Any]:
        """
        Obtém dados do recurso a partir do Cluster Store.
        
        Args:
            resource_id: ID do recurso
            
        Returns:
            Dict[str, Any]: Dados do recurso
        """
        try:
            # Tenta obter de qualquer nó do Cluster Store
            stores = os.getenv("STORES", "cluster-store-1:8080,cluster-store-2:8080,cluster-store-3:8080").split(",")
            
            for store in stores:
                try:
                    url = f"http://{store}/resource/{resource_id}"
                    response = await self.http_client.get(url, timeout=2.0)
                    
                    if response.status_code == 200:
                        return response.json()
                except Exception as e:
                    logger.debug(f"Erro ao obter recurso de {store}: {e}")
                    continue
            
            # Se não conseguir obter de nenhum nó, retorna dados simulados
            return {
                "data": "Dados simulados do recurso",
                "version": 0,
                "timestamp": int(time.time() * 1000),
                "node_id": 0
            }
        except Exception as e:
            logger.error(f"Erro ao obter dados do recurso: {e}")
            return {
                "data": "Erro ao obter dados do recurso",
                "version": 0,
                "timestamp": int(time.time() * 1000),
                "node_id": 0,
                "error": str(e)
            }
    
    async def _fetch_system_logs(self) -> List[Dict[str, Any]]:
        """
        Obtém logs de todos os componentes do sistema.
        
        Returns:
            List[Dict[str, Any]]: Logs consolidados
        """
        # Verifica se o cache ainda é válido (5 segundos)
        current_time = time.time()
        if current_time - self.system_logs_timestamp < 5.0 and self.system_logs_cache:
            return self.system_logs_cache
        
        try:
            # Lista de componentes para coletar logs
            components = {
                "proposers": os.getenv("PROPOSERS", "proposer-1:8080,proposer-2:8080,proposer-3:8080,proposer-4:8080,proposer-5:8080").split(","),
                "acceptors": os.getenv("ACCEPTORS", "acceptor-1:8080,acceptor-2:8080,acceptor-3:8080,acceptor-4:8080,acceptor-5:8080").split(","),
                "learners": os.getenv("LEARNERS", "learner-1:8080,learner-2:8080").split(","),
                "stores": os.getenv("STORES", "cluster-store-1:8080,cluster-store-2:8080,cluster-store-3:8080").split(",")
            }
            
            logs = []
            
            # Coleta logs de cada componente
            for component_type, endpoints in components.items():
                for endpoint in endpoints:
                    try:
                        url = f"http://{endpoint}/logs"
                        response = await self.http_client.get(url, timeout=1.0)
                        
                        if response.status_code == 200:
                            component_logs = response.json().get("logs", [])
                            
                            # Adiciona tipo de componente aos logs
                            for log in component_logs:
                                log["component_type"] = component_type
                                log["endpoint"] = endpoint
                            
                            logs.extend(component_logs)
                    except Exception as e:
                        logger.debug(f"Erro ao obter logs de {endpoint}: {e}")
                        continue
            
            # Ordena por timestamp
            logs.sort(key=lambda x: x.get("timestamp", 0), reverse=True)
            
            # Atualiza cache
            self.system_logs_cache = logs
            self.system_logs_timestamp = current_time
            
            return logs
            
        except Exception as e:
            logger.error(f"Erro ao obter logs do sistema: {e}")
            return []
    
    async def _fetch_system_logs_important(self) -> List[Dict[str, Any]]:
        """
        Obtém logs importantes de todos os componentes do sistema.
        
        Returns:
            List[Dict[str, Any]]: Logs importantes consolidados
        """
        logs = await self._fetch_system_logs()
        
        # Filtra logs importantes
        important_levels = ["IMPORTANT", "WARNING", "ERROR", "CRITICAL"]
        return [log for log in logs if log.get("level") in important_levels]
    
    def start(self):
        """Inicia o servidor web."""
        import uvicorn
        logger.info(f"Iniciando servidor web em {self.host}:{self.port}")
        uvicorn.run(self.app, host=self.host, port=self.port)