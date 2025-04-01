import os
import yaml
import json
import time
import asyncio
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import structlog
from prometheus_client import Counter, Gauge, Histogram, generate_latest
import aiohttp
from typing import Dict, List, Set, Optional, Any, Union
from tinydb import TinyDB, Query
import aiofiles
import os.path

# Configurar logging estruturado
log = structlog.get_logger()

# Carregar configuração
def load_config():
    with open("config/config.yaml", "r") as f:
        config = yaml.safe_load(f)
    
    # Substituir variáveis de ambiente
    config["node"]["id"] = int(os.environ.get("NODE_ID", "1"))
    
    return config

config = load_config()

# Inicializar a aplicação FastAPI
app = FastAPI(title="Paxos Learner")

# Configurar CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Métricas Prometheus
decisions_total = Counter("paxos_learner_decisions_total", "Total number of decisions learned")
file_operations = Counter("paxos_learner_file_operations_total", "File operations performed", ["operation"])
sync_count = Counter("paxos_learner_sync_count", "Number of synchronizations with other learners")
sync_instances = Counter("paxos_learner_sync_instances", "Number of instances synchronized from other learners")
last_applied = Gauge("paxos_learner_last_applied_instance", "Last applied instance ID")
decision_latency = Histogram("paxos_learner_decision_latency_seconds", "Time between first acceptance and reaching quorum")
apply_latency = Histogram("paxos_learner_apply_latency_seconds", "Time to apply a decision to the filesystem")
pending_decisions = Gauge("paxos_learner_pending_decisions", "Number of decisions pending application")
active_websockets = Gauge("paxos_learner_active_websockets", "Number of active WebSocket connections")
file_count = Gauge("paxos_learner_file_count", "Number of files in the shared filesystem")
directory_count = Gauge("paxos_learner_directory_count", "Number of directories in the shared filesystem")

# Modelos de dados Pydantic
class LearnRequest(BaseModel):
    """Modelo para notificações de aceitação de propostas por acceptors"""
    instance_id: int
    proposal_number: int
    acceptor_id: int
    value: Dict[str, Any]

class DecisionInfo(BaseModel):
    """Modelo para informações de sincronização entre learners"""
    instance_id: int
    proposal_number: int
    value: Dict[str, Any]
    decided_at: float

class SyncRequest(BaseModel):
    """Modelo para solicitações de sincronização entre learners"""
    from_instance: int
    to_instance: Optional[int] = None
    learner_id: int

class FileOperation(BaseModel):
    """Modelo para operações de arquivo"""
    operation: str  # "CREATE", "MODIFY", "DELETE"
    path: str
    content: Optional[str] = None

# Gerenciador de conexões WebSocket
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        active_websockets.set(len(self.active_connections))
        log.info("WebSocket connection established", 
                active_connections=len(self.active_connections))

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)
        active_websockets.set(len(self.active_connections))
        log.info("WebSocket connection closed", 
                active_connections=len(self.active_connections))

    async def broadcast(self, message: dict):
        disconnected = []
        for i, connection in enumerate(self.active_connections):
            try:
                await connection.send_json(message)
            except Exception as e:
                log.error("Error broadcasting to WebSocket", 
                         error=str(e), 
                         connection_index=i)
                disconnected.append(connection)
        
        # Remover conexões desconectadas
        for conn in disconnected:
            if conn in self.active_connections:
                self.active_connections.remove(conn)
        
        active_websockets.set(len(self.active_connections))

manager = ConnectionManager()

# Estado do learner
class LearnerState:
    def __init__(self, node_id: int, quorum_size: int):
        self.node_id = node_id
        self.quorum_size = quorum_size
        self.decisions = {}  # instanceId -> {value, proposal_number, decided_at}
        self.acceptances = {}  # instanceId -> {proposal_number -> {acceptor_ids, first_seen_at}}
        self.last_applied_instance = 0
        self.sync_in_progress = False
        
        # Garantir que o diretório de dados existe
        os.makedirs("/app/data", exist_ok=True)
        
        self.db = TinyDB("/app/data/learner.json")
        self.load_state()
        
        # Atualizar métrica de última instância aplicada
        last_applied.set(self.last_applied_instance)
    
    def load_state(self):
        """Carrega o estado persistido do disco"""
        try:
            log.info("Loading state from disk")
            decisions_table = self.db.table("decisions")
            
            for decision in decisions_table.all():
                self.decisions[decision["instance_id"]] = {
                    "value": decision["value"],
                    "proposal_number": decision["proposal_number"],
                    "decided_at": decision["decided_at"]
                }
                
                # Atualizar última instância aplicada
                if decision["instance_id"] > self.last_applied_instance and decision.get("applied", False):
                    self.last_applied_instance = decision["instance_id"]
            
            log.info("State loaded successfully", 
                    decisions_count=len(self.decisions),
                    last_applied_instance=self.last_applied_instance)
        except Exception as e:
            log.error("Error loading state", error=str(e))
    
    async def save_decision(self, instance_id: int, proposal_number: int, value: Dict[str, Any], applied: bool = False):
        """Persiste uma decisão no disco"""
        try:
            decisions_table = self.db.table("decisions")
            Decision = Query()
            
            decisions_table.upsert(
                {
                    "instance_id": instance_id,
                    "proposal_number": proposal_number,
                    "value": value,
                    "decided_at": time.time(),
                    "applied": applied
                },
                Decision.instance_id == instance_id
            )
            
            log.debug("Decision saved to disk", 
                     instance_id=instance_id,
                     applied=applied)
        except Exception as e:
            log.error("Error saving decision", 
                     instance_id=instance_id, 
                     error=str(e))
    
    async def mark_as_applied(self, instance_id: int):
        """Marca uma decisão como aplicada no disco"""
        try:
            decisions_table = self.db.table("decisions")
            Decision = Query()
            
            decisions_table.update(
                {"applied": True},
                Decision.instance_id == instance_id
            )
            
            log.debug("Decision marked as applied", instance_id=instance_id)
        except Exception as e:
            log.error("Error marking decision as applied", 
                     instance_id=instance_id, 
                     error=str(e))
    
    def is_decided(self, instance_id: int) -> bool:
        """Verifica se uma instância já foi decidida"""
        return instance_id in self.decisions
    
    def record_acceptance(self, instance_id: int, proposal_number: int, acceptor_id: int) -> bool:
        """Registra a aceitação de um acceptor e verifica se atingimos quórum"""
        # Inicializar estruturas se não existirem
        if instance_id not in self.acceptances:
            self.acceptances[instance_id] = {}
        
        if proposal_number not in self.acceptances[instance_id]:
            self.acceptances[instance_id][proposal_number] = {
                "acceptor_ids": set(),
                "first_seen_at": time.time()
            }
        
        # Registrar aceitação deste acceptor
        self.acceptances[instance_id][proposal_number]["acceptor_ids"].add(acceptor_id)
        
        # Verificar se atingimos quórum
        quorum_reached = len(self.acceptances[instance_id][proposal_number]["acceptor_ids"]) >= self.quorum_size
        
        if quorum_reached:
            # Calcular latência da decisão
            start_time = self.acceptances[instance_id][proposal_number]["first_seen_at"]
            decision_latency.observe(time.time() - start_time)
        
        return quorum_reached
    
    async def cleanup_old_acceptances(self):
        """Limpar registros de aceitação antigos para economizar memória"""
        # Limpar apenas informações de aceitação para instâncias já decididas
        keys_to_remove = []
        
        for instance_id in self.acceptances:
            if instance_id in self.decisions:
                keys_to_remove.append(instance_id)
        
        for key in keys_to_remove:
            del self.acceptances[key]
        
        log.debug("Cleaned up old acceptances", 
                 removed_count=len(keys_to_remove),
                 remaining_count=len(self.acceptances))

# Inicializar estado do learner
state = LearnerState(config["node"]["id"], quorum_size=3)  # Quórum de 3 (maioria de 5 acceptors)

# Gerenciador de sistema de arquivos
class FileSystemManager:
    def __init__(self, shared_path: str):
        self.shared_path = shared_path
        # Garantir que o diretório compartilhado existe
        os.makedirs(shared_path, exist_ok=True)
        # Inicializar métricas de contagem de arquivos/diretórios
        self.update_file_counts()
    
    def update_file_counts(self):
        """Atualiza métricas de contagem de arquivos e diretórios"""
        files = 0
        directories = 0
        
        for root, dirs, filenames in os.walk(self.shared_path):
            directories += len(dirs)
            files += len(filenames)
        
        file_count.set(files)
        directory_count.set(directories)
    
    async def create_file(self, path: str, content: str = "") -> Dict[str, Any]:
        """Cria um novo arquivo no sistema de arquivos compartilhado"""
        start_time = time.time()
        
        try:
            # Normalizar caminho
            norm_path = self._normalize_path(path)
            full_path = os.path.join(self.shared_path, norm_path)
            directory = os.path.dirname(full_path)
            
            # Criar diretório se não existir
            if not os.path.exists(directory):
                os.makedirs(directory, exist_ok=True)
            
            # Escrever conteúdo no arquivo
            async with aiofiles.open(full_path, 'w') as f:
                await f.write(content)
            
            file_operations.labels(operation="create").inc()
            self.update_file_counts()
            
            log.info("File created", 
                    path=path, 
                    size=len(content))
            
            return {"success": True, "path": path}
        except Exception as e:
            log.error("Error creating file", 
                     path=path, 
                     error=str(e))
            
            return {"success": False, "error": str(e)}
        finally:
            apply_latency.observe(time.time() - start_time)
    
    async def modify_file(self, path: str, content: str) -> Dict[str, Any]:
        """Modifica um arquivo existente no sistema de arquivos compartilhado"""
        start_time = time.time()
        
        try:
            # Normalizar caminho
            norm_path = self._normalize_path(path)
            full_path = os.path.join(self.shared_path, norm_path)
            
            # Verificar se arquivo existe
            if not os.path.exists(full_path):
                log.warning("File not found for modification", path=path)
                return {"success": False, "error": "File not found"}
            
            # Atualizar conteúdo
            async with aiofiles.open(full_path, 'w') as f:
                await f.write(content)
            
            file_operations.labels(operation="modify").inc()
            
            log.info("File modified", 
                    path=path, 
                    size=len(content))
            
            return {"success": True, "path": path}
        except Exception as e:
            log.error("Error modifying file", 
                     path=path, 
                     error=str(e))
            
            return {"success": False, "error": str(e)}
        finally:
            apply_latency.observe(time.time() - start_time)
    
    async def delete_file(self, path: str) -> Dict[str, Any]:
        """Exclui um arquivo do sistema de arquivos compartilhado"""
        start_time = time.time()
        
        try:
            # Normalizar caminho
            norm_path = self._normalize_path(path)
            full_path = os.path.join(self.shared_path, norm_path)
            
            # Verificar se caminho existe
            if not os.path.exists(full_path):
                log.warning("Path not found for deletion", path=path)
                return {"success": False, "error": "Path not found"}
            
            # Verificar se é um diretório ou arquivo
            if os.path.isdir(full_path):
                # Remover diretório e conteúdo
                import shutil
                shutil.rmtree(full_path)
            else:
                # Remover arquivo
                os.remove(full_path)
            
            file_operations.labels(operation="delete").inc()
            self.update_file_counts()
            
            log.info("Path deleted", path=path)
            
            return {"success": True, "path": path}
        except Exception as e:
            log.error("Error deleting path", 
                     path=path, 
                     error=str(e))
            
            return {"success": False, "error": str(e)}
        finally:
            apply_latency.observe(time.time() - start_time)
    
    async def list_files(self, directory: str = "/") -> Dict[str, Any]:
        """Lista arquivos em um diretório do sistema de arquivos compartilhado"""
        try:
            # Normalizar caminho
            norm_path = self._normalize_path(directory)
            full_path = os.path.join(self.shared_path, norm_path)
            
            if not os.path.exists(full_path):
                log.warning("Directory not found", directory=directory)
                return {"success": False, "error": "Directory not found"}
            
            if not os.path.isdir(full_path):
                log.warning("Path is not a directory", path=directory)
                return {"success": False, "error": "Path is not a directory"}
            
            result = []
            for item in os.listdir(full_path):
                item_path = os.path.join(full_path, item)
                rel_path = os.path.join(directory, item).replace(self.shared_path, "")
                
                # Garantir que o caminho começa com /
                if not rel_path.startswith("/"):
                    rel_path = "/" + rel_path
                
                # Normalizar para evitar caminhos com //
                rel_path = os.path.normpath(rel_path)
                
                if os.path.isdir(item_path):
                    result.append({
                        "name": item,
                        "path": rel_path,
                        "type": "directory"
                    })
                else:
                    stat = os.stat(item_path)
                    result.append({
                        "name": item,
                        "path": rel_path,
                        "type": "file",
                        "size": stat.st_size,
                        "modified": stat.st_mtime
                    })
            
            file_operations.labels(operation="list").inc()
            
            # Ordenar por tipo (diretórios primeiro) e depois por nome
            result.sort(key=lambda x: (0 if x["type"] == "directory" else 1, x["name"]))
            
            return {"success": True, "directory": directory, "items": result}
        except Exception as e:
            log.error("Error listing directory", 
                     directory=directory, 
                     error=str(e))
            
            return {"success": False, "error": str(e)}
    
    async def read_file(self, path: str) -> Dict[str, Any]:
        """Lê o conteúdo de um arquivo"""
        try:
            # Normalizar caminho
            norm_path = self._normalize_path(path)
            full_path = os.path.join(self.shared_path, norm_path)
            
            if not os.path.exists(full_path):
                log.warning("File not found", path=path)
                return {"success": False, "error": "File not found"}
            
            if os.path.isdir(full_path):
                log.warning("Path is a directory, not a file", path=path)
                return {"success": False, "error": "Path is a directory, not a file"}
            
            # Ler conteúdo do arquivo
            async with aiofiles.open(full_path, 'r') as f:
                content = await f.read()
            
            file_operations.labels(operation="read").inc()
            
            return {
                "success": True, 
                "path": path, 
                "content": content,
                "size": len(content)
            }
        except Exception as e:
            log.error("Error reading file", 
                     path=path, 
                     error=str(e))
            
            return {"success": False, "error": str(e)}
    
    def _normalize_path(self, path: str) -> str:
        """Normaliza um caminho, removendo .., /, etc."""
        # Remover / inicial para juntar corretamente com o caminho base
        if path.startswith("/"):
            path = path[1:]
        
        # Usar os.path.normpath para lidar com .. e .
        return os.path.normpath(path)

# Inicializar gerenciador de sistema de arquivos
fs_manager = FileSystemManager(config["filesystem"]["sharedPath"])

# Aplicador de decisões
class DecisionApplier:
    def __init__(self, state: LearnerState, fs_manager: FileSystemManager):
        self.state = state
        self.fs_manager = fs_manager
        self.pending_decisions = {}  # instance_id -> value
        pending_decisions.set(0)
    
    async def apply_decision(self, instance_id: int, value: Dict[str, Any]) -> Dict[str, Any]:
        """Aplica uma decisão ao sistema de arquivos"""
        operation = value.get("operation")
        path = value.get("path")
        content = value.get("content", "")
        
        if not path:
            log.error("Invalid decision: missing path", 
                     instance_id=instance_id,
                     value=value)
            return {"success": False, "error": "Invalid decision: missing path"}
        
        log.info("Applying decision", 
                instance_id=instance_id,
                operation=operation,
                path=path)
        
        result = {"success": False, "error": "Unknown operation"}
        
        if operation == "CREATE":
            result = await self.fs_manager.create_file(path, content)
        elif operation == "MODIFY":
            result = await self.fs_manager.modify_file(path, content)
        elif operation == "DELETE":
            result = await self.fs_manager.delete_file(path)
        else:
            log.error("Unknown operation", 
                     instance_id=instance_id,
                     operation=operation)
        
        if result["success"]:
            # Marcar decisão como aplicada no estado persistente
            await self.state.mark_as_applied(instance_id)
            log.info("Decision applied successfully", 
                    instance_id=instance_id,
                    operation=operation,
                    path=path)
        else:
            log.error("Failed to apply decision", 
                     instance_id=instance_id,
                     operation=operation,
                     path=path,
                     error=result.get("error"))
        
        return result
    
    async def add_pending_decision(self, instance_id: int, value: Dict[str, Any]):
        """Adiciona uma decisão à lista de pendentes"""
        self.pending_decisions[instance_id] = value
        pending_decisions.set(len(self.pending_decisions))
    
    async def apply_pending_decisions(self) -> int:
        """Aplica decisões pendentes em ordem"""
        if not self.pending_decisions:
            return 0
        
        applied_count = 0
        next_instance = self.state.last_applied_instance + 1
        keys_to_try = sorted(self.pending_decisions.keys())
        
        for instance_id in keys_to_try:
            if instance_id == next_instance:
                value = self.pending_decisions[instance_id]
                await self.apply_decision(instance_id, value)
                
                # Atualizar última instância aplicada
                self.state.last_applied_instance = instance_id
                last_applied.set(instance_id)
                
                # Remover da lista de pendentes
                del self.pending_decisions[instance_id]
                applied_count += 1
                
                # Atualizar próxima instância esperada
                next_instance = instance_id + 1
        
        pending_decisions.set(len(self.pending_decisions))
        
        return applied_count
    
    async def process_decision(self, instance_id: int, value: Dict[str, Any]) -> bool:
        """Processa uma decisão, aplicando imediatamente se for a próxima"""
        # Se for a próxima instância a aplicar, aplicar diretamente
        if instance_id == self.state.last_applied_instance + 1:
            await self.apply_decision(instance_id, value)
            self.state.last_applied_instance = instance_id
            last_applied.set(instance_id)
            
            # Verificar se temos decisões pendentes para aplicar em sequência
            applied_count = await self.apply_pending_decisions()
            
            if applied_count > 0:
                log.info("Applied pending decisions", applied_count=applied_count)
            
            return True
        elif instance_id > self.state.last_applied_instance:
            # Se for uma instância futura, guardar para aplicação posterior
            await self.add_pending_decision(instance_id, value)
            log.info("Added decision to pending queue", 
                    instance_id=instance_id,
                    operation=value.get("operation"),
                    pending_count=len(self.pending_decisions))
            return False
        else:
            # Se for uma instância já aplicada, ignorar
            log.warning("Ignoring already applied decision", 
                       instance_id=instance_id,
                       last_applied=self.state.last_applied_instance)
            return False

# Inicializar aplicador de decisões
decision_applier = DecisionApplier(state, fs_manager)

# Rotas da API
@app.post("/learn")
async def learn(request: LearnRequest, background_tasks: BackgroundTasks):
    """
    Endpoint para receber notificações de aceitação de propostas pelos acceptors.
    Quando um quórum de acceptors aceita uma mesma proposta, a decisão é tomada.
    """
    instance_id = request.instance_id
    proposal_number = request.proposal_number
    acceptor_id = request.acceptor_id
    value = request.value
    
    log.debug("Received learn request", 
             instance_id=instance_id,
             proposal_number=proposal_number,
             acceptor_id=acceptor_id,
             operation=value.get("operation", "unknown"),
             path=value.get("path", "unknown"))
    
    # Se já decidimos esta instância, ignorar duplicatas
    if state.is_decided(instance_id):
        return {"success": True, "already_decided": True}
    
    # Registrar aceitação deste acceptor
    quorum_reached = state.record_acceptance(instance_id, proposal_number, acceptor_id)
    
    # Se atingimos quórum, registrar decisão
    if quorum_reached:
        log.info("Decision reached", 
                instance_id=instance_id,
                proposal_number=proposal_number,
                operation=value.get("operation"),
                path=value.get("path"))
        
        state.decisions[instance_id] = {
            "value": value,
            "proposal_number": proposal_number,
            "decided_at": time.time()
        }
        
        # Persistir decisão
        await state.save_decision(instance_id, proposal_number, value)
        decisions_total.inc()
        
        # Aplicar a decisão ao sistema de arquivos
        await decision_applier.process_decision(instance_id, value)
        
        # Notificar clientes via WebSocket
        await manager.broadcast({
            "type": "decision",
            "instance_id": instance_id,
            "operation": value.get("operation"),
            "path": value.get("path")
        })
        
        # Limpar aceitações antigas em background para economizar memória
        background_tasks.add_task(state.cleanup_old_acceptances)
        
        return {"success": True, "decision_reached": True}
    
    return {"success": True, "decision_reached": False}

@app.get("/files")
async def list_files(directory: str = "/"):
    """Lista arquivos em um diretório"""
    return await fs_manager.list_files(directory)

@app.get("/files/{path:path}")
async def read_file(path: str):
    """Lê o conteúdo de um arquivo"""
    return await fs_manager.read_file(path)

@app.post("/sync")
async def sync(request: SyncRequest):
    """
    Sincroniza decisões com outro learner.
    Usado para recuperar decisões perdidas quando um learner fica offline.
    """
    if state.sync_in_progress:
        return {"success": False, "error": "Sync already in progress"}
    
    state.sync_in_progress = True
    from_instance = request.from_instance
    to_instance = request.to_instance
    learner_id = request.learner_id
    
    log.info("Sync requested", 
            from_instance=from_instance,
            to_instance=to_instance,
            learner_id=learner_id)
    
    sync_count.inc()
    
    try:
        # Preparar decisões para enviar
        decisions_to_send = []
        
        for instance_id, decision in state.decisions.items():
            if instance_id >= from_instance and (to_instance is None or instance_id <= to_instance):
                decisions_to_send.append({
                    "instance_id": instance_id,
                    "proposal_number": decision["proposal_number"],
                    "value": decision["value"],
                    "decided_at": decision.get("decided_at", time.time())
                })
        
        # Enviar decisões para o learner solicitante
        if decisions_to_send:
            sync_instances.inc(len(decisions_to_send))
            log.info("Sending decisions for sync", 
                    count=len(decisions_to_send),
                    learner_id=learner_id)
        
        return {
            "success": True, 
            "decisions": decisions_to_send,
            "count": len(decisions_to_send)
        }
    except Exception as e:
        log.error("Error during sync", error=str(e))
        return {"success": False, "error": str(e)}
    finally:
        state.sync_in_progress = False

@app.post("/apply_synced_decisions")
async def apply_synced_decisions(decisions: List[DecisionInfo]):
    """
    Aplica decisões recebidas durante sincronização.
    Endpoint interno usado no processo de sincronização.
    """
    applied_count = 0
    
    for decision in decisions:
        instance_id = decision.instance_id
        
        # Pular instâncias que já conhecemos
        if state.is_decided(instance_id):
            continue
        
        # Registrar decisão
        state.decisions[instance_id] = {
            "value": decision.value,
            "proposal_number": decision.proposal_number,
            "decided_at": decision.decided_at
        }
        
        # Persistir decisão
        await state.save_decision(instance_id, decision.proposal_number, decision.value)
        
        # Tentar aplicar
        await decision_applier.add_pending_decision(instance_id, decision.value)
        applied_count += 1
    
    # Tentar aplicar decisões pendentes em ordem
    await decision_applier.apply_pending_decisions()
    
    log.info("Applied synced decisions", count=applied_count)
    
    return {"success": True, "applied_count": applied_count}

@app.get("/status")
async def get_status():
    """
    Retorna informações sobre o estado atual do learner.
    Útil para monitoramento e depuração.
    """
    # Contar arquivos e diretórios para atualizar métricas
    fs_manager.update_file_counts()
    
    return {
        "node_id": state.node_id,
        "role": "learner",
        "decisions_count": len(state.decisions),
        "last_applied_instance": state.last_applied_instance,
        "pending_decisions": len(decision_applier.pending_decisions),
        "system_time": time.time(),
        "file_count": int(file_count._value.get()),
        "directory_count": int(directory_count._value.get()),
        "uptime_seconds": time.time() - app.state.start_time
    }

# Websocket para atualizações em tempo real
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        # Enviar estado inicial
        await websocket.send_json({
            "type": "initial_state",
            "last_applied_instance": state.last_applied_instance,
            "decisions_count": len(state.decisions)
        })
        
        while True:
            # Manter conexão ativa e processar mensagens do cliente
            data = await websocket.receive_text()
            try:
                message = json.loads(data)
                if message.get("type") == "get_files":
                    directory = message.get("directory", "/")
                    files = await fs_manager.list_files(directory)
                    await websocket.send_json({
                        "type": "file_list",
                        "data": files
                    })
            except json.JSONDecodeError:
                log.warning("Received invalid JSON from WebSocket")
            except Exception as e:
                log.error("Error processing WebSocket message", error=str(e))
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        log.error("WebSocket error", error=str(e))
        manager.disconnect(websocket)

# Configuração de métricas Prometheus
@app.get("/metrics")
async def metrics():
    """
    Expõe métricas no formato Prometheus.
    """
    return generate_latest()

# Eventos do ciclo de vida da aplicação
@app.on_event("startup")
async def startup_event():
    """
    Evento executado ao iniciar a aplicação.
    """
    app.state.start_time = time.time()
    log.info("Learner starting up", 
            node_id=state.node_id, 
            config=config)
    
    # Iniciar tarefa de sincronização periódica em background
    asyncio.create_task(periodic_sync())
    asyncio.create_task(periodic_file_count_update())

@app.on_event("shutdown")
async def shutdown_event():
    """
    Evento executado ao encerrar a aplicação.
    """
    log.info("Learner shutting down", 
            node_id=state.node_id,
            decisions_count=len(state.decisions),
            last_applied_instance=state.last_applied_instance)

async def periodic_sync():
    """
    Sincroniza periodicamente com outros learners.
    Garante que todos os learners tenham as mesmas decisões.
    """
    # Esperar alguns segundos antes de iniciar sincronização
    await asyncio.sleep(5)
    
    while True:
        try:
            # Sincronizar apenas se não estivermos em sincronização no momento
            if not state.sync_in_progress:
                current_node_id = state.node_id
                
                # Para cada learner, verificar se está atualizado
                for learner_url in config["networking"]["learners"]:
                    # Extrair ID do learner da URL
                    if "-" in learner_url:
                        target_id = int(learner_url.split("-")[1].split(":")[0])
                        
                        # Pular a si mesmo
                        if target_id == current_node_id:
                            continue
                        
                        log.debug("Checking sync status with learner", 
                                target_id=target_id)
                        
                        try:
                            async with aiohttp.ClientSession() as session:
                                # Verificar status do outro learner
                                async with session.get(f"http://{learner_url}/status", timeout=2) as response:
                                    if response.status == 200:
                                        data = await response.json()
                                        other_last_applied = data.get("last_applied_instance", 0)
                                        
                                        # Se o outro learner estiver à frente, solicitar sincronização
                                        if other_last_applied > state.last_applied_instance:
                                            log.info("Requesting sync from learner", 
                                                   target_id=target_id,
                                                   our_last=state.last_applied_instance,
                                                   their_last=other_last_applied)
                                            
                                            # Solicitar decisões que não temos
                                            sync_request = {
                                                "from_instance": state.last_applied_instance + 1,
                                                "to_instance": other_last_applied,
                                                "learner_id": current_node_id
                                            }
                                            
                                            async with session.post(
                                                f"http://{learner_url}/sync", 
                                                json=sync_request,
                                                timeout=5
                                            ) as sync_response:
                                                if sync_response.status == 200:
                                                    sync_data = await sync_response.json()
                                                    
                                                    if sync_data.get("success") and sync_data.get("decisions"):
                                                        decisions = sync_data.get("decisions", [])
                                                        
                                                        log.info("Received decisions from sync", 
                                                               count=len(decisions),
                                                               target_id=target_id)
                                                        
                                                        # Aplicar decisões recebidas
                                                        await apply_synced_decisions(
                                                            [DecisionInfo(**d) for d in decisions]
                                                        )
                                        # Se estivermos à frente, nada a fazer
                                        elif state.last_applied_instance > other_last_applied:
                                            log.debug("We are ahead of other learner", 
                                                    target_id=target_id,
                                                    our_last=state.last_applied_instance,
                                                    their_last=other_last_applied)
                                        # Se estivermos em sincronia, tudo bem
                                        else:
                                            log.debug("In sync with other learner", 
                                                    target_id=target_id,
                                                    last_applied=state.last_applied_instance)
                        except asyncio.TimeoutError:
                            log.warning("Timeout connecting to learner", target_id=target_id)
                        except Exception as e:
                            log.error("Error syncing with learner", 
                                    target_id=target_id, 
                                    error=str(e))
        except Exception as e:
            log.error("Error in periodic sync task", error=str(e))
        
        # Aguardar antes da próxima sincronização
        await asyncio.sleep(10)  # Sincronizar a cada 10 segundos

async def periodic_file_count_update():
    """Atualiza periodicamente as contagens de arquivos e diretórios para métricas"""
    while True:
        try:
            fs_manager.update_file_counts()
        except Exception as e:
            log.error("Error updating file counts", error=str(e))
        
        # Atualizar a cada 30 segundos
        await asyncio.sleep(30)