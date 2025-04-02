import os
import yaml
import json
import time
import asyncio
import uuid
import hashlib
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, BackgroundTasks, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import structlog
from prometheus_client import Counter, Gauge, Histogram, generate_latest
import aiohttp
from typing import Dict, List, Set, Optional, Any, Union, Tuple
from tinydb import TinyDB, Query
import aiofiles
import os.path
import random

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
apply_latency = Histogram("paxos_learner_apply_latency_seconds", "Time to apply a decision to the store")
pending_decisions = Gauge("paxos_learner_pending_decisions", "Number of decisions pending application")
active_websockets = Gauge("paxos_learner_active_websockets", "Number of active WebSocket connections")
store_operations = Counter("paxos_learner_store_operations_total", "Operations sent to store nodes", ["operation", "result"])
store_latency = Histogram("paxos_learner_store_latency_seconds", "Time to complete store operations")
store_failures = Counter("paxos_learner_store_failures_total", "Store operation failures", ["store_id"])
transaction_success = Counter("paxos_learner_transaction_success_total", "Successful transactions")
transaction_failures = Counter("paxos_learner_transaction_failures_total", "Failed transactions")
store_response_time = Histogram("paxos_learner_store_response_time", "Response time of store nodes", ["store_id"])
client_notifications = Counter("paxos_learner_client_notifications", "Notifications sent to clients", ["status"])

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
    
class DirectoryListRequest(BaseModel):
    """Modelo para solicitações de listagem de diretório"""
    directory: str = "/"

class PrepareRequest(BaseModel):
    """Modelo para fase 1 do 2PC enviado ao Store"""
    transaction_id: str
    operations: List[Dict[str, Any]]
    learner_id: int
    timestamp: float

class CommitRequest(BaseModel):
    """Modelo para fase 2 do 2PC enviado ao Store"""
    transaction_id: str
    commit: bool
    learner_id: int

class PrepareResponse(BaseModel):
    """Modelo para respostas de preparação"""
    ready: bool
    transaction_id: str
    store_id: int
    reason: Optional[str] = None

class CommitResponse(BaseModel):
    """Modelo para respostas de commit"""
    success: bool
    transaction_id: str
    store_id: int
    reason: Optional[str] = None

class ClientNotification(BaseModel):
    """Modelo para notificações enviadas aos clientes"""
    status: str  # "COMMITTED" ou "NOT_COMMITTED"
    instance_id: int
    operation: str
    path: str
    timestamp: float
    transaction_id: Optional[str] = None
    content_preview: Optional[str] = None

# Gerenciador de conexões WebSocket
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self.client_connections: Dict[str, WebSocket] = {}  # client_id -> websocket

    async def connect(self, websocket: WebSocket, client_id: Optional[str] = None):
        await websocket.accept()
        self.active_connections.append(websocket)
        
        if client_id:
            self.client_connections[client_id] = websocket
            
        active_websockets.set(len(self.active_connections))
        log.info("WebSocket connection established", 
                active_connections=len(self.active_connections),
                client_id=client_id)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)
        
        # Remover também dos client_connections se estiver lá
        for client_id, ws in list(self.client_connections.items()):
            if ws == websocket:
                del self.client_connections[client_id]
                break
                
        active_websockets.set(len(self.active_connections))
        log.info("WebSocket connection closed", 
                active_connections=len(self.active_connections))

    async def send_personal_message(self, message: dict, client_id: str):
        """Envia mensagem para um cliente específico"""
        if client_id in self.client_connections:
            try:
                await self.client_connections[client_id].send_json(message)
                return True
            except Exception as e:
                log.error("Error sending personal message", 
                         error=str(e), 
                         client_id=client_id)
                # Remover conexão problemática
                self.client_connections.pop(client_id, None)
                return False
        return False

    async def broadcast(self, message: dict):
        """Envia mensagem para todos os clientes conectados"""
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
    def __init__(self, node_id: int):
        self.node_id = node_id
        self.decisions = {}  # instanceId -> {value, proposal_number, decided_at}
        self.acceptances = {}  # instanceId -> {proposal_number -> {acceptor_ids, first_seen_at}}
        self.last_applied_instance = 0
        self.sync_in_progress = False
        self.store_status = {}  # store_id -> {status, last_check}
        self.transactions = {}  # transaction_id -> {status, instance_id, timestamp, client_id}
        self.directories = {}  # path -> {items: [], last_updated: timestamp}
        self.client_operations = {}  # client_id -> {last_operation: timestamp, operation_count: int}
        
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
            
            # Carregar transações
            transactions_table = self.db.table("transactions")
            for transaction in transactions_table.all():
                self.transactions[transaction["transaction_id"]] = {
                    "status": transaction["status"],
                    "instance_id": transaction["instance_id"],
                    "timestamp": transaction["timestamp"],
                    "client_id": transaction.get("client_id")
                }
            
            # Carregar diretórios
            directories_table = self.db.table("directories")
            for directory in directories_table.all():
                self.directories[directory["path"]] = {
                    "items": directory["items"],
                    "last_updated": directory["last_updated"]
                }
            
            # Carregar operações de clientes
            clients_table = self.db.table("clients")
            for client in clients_table.all():
                self.client_operations[client["client_id"]] = {
                    "last_operation": client["last_operation"],
                    "operation_count": client["operation_count"]
                }
            
            log.info("State loaded successfully", 
                    decisions_count=len(self.decisions),
                    last_applied_instance=self.last_applied_instance,
                    transactions_count=len(self.transactions),
                    directories_count=len(self.directories),
                    clients_count=len(self.client_operations))
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
    
    async def save_transaction(self, transaction_id: str, status: str, instance_id: int, client_id: Optional[str] = None):
        """Persiste uma transação no disco"""
        try:
            transactions_table = self.db.table("transactions")
            Transaction = Query()
            
            transactions_table.upsert(
                {
                    "transaction_id": transaction_id,
                    "status": status,
                    "instance_id": instance_id,
                    "timestamp": time.time(),
                    "client_id": client_id
                },
                Transaction.transaction_id == transaction_id
            )
            
            log.debug("Transaction saved to disk", 
                     transaction_id=transaction_id,
                     status=status,
                     instance_id=instance_id,
                     client_id=client_id)
        except Exception as e:
            log.error("Error saving transaction", 
                     transaction_id=transaction_id, 
                     error=str(e))
    
    async def save_directory(self, path: str, items: List[Dict]):
        """Persiste informações de diretório no disco"""
        try:
            directories_table = self.db.table("directories")
            Directory = Query()
            
            directories_table.upsert(
                {
                    "path": path,
                    "items": items,
                    "last_updated": time.time()
                },
                Directory.path == path
            )
            
            log.debug("Directory saved to disk", 
                     path=path,
                     items_count=len(items))
        except Exception as e:
            log.error("Error saving directory", 
                     path=path, 
                     error=str(e))
    
    async def update_client_operation(self, client_id: str):
        """Atualiza informações de operação do cliente"""
        try:
            clients_table = self.db.table("clients")
            Client = Query()
            
            current_time = time.time()
            
            if client_id in self.client_operations:
                operation_count = self.client_operations[client_id]["operation_count"] + 1
                self.client_operations[client_id] = {
                    "last_operation": current_time,
                    "operation_count": operation_count
                }
            else:
                self.client_operations[client_id] = {
                    "last_operation": current_time,
                    "operation_count": 1
                }
            
            clients_table.upsert(
                {
                    "client_id": client_id,
                    "last_operation": current_time,
                    "operation_count": self.client_operations[client_id]["operation_count"]
                },
                Client.client_id == client_id
            )
            
            log.debug("Client operation updated", 
                     client_id=client_id,
                     operation_count=self.client_operations[client_id]["operation_count"])
        except Exception as e:
            log.error("Error updating client operation", 
                     client_id=client_id, 
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
        quorum_reached = len(self.acceptances[instance_id][proposal_number]["acceptor_ids"]) >= 3  # Quórum de 3 (maioria de 5 acceptors)
        
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
    
    def get_transaction_by_instance(self, instance_id: int) -> Optional[str]:
        """Busca uma transação pelo ID da instância"""
        for transaction_id, transaction in self.transactions.items():
            if transaction["instance_id"] == instance_id:
                return transaction_id
        return None
    
    def get_client_by_instance(self, instance_id: int) -> Optional[str]:
        """Busca um cliente pelo ID da instância"""
        transaction_id = self.get_transaction_by_instance(instance_id)
        if transaction_id and transaction_id in self.transactions:
            return self.transactions[transaction_id].get("client_id")
        return None
    
    def get_responsible_learner(self, client_id: str) -> int:
        """Determina qual learner é responsável por notificar o cliente"""
        if not client_id:
            return self.node_id
        
        # Usamos o hash do client_id para determinar o learner
        # Garantimos que é sempre o mesmo para um determinado cliente
        hash_val = int(hashlib.md5(client_id.encode()).hexdigest(), 16)
        return (hash_val % len(config["networking"]["learners"])) + 1
    
    def should_notify_client(self, client_id: str) -> bool:
        """Verifica se este learner é responsável por notificar o cliente"""
        if not client_id:
            return False
        
        responsible_learner = self.get_responsible_learner(client_id)
        return responsible_learner == self.node_id

# Inicializar estado do learner
state = LearnerState(config["node"]["id"])

# Gerenciador de Cluster Store
class StoreManager:
    def __init__(self, state: LearnerState):
        self.state = state
        self.store_nodes = [f"store-{i}:8080" for i in range(1, 4)]  # 3 nós do Store
        self.read_quorum = config.get("store", {}).get("readQuorum", 1)  # Nr = 1
        self.write_quorum = config.get("store", {}).get("writeQuorum", 3)  # Nw = 3
        self.retry_count = 3  # Número de tentativas
        self.retry_delay = 0.5  # Delay entre tentativas (segundos)
        self.adaptive_timeouts = True  # Usar timeouts adaptativos
        self.response_times = {}  # store_id -> [tempos de resposta]
        
        # Inicializar status dos nós do Store
        for i in range(1, 4):
            self.state.store_status[i] = {
                "status": "UNKNOWN",
                "last_check": 0,
                "response_time": 0.5  # Tempo de resposta inicial (segundos)
            }
            self.response_times[i] = []
    
    async def check_store_status(self):
        """Verifica o status dos nós do Store"""
        for i, store_url in enumerate(self.store_nodes, 1):
            try:
                start_time = time.time()
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        f"http://{store_url}/status",
                        timeout=2
                    ) as response:
                        response_time = time.time() - start_time
                        
                        if response.status == 200:
                            data = await response.json()
                            self.state.store_status[i] = {
                                "status": "ACTIVE",
                                "last_check": time.time(),
                                "resources_count": data.get("resources_count", 0),
                                "is_recovering": data.get("is_recovering", False),
                                "response_time": response_time
                            }
                            
                            # Atualizar lista de tempos de resposta
                            self.response_times[i].append(response_time)
                            if len(self.response_times[i]) > 10:  # Manter apenas os 10 últimos
                                self.response_times[i].pop(0)
                                
                            # Atualizar timeout adaptativo
                            self._update_adaptive_timeout(i)
                        else:
                            self.state.store_status[i] = {
                                "status": "ERROR",
                                "last_check": time.time(),
                                "error": f"Unexpected status: {response.status}"
                            }
            except Exception as e:
                self.state.store_status[i] = {
                    "status": "ERROR",
                    "last_check": time.time(),
                    "error": str(e)
                }
                
                store_failures.labels(store_id=i).inc()
    
    def _update_adaptive_timeout(self, store_id: int):
        """Atualiza o timeout adaptativo para um nó do Store"""
        if not self.adaptive_timeouts or len(self.response_times[store_id]) < 3:
            return
        
        import statistics
        
        try:
            # Calcular média e desvio padrão
            mean_time = statistics.mean(self.response_times[store_id])
            std_dev = statistics.stdev(self.response_times[store_id])
            
            # Timeout adaptativo: média + 3 * desvio padrão, limitado entre 0.2 e 5.0 segundos
            adaptive_timeout = min(max(mean_time + 3 * std_dev, 0.2), 5.0)
            
            # Atualizar no estado
            self.state.store_status[store_id]["timeout"] = adaptive_timeout
        except Exception as e:
            log.warning("Error calculating adaptive timeout", 
                      store_id=store_id, 
                      error=str(e))
    
    def _get_timeout(self, store_id: int) -> float:
        """Obtém o timeout para um nó do Store"""
        if self.adaptive_timeouts and store_id in self.state.store_status:
            return self.state.store_status[store_id].get("timeout", 2.0)
        return 2.0  # Timeout padrão
    
    async def read_resource(self, path: str) -> Dict[str, Any]:
        """Lê um recurso usando o protocolo de votação (Nr=1)"""
        start_time = time.time()
        
        # Para leitura, basta consultar um nó do Store (Nr=1)
        # Tentativa com cada nó em ordem aleatória
        import random
        store_ids = list(range(1, 4))
        random.shuffle(store_ids)
        
        for store_id in store_ids:
            store_url = f"store-{store_id}:8080"
            
            try:
                request_start = time.time()
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        f"http://{store_url}/resources/{path}",
                        timeout=self._get_timeout(store_id)
                    ) as response:
                        response_time = time.time() - request_start
                        store_response_time.labels(store_id=store_id).observe(response_time)
                        
                        # Atualizar lista de tempos de resposta
                        self.response_times[store_id].append(response_time)
                        if len(self.response_times[store_id]) > 10:
                            self.response_times[store_id].pop(0)
                        
                        # Atualizar timeout adaptativo
                        self._update_adaptive_timeout(store_id)
                        
                        if response.status == 200:
                            data = await response.json()
                            
                            if data.get("success", False):
                                store_operations.labels(operation="read", result="success").inc()
                                store_latency.observe(time.time() - start_time)
                                
                                return data
                            else:
                                log.warning("Read operation failed", 
                                          store_id=store_id,
                                          path=path,
                                          error=data.get("error"))
                        else:
                            log.warning("Unexpected status from store", 
                                      store_id=store_id,
                                      path=path,
                                      status=response.status)
            except asyncio.TimeoutError:
                log.warning("Timeout reading from store", 
                          store_id=store_id,
                          path=path)
                store_failures.labels(store_id=store_id).inc()
            except Exception as e:
                log.warning("Error reading from store", 
                          store_id=store_id,
                          path=path,
                          error=str(e))
                
                store_failures.labels(store_id=store_id).inc()
        
        # Se chegamos aqui, nenhum nó respondeu com sucesso
        store_operations.labels(operation="read", result="failure").inc()
        store_latency.observe(time.time() - start_time)
        
        return {
            "success": False,
            "error": "Failed to read resource from any store node",
            "path": path
        }
    
    async def list_directory(self, directory: str) -> Dict[str, Any]:
        """Lista um diretório usando o protocolo de votação (Nr=1)"""
        # Normalizar o caminho
        if not directory.startswith("/"):
            directory = "/" + directory
        
        # Se já temos um cache recente deste diretório, usar
        if directory in self.state.directories:
            dir_info = self.state.directories[directory]
            if time.time() - dir_info["last_updated"] < 30:  # Cache de 30 segundos
                return {
                    "success": True,
                    "directory": directory,
                    "items": dir_info["items"]
                }
        
        # Caso contrário, buscar do Store
        result = await self.read_resource(directory)
        if not result.get("success", False):
            return {
                "success": False,
                "error": result.get("error", "Failed to list directory"),
                "directory": directory
            }
        
        try:
            content = result.get("content", "")
            if not content:
                items = []
            else:
                items = json.loads(content)
            
            # Atualizar cache
            await self.state.save_directory(directory, items)
            
            return {
                "success": True,
                "directory": directory,
                "items": items
            }
        except Exception as e:
            log.error("Error parsing directory content", 
                     directory=directory, 
                     error=str(e))
            
            return {
                "success": False,
                "error": f"Failed to parse directory content: {str(e)}",
                "directory": directory
            }
    
    async def execute_transaction(self, operations: List[Dict[str, Any]], instance_id: int, client_id: Optional[str] = None) -> Dict[str, Any]:
        """Executa uma transação usando Two-Phase Commit com o protocolo de votação (Nw=3)"""
        start_time = time.time()
        transaction_id = f"tx-{instance_id}-{int(time.time())}-{self.state.node_id}"
        
        log.info("Starting transaction", 
                transaction_id=transaction_id,
                operations_count=len(operations),
                instance_id=instance_id,
                client_id=client_id)
        
        # Salvar transação no estado do learner
        self.state.transactions[transaction_id] = {
            "status": "PREPARING",
            "instance_id": instance_id,
            "timestamp": time.time(),
            "client_id": client_id
        }
        
        await self.state.save_transaction(transaction_id, "PREPARING", instance_id, client_id)
        
        # Fase 1: Preparação
        prepare_request = PrepareRequest(
            transaction_id=transaction_id,
            operations=operations,
            learner_id=self.state.node_id,
            timestamp=time.time()
        ).dict()
        
        prepare_responses = []
        prepare_success = True
        
        for store_id in range(1, 4):
            store_url = f"store-{store_id}:8080"
            
            for attempt in range(self.retry_count):
                try:
                    request_start = time.time()
                    async with aiohttp.ClientSession() as session:
                        async with session.post(
                            f"http://{store_url}/prepare",
                            json=prepare_request,
                            timeout=self._get_timeout(store_id)
                        ) as response:
                            response_time = time.time() - request_start
                            store_response_time.labels(store_id=store_id).observe(response_time)
                            
                            # Atualizar lista de tempos de resposta
                            self.response_times[store_id].append(response_time)
                            if len(self.response_times[store_id]) > 10:
                                self.response_times[store_id].pop(0)
                            
                            # Atualizar timeout adaptativo
                            self._update_adaptive_timeout(store_id)
                            
                            if response.status == 200:
                                data = await response.json()
                                
                                prepare_responses.append(data)
                                
                                if not data.get("ready", False):
                                    log.warning("Store not ready for transaction", 
                                              store_id=store_id,
                                              transaction_id=transaction_id,
                                              reason=data.get("reason"))
                                    
                                    prepare_success = False
                                
                                break  # Sucesso, sair do loop de tentativas
                            else:
                                log.warning("Unexpected status from store during prepare", 
                                          store_id=store_id,
                                          transaction_id=transaction_id,
                                          status=response.status)
                                
                                if attempt == self.retry_count - 1:
                                    prepare_success = False
                except asyncio.TimeoutError:
                    log.warning("Timeout during prepare with store", 
                              store_id=store_id,
                              transaction_id=transaction_id,
                              attempt=attempt + 1)
                    
                    store_failures.labels(store_id=store_id).inc()
                    
                    if attempt < self.retry_count - 1:
                        await asyncio.sleep(self.retry_delay * (2 ** attempt))  # Exponential backoff
                    else:
                        prepare_success = False
                except Exception as e:
                    log.warning("Error preparing transaction with store", 
                              store_id=store_id,
                              transaction_id=transaction_id,
                              error=str(e),
                              attempt=attempt + 1)
                    
                    store_failures.labels(store_id=store_id).inc()
                    
                    if attempt < self.retry_count - 1:
                        await asyncio.sleep(self.retry_delay * (2 ** attempt))  # Exponential backoff
                    else:
                        prepare_success = False
        
        # Verificar se todos os nós estão prontos
        if not prepare_success or len(prepare_responses) < self.write_quorum:
            log.warning("Failed to prepare transaction", 
                      transaction_id=transaction_id,
                      prepare_responses=len(prepare_responses),
                      success=prepare_success)
            
            # Fase 2: Abortar
            await self.abort_transaction(transaction_id)
            
            store_operations.labels(operation="write", result="prepare_failure").inc()
            transaction_failures.inc()
            
            # Atualizar status da transação
            self.state.transactions[transaction_id]["status"] = "ABORTED"
            await self.state.save_transaction(transaction_id, "ABORTED", instance_id, client_id)
            
            # Notificar cliente se formos o learner responsável
            if client_id and self.state.should_notify_client(client_id):
                operation_info = operations[0] if operations else {"operation": "UNKNOWN", "path": "unknown"}
                
                notification = ClientNotification(
                    status="NOT_COMMITTED",
                    instance_id=instance_id,
                    operation=operation_info.get("operation", "UNKNOWN"),
                    path=operation_info.get("path", "unknown"),
                    timestamp=time.time(),
                    transaction_id=transaction_id
                )
                
                await self._notify_client(client_id, notification.dict())
                client_notifications.labels(status="NOT_COMMITTED").inc()
            
            return {
                "success": False,
                "error": "Failed to prepare transaction",
                "transaction_id": transaction_id
            }
        
        # Fase 2: Commit
        commit_success = await self.commit_transaction(transaction_id)
        
        if commit_success:
            log.info("Transaction committed successfully", 
                    transaction_id=transaction_id,
                    instance_id=instance_id)
            
            store_operations.labels(operation="write", result="success").inc()
            transaction_success.inc()
            
            # Atualizar status da transação
            self.state.transactions[transaction_id]["status"] = "COMMITTED"
            await self.state.save_transaction(transaction_id, "COMMITTED", instance_id, client_id)
            
            # Notificar cliente se formos o learner responsável
            if client_id and self.state.should_notify_client(client_id):
                operation_info = operations[0] if operations else {"operation": "UNKNOWN", "path": "unknown"}
                
                # Para operações de leitura, incluir conteúdo na notificação
                content_preview = None
                if operation_info.get("operation") == "READ":
                    resource_result = await self.read_resource(operation_info.get("path", ""))
                    if resource_result.get("success", False) and resource_result.get("content"):
                        content = resource_result.get("content", "")
                        content_preview = content[:200] + "..." if len(content) > 200 else content
                
                notification = ClientNotification(
                    status="COMMITTED",
                    instance_id=instance_id,
                    operation=operation_info.get("operation", "UNKNOWN"),
                    path=operation_info.get("path", "unknown"),
                    timestamp=time.time(),
                    transaction_id=transaction_id,
                    content_preview=content_preview
                )
                
                await self._notify_client(client_id, notification.dict())
                client_notifications.labels(status="COMMITTED").inc()
            
            store_latency.observe(time.time() - start_time)
            
            return {
                "success": True,
                "transaction_id": transaction_id,
                "instance_id": instance_id
            }
        else:
            log.warning("Failed to commit transaction", 
                      transaction_id=transaction_id,
                      instance_id=instance_id)
            
            store_operations.labels(operation="write", result="commit_failure").inc()
            transaction_failures.inc()
            
            # Atualizar status da transação
            self.state.transactions[transaction_id]["status"] = "FAILED"
            await self.state.save_transaction(transaction_id, "FAILED", instance_id, client_id)
            
            # Notificar cliente se formos o learner responsável
            if client_id and self.state.should_notify_client(client_id):
                operation_info = operations[0] if operations else {"operation": "UNKNOWN", "path": "unknown"}
                
                notification = ClientNotification(
                    status="NOT_COMMITTED",
                    instance_id=instance_id,
                    operation=operation_info.get("operation", "UNKNOWN"),
                    path=operation_info.get("path", "unknown"),
                    timestamp=time.time(),
                    transaction_id=transaction_id
                )
                
                await self._notify_client(client_id, notification.dict())
                client_notifications.labels(status="NOT_COMMITTED").inc()
            
            store_latency.observe(time.time() - start_time)
            
            return {
                "success": False,
                "error": "Failed to commit transaction",
                "transaction_id": transaction_id
            }
    
    async def commit_transaction(self, transaction_id: str) -> bool:
        """Envia comando de commit para todos os nós do Store"""
        commit_request = CommitRequest(
            transaction_id=transaction_id,
            commit=True,
            learner_id=self.state.node_id
        ).dict()
        
        commit_success = True
        commit_responses = []
        
        for store_id in range(1, 4):
            store_url = f"store-{store_id}:8080"
            
            for attempt in range(self.retry_count):
                try:
                    request_start = time.time()
                    async with aiohttp.ClientSession() as session:
                        async with session.post(
                            f"http://{store_url}/commit",
                            json=commit_request,
                            timeout=self._get_timeout(store_id)
                        ) as response:
                            response_time = time.time() - request_start
                            store_response_time.labels(store_id=store_id).observe(response_time)
                            
                            # Atualizar lista de tempos de resposta
                            self.response_times[store_id].append(response_time)
                            if len(self.response_times[store_id]) > 10:
                                self.response_times[store_id].pop(0)
                            
                            # Atualizar timeout adaptativo
                            self._update_adaptive_timeout(store_id)
                            
                            if response.status == 200:
                                data = await response.json()
                                
                                commit_responses.append(data)
                                
                                if not data.get("success", False):
                                    log.warning("Commit failed on store", 
                                              store_id=store_id,
                                              transaction_id=transaction_id,
                                              reason=data.get("reason"))
                                    
                                    commit_success = False
                                
                                break  # Sucesso, sair do loop de tentativas
                            else:
                                log.warning("Unexpected status from store during commit", 
                                          store_id=store_id,
                                          transaction_id=transaction_id,
                                          status=response.status)
                                
                                if attempt == self.retry_count - 1:
                                    commit_success = False
                except asyncio.TimeoutError:
                    log.warning("Timeout during commit with store", 
                              store_id=store_id,
                              transaction_id=transaction_id,
                              attempt=attempt + 1)
                    
                    store_failures.labels(store_id=store_id).inc()
                    
                    if attempt < self.retry_count - 1:
                        await asyncio.sleep(self.retry_delay * (2 ** attempt))  # Exponential backoff
                    else:
                        commit_success = False
                except Exception as e:
                    log.warning("Error committing transaction with store", 
                              store_id=store_id,
                              transaction_id=transaction_id,
                              error=str(e),
                              attempt=attempt + 1)
                    
                    store_failures.labels(store_id=store_id).inc()
                    
                    if attempt < self.retry_count - 1:
                        await asyncio.sleep(self.retry_delay * (2 ** attempt))  # Exponential backoff
                    else:
                        commit_success = False
        
        # Verificar se todos os nós confirmaram o commit
        if not commit_success or len(commit_responses) < self.write_quorum:
            log.warning("Failed to commit transaction on all stores", 
                      transaction_id=transaction_id,
                      commit_responses=len(commit_responses),
                      success=commit_success)
            
            return False
        
        return True
    
    async def abort_transaction(self, transaction_id: str) -> bool:
        """Envia comando de abort para todos os nós do Store"""
        abort_request = CommitRequest(
            transaction_id=transaction_id,
            commit=False,
            learner_id=self.state.node_id
        ).dict()
        
        for store_id in range(1, 4):
            store_url = f"store-{store_id}:8080"
            
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        f"http://{store_url}/commit",
                        json=abort_request,
                        timeout=3  # Timeout mais curto para abort
                    ) as response:
                        if response.status == 200:
                            data = await response.json()
                            
                            if not data.get("success", False):
                                log.warning("Abort failed on store", 
                                          store_id=store_id,
                                          transaction_id=transaction_id,
                                          reason=data.get("reason"))
                        else:
                            log.warning("Unexpected status from store during abort", 
                                      store_id=store_id,
                                      transaction_id=transaction_id,
                                      status=response.status)
            except Exception as e:
                log.warning("Error aborting transaction with store", 
                          store_id=store_id,
                          transaction_id=transaction_id,
                          error=str(e))
                
                store_failures.labels(store_id=store_id).inc()
        
        # Atualizar status da transação
        if transaction_id in self.state.transactions:
            self.state.transactions[transaction_id]["status"] = "ABORTED"
            await self.state.save_transaction(
                transaction_id, 
                "ABORTED", 
                self.state.transactions[transaction_id]["instance_id"],
                self.state.transactions[transaction_id].get("client_id")
            )
        
        return True
    
    async def get_transaction_status(self, transaction_id: str) -> Dict[str, Any]:
        """Consulta o status de uma transação"""
        if transaction_id in self.state.transactions:
            return {
                "transaction_id": transaction_id,
                "status": self.state.transactions[transaction_id]["status"],
                "instance_id": self.state.transactions[transaction_id]["instance_id"],
                "timestamp": self.state.transactions[transaction_id]["timestamp"],
                "client_id": self.state.transactions[transaction_id].get("client_id")
            }
        
        # Consultar os nós do Store para ver se algum deles conhece esta transação
        for store_id in range(1, 4):
            store_url = f"store-{store_id}:8080"
            
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        f"http://{store_url}/transaction/{transaction_id}",
                        timeout=2
                    ) as response:
                        if response.status == 200:
                            data = await response.json()
                            
                            if data.get("status") != "NOT_FOUND":
                                return data
            except Exception as e:
                log.warning("Error getting transaction status from store", 
                          store_id=store_id,
                          transaction_id=transaction_id,
                          error=str(e))
        
        return {
            "transaction_id": transaction_id,
            "status": "NOT_FOUND"
        }
    
    async def _notify_client(self, client_id: str, notification: Dict[str, Any]) -> bool:
        """Notifica um cliente sobre o resultado de uma operação"""
        try:
            # Tentar enviar via WebSocket
            if await manager.send_personal_message(notification, client_id):
                return True
            
            # Se não conseguiu via WebSocket, tentar via proposer (REST)
            # Lógica futura: implementar notificação via proposer se necessário
            
            return False
        except Exception as e:
            log.error("Error notifying client", 
                     client_id=client_id, 
                     error=str(e))
            return False

# Inicializar gerenciador de store
store_manager = StoreManager(state)

# Aplicador de decisões
class DecisionApplier:
    def __init__(self, state: LearnerState, store_manager: StoreManager):
        self.state = state
        self.store_manager = store_manager
        self.pending_decisions = {}  # instance_id -> value
        pending_decisions.set(0)
    
    async def apply_decision(self, instance_id: int, value: Dict[str, Any]) -> Dict[str, Any]:
        """Aplica uma decisão ao sistema, coordenando com o Cluster Store"""
        start_time = time.time()
        operation = value.get("operation")
        path = value.get("path")
        content = value.get("content", "")
        client_id = value.get("client_id")
        
        if not path:
            log.error("Invalid decision: missing path", 
                     instance_id=instance_id,
                     value=value)
            return {"success": False, "error": "Invalid decision: missing path"}
        
        log.info("Applying decision", 
                instance_id=instance_id,
                operation=operation,
                path=path,
                client_id=client_id)
        
        # Atualizar operação do cliente, se fornecido
        if client_id:
            await self.state.update_client_operation(client_id)
        
        # Transformar a decisão em operação para o Cluster Store
        operations = [
            {
                "operation": operation,
                "path": path,
                "content": content,
                "timestamp": time.time(),
                "instance_id": instance_id
            }
        ]
        
        # Executar transação no Cluster Store
        result = await self.store_manager.execute_transaction(operations, instance_id, client_id)
        
        if result.get("success", True):
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
        
        # Invalidar cache de diretório, se aplicável
        if operation in ["CREATE", "MODIFY", "DELETE"]:
            # Extrair diretório pai
            parent_dir = os.path.dirname(path)
            if parent_dir in self.state.directories:
                # Invalidar cache do diretório pai
                if parent_dir == "":
                    parent_dir = "/"
                self.state.directories.pop(parent_dir, None)
        
        apply_latency.observe(time.time() - start_time)
        
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
decision_applier = DecisionApplier(state, store_manager)

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
    # Encaminhar para o Cluster Store via store_manager
    result = await store_manager.list_directory(directory)
    file_operations.labels(operation="list").inc()
    return result

@app.get("/files/{path:path}")
async def read_file(path: str):
    """Lê o conteúdo de um arquivo"""
    # Encaminhar para o Cluster Store via store_manager
    result = await store_manager.read_resource(path)
    file_operations.labels(operation="read").inc()
    return result

@app.post("/propose")
async def propose_file_operation(request: FileOperation):
    """Endpoint para propostas de operações de arquivo (para testes diretos)"""
    # Esta é uma rota de teste que permite operações diretas sem passar pelo proposer
    # Em um sistema real, essas solicitações viriam dos proposers
    
    log.info("Received direct file operation", 
            operation=request.operation,
            path=request.path)
    
    # Criar valor para o Paxos
    value = {
        "operation": request.operation,
        "path": request.path,
        "content": request.content,
        "timestamp": time.time(),
        "client_id": "direct-test"  # Cliente fictício para testes
    }
    
    # Simular uma decisão Paxos, usando timestamp como ID de instância para testes
    instance_id = int(time.time() * 1000) % 1000000
    proposal_number = 1
    
    # Registrar decisão
    state.decisions[instance_id] = {
        "value": value,
        "proposal_number": proposal_number,
        "decided_at": time.time()
    }
    
    # Persistir decisão
    await state.save_decision(instance_id, proposal_number, value)
    decisions_total.inc()
    
    # Aplicar ao Cluster Store
    result = await decision_applier.apply_decision(instance_id, value)
    
    # Atualizar última instância aplicada
    if result.get("success", False):
        state.last_applied_instance = instance_id
        last_applied.set(instance_id)
    
    return {
        "success": result.get("success", False),
        "instance_id": instance_id,
        "transaction_id": result.get("transaction_id"),
        "error": result.get("error")
    }

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

@app.get("/transaction/{transaction_id}")
async def get_transaction_status(transaction_id: str):
    """Endpoint para consultar o status de uma transação"""
    result = await store_manager.get_transaction_status(transaction_id)
    return result

@app.post("/notify-client")
async def notify_client(notification: ClientNotification):
    """Endpoint para notificar um cliente sobre uma decisão/transação"""
    # Esta rota é usada pelos proposers para notificar clientes
    client_id = notification.get("client_id")
    if not client_id:
        return {"success": False, "error": "Client ID is required"}
    
    # Verificar se somos o learner responsável por este cliente
    if not state.should_notify_client(client_id):
        # Encaminhar para o learner responsável
        responsible_learner = state.get_responsible_learner(client_id)
        log.info("Forwarding notification to responsible learner", 
                client_id=client_id,
                responsible_learner=responsible_learner)
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"http://learner-{responsible_learner}:8080/notify-client",
                    json=notification.dict(),
                    timeout=2
                ) as response:
                    if response.status == 200:
                        return await response.json()
                    else:
                        return {"success": False, "error": f"Error forwarding notification: {response.status}"}
        except Exception as e:
            log.error("Error forwarding notification", 
                     client_id=client_id,
                     responsible_learner=responsible_learner,
                     error=str(e))
            return {"success": False, "error": f"Error forwarding notification: {str(e)}"}
    
    # Enviar notificação
    await manager.send_personal_message(notification.dict(), client_id)
    client_notifications.labels(status=notification.status).inc()
    
    return {"success": True}

@app.get("/status")
async def get_status():
    """
    Retorna informações sobre o estado atual do learner.
    Útil para monitoramento e depuração.
    """
    # Verificar status dos nós do Store
    await store_manager.check_store_status()
    
    active_stores = len([s for s in state.store_status.values() if s.get("status") == "ACTIVE"])
    recovering_stores = len([s for s in state.store_status.values() if s.get("is_recovering", False)])
    
    return {
        "node_id": state.node_id,
        "role": "learner",
        "decisions_count": len(state.decisions),
        "last_applied_instance": state.last_applied_instance,
        "pending_decisions": len(decision_applier.pending_decisions),
        "active_stores": active_stores,
        "recovering_stores": recovering_stores,
        "store_status": state.store_status,
        "active_websockets": len(manager.active_connections),
        "transactions_count": len(state.transactions),
        "uptime_seconds": time.time() - app.state.start_time
    }

@app.get("/health")
async def health_check():
    """Endpoint para verificações de saúde"""
    return {"status": "healthy", "node_id": state.node_id, "role": "learner"}

# Websocket para atualizações em tempo real
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket, client_id: Optional[str] = None):
    await manager.connect(websocket, client_id)
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
                
                # Processar comandos do cliente
                if message.get("type") == "get_files":
                    directory = message.get("directory", "/")
                    files = await store_manager.list_directory(directory)
                    await websocket.send_json({
                        "type": "file_list",
                        "data": files
                    })
                elif message.get("type") == "read_file":
                    path = message.get("path")
                    if path:
                        content = await store_manager.read_resource(path)
                        await websocket.send_json({
                            "type": "file_content",
                            "path": path,
                            "data": content
                        })
                elif message.get("type") == "register_client":
                    client_id = message.get("client_id")
                    if client_id:
                        # Atualizar WebSocket para este cliente
                        if client_id in manager.client_connections:
                            old_ws = manager.client_connections[client_id]
                            try:
                                await old_ws.close()
                            except:
                                pass
                        
                        manager.client_connections[client_id] = websocket
                        await websocket.send_json({
                            "type": "registration_success",
                            "client_id": client_id
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
    asyncio.create_task(periodic_store_check())
    asyncio.create_task(periodic_pending_transactions_check())

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

async def periodic_store_check():
    """Verifica periodicamente o status dos nós do Store"""
    # Esperar antes da primeira verificação
    await asyncio.sleep(3)
    
    while True:
        try:
            await store_manager.check_store_status()
        except Exception as e:
            log.error("Error checking store status", error=str(e))
        
        # Aguardar antes da próxima verificação
        await asyncio.sleep(5)  # Verificar a cada 5 segundos

async def periodic_pending_transactions_check():
    """Verifica periodicamente transações pendentes"""
    # Esperar antes da primeira verificação
    await asyncio.sleep(8)
    
    while True:
        try:
            # Verificar transações em estado PREPARING ou COMMITTING que possam estar travadas
            current_time = time.time()
            
            for transaction_id, transaction in list(state.transactions.items()):
                if transaction["status"] in ["PREPARING", "COMMITTING"]:
                    # Se a transação estiver pendente por mais de 30 segundos, verificar status
                    if current_time - transaction["timestamp"] > 30:
                        log.warning("Transaction pending for too long", 
                                  transaction_id=transaction_id,
                                  status=transaction["status"],
                                  pending_time=current_time - transaction["timestamp"])
                        
                        # Consultar stores para verificar status
                        transaction_status = await store_manager.get_transaction_status(transaction_id)
                        
                        # Se todos os stores reportam como COMMITTED, marcar como COMMITTED
                        if transaction_status.get("status") == "COMMITTED":
                            log.info("Transaction found committed in stores, updating status", 
                                    transaction_id=transaction_id)
                            
                            transaction["status"] = "COMMITTED"
                            await state.save_transaction(
                                transaction_id, 
                                "COMMITTED", 
                                transaction["instance_id"],
                                transaction.get("client_id")
                            )
                        # Se todos os stores reportam como ABORTED, marcar como ABORTED
                        elif transaction_status.get("status") == "ABORTED":
                            log.info("Transaction found aborted in stores, updating status", 
                                    transaction_id=transaction_id)
                            
                            transaction["status"] = "ABORTED"
                            await state.save_transaction(
                                transaction_id, 
                                "ABORTED", 
                                transaction["instance_id"],
                                transaction.get("client_id")
                            )
                        # Se já se passaram mais de 2 minutos, considerar falha e abortar
                        elif current_time - transaction["timestamp"] > 120:
                            log.warning("Transaction timed out, marking as failed", 
                                      transaction_id=transaction_id)
                            
                            transaction["status"] = "FAILED"
                            await state.save_transaction(
                                transaction_id, 
                                "FAILED", 
                                transaction["instance_id"],
                                transaction.get("client_id")
                            )
        except Exception as e:
            log.error("Error checking pending transactions", error=str(e))
        
        # Aguardar antes da próxima verificação
        await asyncio.sleep(15)  # Verificar a cada 15 segundos