import os
import yaml
import json
import time
import asyncio
import hashlib
from fastapi import FastAPI, HTTPException, BackgroundTasks
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
app = FastAPI(title="Paxos Store")

# Configurar CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Métricas Prometheus
resource_writes = Counter("paxos_store_writes_total", "Total number of resource writes")
resource_reads = Counter("paxos_store_reads_total", "Total number of resource reads")
resource_modifications = Counter("paxos_store_modifications_total", "Total number of resource modifications")
resource_deletes = Counter("paxos_store_deletes_total", "Total number of resource deletions")
resource_versions = Gauge("paxos_store_resource_versions", "Current version of resources", ["resource"])
sync_operations = Counter("paxos_store_sync_operations_total", "Total number of sync operations")
prepare_total = Counter("paxos_store_prepare_total", "Total number of 2PC prepare operations")
commit_total = Counter("paxos_store_commit_total", "Total number of 2PC commit operations")
abort_total = Counter("paxos_store_abort_total", "Total number of 2PC abort operations")
prepare_latency = Histogram("paxos_store_prepare_latency_seconds", "Time spent in 2PC prepare phase")
commit_latency = Histogram("paxos_store_commit_latency_seconds", "Time spent in 2PC commit phase")
heartbeat_failures = Counter("paxos_store_heartbeat_failures_total", "Total number of heartbeat failures")
active_resources = Gauge("paxos_store_active_resources", "Number of active resources in the store")
pending_transactions = Gauge("paxos_store_pending_transactions", "Number of pending transactions")
failed_transactions = Counter("paxos_store_failed_transactions_total", "Total number of failed transactions")

# Modelos de dados Pydantic
class ResourceOperation(BaseModel):
    """Modelo para operações em recursos"""
    operation: str  # "READ", "WRITE", "MODIFY", "DELETE"
    path: str
    content: Optional[str] = None
    version: Optional[int] = None
    timestamp: Optional[float] = None
    transaction_id: Optional[str] = None
    instance_id: Optional[int] = None

class PrepareRequest(BaseModel):
    """Modelo para solicitações de preparação (fase 1 do 2PC)"""
    transaction_id: str
    operations: List[ResourceOperation]
    learner_id: int
    timestamp: float

class PrepareResponse(BaseModel):
    """Modelo para respostas de preparação"""
    ready: bool
    transaction_id: str
    store_id: int
    reason: Optional[str] = None

class CommitRequest(BaseModel):
    """Modelo para solicitações de commit (fase 2 do 2PC)"""
    transaction_id: str
    commit: bool  # True para commit, False para abort
    learner_id: int

class CommitResponse(BaseModel):
    """Modelo para respostas de commit"""
    success: bool
    transaction_id: str
    store_id: int
    reason: Optional[str] = None

class SyncRequest(BaseModel):
    """Modelo para solicitações de sincronização entre nós do Store"""
    store_id: int
    resources: Optional[List[str]] = None
    from_timestamp: Optional[float] = None
    full_sync: bool = False

class HeartbeatRequest(BaseModel):
    """Modelo para heartbeats entre nós do Store"""
    store_id: int
    timestamp: float
    resources_checksum: str  # Checksum dos recursos para detecção rápida de inconsistências

class RecoveryRequest(BaseModel):
    """Modelo para recuperação de um nó após falha"""
    store_id: int
    last_known_transaction: Optional[str] = None
    resources: Optional[List[str]] = None

# Estado do Store
class StoreState:
    def __init__(self, node_id: int, resource_path: str):
        self.node_id = node_id
        self.resource_path = resource_path
        self.resources = {}  # path -> {content, version, timestamp, node_id}
        self.transactions = {}  # transaction_id -> {status, operations, timestamp}
        self.last_sync_timestamp = 0
        self.is_recovering = False
        self.store_status = {}  # store_id -> {timestamp, status, last_heartbeat}
        
        # Garantir que o diretório de recursos existe
        os.makedirs(resource_path, exist_ok=True)
        
        # Inicializar TinyDB para persistência
        self.db = TinyDB(config["storage"]["path"])
        self.load_state()
        
        # Inicializar métricas
        active_resources.set(len(self.resources))
        pending_transactions.set(len([t for t in self.transactions.values() if t["status"] == "PENDING"]))
    
    def load_state(self):
        """Carrega o estado persistido do disco"""
        try:
            log.info("Loading state from disk")
            
            # Carregar recursos
            resources_table = self.db.table("resources")
            for resource in resources_table.all():
                self.resources[resource["path"]] = {
                    "content": resource["content"],
                    "version": resource["version"],
                    "timestamp": resource["timestamp"],
                    "node_id": resource["node_id"]
                }
                
                # Atualizar métrica para este recurso
                resource_versions.labels(resource=resource["path"]).set(resource["version"])
            
            # Carregar transações
            transactions_table = self.db.table("transactions")
            for transaction in transactions_table.all():
                self.transactions[transaction["transaction_id"]] = {
                    "status": transaction["status"],
                    "operations": transaction["operations"],
                    "timestamp": transaction["timestamp"],
                    "learner_id": transaction.get("learner_id")
                }
            
            log.info("State loaded successfully", 
                    resources_count=len(self.resources),
                    transactions_count=len(self.transactions))
        except Exception as e:
            log.error("Error loading state", error=str(e))
    
    async def save_resource(self, path: str, content: str, version: int, timestamp: float, node_id: int):
        """Persiste um recurso no disco"""
        try:
            # Salvar no TinyDB
            resources_table = self.db.table("resources")
            Resource = Query()
            resources_table.upsert(
                {
                    "path": path,
                    "content": content,
                    "version": version,
                    "timestamp": timestamp,
                    "node_id": node_id
                },
                Resource.path == path
            )
            
            # Salvar como arquivo no sistema de arquivos
            resource_path = os.path.join(self.resource_path, self._normalize_path(path))
            
            # Garantir que o diretório pai existe
            os.makedirs(os.path.dirname(resource_path), exist_ok=True)
            
            async with aiofiles.open(resource_path, 'w') as f:
                await f.write(content)
            
            # Atualizar métrica
            resource_versions.labels(resource=path).set(version)
            
            log.debug("Resource saved to disk", 
                     path=path,
                     version=version)
        except Exception as e:
            log.error("Error saving resource", 
                     path=path, 
                     error=str(e))
    
    async def delete_resource(self, path: str):
        """Remove um recurso do disco"""
        try:
            # Remover do TinyDB
            resources_table = self.db.table("resources")
            Resource = Query()
            resources_table.remove(Resource.path == path)
            
            # Remover arquivo do sistema de arquivos
            resource_path = os.path.join(self.resource_path, self._normalize_path(path))
            
            if os.path.exists(resource_path):
                if os.path.isdir(resource_path):
                    import shutil
                    shutil.rmtree(resource_path)
                else:
                    os.remove(resource_path)
            
            # Atualizar métrica
            resource_versions.labels(resource=path).set(0)
            
            log.debug("Resource deleted from disk", path=path)
        except Exception as e:
            log.error("Error deleting resource", 
                     path=path, 
                     error=str(e))
    
    async def save_transaction(self, transaction_id: str, status: str, operations: List, timestamp: float, learner_id: int):
        """Persiste uma transação no disco"""
        try:
            transactions_table = self.db.table("transactions")
            Transaction = Query()
            transactions_table.upsert(
                {
                    "transaction_id": transaction_id,
                    "status": status,
                    "operations": operations,
                    "timestamp": timestamp,
                    "learner_id": learner_id
                },
                Transaction.transaction_id == transaction_id
            )
            
            # Atualizar métrica
            pending_count = len([t for t in self.transactions.values() if t["status"] == "PENDING"])
            pending_transactions.set(pending_count)
            
            log.debug("Transaction saved to disk", 
                     transaction_id=transaction_id,
                     status=status)
        except Exception as e:
            log.error("Error saving transaction", 
                     transaction_id=transaction_id, 
                     error=str(e))
    
    def calculate_resources_checksum(self):
        """Calcula um checksum para todos os recursos para verificação rápida de inconsistências"""
        checksum_data = ""
        for path, resource in sorted(self.resources.items()):
            checksum_data += f"{path}:{resource['version']}:{resource['timestamp']};"
        
        return hashlib.md5(checksum_data.encode()).hexdigest()
    
    def _normalize_path(self, path: str) -> str:
        """Normaliza um caminho, removendo .., /, etc."""
        # Remover / inicial para juntar corretamente com o caminho base
        if path.startswith("/"):
            path = path[1:]
        
        # Usar os.path.normpath para lidar com .. e .
        return os.path.normpath(path)

# Inicializar estado do Store
state = StoreState(config["node"]["id"], config["storage"]["resourcePath"])

# Gerenciador de recursos
class ResourceManager:
    def __init__(self, state: StoreState):
        self.state = state
    
    async def read_resource(self, path: str) -> Dict[str, Any]:
        """Lê um recurso do sistema"""
        resource_reads.inc()
        
        if path not in self.state.resources:
            return {
                "success": False,
                "error": "Resource not found",
                "path": path
            }
        
        resource = self.state.resources[path]
        
        return {
            "success": True,
            "path": path,
            "content": resource["content"],
            "version": resource["version"],
            "timestamp": resource["timestamp"]
        }
    
    async def write_resource(self, path: str, content: str, transaction_id: str) -> Dict[str, Any]:
        """Escreve um recurso no sistema"""
        resource_writes.inc()
        
        current_time = time.time()
        
        # Determinar próxima versão
        current_version = 0
        if path in self.state.resources:
            current_version = self.state.resources[path]["version"]
        
        new_version = current_version + 1
        
        # Atualizar estado em memória
        self.state.resources[path] = {
            "content": content,
            "version": new_version,
            "timestamp": current_time,
            "node_id": self.state.node_id
        }
        
        # Persistir no disco
        await self.state.save_resource(path, content, new_version, current_time, self.state.node_id)
        
        # Atualizar métrica
        active_resources.set(len(self.state.resources))
        
        log.info("Resource written", 
                path=path, 
                version=new_version,
                transaction_id=transaction_id)
        
        return {
            "success": True,
            "path": path,
            "version": new_version,
            "timestamp": current_time
        }
    
    async def modify_resource(self, path: str, content: str, transaction_id: str) -> Dict[str, Any]:
        """Modifica um recurso existente"""
        resource_modifications.inc()
        
        if path not in self.state.resources:
            return {
                "success": False,
                "error": "Resource not found",
                "path": path
            }
        
        # A partir daqui é igual a write_resource
        return await self.write_resource(path, content, transaction_id)
    
    async def delete_resource(self, path: str, transaction_id: str) -> Dict[str, Any]:
        """Remove um recurso do sistema"""
        resource_deletes.inc()
        
        if path not in self.state.resources:
            return {
                "success": False,
                "error": "Resource not found",
                "path": path
            }
        
        # Remover do estado em memória
        del self.state.resources[path]
        
        # Remover do disco
        await self.state.delete_resource(path)
        
        # Atualizar métrica
        active_resources.set(len(self.state.resources))
        
        log.info("Resource deleted", 
                path=path, 
                transaction_id=transaction_id)
        
        return {
            "success": True,
            "path": path
        }

# Inicializar gerenciador de recursos
resource_manager = ResourceManager(state)

# Gerenciador de transações (Two-Phase Commit)
class TransactionManager:
    def __init__(self, state: StoreState, resource_manager: ResourceManager):
        self.state = state
        self.resource_manager = resource_manager
        self.locks = set()  # Conjunto de recursos bloqueados
    
    async def prepare(self, request: PrepareRequest) -> PrepareResponse:
        """Fase 1 do 2PC: Preparação"""
        start_time = time.time()
        prepare_total.inc()
        
        transaction_id = request.transaction_id
        operations = request.operations
        learner_id = request.learner_id
        
        log.info("Preparing transaction", 
                transaction_id=transaction_id,
                operations_count=len(operations),
                learner_id=learner_id)
        
        # Verificar se todas as operações podem ser executadas
        try:
            # Verificar se algum recurso está bloqueado
            for operation in operations:
                if operation.path in self.locks:
                    log.warning("Resource locked during prepare", 
                               path=operation.path,
                               transaction_id=transaction_id)
                    prepare_latency.observe(time.time() - start_time)
                    return PrepareResponse(
                        ready=False,
                        transaction_id=transaction_id,
                        store_id=self.state.node_id,
                        reason=f"Resource {operation.path} is locked by another transaction"
                    )
            
            # Bloquear recursos
            for operation in operations:
                self.locks.add(operation.path)
            
            # Salvar a transação como PREPARED
            self.state.transactions[transaction_id] = {
                "status": "PREPARED",
                "operations": [op.dict() for op in operations],
                "timestamp": time.time(),
                "learner_id": learner_id
            }
            
            await self.state.save_transaction(
                transaction_id, 
                "PREPARED", 
                [op.dict() for op in operations], 
                time.time(),
                learner_id
            )
            
            # Atualizar métrica
            pending_transactions.set(len([t for t in self.state.transactions.values() if t["status"] in ["PREPARED", "COMMITTING"]]))
            
            prepare_latency.observe(time.time() - start_time)
            
            return PrepareResponse(
                ready=True,
                transaction_id=transaction_id,
                store_id=self.state.node_id
            )
            
        except Exception as e:
            log.error("Error during prepare", 
                     transaction_id=transaction_id,
                     error=str(e))
            
            # Liberar locks em caso de erro
            for operation in operations:
                if operation.path in self.locks:
                    self.locks.remove(operation.path)
            
            # Atualizar métrica
            failed_transactions.inc()
            
            prepare_latency.observe(time.time() - start_time)
            
            return PrepareResponse(
                ready=False,
                transaction_id=transaction_id,
                store_id=self.state.node_id,
                reason=f"Error during prepare: {str(e)}"
            )
    
    async def commit(self, request: CommitRequest) -> CommitResponse:
        """Fase 2 do 2PC: Commit ou Abort"""
        start_time = time.time()
        commit_total.inc()
        
        transaction_id = request.transaction_id
        should_commit = request.commit
        learner_id = request.learner_id
        
        log.info("Committing transaction", 
                transaction_id=transaction_id,
                commit=should_commit,
                learner_id=learner_id)
        
        # Verificar se a transação existe e está preparada
        if transaction_id not in self.state.transactions:
            log.warning("Transaction not found", transaction_id=transaction_id)
            commit_latency.observe(time.time() - start_time)
            return CommitResponse(
                success=False,
                transaction_id=transaction_id,
                store_id=self.state.node_id,
                reason="Transaction not found"
            )
        
        transaction = self.state.transactions[transaction_id]
        
        if transaction["status"] != "PREPARED":
            log.warning("Transaction not in PREPARED state", 
                       transaction_id=transaction_id,
                       status=transaction["status"])
            commit_latency.observe(time.time() - start_time)
            return CommitResponse(
                success=False,
                transaction_id=transaction_id,
                store_id=self.state.node_id,
                reason=f"Transaction not in PREPARED state: {transaction['status']}"
            )
        
        try:
            if should_commit:
                # Atualizar status para COMMITTING
                transaction["status"] = "COMMITTING"
                await self.state.save_transaction(
                    transaction_id, 
                    "COMMITTING", 
                    transaction["operations"], 
                    time.time(),
                    learner_id
                )
                
                # Aplicar operações
                for operation_data in transaction["operations"]:
                    operation = ResourceOperation(**operation_data)
                    
                    if operation.operation == "WRITE":
                        await self.resource_manager.write_resource(
                            operation.path, 
                            operation.content or "", 
                            transaction_id
                        )
                    elif operation.operation == "MODIFY":
                        await self.resource_manager.modify_resource(
                            operation.path, 
                            operation.content or "", 
                            transaction_id
                        )
                    elif operation.operation == "DELETE":
                        await self.resource_manager.delete_resource(
                            operation.path, 
                            transaction_id
                        )
                
                # Atualizar status para COMMITTED
                transaction["status"] = "COMMITTED"
                await self.state.save_transaction(
                    transaction_id, 
                    "COMMITTED", 
                    transaction["operations"], 
                    time.time(),
                    learner_id
                )
                
                log.info("Transaction committed successfully", 
                        transaction_id=transaction_id)
            else:
                # Abortar a transação
                transaction["status"] = "ABORTED"
                await self.state.save_transaction(
                    transaction_id, 
                    "ABORTED", 
                    transaction["operations"], 
                    time.time(),
                    learner_id
                )
                
                log.info("Transaction aborted", 
                        transaction_id=transaction_id)
                abort_total.inc()
            
            # Liberar locks
            for operation_data in transaction["operations"]:
                operation = ResourceOperation(**operation_data)
                if operation.path in self.locks:
                    self.locks.remove(operation.path)
            
            # Atualizar métrica
            pending_transactions.set(len([t for t in self.state.transactions.values() if t["status"] in ["PREPARED", "COMMITTING"]]))
            
            commit_latency.observe(time.time() - start_time)
            
            return CommitResponse(
                success=True,
                transaction_id=transaction_id,
                store_id=self.state.node_id
            )
        
        except Exception as e:
            log.error("Error during commit", 
                     transaction_id=transaction_id,
                     error=str(e))
            
            # Em caso de erro, marcar como pendente para resolver depois
            if should_commit:
                transaction["status"] = "COMMIT_FAILED"
            else:
                transaction["status"] = "ABORT_FAILED"
            
            await self.state.save_transaction(
                transaction_id, 
                transaction["status"], 
                transaction["operations"], 
                time.time(),
                learner_id
            )
            
            # Liberar locks mesmo em caso de erro
            for operation_data in transaction["operations"]:
                operation = ResourceOperation(**operation_data)
                if operation.path in self.locks:
                    self.locks.remove(operation.path)
            
            # Atualizar métrica
            failed_transactions.inc()
            
            commit_latency.observe(time.time() - start_time)
            
            return CommitResponse(
                success=False,
                transaction_id=transaction_id,
                store_id=self.state.node_id,
                reason=f"Error during commit: {str(e)}"
            )
    
    async def recover_pending_transactions(self, background_tasks: BackgroundTasks):
        """Recupera transações pendentes após reinicialização do nó"""
        pending_commits = [
            tid for tid, t in self.state.transactions.items() 
            if t["status"] in ["PREPARED", "COMMITTING", "COMMIT_FAILED", "ABORT_FAILED"]
        ]
        
        if not pending_commits:
            log.info("No pending transactions to recover")
            return
        
        log.info("Recovering pending transactions", 
                count=len(pending_commits))
        
        for transaction_id in pending_commits:
            background_tasks.add_task(
                self.recover_transaction, 
                transaction_id
            )
    
    async def recover_transaction(self, transaction_id: str):
        """Recupera uma transação específica consultando outros nós"""
        if transaction_id not in self.state.transactions:
            log.warning("Transaction not found for recovery", 
                       transaction_id=transaction_id)
            return
        
        transaction = self.state.transactions[transaction_id]
        learner_id = transaction.get("learner_id")
        
        if not learner_id:
            log.warning("Learner ID not found for transaction", 
                       transaction_id=transaction_id)
            return
        
        log.info("Recovering transaction", 
                transaction_id=transaction_id,
                status=transaction["status"],
                learner_id=learner_id)
        
        # Consultar o learner para determinar o status final da transação
        try:
            async with aiohttp.ClientSession() as session:
                learner_url = f"http://learner-{learner_id}:8080"
                
                async with session.get(
                    f"{learner_url}/transaction/{transaction_id}",
                    timeout=5
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        final_status = data.get("status")
                        
                        log.info("Retrieved transaction status from learner", 
                                transaction_id=transaction_id,
                                final_status=final_status)
                        
                        if final_status == "COMMITTED":
                            # Reexecutar commit
                            await self.commit(
                                CommitRequest(
                                    transaction_id=transaction_id,
                                    commit=True,
                                    learner_id=learner_id
                                )
                            )
                        elif final_status == "ABORTED":
                            # Reexecutar abort
                            await self.commit(
                                CommitRequest(
                                    transaction_id=transaction_id,
                                    commit=False,
                                    learner_id=learner_id
                                )
                            )
                        else:
                            log.warning("Learner has no final status for transaction", 
                                      transaction_id=transaction_id,
                                      learner_status=final_status)
                            
                            # Tentar consultar outros stores
                            await self.check_transaction_with_other_stores(transaction_id)
                    else:
                        log.warning("Failed to retrieve transaction status from learner", 
                                  transaction_id=transaction_id,
                                  status_code=response.status)
                        
                        # Tentar consultar outros stores
                        await self.check_transaction_with_other_stores(transaction_id)
        except Exception as e:
            log.error("Error recovering transaction", 
                     transaction_id=transaction_id,
                     error=str(e))
            
            # Tentar consultar outros stores
            await self.check_transaction_with_other_stores(transaction_id)
    
    async def check_transaction_with_other_stores(self, transaction_id: str):
        """Consulta outros nós do Store para determinar o status de uma transação"""
        if transaction_id not in self.state.transactions:
            return
        
        transaction = self.state.transactions[transaction_id]
        
        log.info("Checking transaction with other stores", 
                transaction_id=transaction_id)
        
        committed_count = 0
        aborted_count = 0
        
        for store_id in range(1, 4):
            if store_id == self.state.node_id:
                continue
            
            try:
                async with aiohttp.ClientSession() as session:
                    store_url = f"http://store-{store_id}:8080"
                    
                    async with session.get(
                        f"{store_url}/transaction/{transaction_id}",
                        timeout=2
                    ) as response:
                        if response.status == 200:
                            data = await response.json()
                            status = data.get("status")
                            
                            if status == "COMMITTED":
                                committed_count += 1
                            elif status == "ABORTED":
                                aborted_count += 1
            except Exception as e:
                log.warning("Error checking transaction with store", 
                          transaction_id=transaction_id,
                          store_id=store_id,
                          error=str(e))
        
        log.info("Transaction status from other stores", 
                transaction_id=transaction_id,
                committed_count=committed_count,
                aborted_count=aborted_count)
        
        # Decidir com base na maioria
        if committed_count > 0 and committed_count >= aborted_count:
            # Pelo menos um nó commitou e nenhum abortou
            log.info("Committing transaction based on other stores", 
                    transaction_id=transaction_id)
            
            await self.commit(
                CommitRequest(
                    transaction_id=transaction_id,
                    commit=True,
                    learner_id=transaction.get("learner_id", 1)
                )
            )
        elif aborted_count > 0:
            # Pelo menos um nó abortou
            log.info("Aborting transaction based on other stores", 
                    transaction_id=transaction_id)
            
            await self.commit(
                CommitRequest(
                    transaction_id=transaction_id,
                    commit=False,
                    learner_id=transaction.get("learner_id", 1)
                )
            )
        else:
            # Nenhum nó tem informação conclusiva
            log.warning("No conclusive information from other stores", 
                      transaction_id=transaction_id)
            
            # Manter como pendente para tentar novamente mais tarde
            pass

# Inicializar gerenciador de transações
transaction_manager = TransactionManager(state, resource_manager)

# Gerenciador de sincronização
class SyncManager:
    def __init__(self, state: StoreState, resource_manager: ResourceManager):
        self.state = state
        self.resource_manager = resource_manager
    
    async def sync_with_other_stores(self, background_tasks: BackgroundTasks):
        """Sincroniza com outros nós do Store"""
        if self.state.is_recovering:
            log.info("Node is in recovery mode, skipping sync")
            return
        
        sync_operations.inc()
        
        log.info("Starting sync with other stores")
        
        # Para cada nó do Store, verificar recursos
        for store_id in range(1, 4):
            if store_id == self.state.node_id:
                continue
            
            try:
                async with aiohttp.ClientSession() as session:
                    store_url = f"http://store-{store_id}:8080"
                    
                    # Verificar se o nó está ativo com heartbeat
                    try:
                        async with session.post(
                            f"{store_url}/heartbeat",
                            json={
                                "store_id": self.state.node_id,
                                "timestamp": time.time(),
                                "resources_checksum": self.state.calculate_resources_checksum()
                            },
                            timeout=2
                        ) as response:
                            if response.status == 200:
                                data = await response.json()
                                their_checksum = data.get("resources_checksum")
                                their_timestamp = data.get("timestamp")
                                
                                self.state.store_status[store_id] = {
                                    "status": "ACTIVE",
                                    "last_heartbeat": time.time(),
                                    "resources_checksum": their_checksum,
                                    "timestamp": their_timestamp
                                }
                                
                                # Se checksums diferentes, sincronizar
                                if their_checksum != self.state.calculate_resources_checksum():
                                    log.info("Resource checksums differ, syncing resources", 
                                            store_id=store_id)
                                    
                                    background_tasks.add_task(
                                        self.sync_resources_with_store,
                                        store_id,
                                        their_timestamp
                                    )
                            else:
                                log.warning("Unexpected status from heartbeat", 
                                          store_id=store_id,
                                          status=response.status)
                                
                                heartbeat_failures.inc()
                                
                                self.state.store_status[store_id] = {
                                    "status": "ERROR",
                                    "last_heartbeat": time.time(),
                                    "error": f"Unexpected status: {response.status}"
                                }
                    except Exception as e:
                        log.warning("Error sending heartbeat", 
                                  store_id=store_id,
                                  error=str(e))
                        
                        heartbeat_failures.inc()
                        
                        self.state.store_status[store_id] = {
                            "status": "ERROR",
                            "last_heartbeat": time.time(),
                            "error": str(e)
                        }
            except Exception as e:
                log.error("Error during sync", 
                         store_id=store_id,
                         error=str(e))
    
    async def sync_resources_with_store(self, store_id: int, their_timestamp: float):
        """Sincroniza recursos com um nó específico"""
        try:
            async with aiohttp.ClientSession() as session:
                store_url = f"http://store-{store_id}:8080"
                
                # Solicitar catálogo de recursos
                async with session.get(
                    f"{store_url}/resources/catalog",
                    timeout=5
                ) as response:
                    if response.status == 200:
                        catalog = await response.json()
                        
                        # Comparar com recursos locais
                        for resource_info in catalog.get("resources", []):
                            path = resource_info["path"]
                            version = resource_info["version"]
                            timestamp = resource_info["timestamp"]
                            
                            # Se recurso não existir localmente ou tiver versão menor
                            if (path not in self.state.resources or 
                                self.state.resources[path]["version"] < version or 
                                (self.state.resources[path]["version"] == version and 
                                 self.state.resources[path]["timestamp"] < timestamp)):
                                
                                log.info("Syncing resource from other store", 
                                        path=path,
                                        store_id=store_id,
                                        their_version=version,
                                        our_version=self.state.resources.get(path, {}).get("version", 0))
                                
                                # Buscar recurso atualizado
                                async with session.get(
                                    f"{store_url}/resources/{path}",
                                    timeout=5
                                ) as resource_response:
                                    if resource_response.status == 200:
                                        resource_data = await resource_response.json()
                                        
                                        if resource_data.get("success", False):
                                            # Atualizar localmente
                                            self.state.resources[path] = {
                                                "content": resource_data["content"],
                                                "version": resource_data["version"],
                                                "timestamp": resource_data["timestamp"],
                                                "node_id": store_id  # Manter rastreamento de origem
                                            }
                                            
                                            # Persistir no disco
                                            await self.state.save_resource(
                                                path,
                                                resource_data["content"],
                                                resource_data["version"],
                                                resource_data["timestamp"],
                                                store_id
                                            )
                        
                        # Verificar recursos locais que não existem no outro nó
                        their_paths = set(res["path"] for res in catalog.get("resources", []))
                        our_paths = set(self.state.resources.keys())
                        
                        for path in our_paths - their_paths:
                            # Se o recurso existir apenas localmente, verificar se é mais recente
                            if path in self.state.resources and self.state.resources[path]["timestamp"] > their_timestamp:
                                log.info("We have a resource they don't", 
                                        path=path,
                                        store_id=store_id)
                                
                                # Enviar nosso recurso para o outro nó
                                resource_data = {
                                    "operation": "WRITE",
                                    "path": path,
                                    "content": self.state.resources[path]["content"],
                                    "version": self.state.resources[path]["version"],
                                    "timestamp": self.state.resources[path]["timestamp"]
                                }
                                
                                async with session.post(
                                    f"{store_url}/resources/sync",
                                    json=resource_data,
                                    timeout=5
                                ) as sync_response:
                                    if sync_response.status == 200:
                                        log.info("Successfully sent resource to other store", 
                                                path=path,
                                                store_id=store_id)
                                    else:
                                        log.warning("Failed to send resource to other store", 
                                                  path=path,
                                                  store_id=store_id,
                                                  status=sync_response.status)
                    else:
                        log.warning("Failed to get resource catalog", 
                                  store_id=store_id,
                                  status=response.status)
        except Exception as e:
            log.error("Error syncing resources with store", 
                     store_id=store_id,
                     error=str(e))
    
    async def handle_sync_request(self, resource_data: ResourceOperation):
        """Processa uma requisição de sincronização de recurso de outro nó"""
        path = resource_data.path
        content = resource_data.content or ""
        version = resource_data.version or 1
        timestamp = resource_data.timestamp or time.time()
        
        log.info("Received sync request", 
                path=path,
                version=version)
        
        # Verificar se devemos aceitar a sincronização
        if path in self.state.resources:
            our_version = self.state.resources[path]["version"]
            our_timestamp = self.state.resources[path]["timestamp"]
            
            if our_version > version or (our_version == version and our_timestamp >= timestamp):
                log.info("Rejecting sync, our version is newer", 
                        path=path,
                        our_version=our_version,
                        their_version=version)
                
                return {
                    "success": False,
                    "reason": "Local version is newer",
                    "path": path,
                    "our_version": our_version,
                    "our_timestamp": our_timestamp
                }
        
        # Atualizar recurso localmente
        self.state.resources[path] = {
            "content": content,
            "version": version,
            "timestamp": timestamp,
            "node_id": self.state.node_id
        }
        
        # Persistir no disco
        await self.state.save_resource(
            path,
            content,
            version,
            timestamp,
            self.state.node_id
        )
        
        return {
            "success": True,
            "path": path,
            "version": version,
            "timestamp": timestamp
        }
    
    async def start_recovery(self, background_tasks: BackgroundTasks):
        """Inicia o processo de recuperação do nó"""
        log.info("Starting recovery process")
        
        self.state.is_recovering = True
        
        # Recuperar transações pendentes
        background_tasks.add_task(
            transaction_manager.recover_pending_transactions,
            background_tasks
        )
        
        # Sincronizar recursos com outros nós
        for store_id in range(1, 4):
            if store_id == self.state.node_id:
                continue
            
            background_tasks.add_task(
                self.sync_resources_with_store,
                store_id,
                0  # Timestamp zero para forçar sincronização completa
            )
        
        # Após completar a recuperação, sair do modo de recuperação
        # (Isto deveria ser feito após todas as tarefas acima, mas por simplicidade colocamos aqui)
        await asyncio.sleep(10)  # Esperar algum tempo para sincronização
        self.state.is_recovering = False
        
        log.info("Recovery process completed")

# Inicializar gerenciador de sincronização
sync_manager = SyncManager(state, resource_manager)

# Rotas da API
@app.post("/prepare")
async def prepare(request: PrepareRequest):
    """Endpoint para a fase 1 do protocolo 2PC (Preparação)"""
    response = await transaction_manager.prepare(request)
    return response

@app.post("/commit")
async def commit(request: CommitRequest):
    """Endpoint para a fase 2 do protocolo 2PC (Commit)"""
    response = await transaction_manager.commit(request)
    return response

@app.get("/resources/{path:path}")
async def get_resource(path: str):
    """Endpoint para ler um recurso"""
    result = await resource_manager.read_resource(path)
    return result

@app.post("/resources/sync")
async def sync_resource(resource: ResourceOperation):
    """Endpoint para sincronização de recursos entre nós do Store"""
    result = await sync_manager.handle_sync_request(resource)
    return result

@app.get("/resources/catalog")
async def get_resources_catalog():
    """Retorna catálogo de recursos com metadados"""
    resources_list = []
    
    for path, resource in state.resources.items():
        resources_list.append({
            "path": path,
            "version": resource["version"],
            "timestamp": resource["timestamp"],
            "node_id": resource["node_id"],
            "size": len(resource["content"])
        })
    
    return {
        "store_id": state.node_id,
        "timestamp": time.time(),
        "resources": resources_list,
        "checksum": state.calculate_resources_checksum()
    }

@app.post("/heartbeat")
async def heartbeat(request: HeartbeatRequest, background_tasks: BackgroundTasks):
    """Endpoint para heartbeats entre nós do Store"""
    store_id = request.store_id
    timestamp = request.timestamp
    their_checksum = request.resources_checksum
    
    # Atualizar estado do nó que enviou o heartbeat
    state.store_status[store_id] = {
        "status": "ACTIVE",
        "last_heartbeat": time.time(),
        "resources_checksum": their_checksum,
        "timestamp": timestamp
    }
    
    # Se checksums forem diferentes, iniciar sincronização em background
    our_checksum = state.calculate_resources_checksum()
    if their_checksum != our_checksum:
        log.info("Resource checksums differ during heartbeat", 
                store_id=store_id,
                their_checksum=their_checksum,
                our_checksum=our_checksum)
        
        background_tasks.add_task(
            sync_manager.sync_resources_with_store,
            store_id,
            timestamp
        )
    
    return {
        "store_id": state.node_id,
        "timestamp": time.time(),
        "resources_checksum": our_checksum
    }

@app.get("/transaction/{transaction_id}")
async def get_transaction(transaction_id: str):
    """Endpoint para consultar o status de uma transação"""
    if transaction_id not in state.transactions:
        return {"status": "NOT_FOUND", "transaction_id": transaction_id}
    
    transaction = state.transactions[transaction_id]
    
    return {
        "transaction_id": transaction_id,
        "status": transaction["status"],
        "timestamp": transaction["timestamp"],
        "operations_count": len(transaction["operations"])
    }

@app.get("/status")
async def get_status():
    """Retorna informações sobre o estado atual do Store"""
    pending_count = len([t for t in state.transactions.values() if t["status"] in ["PREPARED", "COMMITTING"]])
    
    return {
        "node_id": state.node_id,
        "role": "store",
        "is_recovering": state.is_recovering,
        "resources_count": len(state.resources),
        "transactions_count": len(state.transactions),
        "pending_transactions": pending_count,
        "store_status": state.store_status,
        "resources_checksum": state.calculate_resources_checksum(),
        "uptime_seconds": time.time() - app.state.start_time
    }

@app.post("/recover")
async def recover(background_tasks: BackgroundTasks):
    """Inicia o processo de recuperação do nó"""
    if state.is_recovering:
        return {"success": False, "message": "Recovery already in progress"}
    
    background_tasks.add_task(sync_manager.start_recovery, background_tasks)
    
    return {"success": True, "message": "Recovery process initiated"}

@app.get("/metrics")
async def metrics():
    """Expõe métricas no formato Prometheus"""
    return generate_latest()

# Tarefas periódicas
async def periodic_sync():
    """Sincroniza periodicamente com outros nós do Store"""
    while True:
        try:
            background_tasks = BackgroundTasks()
            await sync_manager.sync_with_other_stores(background_tasks)
        except Exception as e:
            log.error("Error in periodic sync", error=str(e))
        
        # Esperar antes da próxima sincronização
        await asyncio.sleep(config["storage"]["syncIntervalSeconds"])

async def periodic_pending_transactions_check():
    """Verifica periodicamente transações pendentes"""
    while True:
        try:
            background_tasks = BackgroundTasks()
            await transaction_manager.recover_pending_transactions(background_tasks)
        except Exception as e:
            log.error("Error checking pending transactions", error=str(e))
        
        # Esperar antes da próxima verificação
        await asyncio.sleep(30)  # A cada 30 segundos

# Eventos do ciclo de vida da aplicação
@app.on_event("startup")
async def startup_event():
    """Executado ao iniciar a aplicação"""
    app.state.start_time = time.time()
    log.info("Store starting up", 
            node_id=state.node_id, 
            config=config)
    
    # Iniciar tarefas de background
    asyncio.create_task(periodic_sync())
    asyncio.create_task(periodic_pending_transactions_check())
    
    # Verificar por transações pendentes na inicialização
    background_tasks = BackgroundTasks()
    await transaction_manager.recover_pending_transactions(background_tasks)

@app.on_event("shutdown")
async def shutdown_event():
    """Executado ao encerrar a aplicação"""
    log.info("Store shutting down", 
            node_id=state.node_id,
            resources_count=len(state.resources),
            transactions_count=len(state.transactions))