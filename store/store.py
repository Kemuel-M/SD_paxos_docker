"""
Implementação do componente Cluster Store.
"""
import asyncio
import time
import logging
import threading
from typing import Dict, Any, List, Tuple, Optional, Set

from common.utils import HttpClient, get_current_timestamp
from common.metrics import store_metrics
from common.models import (
    ResourceData,
    TwoPhaseCommitPrepareRequest,
    TwoPhaseCommitPrepareResponse,
    TwoPhaseCommitCommitRequest,
    TwoPhaseCommitCommitResponse
)
from store import persistence
from store.config import (
    NODE_ID,
    STORE_ENDPOINTS,
    SYNC_INTERVAL,
    LOCK_TIMEOUT
)


class ClusterStore:
    """
    Implementação do componente Cluster Store.
    """
    def __init__(self, logger: logging.Logger):
        """
        Inicializa o Cluster Store.
        
        Args:
            logger: Logger configurado.
        """
        self.logger = logger
        
        # Recursos armazenados (apenas um neste projeto: "R")
        self.resources = {}  # {resource_id: ResourceData}
        
        # Transações pendentes
        self.pending_txns = {}  # {instance_id: {...}}
        
        # Locks para operações
        self.locks = {}  # {resource_id: asyncio.Lock}
        
        # Tempo de expiração de locks
        self.lock_expiry = {}  # {resource_id: timestamp}
        
        # Clientes HTTP para outros nós do Cluster Store
        self.store_clients = {
            endpoint: HttpClient(endpoint) for endpoint in STORE_ENDPOINTS
        }
        
        # Tarefa de sincronização periódica
        self.sync_task = None
        self.running = False
        
    async def initialize(self):
        """
        Inicializa o Cluster Store carregando dados persistentes.
        """
        # Carregar recurso R
        self.resources["R"] = await persistence.load_resource("R")
        
        # Carregar transações pendentes
        self.pending_txns = await persistence.load_pending_transactions()
        
        # Inicializar lock para recurso R
        self.locks["R"] = asyncio.Lock()
        self.lock_expiry["R"] = 0
        
        # Registrar versão do recurso R no Prometheus
        store_metrics["resource_version"].labels(node_id=NODE_ID, resource="R").set(self.resources["R"].version)
        
        # Iniciar tarefa de sincronização periódica
        self.running = True
        self.sync_task = asyncio.create_task(self._sync_loop())
        
        self.logger.info(f"Cluster Store inicializado: recurso R (versão {self.resources['R'].version})")
        
    async def shutdown(self):
        """
        Finaliza o Cluster Store, salvando dados persistentes.
        """
        # Parar tarefa de sincronização
        self.running = False
        if self.sync_task:
            self.sync_task.cancel()
            try:
                await self.sync_task
            except asyncio.CancelledError:
                pass
                
        # Salvar recursos
        for resource_id, resource in self.resources.items():
            await persistence.save_resource(resource, resource_id)
            
        # Salvar transações pendentes
        await persistence.save_pending_transactions(self.pending_txns)
        
        # Fechar conexões HTTP
        for client in self.store_clients.values():
            await client.close()
        
        self.logger.info("Cluster Store finalizado")
        
    async def prepare(self, request: TwoPhaseCommitPrepareRequest) -> TwoPhaseCommitPrepareResponse:
        """
        Processa uma requisição prepare do protocolo 2PC.
        
        Args:
            request: Requisição prepare.
            
        Returns:
            Resposta prepare.
        """
        # Incrementar contador de mensagens prepare recebidas
        store_metrics["prepare"].labels(node_id=NODE_ID, status="received").inc()
        
        # Extrair dados da requisição
        instance_id = request.instanceId
        resource_id = request.resource
        data = request.data
        client_id = request.clientId
        timestamp = request.timestamp
        
        # Converter instanceId para string para usar como chave
        instance_id_str = str(instance_id)
        
        # Verificar se o recurso existe
        if resource_id not in self.resources:
            self.logger.warning(f"Recurso {resource_id} não encontrado")
            store_metrics["prepare"].labels(node_id=NODE_ID, status="resource_not_found").inc()
            return TwoPhaseCommitPrepareResponse(ready=False, currentVersion=-1)
            
        # Verificar se estamos prontos para aceitar a escrita
        try:
            # Adquirir lock com timeout (evitar deadlocks)
            lock_acquired = await self._acquire_lock(resource_id)
            
            if not lock_acquired:
                self.logger.warning(f"Não foi possível adquirir lock para o recurso {resource_id}")
                store_metrics["prepare"].labels(node_id=NODE_ID, status="lock_timeout").inc()
                return TwoPhaseCommitPrepareResponse(ready=False, currentVersion=self.resources[resource_id].version)
                
            # Registrar transação pendente
            self.pending_txns[instance_id_str] = {
                "instanceId": instance_id,
                "resourceId": resource_id,
                "data": data,
                "clientId": client_id,
                "timestamp": timestamp,
                "status": "prepared",
                "prepare_time": get_current_timestamp()
            }
            
            # Persistir transações pendentes
            await persistence.save_pending_transactions(self.pending_txns)
            
            # Incrementar contador de prepares bem-sucedidos
            store_metrics["prepare"].labels(node_id=NODE_ID, status="success").inc()
            
            # Construir resposta
            return TwoPhaseCommitPrepareResponse(
                ready=True,
                currentVersion=self.resources[resource_id].version
            )
            
        except Exception as e:
            self.logger.error(f"Erro ao processar prepare: {e}")
            store_metrics["prepare"].labels(node_id=NODE_ID, status="error").inc()
            return TwoPhaseCommitPrepareResponse(ready=False, currentVersion=self.resources[resource_id].version)
        finally:
            # Liberar lock
            self._release_lock(resource_id)
            
    async def commit(self, request: TwoPhaseCommitCommitRequest) -> TwoPhaseCommitCommitResponse:
        """
        Processa uma requisição commit do protocolo 2PC.
        
        Args:
            request: Requisição commit.
            
        Returns:
            Resposta commit.
        """
        # Incrementar contador de mensagens commit recebidas
        store_metrics["commit"].labels(node_id=NODE_ID, status="received").inc()
        
        # Extrair dados da requisição
        instance_id = request.instanceId
        resource_id = request.resource
        data = request.data
        version = request.version
        client_id = request.clientId
        timestamp = request.timestamp
        
        # Converter instanceId para string para usar como chave
        instance_id_str = str(instance_id)
        
        # Verificar se o recurso existe
        if resource_id not in self.resources:
            self.logger.warning(f"Recurso {resource_id} não encontrado")
            store_metrics["commit"].labels(node_id=NODE_ID, status="resource_not_found").inc()
            return TwoPhaseCommitCommitResponse(success=False, resource=resource_id, version=-1, timestamp=get_current_timestamp())
            
        # Verificar se recebemos prepare para esta transação
        if instance_id_str not in self.pending_txns:
            self.logger.warning(f"Commit sem prepare para instance_id={instance_id}")
            store_metrics["commit"].labels(node_id=NODE_ID, status="no_prepare").inc()
            return TwoPhaseCommitCommitResponse(success=False, resource=resource_id, version=self.resources[resource_id].version, timestamp=get_current_timestamp())
            
        try:
            # Adquirir lock com timeout (evitar deadlocks)
            lock_acquired = await self._acquire_lock(resource_id)
            
            if not lock_acquired:
                self.logger.warning(f"Não foi possível adquirir lock para o recurso {resource_id}")
                store_metrics["commit"].labels(node_id=NODE_ID, status="lock_timeout").inc()
                return TwoPhaseCommitCommitResponse(success=False, resource=resource_id, version=self.resources[resource_id].version, timestamp=get_current_timestamp())
                
            # Atualizar o recurso
            current_resource = self.resources[resource_id]
            
            # Verificar se a versão é consistente
            if current_resource.version >= version:
                self.logger.warning(f"Versão inconsistente: atual={current_resource.version}, solicitada={version}")
                store_metrics["commit"].labels(node_id=NODE_ID, status="version_conflict").inc()
                return TwoPhaseCommitCommitResponse(success=False, resource=resource_id, version=current_resource.version, timestamp=get_current_timestamp())
                
            # Criar novo recurso com versão atualizada
            new_resource = ResourceData(
                data=data,
                version=version,
                timestamp=get_current_timestamp(),
                node_id=NODE_ID
            )
            
            # Atualizar recurso
            self.resources[resource_id] = new_resource
            
            # Persistir recurso
            await persistence.save_resource(new_resource, resource_id)
            
            # Atualizar métrica de versão
            store_metrics["resource_version"].labels(node_id=NODE_ID, resource=resource_id).set(version)
            
            # Atualizar status da transação pendente
            self.pending_txns[instance_id_str]["status"] = "committed"
            self.pending_txns[instance_id_str]["commit_time"] = get_current_timestamp()
            
            # Persistir transações pendentes
            await persistence.save_pending_transactions(self.pending_txns)
            
            # Incrementar contador de commits bem-sucedidos
            store_metrics["commit"].labels(node_id=NODE_ID, status="success").inc()
            
            # Incrementar contador de escritas
            store_metrics["writes"].labels(node_id=NODE_ID).inc()
            
            # Limpar transação pendente (opcional, podemos manter para histórico)
            # del self.pending_txns[instance_id_str]
            
            # Construir resposta
            return TwoPhaseCommitCommitResponse(
                success=True,
                resource=resource_id,
                version=version,
                timestamp=new_resource.timestamp
            )
            
        except Exception as e:
            self.logger.error(f"Erro ao processar commit: {e}")
            store_metrics["commit"].labels(node_id=NODE_ID, status="error").inc()
            return TwoPhaseCommitCommitResponse(success=False, resource=resource_id, version=self.resources[resource_id].version, timestamp=get_current_timestamp())
        finally:
            # Liberar lock
            self._release_lock(resource_id)
            
    async def abort(self, instance_id: int, resource_id: str = "R"):
        """
        Aborta uma transação pendente.
        
        Args:
            instance_id: ID da instância.
            resource_id: ID do recurso.
        """
        # Incrementar contador de mensagens abort recebidas
        store_metrics["abort"].labels(node_id=NODE_ID).inc()
        
        # Converter instanceId para string para usar como chave
        instance_id_str = str(instance_id)
        
        # Verificar se temos esta transação pendente
        if instance_id_str in self.pending_txns:
            # Atualizar status da transação
            self.pending_txns[instance_id_str]["status"] = "aborted"
            self.pending_txns[instance_id_str]["abort_time"] = get_current_timestamp()
            
            # Persistir transações pendentes
            await persistence.save_pending_transactions(self.pending_txns)
            
            self.logger.info(f"Transação {instance_id} abortada")
            
            # Liberar lock se estiver ativo
            if resource_id in self.locks:
                self._release_lock(resource_id)
                
    async def read_resource(self, resource_id: str) -> Optional[ResourceData]:
        """
        Lê um recurso.
        
        Args:
            resource_id: ID do recurso.
            
        Returns:
            Dados do recurso ou None se não existir.
        """
        # Incrementar contador de leituras
        store_metrics["reads"].labels(node_id=NODE_ID).inc()
        
        # Verificar se o recurso existe
        if resource_id not in self.resources:
            return None
            
        # Retornar recurso atual
        return self.resources[resource_id]
        
    async def _sync_loop(self):
        """
        Loop de sincronização periódica com outros nós do Cluster Store.
        """
        try:
            while self.running:
                # Executar sincronização
                await self._sync_with_others()
                
                # Esperar intervalo de sincronização
                await asyncio.sleep(SYNC_INTERVAL)
        except asyncio.CancelledError:
            # Tarefa cancelada, finalizar graciosamente
            self.logger.info("Tarefa de sincronização cancelada")
        except Exception as e:
            self.logger.error(f"Erro na tarefa de sincronização: {e}")
            # Reiniciar a tarefa em caso de erro
            if self.running:
                self.sync_task = asyncio.create_task(self._sync_loop())
                
    async def _sync_with_others(self):
        """
        Sincroniza recursos com outros nós do Cluster Store.
        """
        self.logger.info("Iniciando sincronização com outros nós")
        
        # Para cada recurso (apenas R neste projeto)
        for resource_id in self.resources:
            try:
                # Adquirir lock com timeout (evitar deadlocks)
                lock_acquired = await self._acquire_lock(resource_id)
                
                if not lock_acquired:
                    self.logger.warning(f"Não foi possível adquirir lock para sincronização do recurso {resource_id}")
                    continue
                    
                # Obter versões de todos os nós
                versions = {}
                versions[NODE_ID] = {
                    "version": self.resources[resource_id].version,
                    "timestamp": self.resources[resource_id].timestamp,
                    "node_id": self.resources[resource_id].node_id
                }
                
                for endpoint, client in self.store_clients.items():
                    try:
                        status, response = await client.get(f"/resource/{resource_id}")
                        
                        if status == 200:
                            remote_resource = ResourceData(**response)
                            versions[remote_resource.node_id] = {
                                "version": remote_resource.version,
                                "timestamp": remote_resource.timestamp,
                                "node_id": remote_resource.node_id,
                                "endpoint": endpoint,
                                "data": remote_resource.data
                            }
                    except Exception as e:
                        self.logger.warning(f"Erro ao obter versão do nó {endpoint}: {e}")
                        
                # Verificar se há inconsistências
                if len(versions) > 1:
                    # Encontrar a versão mais recente
                    latest_version = -1
                    latest_timestamp = 0
                    latest_node_id = 0
                    latest_data = None
                    
                    for node_id, info in versions.items():
                        # Regra 1: Maior número de versão prevalece
                        if info["version"] > latest_version:
                            latest_version = info["version"]
                            latest_timestamp = info["timestamp"]
                            latest_node_id = info["node_id"]
                            latest_data = info.get("data")
                        # Regra 2: Em caso de empate, timestamp mais recente prevalece
                        elif info["version"] == latest_version and info["timestamp"] > latest_timestamp:
                            latest_timestamp = info["timestamp"]
                            latest_node_id = info["node_id"]
                            latest_data = info.get("data")
                        # Regra 3: Em caso de empate de timestamp, ID do nó maior prevalece
                        elif info["version"] == latest_version and info["timestamp"] == latest_timestamp and info["node_id"] > latest_node_id:
                            latest_node_id = info["node_id"]
                            latest_data = info.get("data")
                            
                    # Verificar se preciso atualizar minha versão
                    if latest_version > self.resources[resource_id].version:
                        # Atualizar para a versão mais recente
                        self.logger.info(f"Atualizando recurso {resource_id} para versão {latest_version} do nó {latest_node_id}")
                        
                        # Se tenho os dados, atualizar diretamente
                        if latest_data is not None:
                            new_resource = ResourceData(
                                data=latest_data,
                                version=latest_version,
                                timestamp=latest_timestamp,
                                node_id=latest_node_id
                            )
                            
                            # Atualizar recurso
                            self.resources[resource_id] = new_resource
                            
                            # Persistir recurso
                            await persistence.save_resource(new_resource, resource_id)
                            
                            # Atualizar métrica de versão
                            store_metrics["resource_version"].labels(node_id=NODE_ID, resource=resource_id).set(latest_version)
                            
                            # Incrementar contador de sincronizações
                            store_metrics["sync"].labels(node_id=NODE_ID, result="updated").inc()
                        else:
                            # Preciso obter os dados completos do nó com a versão mais recente
                            # Isso não deve acontecer no nosso caso, pois sempre incluímos os dados
                            self.logger.warning(f"Não tenho os dados da versão {latest_version} do recurso {resource_id}")
                            store_metrics["sync"].labels(node_id=NODE_ID, result="failed").inc()
                    else:
                        # Minha versão já está atualizada
                        store_metrics["sync"].labels(node_id=NODE_ID, result="already_updated").inc()
                        
            except Exception as e:
                self.logger.error(f"Erro ao sincronizar recurso {resource_id}: {e}")
                store_metrics["sync"].labels(node_id=NODE_ID, result="error").inc()
            finally:
                # Liberar lock
                self._release_lock(resource_id)
                
    async def _acquire_lock(self, resource_id: str) -> bool:
        """
        Adquire lock para um recurso com timeout.
        
        Args:
            resource_id: ID do recurso.
            
        Returns:
            True se o lock foi adquirido, False caso contrário.
        """
        if resource_id not in self.locks:
            self.locks[resource_id] = asyncio.Lock()
            self.lock_expiry[resource_id] = 0
            
        # Verificar se o lock expirou
        current_time = get_current_timestamp()
        if self.locks[resource_id].locked() and self.lock_expiry[resource_id] < current_time:
            # Lock expirou, forçar liberação
            self.logger.warning(f"Lock para {resource_id} expirou, forçando liberação")
            self._release_lock(resource_id)
            
        try:
            # Tentar adquirir lock com timeout
            lock_acquired = await asyncio.wait_for(self.locks[resource_id].acquire(), timeout=1.0)
            
            if lock_acquired:
                # Definir tempo de expiração
                self.lock_expiry[resource_id] = get_current_timestamp() + int(LOCK_TIMEOUT * 1000)
                
            return lock_acquired
        except asyncio.TimeoutError:
            return False
            
    def _release_lock(self, resource_id: str):
        """
        Libera lock para um recurso.
        
        Args:
            resource_id: ID do recurso.
        """
        if resource_id in self.locks and self.locks[resource_id].locked():
            self.locks[resource_id].release()
            
    def get_status(self) -> Dict[str, Any]:
        """
        Obtém o status atual do Cluster Store.
        
        Returns:
            Status do Cluster Store.
        """
        status = {
            "node_id": NODE_ID,
            "role": "store",
            "resources": {},
            "pending_transactions": len(self.pending_txns),
            "timestamp": get_current_timestamp()
        }
        
        # Adicionar informações dos recursos
        for resource_id, resource in self.resources.items():
            status["resources"][resource_id] = {
                "version": resource.version,
                "timestamp": resource.timestamp,
                "node_id": resource.node_id,
                "data_length": len(resource.data) if resource.data else 0
            }
            
        return status