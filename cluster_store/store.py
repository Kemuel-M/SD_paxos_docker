"""
File: cluster_store/store.py
Implementação do gerenciamento de recursos para o Cluster Store.
"""
import os
import json
import time
import logging
import asyncio
import threading
from typing import Dict, Any, List, Optional, Tuple, Set

from persistence import ResourceStorage

logger = logging.getLogger("store")

class ResourceManager:
    """
    Gerencia o armazenamento e acesso aos recursos no Cluster Store.
    
    Características:
    1. Gerencia o recurso R com operações de leitura e escrita
    2. Utiliza sistema de locks para controle de concorrência
    3. Implementa mecanismo de versionamento e histórico
    """
    
    def __init__(self, node_id: int, storage: ResourceStorage):
        """
        Inicializa o gerenciador de recursos.
        
        Args:
            node_id: ID do nó no cluster (1-3)
            storage: Interface de armazenamento persistente
        """
        self.node_id = node_id
        self.storage = storage
        
        # Estruturas de controle
        self.locks = {}  # resource_id -> {"exclusive": bool, "owner": str, "expires": int}
        self.lock = threading.RLock()  # Para sincronização das estruturas internas
        
        # Cache em memória para recursos ativos
        self.resources_cache = {}  # resource_id -> {dados do recurso}
        
        # Transações pendentes
        self.pending_transactions = {}  # transaction_id -> {"resource_id": str, "data": Any, "state": str}
        
        # Inicializa recurso R se não existir
        self._initialize_resource("R")
        
        logger.info(f"ResourceManager inicializado no nó {node_id}")
    
    def _initialize_resource(self, resource_id: str):
        """
        Inicializa um recurso se não existir.
        
        Args:
            resource_id: ID do recurso a inicializar
        """
        # Tenta carregar o recurso
        resource = self.storage.load_resource(resource_id)
        
        # Se não existir, cria com valores iniciais
        if resource is None:
            logger.important(f"Criando recurso inicial {resource_id}")
            resource = {
                "data": "Conteúdo inicial do recurso",
                "version": 0,
                "timestamp": int(time.time() * 1000),
                "node_id": self.node_id
            }
            # Persiste o recurso inicial
            self.storage.save_resource(resource_id, resource)
        
        # Adiciona ao cache
        self.resources_cache[resource_id] = resource
        
        logger.info(f"Recurso {resource_id} inicializado: versão {resource['version']}")
    
    def get_resource(self, resource_id: str) -> Dict[str, Any]:
        """
        Obtém um recurso pelo ID.
        
        Args:
            resource_id: ID do recurso a obter
            
        Returns:
            Dict[str, Any]: Dados do recurso
        """
        with self.lock:
            # Verifica se está no cache
            if resource_id in self.resources_cache:
                logger.debug(f"Recurso {resource_id} obtido do cache")
                return self.resources_cache[resource_id].copy()
            
            # Tenta carregar do armazenamento
            resource = self.storage.load_resource(resource_id)
            
            # Se não existir, inicializa
            if resource is None:
                self._initialize_resource(resource_id)
                resource = self.resources_cache[resource_id]
            else:
                # Adiciona ao cache
                self.resources_cache[resource_id] = resource
            
            logger.debug(f"Recurso {resource_id} obtido do armazenamento: versão {resource['version']}")
            return resource.copy()
    
    def acquire_lock(self, resource_id: str, transaction_id: str, exclusive: bool = False, timeout: int = 30) -> bool:
        """
        Adquire um lock para o recurso.
        
        Args:
            resource_id: ID do recurso
            transaction_id: ID da transação solicitante
            exclusive: Se True, lock exclusivo. Se False, lock compartilhado.
            timeout: Tempo em segundos para expiração do lock
            
        Returns:
            bool: True se conseguiu adquirir o lock, False caso contrário
        """
        with self.lock:
            current_time = int(time.time())
            
            # Se o recurso já tem lock
            if resource_id in self.locks:
                lock_info = self.locks[resource_id]
                
                # Verifica se o lock expirou
                if lock_info["expires"] < current_time:
                    logger.warning(f"Lock expirado para recurso {resource_id}, transação {lock_info['owner']}")
                    # Remove o lock expirado
                    del self.locks[resource_id]
                # Se a própria transação já tem o lock
                elif lock_info["owner"] == transaction_id:
                    # Atualiza expiração
                    lock_info["expires"] = current_time + timeout
                    # Se já tem lock exclusivo, ou está solicitando lock compartilhado, retorna sucesso
                    if lock_info["exclusive"] or not exclusive:
                        return True
                    # Se tem lock compartilhado mas quer exclusivo, falha
                    else:
                        logger.warning(f"Transação {transaction_id} tentou promover lock compartilhado para exclusivo")
                        return False
                # Se é outra transação
                else:
                    # Se o lock atual é exclusivo, ou o solicitado é exclusivo, falha
                    if lock_info["exclusive"] or exclusive:
                        logger.warning(f"Conflito de lock para recurso {resource_id}, já obtido por {lock_info['owner']}")
                        return False
                    # Ambos compartilhados, pode adquirir
                    else:
                        return True
            
            # Não há lock ou foi removido por expiração, cria novo
            self.locks[resource_id] = {
                "exclusive": exclusive,
                "owner": transaction_id,
                "expires": current_time + timeout
            }
            
            logger.debug(f"Lock {'exclusivo' if exclusive else 'compartilhado'} adquirido para recurso {resource_id}, transação {transaction_id}")
            return True
    
    def release_lock(self, resource_id: str, transaction_id: str) -> bool:
        """
        Libera um lock para o recurso.
        
        Args:
            resource_id: ID do recurso
            transaction_id: ID da transação solicitante
            
        Returns:
            bool: True se conseguiu liberar o lock, False caso contrário
        """
        with self.lock:
            # Se o recurso tem lock
            if resource_id in self.locks:
                lock_info = self.locks[resource_id]
                
                # Se a transação é a dona do lock
                if lock_info["owner"] == transaction_id:
                    # Remove o lock
                    del self.locks[resource_id]
                    logger.debug(f"Lock liberado para recurso {resource_id}, transação {transaction_id}")
                    return True
                else:
                    logger.warning(f"Tentativa de liberar lock não pertencente: recurso {resource_id}, transação {transaction_id}")
            else:
                logger.warning(f"Tentativa de liberar lock inexistente: recurso {resource_id}")
            
            return False
    
    def prepare_transaction(self, transaction_id: str, resource_id: str, data: Any, client_id: str) -> Tuple[bool, Dict[str, Any]]:
        """
        Prepara uma transação para escrita (fase 1 do 2PC).
        
        Args:
            transaction_id: ID da transação
            resource_id: ID do recurso
            data: Novos dados para o recurso
            client_id: ID do cliente
            
        Returns:
            Tuple[bool, Dict[str, Any]]: (ready, info)
                - ready: True se o nó está pronto para commit, False caso contrário
                - info: Informações adicionais (versão atual, razão da falha, etc.)
        """
        with self.lock:
            # Obtém o recurso atual
            current_resource = self.get_resource(resource_id)
            
            # Tenta adquirir lock exclusivo
            if not self.acquire_lock(resource_id, transaction_id, exclusive=True):
                return False, {
                    "reason": "Recurso bloqueado por outra transação",
                    "currentVersion": current_resource["version"]
                }
            
            # Registra transação como pendente
            self.pending_transactions[transaction_id] = {
                "resource_id": resource_id,
                "data": data,
                "client_id": client_id,
                "timestamp": int(time.time() * 1000),
                "current_version": current_resource["version"],
                "state": "prepared"
            }
            
            logger.info(f"Transação {transaction_id} preparada para recurso {resource_id}, versão atual {current_resource['version']}")
            
            return True, {
                "currentVersion": current_resource["version"]
            }
    
    def commit_transaction(self, transaction_id: str, resource_id: str, data: Any, version: int, client_id: str, timestamp: int) -> Tuple[bool, Dict[str, Any]]:
        """
        Confirma uma transação preparada (fase 2 do 2PC).
        
        Args:
            transaction_id: ID da transação
            resource_id: ID do recurso
            data: Novos dados para o recurso
            version: Nova versão do recurso
            client_id: ID do cliente
            timestamp: Timestamp da operação em milissegundos
            
        Returns:
            Tuple[bool, Dict[str, Any]]: (success, info)
                - success: True se commit concluído com sucesso, False caso contrário
                - info: Informações adicionais (nova versão, razão da falha, etc.)
        """
        with self.lock:
            # Verifica se a transação está pendente
            if transaction_id not in self.pending_transactions:
                logger.warning(f"Tentativa de commit para transação não preparada: {transaction_id}")
                return False, {
                    "reason": "Transação não preparada"
                }
            
            # Obtém informações da transação pendente
            pending = self.pending_transactions[transaction_id]
            
            # Verifica se corresponde ao mesmo recurso
            if pending["resource_id"] != resource_id:
                logger.warning(f"Mismatch de recurso para transação {transaction_id}: esperado {pending['resource_id']}, recebido {resource_id}")
                return False, {
                    "reason": "Mismatch de recurso"
                }
            
            # Atualiza o recurso
            try:
                # Cria novo estado do recurso
                new_resource = {
                    "data": data,
                    "version": version,
                    "timestamp": timestamp,
                    "node_id": self.node_id
                }
                
                # Persiste o recurso
                self.storage.save_resource(resource_id, new_resource)
                
                # Atualiza o cache
                self.resources_cache[resource_id] = new_resource
                
                # Atualiza estado da transação
                pending["state"] = "committed"
                
                # Libera o lock
                self.release_lock(resource_id, transaction_id)
                
                logger.important(f"Transação {transaction_id} confirmada para recurso {resource_id}, nova versão {version}")
                
                return True, {
                    "resource": resource_id,
                    "version": version,
                    "timestamp": timestamp
                }
            except Exception as e:
                logger.error(f"Erro ao confirmar transação {transaction_id}: {e}", exc_info=True)
                return False, {
                    "reason": f"Erro interno: {str(e)}"
                }
    
    def abort_transaction(self, transaction_id: str, resource_id: str) -> bool:
        """
        Aborta uma transação preparada.
        
        Args:
            transaction_id: ID da transação
            resource_id: ID do recurso
            
        Returns:
            bool: True se abort concluído com sucesso, False caso contrário
        """
        with self.lock:
            # Verifica se a transação está pendente
            if transaction_id not in self.pending_transactions:
                logger.warning(f"Tentativa de abort para transação não preparada: {transaction_id}")
                return False
            
            # Obtém informações da transação pendente
            pending = self.pending_transactions[transaction_id]
            
            # Verifica se corresponde ao mesmo recurso
            if pending["resource_id"] != resource_id:
                logger.warning(f"Mismatch de recurso para abort de transação {transaction_id}")
                return False
            
            # Atualiza estado da transação
            pending["state"] = "aborted"
            
            # Libera o lock
            self.release_lock(resource_id, transaction_id)
            
            logger.info(f"Transação {transaction_id} abortada para recurso {resource_id}")
            
            return True
    
    def clean_expired_transactions(self):
        """
        Limpa transações e locks expirados.
        """
        with self.lock:
            current_time = int(time.time())
            
            # Limpa locks expirados
            expired_locks = []
            for resource_id, lock_info in self.locks.items():
                if lock_info["expires"] < current_time:
                    expired_locks.append(resource_id)
            
            for resource_id in expired_locks:
                transaction_id = self.locks[resource_id]["owner"]
                logger.warning(f"Removendo lock expirado: recurso {resource_id}, transação {transaction_id}")
                del self.locks[resource_id]
                
                # Se a transação ainda está pendente, marca como abortada
                if transaction_id in self.pending_transactions:
                    self.pending_transactions[transaction_id]["state"] = "aborted"
            
            # Limpa transações muito antigas (mais de 24 horas)
            expired_time = current_time - (24 * 60 * 60)
            expired_transactions = []
            for transaction_id, transaction in self.pending_transactions.items():
                if transaction["timestamp"] < expired_time * 1000:  # Convertendo para milissegundos
                    expired_transactions.append(transaction_id)
            
            for transaction_id in expired_transactions:
                logger.warning(f"Removendo transação antiga: {transaction_id}")
                del self.pending_transactions[transaction_id]
    
    def get_transaction_status(self, transaction_id: str) -> Optional[Dict[str, Any]]:
        """
        Obtém o status de uma transação.
        
        Args:
            transaction_id: ID da transação
            
        Returns:
            Optional[Dict[str, Any]]: Informações da transação ou None se não existir
        """
        with self.lock:
            if transaction_id in self.pending_transactions:
                return self.pending_transactions[transaction_id].copy()
            return None
    
    def get_status(self) -> Dict[str, Any]:
        """
        Obtém o status do gerenciador de recursos.
        
        Returns:
            Dict[str, Any]: Informações de status
        """
        with self.lock:
            # Conta transações por estado
            transaction_counts = {
                "prepared": 0,
                "committed": 0,
                "aborted": 0
            }
            
            for transaction in self.pending_transactions.values():
                state = transaction.get("state", "unknown")
                if state in transaction_counts:
                    transaction_counts[state] += 1
                else:
                    transaction_counts[state] = 1
            
            # Informações de recursos
            resources_info = {}
            for resource_id, resource in self.resources_cache.items():
                resources_info[resource_id] = {
                    "version": resource["version"],
                    "timestamp": resource["timestamp"],
                    "locked": resource_id in self.locks
                }
            
            return {
                "node_id": self.node_id,
                "transactions": {
                    "total": len(self.pending_transactions),
                    "counts": transaction_counts
                },
                "resources": resources_info,
                "locks": len(self.locks)
            }
    
    def compare_version(self, resource_id: str, other_version: Dict[str, Any]) -> int:
        """
        Compara versão local do recurso com outra versão.
        
        Args:
            resource_id: ID do recurso
            other_version: Dados da outra versão
            
        Returns:
            int: 1 se local é mais recente, -1 se externa é mais recente, 0 se iguais
        """
        # Obtém versão local
        local = self.get_resource(resource_id)
        
        # Compara número de versão
        if local["version"] > other_version["version"]:
            return 1
        elif local["version"] < other_version["version"]:
            return -1
        
        # Versões iguais, compara timestamp
        if local["timestamp"] > other_version["timestamp"]:
            return 1
        elif local["timestamp"] < other_version["timestamp"]:
            return -1
        
        # Timestamps iguais, compara node_id
        if local["node_id"] > other_version["node_id"]:
            return 1
        elif local["node_id"] < other_version["node_id"]:
            return -1
        
        # Completamente iguais
        return 0
    
    def update_resource(self, resource_id: str, resource_data: Dict[str, Any]) -> bool:
        """
        Atualiza um recurso com dados externos (usado para sincronização).
        
        Args:
            resource_id: ID do recurso
            resource_data: Novos dados do recurso
            
        Returns:
            bool: True se atualizado com sucesso, False caso contrário
        """
        with self.lock:
            # Obtém versão local
            local = self.get_resource(resource_id)
            
            # Compara versões
            comparison = self.compare_version(resource_id, resource_data)
            
            # Se externo é mais recente
            if comparison < 0:
                # Atualiza cache e persistência
                self.resources_cache[resource_id] = resource_data.copy()
                self.storage.save_resource(resource_id, resource_data)
                
                logger.important(f"Recurso {resource_id} atualizado de versão {local['version']} para {resource_data['version']} durante sincronização")
                return True
            else:
                logger.debug(f"Recurso {resource_id} não atualizado, versão local {local['version']} mais recente que {resource_data['version']}")
                return False