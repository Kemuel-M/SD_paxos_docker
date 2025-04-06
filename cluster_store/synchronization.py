"""
File: cluster_store/synchronization.py
Implementação da sincronização entre nós do Cluster Store.
"""
import os
import time
import logging
import asyncio
from typing import Dict, Any, List, Optional, Tuple, Set
import httpx

from common.utils import wait_with_backoff

logger = logging.getLogger("synchronization")

class SynchronizationManager:
    """
    Gerencia a sincronização entre nós do Cluster Store.
    
    Características:
    1. Sincronização periódica de estado entre nós
    2. Detecção e resolução de conflitos de versão
    3. Recuperação após falhas de nós
    """
    
    def __init__(self, node_id: int, all_nodes: List[str], resource_manager):
        """
        Inicializa o gerente de sincronização.
        
        Args:
            node_id: ID do nó no cluster (1-3)
            all_nodes: Lista de endereços de todos os nós do cluster
            resource_manager: Instância do gerenciador de recursos
        """
        self.node_id = node_id
        self.all_nodes = all_nodes
        self.resource_manager = resource_manager
        
        # Estado da sincronização
        self.last_sync_time = 0
        self.running = False
        self.sync_interval = 10  # segundos
        self.sync_task = None
        
        # Cliente HTTP para comunicação
        self.http_client = httpx.AsyncClient(timeout=5.0)
        
        # Mapeamento de nome de host para ID
        self.hostname_to_id = {}
        for i, host in enumerate(all_nodes, 1):
            self.hostname_to_id[host] = i
        
        logger.info(f"SynchronizationManager inicializado no nó {node_id}")
        logger.info(f"Nós do cluster: {all_nodes}")
    
    async def start(self):
        """Inicia o processo de sincronização periódica."""
        if self.running:
            logger.warning("Gerenciador de sincronização já está em execução")
            return
        
        self.running = True
        self.sync_task = asyncio.create_task(self._sync_loop())
        logger.info("Processo de sincronização periódica iniciado")
    
    async def stop(self):
        """Para o processo de sincronização periódica."""
        if not self.running:
            logger.warning("Gerenciador de sincronização já está parado")
            return
        
        self.running = False
        
        if self.sync_task:
            self.sync_task.cancel()
            try:
                await self.sync_task
            except asyncio.CancelledError:
                pass
            
            self.sync_task = None
        
        await self.http_client.aclose()
        logger.info("Processo de sincronização periódica parado")
    
    async def _sync_loop(self):
        """Loop principal de sincronização periódica."""
        logger.info(f"Iniciando loop de sincronização com intervalo de {self.sync_interval}s")
        
        # Inicialização - espera alguns segundos antes da primeira sincronização
        await asyncio.sleep(2.0)
        
        attempt = 0
        while self.running:
            try:
                # Tenta realizar a sincronização
                synced = await self.synchronize_all_resources()
                
                if synced:
                    logger.info("Sincronização concluída com sucesso")
                    # Reseta contador de tentativas após sucesso
                    attempt = 0
                else:
                    logger.warning("Sincronização falhou ou não foi necessária")
                    # Incrementa contador para backoff
                    attempt += 1
                
                # Atualiza timestamp da última sincronização
                self.last_sync_time = time.time()
                
                # Espera até o próximo intervalo
                await asyncio.sleep(self.sync_interval)
                
            except asyncio.CancelledError:
                logger.info("Loop de sincronização cancelado")
                raise
            except Exception as e:
                logger.error(f"Erro no loop de sincronização: {e}", exc_info=True)
                
                # Incrementa contador de tentativas
                attempt += 1
                
                # Aguarda com backoff exponencial antes de tentar novamente
                await wait_with_backoff(attempt)
    
    async def synchronize_all_resources(self) -> bool:
        """
        Realiza a sincronização de todos os recursos com outros nós.
        
        Returns:
            bool: True se sincronização foi concluída com sucesso, False caso contrário
        """
        try:
            # Obtém lista de recursos locais
            local_resources = self.resource_manager.storage.list_resources()
            
            # Obtém lista e versões de recursos de todos os nós
            resources_metadata = await self._collect_resources_metadata()
            
            if not resources_metadata:
                logger.warning("Não foi possível obter metadados de outros nós")
                return False
            
            # Para cada recurso conhecido, verifica se precisa sincronizar
            all_resource_ids = set(local_resources)
            for node_info in resources_metadata.values():
                all_resource_ids.update(node_info.keys())
            
            synchronized = False
            
            # Para cada recurso, verifica e sincroniza se necessário
            for resource_id in all_resource_ids:
                synced = await self._synchronize_resource(resource_id, resources_metadata)
                synchronized = synchronized or synced
            
            return synchronized
            
        except Exception as e:
            logger.error(f"Erro ao sincronizar recursos: {e}", exc_info=True)
            return False
    
    async def _collect_resources_metadata(self) -> Dict[int, Dict[str, Dict[str, Any]]]:
        """
        Coleta metadados de recursos de todos os nós.
        
        Returns:
            Dict[int, Dict[str, Dict[str, Any]]]: 
                Mapeamento de node_id -> (resource_id -> metadata)
        """
        result = {}
        
        # Para cada nó do cluster (exceto o próprio)
        for i, node_address in enumerate(self.all_nodes, 1):
            if i == self.node_id:
                continue
            
            try:
                # Obtém metadados de recursos do nó
                url = f"http://{node_address}/resources/metadata"
                response = await self.http_client.get(url)
                
                if response.status_code == 200:
                    data = response.json()
                    result[i] = data.get("resources", {})
                    logger.debug(f"Metadados obtidos do nó {i}: {len(result[i])} recursos")
                else:
                    logger.warning(f"Falha ao obter metadados do nó {i}: status {response.status_code}")
            
            except Exception as e:
                logger.warning(f"Erro ao obter metadados do nó {i} ({node_address}): {e}")
        
        return result
    
    async def _synchronize_resource(self, resource_id: str, resources_metadata: Dict[int, Dict[str, Dict[str, Any]]]) -> bool:
        """
        Sincroniza um recurso específico.
        
        Args:
            resource_id: ID do recurso a sincronizar
            resources_metadata: Metadados coletados de todos os nós
            
        Returns:
            bool: True se o recurso foi sincronizado, False caso contrário
        """
        # Obtém metadados locais do recurso
        local_metadata = self.resource_manager.storage.get_resource_metadata(resource_id)
        
        # Se não temos o recurso localmente
        if local_metadata is None:
            # Verifica qual nó tem a versão mais recente
            latest_node_id, latest_metadata = self._find_latest_version(resource_id, resources_metadata)
            
            if latest_node_id is None:
                logger.warning(f"Recurso {resource_id} não encontrado em nenhum nó")
                return False
            
            # Obtém o recurso do nó com versão mais recente
            synced = await self._fetch_and_update_resource(resource_id, latest_node_id)
            if synced:
                logger.important(f"Recurso {resource_id} obtido do nó {latest_node_id} (versão {latest_metadata.get('version', 'N/A')})")
                return True
            else:
                logger.warning(f"Falha ao obter recurso {resource_id} do nó {latest_node_id}")
                return False
        
        # Temos o recurso localmente, verifica se alguém tem versão mais recente
        latest_node_id, latest_metadata = self._find_latest_version(resource_id, resources_metadata)
        
        if latest_node_id is None:
            # Ninguém tem o recurso ou somos os únicos
            logger.debug(f"Recurso {resource_id} não requer sincronização (só existe localmente)")
            return False
        
        # Compara versões
        local_version = local_metadata.get("version", 0)
        latest_version = latest_metadata.get("version", 0)
        
        if latest_version > local_version:
            # Outro nó tem versão mais recente
            synced = await self._fetch_and_update_resource(resource_id, latest_node_id)
            if synced:
                logger.important(f"Recurso {resource_id} atualizado do nó {latest_node_id} (versão {local_version} -> {latest_version})")
                return True
            else:
                logger.warning(f"Falha ao atualizar recurso {resource_id} do nó {latest_node_id}")
                return False
        
        # Verifica se algum nó precisa de nossa versão
        needs_update = False
        for node_id, node_metadata in resources_metadata.items():
            if resource_id in node_metadata:
                node_version = node_metadata[resource_id].get("version", 0)
                if node_version < local_version:
                    needs_update = True
                    break
        
        if needs_update:
            # Não fazemos push ativo, os outros nós detectarão e farão pull
            logger.debug(f"Outros nós precisam atualizar o recurso {resource_id} (temos versão {local_version})")
        else:
            logger.debug(f"Recurso {resource_id} está sincronizado em todos os nós (versão {local_version})")
        
        return False
    
    def _find_latest_version(self, resource_id: str, resources_metadata: Dict[int, Dict[str, Dict[str, Any]]]) -> Tuple[Optional[int], Optional[Dict[str, Any]]]:
        """
        Encontra o nó com a versão mais recente de um recurso.
        
        Args:
            resource_id: ID do recurso
            resources_metadata: Metadados coletados de todos os nós
            
        Returns:
            Tuple[Optional[int], Optional[Dict[str, Any]]]: 
                (node_id com versão mais recente, metadados mais recentes)
                ou (None, None) se nenhum nó tiver o recurso
        """
        latest_node_id = None
        latest_metadata = None
        
        for node_id, node_metadata in resources_metadata.items():
            if resource_id in node_metadata:
                current_metadata = node_metadata[resource_id]
                
                if latest_metadata is None:
                    latest_node_id = node_id
                    latest_metadata = current_metadata
                    continue
                
                # Compara versões
                current_version = current_metadata.get("version", 0)
                latest_version = latest_metadata.get("version", 0)
                
                if current_version > latest_version:
                    # Versão mais recente
                    latest_node_id = node_id
                    latest_metadata = current_metadata
                elif current_version == latest_version:
                    # Versões iguais, compara timestamp
                    current_timestamp = current_metadata.get("timestamp", 0)
                    latest_timestamp = latest_metadata.get("timestamp", 0)
                    
                    if current_timestamp > latest_timestamp:
                        # Timestamp mais recente
                        latest_node_id = node_id
                        latest_metadata = current_metadata
                    elif current_timestamp == latest_timestamp:
                        # Timestamps iguais, maior node_id vence
                        current_node_id = current_metadata.get("node_id", 0)
                        latest_node_owner = latest_metadata.get("node_id", 0)
                        
                        if current_node_id > latest_node_owner:
                            latest_node_id = node_id
                            latest_metadata = current_metadata
        
        return latest_node_id, latest_metadata
    
    async def _fetch_and_update_resource(self, resource_id: str, target_node_id: int) -> bool:
        """
        Obtém e atualiza um recurso de outro nó.
        
        Args:
            resource_id: ID do recurso
            target_node_id: ID do nó de origem
            
        Returns:
            bool: True se atualizado com sucesso, False caso contrário
        """
        try:
            # Obtém endereço do nó alvo
            if target_node_id < 1 or target_node_id > len(self.all_nodes):
                logger.warning(f"ID de nó inválido: {target_node_id}")
                return False
            
            target_address = self.all_nodes[target_node_id - 1]
            
            # Obtém o recurso do nó alvo
            url = f"http://{target_address}/resource/{resource_id}"
            response = await self.http_client.get(url)
            
            if response.status_code != 200:
                logger.warning(f"Falha ao obter recurso {resource_id} do nó {target_node_id}: status {response.status_code}")
                return False
            
            # Processa a resposta
            resource_data = response.json()
            
            # Atualiza o recurso localmente
            self.resource_manager.update_resource(resource_id, resource_data)
            
            return True
            
        except Exception as e:
            logger.error(f"Erro ao sincronizar recurso {resource_id} do nó {target_node_id}: {e}", exc_info=True)
            return False
    
    async def force_synchronize(self, resource_id: str = None) -> bool:
        """
        Força sincronização imediata de um ou todos os recursos.
        
        Args:
            resource_id: ID do recurso específico ou None para todos
            
        Returns:
            bool: True se sincronização foi concluída com sucesso, False caso contrário
        """
        try:
            # Se for um recurso específico
            if resource_id:
                logger.info(f"Iniciando sincronização forçada do recurso {resource_id}")
                
                # Obtém metadados de todos os nós
                resources_metadata = await self._collect_resources_metadata()
                
                if not resources_metadata:
                    logger.warning("Não foi possível obter metadados de outros nós")
                    return False
                
                # Sincroniza o recurso específico
                return await self._synchronize_resource(resource_id, resources_metadata)
            
            # Se for todos os recursos
            else:
                logger.info("Iniciando sincronização forçada de todos os recursos")
                return await self.synchronize_all_resources()
                
        except Exception as e:
            logger.error(f"Erro na sincronização forçada: {e}", exc_info=True)
            return False
    
    def get_status(self) -> Dict[str, Any]:
        """
        Obtém o status do gerenciador de sincronização.
        
        Returns:
            Dict[str, Any]: Informações de status
        """
        return {
            "node_id": self.node_id,
            "running": self.running,
            "last_sync_time": self.last_sync_time,
            "sync_interval": self.sync_interval,
            "last_sync_age": time.time() - self.last_sync_time if self.last_sync_time > 0 else None,
            "nodes_count": len(self.all_nodes),
            "nodes": self.all_nodes
        }