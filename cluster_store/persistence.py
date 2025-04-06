"""
File: cluster_store/persistence.py
Implementação da camada de persistência para o Cluster Store.
"""
import os
import json
import time
import logging
import shutil
import threading
from typing import Dict, Any, List, Optional, Tuple, Set

logger = logging.getLogger("persistence")

class ResourceStorage:
    """
    Interface para armazenamento persistente de recursos.
    
    Características:
    1. Armazenamento baseado em arquivos para recursos
    2. Controle transacional de escrita para evitar corrupção
    3. Backup automático para maior segurança
    """
    
    def __init__(self, node_id: int, data_dir: str = "/data/resources"):
        """
        Inicializa o armazenamento de recursos.
        
        Args:
            node_id: ID do nó no cluster (1-3)
            data_dir: Diretório para armazenamento dos recursos
        """
        self.node_id = node_id
        self.data_dir = data_dir
        self.lock = threading.RLock()  # Para sincronização das operações de I/O
        
        # Dicionário para rastrear quando foi a última verificação de backup
        self.last_backup_check = {}  # resource_id -> timestamp
        
        # Garante que o diretório de dados existe
        os.makedirs(self.data_dir, exist_ok=True)
        
        # Garante que o diretório de backup existe
        backup_dir = os.path.join(self.data_dir, "backup")
        os.makedirs(backup_dir, exist_ok=True)
        
        logger.info(f"ResourceStorage inicializado no diretório {self.data_dir}")
    
    def _get_resource_path(self, resource_id: str) -> str:
        """
        Obtém o caminho completo para o arquivo do recurso.
        
        Args:
            resource_id: ID do recurso
            
        Returns:
            str: Caminho completo para o arquivo
        """
        # Sanitiza o ID do recurso para evitar traversal de diretório
        safe_id = resource_id.replace('/', '_').replace('\\', '_')
        
        return os.path.join(self.data_dir, f"{safe_id}.json")
    
    def _get_backup_path(self, resource_id: str) -> str:
        """
        Obtém o caminho completo para o arquivo de backup do recurso.
        
        Args:
            resource_id: ID do recurso
            
        Returns:
            str: Caminho completo para o arquivo de backup
        """
        # Sanitiza o ID do recurso para evitar traversal de diretório
        safe_id = resource_id.replace('/', '_').replace('\\', '_')
        
        backup_dir = os.path.join(self.data_dir, "backup")
        return os.path.join(backup_dir, f"{safe_id}.json")
    
    def _get_temp_path(self, resource_id: str) -> str:
        """
        Obtém o caminho completo para o arquivo temporário do recurso.
        
        Args:
            resource_id: ID do recurso
            
        Returns:
            str: Caminho completo para o arquivo temporário
        """
        # Sanitiza o ID do recurso para evitar traversal de diretório
        safe_id = resource_id.replace('/', '_').replace('\\', '_')
        
        return os.path.join(self.data_dir, f"{safe_id}.tmp.json")
    
    def load_resource(self, resource_id: str) -> Optional[Dict[str, Any]]:
        """
        Carrega um recurso do armazenamento.
        
        Args:
            resource_id: ID do recurso
            
        Returns:
            Optional[Dict[str, Any]]: Dados do recurso ou None se não existir
        """
        with self.lock:
            resource_path = self._get_resource_path(resource_id)
            
            # Verifica se o arquivo existe
            if not os.path.exists(resource_path):
                # Tenta carregar do backup
                backup_path = self._get_backup_path(resource_id)
                if os.path.exists(backup_path):
                    logger.warning(f"Arquivo principal não encontrado para recurso {resource_id}, usando backup")
                    resource_path = backup_path
                else:
                    logger.info(f"Recurso {resource_id} não encontrado em armazenamento")
                    return None
            
            try:
                # Abre e lê o arquivo
                with open(resource_path, 'r', encoding='utf-8') as f:
                    resource = json.load(f)
                
                logger.debug(f"Recurso {resource_id} carregado do armazenamento: versão {resource.get('version', 'N/A')}")
                return resource
            except Exception as e:
                logger.error(f"Erro ao carregar recurso {resource_id}: {e}", exc_info=True)
                
                # Tenta carregar do backup em caso de erro
                backup_path = self._get_backup_path(resource_id)
                if os.path.exists(backup_path):
                    try:
                        with open(backup_path, 'r', encoding='utf-8') as f:
                            resource = json.load(f)
                        
                        logger.warning(f"Recurso {resource_id} carregado do backup após falha: versão {resource.get('version', 'N/A')}")
                        return resource
                    except Exception as backup_error:
                        logger.error(f"Erro ao carregar backup do recurso {resource_id}: {backup_error}", exc_info=True)
                
                return None
    
    def save_resource(self, resource_id: str, resource: Dict[str, Any]) -> bool:
        """
        Salva um recurso no armazenamento.
        
        Args:
            resource_id: ID do recurso
            resource: Dados do recurso
            
        Returns:
            bool: True se salvo com sucesso, False caso contrário
        """
        with self.lock:
            resource_path = self._get_resource_path(resource_id)
            temp_path = self._get_temp_path(resource_id)
            
            try:
                # Primeiro, cria um backup se necessário
                self._create_backup_if_needed(resource_id)
                
                # Escreve em arquivo temporário primeiro
                with open(temp_path, 'w', encoding='utf-8') as f:
                    json.dump(resource, f, indent=2)
                
                # Renomeia para o arquivo final (operação atômica em muitos sistemas)
                shutil.move(temp_path, resource_path)
                
                logger.debug(f"Recurso {resource_id} salvo no armazenamento: versão {resource.get('version', 'N/A')}")
                return True
            except Exception as e:
                logger.error(f"Erro ao salvar recurso {resource_id}: {e}", exc_info=True)
                
                # Tenta limpar arquivo temporário se permanecer
                if os.path.exists(temp_path):
                    try:
                        os.remove(temp_path)
                    except:
                        pass
                
                return False
    
    def _create_backup_if_needed(self, resource_id: str):
        """
        Cria um backup do recurso se necessário.
        
        Args:
            resource_id: ID do recurso
        """
        # Verifica se precisamos fazer backup (no máximo a cada 5 minutos)
        current_time = time.time()
        last_check_time = self.last_backup_check.get(resource_id, 0)
        
        if current_time - last_check_time < 300:  # 5 minutos em segundos
            return
        
        # Atualiza o timestamp de verificação
        self.last_backup_check[resource_id] = current_time
        
        # Verifica se o arquivo principal existe
        resource_path = self._get_resource_path(resource_id)
        if not os.path.exists(resource_path):
            return
        
        # Cria o backup
        backup_path = self._get_backup_path(resource_id)
        try:
            shutil.copy2(resource_path, backup_path)
            logger.debug(f"Backup criado para recurso {resource_id}")
        except Exception as e:
            logger.error(f"Erro ao criar backup do recurso {resource_id}: {e}", exc_info=True)
    
    def delete_resource(self, resource_id: str) -> bool:
        """
        Remove um recurso do armazenamento.
        
        Args:
            resource_id: ID do recurso
            
        Returns:
            bool: True se removido com sucesso, False caso contrário
        """
        with self.lock:
            resource_path = self._get_resource_path(resource_id)
            
            # Verifica se o arquivo existe
            if not os.path.exists(resource_path):
                logger.warning(f"Tentativa de remover recurso inexistente {resource_id}")
                return False
            
            try:
                # Remove o arquivo
                os.remove(resource_path)
                
                logger.info(f"Recurso {resource_id} removido do armazenamento")
                return True
            except Exception as e:
                logger.error(f"Erro ao remover recurso {resource_id}: {e}", exc_info=True)
                return False
    
    def list_resources(self) -> List[str]:
        """
        Lista todos os recursos disponíveis.
        
        Returns:
            List[str]: Lista de IDs dos recursos
        """
        with self.lock:
            resources = []
            
            try:
                # Lista arquivos no diretório
                for filename in os.listdir(self.data_dir):
                    # Filtra apenas arquivos .json e ignora temporários e backups
                    if filename.endswith('.json') and not filename.endswith('.tmp.json'):
                        # Remove a extensão
                        resource_id = os.path.splitext(filename)[0]
                        resources.append(resource_id)
            except Exception as e:
                logger.error(f"Erro ao listar recursos: {e}", exc_info=True)
            
            return resources
    
    def get_resource_metadata(self, resource_id: str) -> Optional[Dict[str, Any]]:
        """
        Obtém metadados de um recurso sem carregar todo o conteúdo.
        
        Args:
            resource_id: ID do recurso
            
        Returns:
            Optional[Dict[str, Any]]: Metadados do recurso ou None se não existir
        """
        resource = self.load_resource(resource_id)
        if resource is None:
            return None
        
        # Extrai apenas os metadados
        return {
            "version": resource.get("version", 0),
            "timestamp": resource.get("timestamp", 0),
            "node_id": resource.get("node_id", 0)
        }