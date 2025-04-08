import json
import logging
import os
import time
from typing import Dict, Optional

logger = logging.getLogger(__name__)

class PersistenceManager:
    """
    Gerenciador de persistência para o recurso R.
    Responsável por salvar e carregar o estado do recurso.
    """
    
    def __init__(self, store_id: str, data_dir: str = "data", debug: bool = False):
        """
        Inicializa o gerenciador de persistência.
        
        Args:
            store_id: ID do store
            data_dir: Diretório para armazenar os dados
            debug: Flag para ativar modo de depuração
        """
        self.store_id = store_id
        self.data_dir = data_dir
        self.debug = debug
        self.main_file = os.path.join(data_dir, f"resource_{store_id}.json")
        self.backup_file = os.path.join(data_dir, f"resource_{store_id}.backup.json")
        
        # Cria o diretório de dados, se não existir
        if not os.path.exists(data_dir):
            try:
                os.makedirs(data_dir)
                logger.debug(f"Diretório de dados criado: {data_dir}")
            except Exception as e:
                logger.error(f"Erro ao criar diretório de dados {data_dir}: {str(e)}")
        
        logger.debug(f"Gerenciador de persistência inicializado para store {store_id}")
    
    def save(self, data: Dict) -> bool:
        """
        Salva os dados do recurso no armazenamento persistente.
        
        Args:
            data: Dados a serem salvos
        
        Returns:
            bool: True se o salvamento foi bem-sucedido, False caso contrário
        """
        try:
            # Adiciona metadados
            data_with_meta = data.copy()
            data_with_meta["_meta"] = {
                "store_id": self.store_id,
                "saved_at": time.time()
            }
            
            # Primeiro, tenta criar um backup do arquivo existente
            if os.path.exists(self.main_file):
                try:
                    # Copia o conteúdo do arquivo principal para o backup
                    with open(self.main_file, "r") as main_f:
                        with open(self.backup_file, "w") as backup_f:
                            backup_f.write(main_f.read())
                except Exception as e:
                    logger.warning(f"Erro ao criar backup do recurso: {str(e)}")
            
            # Salva os dados em um arquivo temporário
            temp_file = f"{self.main_file}.tmp"
            with open(temp_file, "w") as f:
                json.dump(data_with_meta, f, indent=2)
            
            # Renomeia o arquivo temporário para o arquivo principal
            os.replace(temp_file, self.main_file)
            
            if self.debug:
                logger.debug(f"Dados salvos com sucesso: versão {data.get('version')}")
            
            return True
        except Exception as e:
            logger.error(f"Erro ao salvar dados: {str(e)}")
            return False
    
    def load(self) -> Optional[Dict]:
        """
        Carrega os dados do recurso do armazenamento persistente.
        
        Returns:
            Optional[Dict]: Dados carregados ou None se ocorrer um erro
        """
        try:
            # Tenta carregar o arquivo principal
            if os.path.exists(self.main_file):
                try:
                    with open(self.main_file, "r") as f:
                        data = json.load(f)
                        logger.debug(f"Dados carregados do arquivo principal: versão {data.get('version')}")
                        return data
                except Exception as e:
                    logger.error(f"Erro ao carregar arquivo principal: {str(e)}")
                    
                    # Se falhar, tenta carregar o backup
                    if os.path.exists(self.backup_file):
                        try:
                            with open(self.backup_file, "r") as f:
                                data = json.load(f)
                                logger.warning(f"Dados carregados do arquivo de backup: versão {data.get('version')}")
                                return data
                        except Exception as e2:
                            logger.error(f"Erro ao carregar arquivo de backup: {str(e2)}")
            
            # Se não existir arquivo, retorna None
            if not os.path.exists(self.main_file):
                logger.info("Nenhum arquivo de dados encontrado.")
                return None
            
            return None
        except Exception as e:
            logger.error(f"Erro ao carregar dados: {str(e)}")
            return None
    
    def list_backups(self) -> list:
        """
        Lista todos os backups disponíveis.
        
        Returns:
            list: Lista de nomes de arquivos de backup
        """
        backups = []
        try:
            # Procura por arquivos de backup no diretório de dados
            for filename in os.listdir(self.data_dir):
                if filename.startswith(f"resource_{self.store_id}") and filename.endswith(".backup.json"):
                    backups.append(filename)
            
            return backups
        except Exception as e:
            logger.error(f"Erro ao listar backups: {str(e)}")
            return []