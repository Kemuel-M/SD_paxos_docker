import asyncio
import json
import logging
import time
from typing import Dict, List, Optional, Tuple, Union

from common.utils import generate_id

logger = logging.getLogger(__name__)

class Store:
    """
    Implementação do componente Cluster Store.
    Responsável por armazenar e gerenciar o acesso ao recurso R.
    """
    
    def __init__(self, store_id: str, persistence_manager=None, debug: bool = False):
        """
        Inicializa o store.
        
        Args:
            store_id: ID único do store
            persistence_manager: Gerenciador de persistência para o recurso R
            debug: Flag para ativar modo de depuração
        """
        self.id = store_id
        self.debug = debug
        self.persistence_manager = persistence_manager
        
        # Versão atual do recurso
        self.version = 0
        
        # O último TID aceito
        self.last_tid = 0
        
        # Estado do recurso R
        self.resource_data = {}
        
        # Histórico de atualizações
        self.update_history: List[Dict] = []
        
        # Controle de acesso
        self.locked = False
        self.lock_holder = None
        self.lock_expiry = 0
        
        # Locks para operações concorrentes
        self.resource_lock = asyncio.Lock()
        
        logger.debug(f"Store {store_id} inicializado (debug={debug})")
        
        # Tenta carregar o estado inicial do recurso, se houver
        if self.persistence_manager:
            self._load_initial_state()
    
    def _load_initial_state(self) -> None:
        """
        Carrega o estado inicial do recurso a partir do gerenciador de persistência.
        """
        try:
            if self.persistence_manager:
                initial_data = self.persistence_manager.load()
                if initial_data:
                    self.resource_data = initial_data.get("data", {})
                    self.version = initial_data.get("version", 0)
                    self.last_tid = initial_data.get("tid", 0)
                    logger.info(f"Estado inicial carregado: versão {self.version}, TID {self.last_tid}")
                else:
                    logger.info("Nenhum estado inicial encontrado. Usando valores padrão.")
        except Exception as e:
            logger.error(f"Erro ao carregar estado inicial: {str(e)}")
    
    async def prepare(self, proposal_id: str, tid: int) -> Dict:
        """
        Processa uma mensagem PREPARE para acesso ao recurso.
        
        Args:
            proposal_id: ID da proposta
            tid: TID da proposta
        
        Returns:
            Dict: Resposta à mensagem PREPARE
        """
        async with self.resource_lock:
            # Se o recurso estiver bloqueado, rejeitamos
            if self.locked and time.time() < self.lock_expiry:
                logger.info(f"Rejeitando PREPARE para proposta {proposal_id}: recurso bloqueado por {self.lock_holder}")
                return {
                    "success": False,
                    "tid": tid,
                    "message": "resource_locked",
                    "lock_holder": self.lock_holder,
                    "lock_expiry": self.lock_expiry
                }
            
            # Se o TID for menor que o último aceito, rejeitamos
            if tid < self.last_tid:
                logger.info(f"Rejeitando PREPARE para proposta {proposal_id}: TID={tid} < último TID={self.last_tid}")
                return {
                    "success": False,
                    "tid": self.last_tid,
                    "message": "tid_too_low"
                }
            
            # Aceitamos o PREPARE
            logger.info(f"Aceitando PREPARE para proposta {proposal_id}: TID={tid}")
            
            # Bloqueia o recurso temporariamente (10 segundos)
            self.locked = True
            self.lock_holder = proposal_id
            self.lock_expiry = time.time() + 10
            
            return {
                "success": True,
                "tid": tid,
                "message": "promise",
                "version": self.version,
                "resource_data": self.resource_data
            }
    
    async def commit(self, proposal_id: str, tid: int, resource_data: Dict, 
                   learner_id: str, timestamp: float) -> Dict:
        """
        Processa uma mensagem COMMIT para atualizar o recurso.
        
        Args:
            proposal_id: ID da proposta
            tid: TID da proposta
            resource_data: Novos dados do recurso
            learner_id: ID do learner que enviou o commit
            timestamp: Timestamp do commit
        
        Returns:
            Dict: Resultado do commit
        """
        async with self.resource_lock:
            # Se o TID for menor que o último aceito, rejeitamos
            if tid < self.last_tid:
                logger.info(f"Rejeitando COMMIT para proposta {proposal_id}: TID={tid} < último TID={self.last_tid}")
                return {
                    "success": False,
                    "tid": self.last_tid,
                    "message": "tid_too_low"
                }
            
            # Aceita o commit
            self.last_tid = tid
            self.version += 1
            
            # Atualiza o recurso
            prev_data = self.resource_data.copy()
            self.resource_data = resource_data
            
            # Registra a atualização
            update_record = {
                "proposal_id": proposal_id,
                "tid": tid,
                "learner_id": learner_id,
                "timestamp": timestamp,
                "committed_at": time.time(),
                "version": self.version,
                "previous_version": self.version - 1
            }
            self.update_history.append(update_record)
            
            # Persiste o estado, se houver gerenciador de persistência
            if self.persistence_manager:
                try:
                    self.persistence_manager.save({
                        "data": self.resource_data,
                        "version": self.version,
                        "tid": self.last_tid,
                        "last_update": update_record
                    })
                    logger.debug(f"Estado persistido após commit da proposta {proposal_id}")
                except Exception as e:
                    logger.error(f"Erro ao persistir estado após commit da proposta {proposal_id}: {str(e)}")
            
            # Libera o lock, se esta proposta o possuir
            if self.lock_holder == proposal_id:
                self.locked = False
                self.lock_holder = None
                self.lock_expiry = 0
            
            logger.info(f"COMMIT aceito para proposta {proposal_id}: TID={tid}, versão={self.version}")
            
            return {
                "success": True,
                "tid": tid,
                "version": self.version,
                "message": "committed"
            }
    
    async def get_resource(self) -> Dict:
        """
        Obtém o estado atual do recurso R.
        
        Returns:
            Dict: Estado atual do recurso
        """
        async with self.resource_lock:
            return {
                "data": self.resource_data,
                "version": self.version,
                "tid": self.last_tid,
                "timestamp": time.time()
            }
    
    def get_status(self) -> Dict:
        """
        Obtém o status do store.
        
        Returns:
            Dict: Status do store
        """
        return {
            "id": self.id,
            "version": self.version,
            "last_tid": self.last_tid,
            "locked": self.locked,
            "lock_holder": self.lock_holder,
            "lock_expiry": self.lock_expiry if self.locked else None,
            "updates_count": len(self.update_history),
            "persistence_enabled": self.persistence_manager is not None
        }