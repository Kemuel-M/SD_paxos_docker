import asyncio
import logging
import time
from typing import Dict, List, Optional, Set

from common.utils import http_request

logger = logging.getLogger(__name__)

class ROWAManager:
    """
    Implementação do protocolo Read One, Write All (ROWA).
    
    No protocolo ROWA:
    - Leituras são realizadas em apenas um servidor (Nr = 1)
    - Escritas precisam ser confirmadas por todos os servidores (Nw = N)
    
    Este protocolo favorece leituras rápidas, mas torna as escritas
    vulneráveis a falhas de qualquer servidor.
    """
    
    def __init__(self, learner_id: str, debug: bool = False):
        """
        Inicializa o gerenciador ROWA.
        
        Args:
            learner_id: ID do learner
            debug: Flag para ativar modo de depuração
        """
        self.learner_id = learner_id
        self.debug = debug
        self.enabled = True  # Por padrão, ROWA está ativado
        
        # Controle de aceitação
        self.acceptor_count = 0
        
        # Estado das propostas
        self.proposals: Dict[str, Dict] = {}  # {proposal_id: {acceptors: {}, tid: int, ...}}
        
        # Lista de stores
        self.stores: Dict[str, str] = {}  # {store_id: url}
        
        # Lista de clientes
        self.clients: Dict[str, str] = {}  # {client_id: url}
        
        # Locks
        self.proposals_lock = asyncio.Lock()
        
        logger.debug(f"ROWA Manager inicializado para learner {learner_id} (debug={debug})")
    
    def is_enabled(self) -> bool:
        """
        Verifica se o protocolo ROWA está ativado.
        
        Returns:
            bool: True se o ROWA está ativado, False caso contrário
        """
        return self.enabled
    
    def set_enabled(self, enabled: bool) -> None:
        """
        Ativa ou desativa o protocolo ROWA.
        
        Args:
            enabled: Flag para ativar/desativar o ROWA
        """
        self.enabled = enabled
        logger.info(f"Protocolo ROWA {'ativado' if enabled else 'desativado'}")
    
    def set_acceptor_count(self, count: int) -> None:
        """
        Define o número total de acceptors.
        
        Args:
            count: Número total de acceptors
        """
        self.acceptor_count = count
        logger.debug(f"Número de acceptors definido: {count}")
    
    def add_store(self, store_id: str, url: str) -> None:
        """
        Adiciona um store à lista de stores conhecidos.
        
        Args:
            store_id: ID do store
            url: URL base do store
        """
        self.stores[store_id] = url
        logger.debug(f"Store adicionado: {store_id} -> {url}")
    
    def add_client(self, client_id: str, url: str) -> None:
        """
        Adiciona um cliente à lista de clientes conhecidos.
        
        Args:
            client_id: ID do cliente
            url: URL base do cliente
        """
        self.clients[client_id] = url
        logger.debug(f"Cliente adicionado: {client_id} -> {url}")
    
    async def process_learn(self, proposal_id: str, acceptor_id: str, proposer_id: str, 
                          tid: int, client_id: str, resource_data: Dict, timestamp: int) -> Dict:
        """
        Processa um aprendizado de proposta usando o protocolo ROWA.
        
        Args:
            proposal_id: ID da proposta
            acceptor_id: ID do acceptor que aceitou a proposta
            proposer_id: ID do proposer que propôs o valor
            tid: TID da proposta
            client_id: ID do cliente
            resource_data: Dados do recurso
            timestamp: Timestamp da proposta
        
        Returns:
            Dict: Resultado do processamento
        """
        async with self.proposals_lock:
            # Se já conhecemos esta proposta, atualizamos com o novo acceptor
            if proposal_id in self.proposals:
                proposal = self.proposals[proposal_id]
                
                # Adiciona o acceptor à lista de acceptors que aceitaram esta proposta
                proposal["acceptors"][acceptor_id] = {
                    "tid": tid,
                    "timestamp": time.time()
                }
                
                # Se o TID for diferente do que já conhecemos, algo está errado
                if tid != proposal["tid"]:
                    logger.warning(f"TID diferente para proposta {proposal_id}: {tid} != {proposal['tid']}")
                
                # Verifica se todos os acceptors aceitaram a proposta (ROWA)
                if len(proposal["acceptors"]) == self.acceptor_count and not proposal.get("committed", False):
                    # Todos os acceptors aceitaram! Podemos commitar a proposta
                    logger.info(f"Todos os acceptors ({len(proposal['acceptors'])}/{self.acceptor_count}) aceitaram a proposta {proposal_id} (ROWA)")
                    
                    # Marca a proposta como commitada
                    proposal["committed"] = True
                    proposal["committed_at"] = time.time()
                    
                    # Notifica o cliente
                    await self._notify_client(client_id, proposal_id, tid, resource_data, "COMMITTED")
                    
                    # Notifica os stores
                    await self._update_stores(proposal_id, tid, resource_data)
                else:
                    logger.debug(f"Aguardando mais acceptors para proposta {proposal_id}: {len(proposal['acceptors'])}/{self.acceptor_count} (ROWA)")
            else:
                # Primeira vez que vemos esta proposta
                self.proposals[proposal_id] = {
                    "proposal_id": proposal_id,
                    "tid": tid,
                    "proposer_id": proposer_id,
                    "client_id": client_id,
                    "resource_data": resource_data,
                    "timestamp": timestamp,
                    "first_learned_at": time.time(),
                    "acceptors": {
                        acceptor_id: {
                            "tid": tid,
                            "timestamp": time.time()
                        }
                    },
                    "committed": False
                }
                
                # Se só existe um acceptor, podemos commitar imediatamente
                if self.acceptor_count == 1:
                    logger.info(f"Único acceptor aceitou a proposta {proposal_id} (ROWA)")
                    
                    # Marca a proposta como commitada
                    self.proposals[proposal_id]["committed"] = True
                    self.proposals[proposal_id]["committed_at"] = time.time()
                    
                    # Notifica o cliente
                    await self._notify_client(client_id, proposal_id, tid, resource_data, "COMMITTED")
                    
                    # Notifica os stores
                    await self._update_stores(proposal_id, tid, resource_data)
                else:
                    logger.debug(f"Aguardando mais acceptors para proposta {proposal_id}: 1/{self.acceptor_count} (ROWA)")
        
        return {
            "success": True,
            "proposal_id": proposal_id,
            "learner_id": self.learner_id
        }
    
    async def _notify_client(self, client_id: str, proposal_id: str, tid: int, 
                           resource_data: Dict, status: str) -> None:
        """
        Notifica o cliente sobre o resultado de uma proposta.
        
        Args:
            client_id: ID do cliente
            proposal_id: ID da proposta
            tid: TID da proposta
            resource_data: Dados do recurso
            status: Status da proposta (COMMITTED ou NOT_COMMITTED)
        """
        # Verifica se conhecemos o cliente
        if client_id not in self.clients:
            logger.warning(f"Cliente {client_id} não encontrado. Não é possível notificar sobre proposta {proposal_id}")
            return
        
        client_url = self.clients[client_id]
        notification_url = f"{client_url}/notify"
        
        # Dados da notificação
        notification_data = {
            "proposal_id": proposal_id,
            "tid": tid,
            "status": status,
            "resource_data": resource_data,
            "learner_id": self.learner_id,
            "timestamp": time.time(),
            "protocol": "ROWA"
        }
        
        try:
            # Envia a notificação para o cliente
            response = await http_request("POST", notification_url, data=notification_data)
            
            if response.get("success", False):
                logger.debug(f"Cliente {client_id} notificado com sucesso sobre proposta {proposal_id} (ROWA)")
            else:
                logger.warning(f"Falha ao notificar cliente {client_id} sobre proposta {proposal_id} (ROWA): {response.get('error', 'erro desconhecido')}")
        except Exception as e:
            logger.error(f"Erro ao notificar cliente {client_id} sobre proposta {proposal_id} (ROWA): {str(e)}")
    
    async def _update_stores(self, proposal_id: str, tid: int, resource_data: Dict) -> None:
        """
        Atualiza os stores com os dados da proposta commitada.
        Com ROWA, todas as escritas devem ser aplicadas a todos os stores.
        
        Args:
            proposal_id: ID da proposta
            tid: TID da proposta
            resource_data: Dados do recurso
        """
        # Preparar as tarefas de atualização para todos os stores
        update_tasks = []
        
        # No ROWA, precisamos atualizar TODOS os stores
        for store_id, url in self.stores.items():
            update_url = f"{url}/commit"
            
            update_data = {
                "proposal_id": proposal_id,
                "tid": tid,
                "resource_data": resource_data,
                "learner_id": self.learner_id,
                "timestamp": time.time(),
                "protocol": "ROWA"
            }
            
            task = asyncio.create_task(
                self._update_store(store_id, update_url, update_data)
            )
            update_tasks.append(task)
        
        # Aguarda todas as atualizações serem concluídas
        results = await asyncio.gather(*update_tasks, return_exceptions=True)
        
        # Verifica os resultados
        success_count = 0
        error_count = 0
        failed_stores = []
        
        for i, result in enumerate(results):
            store_id = list(self.stores.keys())[i]  # Obtém o store_id correspondente
            
            if isinstance(result, Exception):
                error_count += 1
                failed_stores.append(store_id)
                logger.error(f"Erro ao atualizar store {store_id}: {str(result)}")
            elif result.get("success", False):
                success_count += 1
            else:
                error_count += 1
                failed_stores.append(store_id)
                logger.error(f"Falha ao atualizar store {store_id}: {result.get('error', 'erro desconhecido')}")
        
        # No ROWA, todas as escritas devem ser bem-sucedidas
        if error_count > 0:
            logger.error(f"Falha na atualização de {error_count} stores: {failed_stores}")
            
            # TODO: Implementar estratégia de recuperação
            # Por enquanto, apenas logamos o erro
        else:
            logger.info(f"Todos os {len(self.stores)} stores atualizados com sucesso (ROWA)")
    
    async def _update_store(self, store_id: str, url: str, data: Dict) -> Dict:
        """
        Atualiza um store específico com os dados da proposta.
        
        Args:
            store_id: ID do store
            url: URL do endpoint commit do store
            data: Dados da atualização
        
        Returns:
            Dict: Resposta do store
        """
        try:
            response = await http_request("POST", url, data=data)
            
            if response.get("success", False):
                logger.debug(f"Store {store_id} atualizado com sucesso para proposta {data['proposal_id']} (ROWA)")
            else:
                logger.warning(f"Falha ao atualizar store {store_id} para proposta {data['proposal_id']} (ROWA): {response.get('error', 'erro desconhecido')}")
            
            return response
        except Exception as e:
            logger.error(f"Erro ao atualizar store {store_id} para proposta {data['proposal_id']} (ROWA): {str(e)}")
            raise