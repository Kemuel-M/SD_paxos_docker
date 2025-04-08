import asyncio
import logging
import time
from typing import Dict, List, Optional, Set

from common.utils import http_request

logger = logging.getLogger(__name__)

class Learner:
    """
    Implementação do learner do algoritmo Paxos.
    Responsável por aprender os valores decididos e notificar os clientes.
    """
    
    def __init__(self, learner_id: str, debug: bool = False):
        """
        Inicializa o learner.
        
        Args:
            learner_id: ID único do learner
            debug: Flag para ativar modo de depuração
        """
        self.id = learner_id
        self.debug = debug
        
        # Controle de quóruns e propostas
        self.acceptor_count = 0  # Número total de acceptors
        self.quorum_size = 0  # Tamanho necessário para formar um quórum (maioria)
        
        # Estado das propostas
        self.proposals: Dict[str, Dict] = {}  # {proposal_id: {acceptors: {}, tid: int, ...}}
        
        # Lista de stores
        self.stores: Dict[str, str] = {}  # {store_id: url}
        
        # Lista de clientes
        self.clients: Dict[str, str] = {}  # {client_id: url}
        
        # Locks
        self.proposals_lock = asyncio.Lock()
        
        logger.debug(f"Learner {learner_id} inicializado (debug={debug})")
    
    def set_acceptor_count(self, count: int) -> None:
        """
        Define o número total de acceptors.
        
        Args:
            count: Número total de acceptors
        """
        self.acceptor_count = count
        self.quorum_size = count // 2 + 1
        logger.debug(f"Número de acceptors definido: {count}, quorum_size={self.quorum_size}")
    
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
    
    async def learn(self, proposal_id: str, acceptor_id: str, proposer_id: str, 
                   tid: int, client_id: str, resource_data: Dict, timestamp: int) -> Dict:
        """
        Aprende sobre uma proposta aceita por um acceptor.
        
        Args:
            proposal_id: ID da proposta
            acceptor_id: ID do acceptor que aceitou a proposta
            proposer_id: ID do proposer que propôs o valor
            tid: TID da proposta
            client_id: ID do cliente
            resource_data: Dados do recurso
            timestamp: Timestamp da proposta
        
        Returns:
            Dict: Resultado do aprendizado
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
                
                # Verifica se temos um quórum
                if len(proposal["acceptors"]) >= self.quorum_size and not proposal.get("committed", False):
                    # Temos um quórum! Podemos commitar a proposta
                    logger.info(f"Quórum alcançado para proposta {proposal_id}: {len(proposal['acceptors'])} acceptors")
                    
                    # Marca a proposta como commitada
                    proposal["committed"] = True
                    proposal["committed_at"] = time.time()
                    
                    # Notifica o cliente
                    await self._notify_client(client_id, proposal_id, tid, resource_data, "COMMITTED")
                    
                    # Notifica os stores
                    await self._update_stores(proposal_id, tid, resource_data)
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
                
                # Se este for o único acceptor e tivermos apenas um acceptor, podemos commitar imediatamente
                if self.acceptor_count == 1 or len(self.proposals[proposal_id]["acceptors"]) >= self.quorum_size:
                    logger.info(f"Quórum alcançado imediatamente para proposta {proposal_id}: {len(self.proposals[proposal_id]['acceptors'])} acceptors")
                    
                    # Marca a proposta como commitada
                    self.proposals[proposal_id]["committed"] = True
                    self.proposals[proposal_id]["committed_at"] = time.time()
                    
                    # Notifica o cliente
                    await self._notify_client(client_id, proposal_id, tid, resource_data, "COMMITTED")
                    
                    # Notifica os stores
                    await self._update_stores(proposal_id, tid, resource_data)
                else:
                    logger.debug(f"Aguardando mais acceptors para proposta {proposal_id}: {len(self.proposals[proposal_id]['acceptors'])}/{self.quorum_size}")
        
        return {
            "success": True,
            "proposal_id": proposal_id,
            "learner_id": self.id
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
            "learner_id": self.id,
            "timestamp": time.time()
        }
        
        try:
            # Envia a notificação para o cliente
            response = await http_request("POST", notification_url, data=notification_data)
            
            if response.get("success", False):
                logger.debug(f"Cliente {client_id} notificado com sucesso sobre proposta {proposal_id}")
            else:
                logger.warning(f"Falha ao notificar cliente {client_id} sobre proposta {proposal_id}: {response.get('error', 'erro desconhecido')}")
        except Exception as e:
            logger.error(f"Erro ao notificar cliente {client_id} sobre proposta {proposal_id}: {str(e)}")
    
    async def _update_stores(self, proposal_id: str, tid: int, resource_data: Dict) -> None:
        """
        Atualiza os stores com os dados da proposta commitada.
        
        Args:
            proposal_id: ID da proposta
            tid: TID da proposta
            resource_data: Dados do recurso
        """
        # Preparar as tarefas de atualização para todos os stores
        update_tasks = []
        
        for store_id, url in self.stores.items():
            update_url = f"{url}/commit"
            
            update_data = {
                "proposal_id": proposal_id,
                "tid": tid,
                "resource_data": resource_data,
                "learner_id": self.id,
                "timestamp": time.time()
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
        
        for result in results:
            if isinstance(result, Exception):
                error_count += 1
            elif result.get("success", False):
                success_count += 1
            else:
                error_count += 1
        
        logger.debug(f"Stores atualizados para proposta {proposal_id}: {success_count} sucesso, {error_count} erro")
    
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
                logger.debug(f"Store {store_id} atualizado com sucesso para proposta {data['proposal_id']}")
            else:
                logger.warning(f"Falha ao atualizar store {store_id} para proposta {data['proposal_id']}: {response.get('error', 'erro desconhecido')}")
            
            return response
        except Exception as e:
            logger.error(f"Erro ao atualizar store {store_id} para proposta {data['proposal_id']}: {str(e)}")
            return {
                "success": False,
                "error": str(e)
            }
    
    def get_proposal_status(self, proposal_id: str) -> Dict:
        """
        Obtém o status de uma proposta específica.
        
        Args:
            proposal_id: ID da proposta
        
        Returns:
            Dict: Status da proposta
        """
        if proposal_id in self.proposals:
            proposal = self.proposals[proposal_id]
            
            return {
                "proposal_id": proposal_id,
                "tid": proposal["tid"],
                "client_id": proposal["client_id"],
                "acceptors_count": len(proposal["acceptors"]),
                "committed": proposal.get("committed", False),
                "timestamp": proposal["timestamp"],
                "first_learned_at": proposal["first_learned_at"],
                "committed_at": proposal.get("committed_at")
            }
        else:
            return {
                "proposal_id": proposal_id,
                "error": "Proposta não encontrada"
            }
    
    def get_status(self) -> Dict:
        """
        Obtém o status do learner.
        
        Returns:
            Dict: Status do learner
        """
        # Conta propostas commitadas e pendentes
        committed_count = 0
        pending_count = 0
        
        for proposal in self.proposals.values():
            if proposal.get("committed", False):
                committed_count += 1
            else:
                pending_count += 1
        
        return {
            "id": self.id,
            "acceptor_count": self.acceptor_count,
            "quorum_size": self.quorum_size,
            "stores_count": len(self.stores),
            "clients_count": len(self.clients),
            "proposals_committed": committed_count,
            "proposals_pending": pending_count,
            "total_proposals": len(self.proposals)
        }