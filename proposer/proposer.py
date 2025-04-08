import asyncio
import logging
import random
import time
from typing import Dict, List, Optional, Set, Tuple, Union

from common.utils import (
    DistributedCounter, current_timestamp, generate_id, http_request
)

logger = logging.getLogger(__name__)

class ProposerState:
    """Enum para representar os estados possíveis de uma proposta"""
    IDLE = "idle"              # Estado inicial
    PREPARING = "preparing"    # Fase de prepare
    ACCEPTING = "accepting"    # Fase de accept
    COMMITTED = "committed"    # Proposta aceita
    REJECTED = "rejected"      # Proposta rejeitada
    FAILED = "failed"          # Falha na proposta

class Proposer:
    """
    Implementação do proposer do algoritmo Paxos.
    Responsável por propor valores para os acceptors e coordenar o consenso.
    """
    
    def __init__(self, proposer_id: str, debug: bool = False):
        """
        Inicializa o proposer.
        
        Args:
            proposer_id: ID único do proposer
            debug: Flag para ativar modo de depuração
        """
        self.id = proposer_id
        self.debug = debug
        self.is_leader = False
        self.current_leader: Optional[str] = None
        
        # TID (Transaction ID) é um contador usado para ordenar as propostas
        self.tid_counter = DistributedCounter(initial_value=0)
        
        # Lista de acceptors
        self.acceptors: Dict[str, str] = {}  # {acceptor_id: url}
        
        # Lista de learners
        self.learners: Dict[str, str] = {}  # {learner_id: url}
        
        # Lista de stores
        self.stores: Dict[str, str] = {}  # {store_id: url}
        
        # Propostas em andamento
        self.proposals: Dict[str, Dict] = {}
        
        # Locks para operações concorrentes
        self.leader_lock = asyncio.Lock()
        self.proposal_lock = asyncio.Lock()
        
        logger.debug(f"Proposer {proposer_id} inicializado (debug={debug})")
    
    def add_acceptor(self, acceptor_id: str, url: str) -> None:
        """
        Adiciona um acceptor à lista de acceptors conhecidos.
        
        Args:
            acceptor_id: ID do acceptor
            url: URL base do acceptor
        """
        self.acceptors[acceptor_id] = url
        logger.debug(f"Acceptor adicionado: {acceptor_id} -> {url}")
    
    def add_learner(self, learner_id: str, url: str) -> None:
        """
        Adiciona um learner à lista de learners conhecidos.
        
        Args:
            learner_id: ID do learner
            url: URL base do learner
        """
        self.learners[learner_id] = url
        logger.debug(f"Learner adicionado: {learner_id} -> {url}")
    
    def add_store(self, store_id: str, url: str) -> None:
        """
        Adiciona um store à lista de stores conhecidos.
        
        Args:
            store_id: ID do store
            url: URL base do store
        """
        self.stores[store_id] = url
        logger.debug(f"Store adicionado: {store_id} -> {url}")
    
    async def set_leader(self, leader_id: str) -> None:
        """
        Define o líder atual do cluster.
        
        Args:
            leader_id: ID do proposer líder
        """
        async with self.leader_lock:
            self.current_leader = leader_id
            self.is_leader = (leader_id == self.id)
            logger.info(f"Líder definido: {leader_id} (este proposer é líder: {self.is_leader})")
    
    async def get_next_tid(self) -> int:
        """
        Obtém o próximo TID (Transaction ID) para uma proposta.
        
        Returns:
            int: O próximo TID
        """
        return await self.tid_counter.get_next()
    
    async def update_tid(self, new_tid: int) -> None:
        """
        Atualiza o TID se o novo valor for maior que o atual.
        
        Args:
            new_tid: Novo valor de TID proposto
        """
        await self.tid_counter.update_if_greater(new_tid)
        logger.debug(f"TID atualizado para {new_tid}")
    
    async def prepare(self, proposal_id: str, client_id: str, 
                     resource_data: Dict, timestamp: int) -> Tuple[bool, int]:
        """
        Executa a fase de prepare do algoritmo Paxos.
        
        Args:
            proposal_id: ID único da proposta
            client_id: ID do cliente que fez a solicitação
            resource_data: Dados do recurso a serem propostos
            timestamp: Timestamp da proposta
        
        Returns:
            Tuple[bool, int]: (sucesso, novo_tid)
                - sucesso: True se a fase prepare foi bem-sucedida
                - novo_tid: O TID a ser usado na fase de accept
        """
        # Gera um novo TID para esta proposta
        tid = await self.get_next_tid()
        
        # Cria o objeto de proposta
        async with self.proposal_lock:
            self.proposals[proposal_id] = {
                "id": proposal_id,
                "tid": tid,
                "client_id": client_id,
                "resource_data": resource_data,
                "timestamp": timestamp,
                "state": ProposerState.PREPARING,
                "promises": {},
                "rejections": {},
                "highest_tid_seen": tid
            }
        
        logger.info(f"Iniciando fase PREPARE para proposta {proposal_id} com TID={tid}")
        
        # Para manter o controle de quais acceptors responderam
        promises_count = 0
        rejections_count = 0
        highest_tid = tid
        highest_value = None
        
        # Envia PREPARE para todos os acceptors
        prepare_tasks = []
        
        for acceptor_id, url in self.acceptors.items():
            prepare_url = f"{url}/prepare"
            prepare_data = {
                "proposal_id": proposal_id,
                "proposer_id": self.id,
                "tid": tid
            }
            
            task = asyncio.create_task(
                self._send_prepare(acceptor_id, prepare_url, prepare_data)
            )
            prepare_tasks.append(task)
        
        # Aguarda as respostas dos acceptors
        for task in asyncio.as_completed(prepare_tasks):
            result = await task
            acceptor_id = result.get("acceptor_id")
            success = result.get("success", False)
            
            if success:
                # Recebeu um PROMISE
                promises_count += 1
                
                # Armazena a promessa
                async with self.proposal_lock:
                    self.proposals[proposal_id]["promises"][acceptor_id] = result
                
                # Verifica se o acceptor já aceitou uma proposta anteriormente
                acceptor_tid = result.get("accepted_tid")
                acceptor_value = result.get("accepted_value")
                
                if acceptor_tid is not None and acceptor_tid > highest_tid:
                    highest_tid = acceptor_tid
                    highest_value = acceptor_value
                    
                    # Atualiza nosso contador de TID para ser maior que o maior TID visto
                    await self.update_tid(highest_tid)
            else:
                # Recebeu um NOT_PROMISE
                rejections_count += 1
                
                # Armazena a rejeição
                async with self.proposal_lock:
                    self.proposals[proposal_id]["rejections"][acceptor_id] = result
                
                # Atualiza o maior TID visto
                reject_tid = result.get("tid")
                if reject_tid and reject_tid > highest_tid:
                    highest_tid = reject_tid
                    
                    # Atualiza nosso contador de TID
                    await self.update_tid(highest_tid)
        
        # Determina se a fase PREPARE foi bem-sucedida
        # (precisa de maioria, ou seja, mais da metade dos acceptors)
        majority = len(self.acceptors) // 2 + 1
        success = promises_count >= majority
        
        # Atualiza o estado da proposta
        async with self.proposal_lock:
            if proposal_id in self.proposals:
                if success:
                    self.proposals[proposal_id]["state"] = ProposerState.ACCEPTING
                    self.proposals[proposal_id]["highest_tid_seen"] = highest_tid
                    
                    # Se algum acceptor já tinha aceitado um valor, vamos usar esse valor
                    if highest_value is not None:
                        self.proposals[proposal_id]["resource_data"] = highest_value
                else:
                    self.proposals[proposal_id]["state"] = ProposerState.REJECTED
        
        # Log do resultado
        if success:
            logger.info(f"Fase PREPARE bem-sucedida para proposta {proposal_id}: {promises_count} promises, {rejections_count} rejections")
        else:
            logger.warning(f"Fase PREPARE rejeitada para proposta {proposal_id}: {promises_count} promises, {rejections_count} rejections")
        
        return success, highest_tid
    
    async def _send_prepare(self, acceptor_id: str, url: str, data: Dict) -> Dict:
        """
        Envia uma mensagem PREPARE para um acceptor específico.
        
        Args:
            acceptor_id: ID do acceptor
            url: URL do endpoint prepare do acceptor
            data: Dados da proposta
        
        Returns:
            Dict: Resposta do acceptor
        """
        try:
            response = await http_request("POST", url, data=data)
            response["acceptor_id"] = acceptor_id
            return response
        except Exception as e:
            logger.error(f"Erro ao enviar PREPARE para acceptor {acceptor_id}: {str(e)}")
            return {
                "acceptor_id": acceptor_id,
                "success": False,
                "error": str(e)
            }
    
    async def accept(self, proposal_id: str, tid: int) -> bool:
        """
        Executa a fase de accept do algoritmo Paxos.
        
        Args:
            proposal_id: ID da proposta
            tid: TID a ser usado na fase de accept (pode ser diferente do original)
        
        Returns:
            bool: True se a fase accept foi bem-sucedida
        """
        # Obtém os dados da proposta
        proposal = None
        async with self.proposal_lock:
            if proposal_id in self.proposals:
                proposal = self.proposals[proposal_id].copy()
                self.proposals[proposal_id]["state"] = ProposerState.ACCEPTING
            else:
                logger.error(f"Proposta {proposal_id} não encontrada")
                return False
        
        if proposal is None:
            return False
        
        logger.info(f"Iniciando fase ACCEPT para proposta {proposal_id} com TID={tid}")
        
        # Dados para a fase ACCEPT
        accept_data = {
            "proposal_id": proposal_id,
            "proposer_id": self.id,
            "tid": tid,
            "client_id": proposal["client_id"],
            "resource_data": proposal["resource_data"],
            "timestamp": proposal["timestamp"]
        }
        
        # Para manter o controle de quais acceptors responderam
        accepted_count = 0
        rejections_count = 0
        
        # Envia ACCEPT apenas para os acceptors que enviaram PROMISE
        accept_tasks = []
        
        for acceptor_id in proposal["promises"]:
            acceptor_url = self.acceptors.get(acceptor_id)
            if not acceptor_url:
                continue
                
            accept_url = f"{acceptor_url}/accept"
            
            task = asyncio.create_task(
                self._send_accept(acceptor_id, accept_url, accept_data)
            )
            accept_tasks.append(task)
        
        # Aguarda as respostas dos acceptors
        for task in asyncio.as_completed(accept_tasks):
            result = await task
            acceptor_id = result.get("acceptor_id")
            success = result.get("success", False)
            
            if success:
                # Recebeu um ACCEPTED
                accepted_count += 1
            else:
                # Recebeu um NOT_ACCEPTED
                rejections_count += 1
                
                # Atualiza o maior TID visto
                reject_tid = result.get("tid")
                if reject_tid:
                    await self.update_tid(reject_tid)
        
        # Determina se a fase ACCEPT foi bem-sucedida
        # (precisa de maioria, ou seja, mais da metade dos acceptors)
        majority = len(self.acceptors) // 2 + 1
        success = accepted_count >= majority
        
        # Atualiza o estado da proposta
        async with self.proposal_lock:
            if proposal_id in self.proposals:
                if success:
                    self.proposals[proposal_id]["state"] = ProposerState.COMMITTED
                else:
                    self.proposals[proposal_id]["state"] = ProposerState.REJECTED
        
        # Notifica os learners sobre o resultado
        if success:
            await self._notify_learners(proposal_id, tid, proposal)
            logger.info(f"Fase ACCEPT bem-sucedida para proposta {proposal_id}: {accepted_count} accepted, {rejections_count} rejections")
        else:
            logger.warning(f"Fase ACCEPT rejeitada para proposta {proposal_id}: {accepted_count} accepted, {rejections_count} rejections")
        
        return success
    
    async def _send_accept(self, acceptor_id: str, url: str, data: Dict) -> Dict:
        """
        Envia uma mensagem ACCEPT para um acceptor específico.
        
        Args:
            acceptor_id: ID do acceptor
            url: URL do endpoint accept do acceptor
            data: Dados da proposta
        
        Returns:
            Dict: Resposta do acceptor
        """
        try:
            response = await http_request("POST", url, data=data)
            response["acceptor_id"] = acceptor_id
            return response
        except Exception as e:
            logger.error(f"Erro ao enviar ACCEPT para acceptor {acceptor_id}: {str(e)}")
            return {
                "acceptor_id": acceptor_id,
                "success": False,
                "error": str(e)
            }
    
    async def _notify_learners(self, proposal_id: str, tid: int, proposal: Dict) -> None:
        """
        Notifica os learners sobre uma proposta aceita.
        
        Args:
            proposal_id: ID da proposta
            tid: TID da proposta
            proposal: Dados completos da proposta
        """
        # Dados para os learners
        learn_data = {
            "proposal_id": proposal_id,
            "proposer_id": self.id,
            "tid": tid,
            "client_id": proposal["client_id"],
            "resource_data": proposal["resource_data"],
            "timestamp": proposal["timestamp"]
        }
        
        # Envia para todos os learners
        learn_tasks = []
        
        for learner_id, url in self.learners.items():
            learn_url = f"{url}/learn"
            
            task = asyncio.create_task(
                http_request("POST", learn_url, data=learn_data)
            )
            learn_tasks.append(task)
        
        # Aguarda as respostas (mas não precisamos fazer nada com elas)
        await asyncio.gather(*learn_tasks, return_exceptions=True)
        
        logger.debug(f"Learners notificados sobre proposta {proposal_id}")
    
    async def get_store_status(self) -> Dict:
        """
        Obtém o status de todos os stores.
        
        Returns:
            Dict: Status de cada store
        """
        status = {}
        tasks = []
        
        for store_id, url in self.stores.items():
            status_url = f"{url}/status"
            task = asyncio.create_task(
                self._get_store_status(store_id, status_url)
            )
            tasks.append(task)
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for result in results:
            if isinstance(result, Exception):
                continue
            
            store_id = result.get("store_id")
            if store_id:
                status[store_id] = result
        
        return status
    
    async def _get_store_status(self, store_id: str, url: str) -> Dict:
        """
        Obtém o status de um store específico.
        
        Args:
            store_id: ID do store
            url: URL do endpoint status do store
        
        Returns:
            Dict: Status do store
        """
        try:
            response = await http_request("GET", url)
            response["store_id"] = store_id
            return response
        except Exception as e:
            logger.error(f"Erro ao obter status do store {store_id}: {str(e)}")
            return {
                "store_id": store_id,
                "status": "error",
                "error": str(e)
            }
    
    async def propose(self, client_id: str, resource_data: Dict) -> Dict:
        """
        Inicia uma nova proposta para o recurso R.
        
        Args:
            client_id: ID do cliente que fez a solicitação
            resource_data: Dados do recurso a serem propostos
        
        Returns:
            Dict: Resultado da proposta
        """
        # Se não somos o líder, repassamos a proposta para o líder atual
        if not self.is_leader and self.current_leader:
            logger.info(f"Não somos o líder. Repassando proposta para {self.current_leader}")
            
            # Obter a URL do líder
            leader_url = None
            for prop_id, url in self.other_proposers.items():
                if prop_id == self.current_leader:
                    leader_url = url
                    break
            
            if leader_url:
                # Encaminhar a proposta para o líder
                forward_url = f"{leader_url}/propose"
                forward_data = {
                    "client_id": client_id,
                    "resource_data": resource_data
                }
                
                try:
                    return await http_request("POST", forward_url, data=forward_data)
                except Exception as e:
                    logger.error(f"Erro ao encaminhar proposta para o líder: {str(e)}")
                    return {
                        "success": False,
                        "error": f"Falha ao encaminhar para o líder: {str(e)}",
                        "leader": self.current_leader
                    }
            
            return {
                "success": False,
                "error": "Líder desconhecido",
                "leader": self.current_leader
            }
        
        # Gera um ID único para a proposta
        proposal_id = generate_id()
        timestamp = current_timestamp()
        
        logger.info(f"Iniciando nova proposta {proposal_id} do cliente {client_id}")
        
        # Executa a fase PREPARE
        prepare_success, new_tid = await self.prepare(
            proposal_id, client_id, resource_data, timestamp
        )
        
        if not prepare_success:
            logger.warning(f"Fase PREPARE falhou para proposta {proposal_id}")
            return {
                "success": False,
                "proposal_id": proposal_id,
                "error": "Fase PREPARE falhou"
            }
        
        # Executa a fase ACCEPT
        accept_success = await self.accept(proposal_id, new_tid)
        
        if not accept_success:
            logger.warning(f"Fase ACCEPT falhou para proposta {proposal_id}")
            return {
                "success": False,
                "proposal_id": proposal_id,
                "error": "Fase ACCEPT falhou"
            }
        
        # Se chegamos aqui, a proposta foi aceita
        logger.info(f"Proposta {proposal_id} aceita com sucesso")
        
        return {
            "success": True,
            "proposal_id": proposal_id,
            "tid": new_tid
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
                "state": proposal["state"],
                "client_id": proposal["client_id"],
                "tid": proposal["tid"],
                "timestamp": proposal["timestamp"],
                "promises_count": len(proposal.get("promises", {})),
                "rejections_count": len(proposal.get("rejections", {}))
            }
        else:
            return {
                "proposal_id": proposal_id,
                "error": "Proposta não encontrada"
            }
    
    def get_all_proposals(self) -> List[Dict]:
        """
        Obtém o status de todas as propostas.
        
        Returns:
            List[Dict]: Lista com o status de todas as propostas
        """
        result = []
        for proposal_id, proposal in self.proposals.items():
            result.append({
                "proposal_id": proposal_id,
                "state": proposal["state"],
                "client_id": proposal["client_id"],
                "tid": proposal["tid"],
                "timestamp": proposal["timestamp"],
                "promises_count": len(proposal.get("promises", {})),
                "rejections_count": len(proposal.get("rejections", {}))
            })
        return result
    
    def get_status(self) -> Dict:
        """
        Obtém o status completo do proposer.
        
        Returns:
            Dict: Status do proposer
        """
        return {
            "id": self.id,
            "is_leader": self.is_leader,
            "current_leader": self.current_leader,
            "tid_counter": self.tid_counter.get_current(),
            "acceptors_count": len(self.acceptors),
            "learners_count": len(self.learners),
            "stores_count": len(self.stores),
            "active_proposals": len([p for p in self.proposals.values() 
                                   if p["state"] not in (ProposerState.COMMITTED, ProposerState.REJECTED, ProposerState.FAILED)])
        }