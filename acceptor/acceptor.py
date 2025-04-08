import asyncio
import logging
import time
from typing import Dict, List, Optional, Tuple, Union

from common.utils import http_request

logger = logging.getLogger(__name__)

class Acceptor:
    """
    Implementação do acceptor do algoritmo Paxos.
    Responsável por votar em propostas e manter o estado das aceitações.
    """
    
    def __init__(self, acceptor_id: str, debug: bool = False):
        """
        Inicializa o acceptor.
        
        Args:
            acceptor_id: ID único do acceptor
            debug: Flag para ativar modo de depuração
        """
        self.id = acceptor_id
        self.debug = debug
        
        # Estado do acceptor
        self.highest_promised_tid = 0  # Maior TID prometido
        self.accepted_tid = 0  # TID da última proposta aceita
        self.accepted_value = None  # Valor da última proposta aceita
        self.accepted_proposal_id = None  # ID da última proposta aceita
        
        # Histórico de propostas
        self.promises: Dict[str, Dict] = {}  # {proposal_id: {tid, timestamp, ...}}
        self.accepted: Dict[str, Dict] = {}  # {proposal_id: {tid, value, timestamp, ...}}
        
        # Lista de learners para notificação
        self.learners: Dict[str, str] = {}  # {learner_id: url}
        
        # Locks para operações concorrentes
        self.state_lock = asyncio.Lock()
        
        logger.debug(f"Acceptor {acceptor_id} inicializado (debug={debug})")
    
    def add_learner(self, learner_id: str, url: str) -> None:
        """
        Adiciona um learner à lista de learners conhecidos.
        
        Args:
            learner_id: ID do learner
            url: URL base do learner
        """
        self.learners[learner_id] = url
        logger.debug(f"Learner adicionado: {learner_id} -> {url}")
    
    async def prepare(self, proposal_id: str, proposer_id: str, tid: int) -> Dict:
        """
        Processa uma mensagem PREPARE de um proposer.
        
        Args:
            proposal_id: ID da proposta
            proposer_id: ID do proposer
            tid: TID (Transaction ID) da proposta
        
        Returns:
            Dict: Resposta à mensagem PREPARE (promise ou not_promise)
        """
        async with self.state_lock:
            # Se o TID da proposta for menor que o maior TID já prometido, rejeitamos
            if tid < self.highest_promised_tid:
                logger.info(f"Rejeitando PREPARE para proposta {proposal_id}: TID={tid} < maior TID prometido={self.highest_promised_tid}")
                return {
                    "success": False,
                    "tid": self.highest_promised_tid,
                    "message": "not_promise",
                    "reason": "tid_too_low"
                }
            
            # Atualiza o maior TID prometido
            self.highest_promised_tid = tid
            
            # Registra a promessa
            self.promises[proposal_id] = {
                "tid": tid,
                "proposer_id": proposer_id,
                "timestamp": time.time()
            }
            
            # Prepara a resposta com os dados da última proposta aceita (se houver)
            response = {
                "success": True,
                "tid": tid,
                "message": "promise"
            }
            
            if self.accepted_tid > 0:
                response["accepted_tid"] = self.accepted_tid
                response["accepted_value"] = self.accepted_value
                response["accepted_proposal_id"] = self.accepted_proposal_id
            
            logger.info(f"Enviando PROMISE para proposta {proposal_id}: TID={tid}, maior TID aceito={self.accepted_tid}")
            
            return response
    
    async def accept(self, proposal_id: str, proposer_id: str, tid: int, 
                   client_id: str, resource_data: Dict, timestamp: int) -> Dict:
        """
        Processa uma mensagem ACCEPT de um proposer.
        
        Args:
            proposal_id: ID da proposta
            proposer_id: ID do proposer
            tid: TID da proposta
            client_id: ID do cliente
            resource_data: Dados do recurso
            timestamp: Timestamp da proposta
        
        Returns:
            Dict: Resposta à mensagem ACCEPT (accepted ou not_accepted)
        """
        async with self.state_lock:
            # Se o TID da proposta for menor que o maior TID já prometido, rejeitamos
            if tid < self.highest_promised_tid:
                logger.info(f"Rejeitando ACCEPT para proposta {proposal_id}: TID={tid} < maior TID prometido={self.highest_promised_tid}")
                return {
                    "success": False,
                    "tid": self.highest_promised_tid,
                    "message": "not_accepted",
                    "reason": "tid_too_low"
                }
            
            # Aceita a proposta
            self.accepted_tid = tid
            self.accepted_value = resource_data
            self.accepted_proposal_id = proposal_id
            
            # Registra a aceitação
            self.accepted[proposal_id] = {
                "tid": tid,
                "proposer_id": proposer_id,
                "client_id": client_id,
                "resource_data": resource_data,
                "timestamp": timestamp,
                "accepted_at": time.time()
            }
            
            logger.info(f"Aceitando proposta {proposal_id}: TID={tid}")
            
            # Notifica os learners
            await self._notify_learners(proposal_id, tid, proposer_id, client_id, resource_data, timestamp)
            
            return {
                "success": True,
                "tid": tid,
                "message": "accepted"
            }
    
    async def _notify_learners(self, proposal_id: str, tid: int, proposer_id: str, 
                           client_id: str, resource_data: Dict, timestamp: int) -> None:
        """
        Notifica os learners sobre uma proposta aceita.
        
        Args:
            proposal_id: ID da proposta
            tid: TID da proposta
            proposer_id: ID do proposer
            client_id: ID do cliente
            resource_data: Dados do recurso
            timestamp: Timestamp da proposta
        """
        # Dados para os learners
        learn_data = {
            "proposal_id": proposal_id,
            "acceptor_id": self.id,
            "proposer_id": proposer_id,
            "tid": tid,
            "client_id": client_id,
            "resource_data": resource_data,
            "timestamp": timestamp
        }
        
        # Notifica todos os learners em paralelo
        notification_tasks = []
        
        for learner_id, url in self.learners.items():
            learn_url = f"{url}/learn"
            
            task = asyncio.create_task(
                self._send_notification(learner_id, learn_url, learn_data)
            )
            notification_tasks.append(task)
        
        # Aguarda todas as notificações serem enviadas
        results = await asyncio.gather(*notification_tasks, return_exceptions=True)
        
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
        
        logger.debug(f"Notificações enviadas para {len(notification_tasks)} learners: {success_count} sucesso, {error_count} erro")
    
    async def _send_notification(self, learner_id: str, url: str, data: Dict) -> Dict:
        """
        Envia uma notificação para um learner específico.
        
        Args:
            learner_id: ID do learner
            url: URL do endpoint learn do learner
            data: Dados da notificação
        
        Returns:
            Dict: Resposta do learner
        """
        try:
            response = await http_request("POST", url, data=data)
            if self.debug:
                logger.debug(f"Notificação enviada para learner {learner_id}: {response}")
            return response
        except Exception as e:
            logger.error(f"Erro ao notificar learner {learner_id}: {str(e)}")
            return {
                "success": False,
                "error": str(e)
            }
    
    def get_status(self) -> Dict:
        """
        Obtém o status atual do acceptor.
        
        Returns:
            Dict: Status do acceptor
        """
        return {
            "id": self.id,
            "highest_promised_tid": self.highest_promised_tid,
            "accepted_tid": self.accepted_tid,
            "accepted_proposal_id": self.accepted_proposal_id,
            "active_promises": len(self.promises),
            "total_accepted": len(self.accepted),
            "learners_count": len(self.learners)
        }
    
    def get_proposal_status(self, proposal_id: str) -> Dict:
        """
        Obtém o status de uma proposta específica.
        
        Args:
            proposal_id: ID da proposta
        
        Returns:
            Dict: Status da proposta
        """
        result = {
            "proposal_id": proposal_id
        }
        
        # Verifica se temos uma promessa para esta proposta
        if proposal_id in self.promises:
            result["promise"] = self.promises[proposal_id]
        
        # Verifica se temos uma aceitação para esta proposta
        if proposal_id in self.accepted:
            result["accepted"] = self.accepted[proposal_id]
        
        # Se não temos nenhuma informação sobre esta proposta
        if len(result) == 1:
            result["status"] = "unknown"
        elif "accepted" in result:
            result["status"] = "accepted"
        elif "promise" in result:
            result["status"] = "promised"
        
        return result