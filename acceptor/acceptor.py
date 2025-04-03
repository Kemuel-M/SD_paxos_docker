"""
Implementação do componente Acceptor do protocolo Paxos.
"""
import time
import logging
from typing import Dict, Any, Tuple, Optional

from common.utils import HttpClient, get_current_timestamp
from common.metrics import acceptor_metrics
from common.models import (
    PrepareRequest,
    PromiseResponse,
    AcceptRequest,
    AcceptedResponse,
    LearnMessage
)
from acceptor import persistence
from acceptor.config import (
    NODE_ID,
    LEARNER_ENDPOINTS
)


class Acceptor:
    """
    Implementação do componente Acceptor do protocolo Paxos.
    """
    def __init__(self, logger: logging.Logger):
        """
        Inicializa o acceptor.
        
        Args:
            logger: Logger configurado.
        """
        self.logger = logger
        
        # Estado do acceptor
        self.promises = {}  # {instance_id: {highest_promised, last_promise_timestamp}}
        self.accepted = {}  # {instance_id: {accepted_proposal_number, accepted_value, accept_timestamp}}
        
        # Clientes HTTP para learners
        self.learner_clients = {
            endpoint: HttpClient(endpoint) for endpoint in LEARNER_ENDPOINTS
        }
    
    async def initialize(self):
        """
        Inicializa o acceptor carregando dados persistentes.
        """
        # Carregar estado persistente
        self.promises = await persistence.load_promises()
        self.accepted = await persistence.load_accepted()
        
        self.logger.info(f"Acceptor inicializado: promises={len(self.promises)}, accepted={len(self.accepted)}")
        
    async def shutdown(self):
        """
        Finaliza o acceptor, salvando dados persistentes.
        """
        # Salvar estado persistente
        await persistence.save_promises(self.promises)
        await persistence.save_accepted(self.accepted)
        
        # Fechar conexões HTTP
        for client in self.learner_clients.values():
            await client.close()
        
        self.logger.info("Acceptor finalizado")
        
    async def prepare(self, request: PrepareRequest) -> PromiseResponse:
        """
        Processa uma requisição prepare do protocolo Paxos.
        
        Args:
            request: Requisição prepare.
            
        Returns:
            Resposta promise.
        """
        # Incrementar contador de mensagens prepare recebidas
        acceptor_metrics["prepare_received"].labels(node_id=NODE_ID).inc()
        
        # Extrair dados da requisição
        proposal_number = request.proposalNumber
        instance_id = request.instanceId
        proposer_id = request.proposerId
        
        # Convertemos instance_id para string para usar como chave no dicionário
        instance_id_str = str(instance_id)
        
        # Obter a maior proposta prometida para esta instância
        instance_promises = self.promises.get(instance_id_str, {"highest_promised": -1, "last_promise_timestamp": 0})
        highest_promised = instance_promises.get("highest_promised", -1)
        
        # Verificar se podemos prometer aceitar esta proposta
        promised = highest_promised <= proposal_number
        
        # Construir resposta
        response = PromiseResponse(
            accepted=promised,
            proposalNumber=proposal_number,
            instanceId=instance_id,
            acceptorId=NODE_ID,
            highestAccepted=-1,
            acceptedValue=None
        )
        
        if promised:
            # Atualizar a maior proposta prometida
            self.promises[instance_id_str] = {
                "highest_promised": proposal_number,
                "last_promise_timestamp": get_current_timestamp()
            }
            
            # Persistir promessas
            await persistence.save_promises(self.promises)
            
            # Incrementar contador de promessas enviadas
            acceptor_metrics["promise_sent"].labels(node_id=NODE_ID).inc()
            
            # Se já aceitamos algum valor para esta instância, incluí-lo na resposta
            if instance_id_str in self.accepted:
                accepted_data = self.accepted[instance_id_str]
                response.highestAccepted = accepted_data.get("accepted_proposal_number", -1)
                response.acceptedValue = accepted_data.get("accepted_value")
                
            self.logger.info(f"Promise enviado para proposal_number={proposal_number}, instance_id={instance_id}")
        else:
            # Incrementar contador de not promises enviados
            acceptor_metrics["not_promise_sent"].labels(node_id=NODE_ID).inc()
            
            self.logger.info(f"Not promise enviado para proposal_number={proposal_number}, instance_id={instance_id}. "
                          f"Maior proposta prometida: {highest_promised}")
        
        return response
        
    async def accept(self, request: AcceptRequest) -> AcceptedResponse:
        """
        Processa uma requisição accept do protocolo Paxos.
        
        Args:
            request: Requisição accept.
            
        Returns:
            Resposta accepted.
        """
        # Incrementar contador de mensagens accept recebidas
        acceptor_metrics["accept_received"].labels(node_id=NODE_ID).inc()
        
        # Extrair dados da requisição
        proposal_number = request.proposalNumber
        instance_id = request.instanceId
        proposer_id = request.proposerId
        value = request.value
        
        # Convertemos instance_id para string para usar como chave no dicionário
        instance_id_str = str(instance_id)
        
        # Obter a maior proposta prometida para esta instância
        instance_promises = self.promises.get(instance_id_str, {"highest_promised": -1, "last_promise_timestamp": 0})
        highest_promised = instance_promises.get("highest_promised", -1)
        
        # Verificar se podemos aceitar esta proposta
        accepted = highest_promised <= proposal_number
        
        # Construir resposta
        response = AcceptedResponse(
            accepted=accepted,
            proposalNumber=proposal_number,
            instanceId=instance_id,
            acceptorId=NODE_ID
        )
        
        if accepted:
            # Atualizar o valor aceito
            self.accepted[instance_id_str] = {
                "accepted_proposal_number": proposal_number,
                "accepted_value": value,
                "accept_timestamp": get_current_timestamp()
            }
            
            # Persistir valores aceitos
            await persistence.save_accepted(self.accepted)
            
            # Incrementar contador de accepts enviados
            acceptor_metrics["accepted_sent"].labels(node_id=NODE_ID).inc()
            
            self.logger.info(f"Accept enviado para proposal_number={proposal_number}, instance_id={instance_id}")
            
            # Notificar learners sobre a aceitação
            learn_message = LearnMessage(
                proposalNumber=proposal_number,
                instanceId=instance_id,
                acceptorId=NODE_ID,
                accepted=True,
                value=value
            )
            
            await self._notify_learners(learn_message)
        else:
            # Incrementar contador de not accepts enviados
            acceptor_metrics["not_accepted_sent"].labels(node_id=NODE_ID).inc()
            
            self.logger.info(f"Not accept enviado para proposal_number={proposal_number}, instance_id={instance_id}. "
                          f"Maior proposta prometida: {highest_promised}")
            
        return response
        
    async def _notify_learners(self, learn_message: LearnMessage):
        """
        Notifica os learners sobre uma aceitação.
        
        Args:
            learn_message: Mensagem de aprendizado.
        """
        for endpoint, client in self.learner_clients.items():
            try:
                status, _ = await client.post("/learn", learn_message.dict())
                
                if status != 200:
                    self.logger.warning(f"Falha ao notificar learner {endpoint}: status {status}")
            except Exception as e:
                self.logger.warning(f"Erro ao notificar learner {endpoint}: {e}")
                
    def get_status(self) -> Dict[str, Any]:
        """
        Obtém o status atual do acceptor.
        
        Returns:
            Status do acceptor.
        """
        return {
            "node_id": NODE_ID,
            "role": "acceptor",
            "promises_count": len(self.promises),
            "accepted_count": len(self.accepted),
            "timestamp": get_current_timestamp()
        }