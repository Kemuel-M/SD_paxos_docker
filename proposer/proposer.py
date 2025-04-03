"""
Implementação do componente Proposer do protocolo Paxos.
"""
import asyncio
import time
import random
import logging
from typing import Dict, Any, List, Tuple, Optional, Set

from common.utils import HttpClient, get_random_wait_time, get_current_timestamp
from common.metrics import proposer_metrics, time_and_count
from common.models import (
    ClientRequest,
    PrepareRequest,
    PromiseResponse,
    AcceptRequest,
    AcceptedResponse,
    LearnMessage
)
from proposer import persistence
from proposer.config import (
    NODE_ID,
    ACCEPTOR_ENDPOINTS,
    LEARNER_ENDPOINTS,
    QUORUM_SIZE,
    RESOURCE_ACCESS_MIN_TIME,
    RESOURCE_ACCESS_MAX_TIME
)


class Proposer:
    """
    Implementação do componente Proposer do protocolo Paxos.
    """
    def __init__(self, logger: logging.Logger, leader_election):
        """
        Inicializa o proposer.
        
        Args:
            logger: Logger configurado.
            leader_election: Instância do LeaderElection.
        """
        self.logger = logger
        self.leader_election = leader_election
        
        # Contador de propostas (persistente)
        self.proposal_counter = 0
        
        # Contador de instâncias (persistente)
        self.last_instance_id = 0
        
        # Cache de propostas ativas
        self.active_proposals = {}
        
        # Clientes HTTP para acceptors e learners
        self.acceptor_clients = {
            endpoint: HttpClient(endpoint) for endpoint in ACCEPTOR_ENDPOINTS
        }
        self.learner_clients = {
            endpoint: HttpClient(endpoint) for endpoint in LEARNER_ENDPOINTS
        }
    
    async def initialize(self):
        """
        Inicializa o proposer carregando dados persistentes.
        """
        # Carregar contadores persistentes
        self.proposal_counter = await persistence.load_proposal_counter()
        self.last_instance_id = await persistence.load_instance_counter()
        
        self.logger.info(f"Proposer inicializado: proposal_counter={self.proposal_counter}, "
                      f"last_instance_id={self.last_instance_id}")
        
    async def shutdown(self):
        """
        Finaliza o proposer, salvando dados persistentes.
        """
        # Salvar contadores persistentes
        await persistence.save_proposal_counter(self.proposal_counter)
        await persistence.save_instance_counter(self.last_instance_id)
        
        # Fechar conexões HTTP
        for client in self.acceptor_clients.values():
            await client.close()
            
        for client in self.learner_clients.values():
            await client.close()
        
    async def generate_proposal_number(self) -> int:
        """
        Gera um número de proposta único.
        
        Returns:
            Número de proposta único.
        """
        self.proposal_counter += 1
        await persistence.save_proposal_counter(self.proposal_counter)
        
        # Formato: (contador << 8) | ID_Proposer
        # Isso garante que cada proposer tenha números únicos
        return (self.proposal_counter << 8) | NODE_ID
        
    async def get_last_instance_id(self) -> int:
        """
        Obtém o último ID de instância.
        
        Returns:
            Último ID de instância.
        """
        return self.last_instance_id
        
    async def set_last_instance_id(self, instance_id: int):
        """
        Define o último ID de instância.
        
        Args:
            instance_id: Novo valor para o último ID de instância.
        """
        if instance_id > self.last_instance_id:
            self.last_instance_id = instance_id
            await persistence.save_instance_counter(self.last_instance_id)
            
    async def propose(self, request: ClientRequest) -> Tuple[bool, int]:
        """
        Propõe uma requisição de cliente.
        
        Args:
            request: Requisição do cliente.
            
        Returns:
            Tupla (success, instance_id).
        """
        # Incrementar contador de instâncias
        self.last_instance_id += 1
        instance_id = self.last_instance_id
        await persistence.save_instance_counter(self.last_instance_id)
        
        # Incrementar contador de propostas
        proposer_metrics["proposals_total"].labels(node_id=NODE_ID).inc()
        
        # Verificar se sou o líder
        if not self.leader_election.is_leader():
            current_leader = self.leader_election.get_current_leader()
            self.logger.info(f"Encaminhando proposta para o líder (node_id={current_leader})")
            
            # Implementar encaminhamento para o líder
            # TODO: Implementar forwarding para o líder
            # Por enquanto, vamos apenas seguir com a proposta como se fossemos o líder
            
        # Preparar dados da proposta
        request_dict = request.dict()
        
        # Cache de proposta ativa
        self.active_proposals[instance_id] = {
            "request": request_dict,
            "proposal_number": None,
            "timestamp": get_current_timestamp(),
            "expiry": get_current_timestamp() + 60000  # 60 segundos de validade
        }
        
        # Executar o protocolo Paxos
        prepare_ok, highest_accepted_value = await self._prepare_phase(instance_id, request_dict)
        
        if not prepare_ok:
            self.logger.warning(f"Fase prepare falhou para instance_id={instance_id}")
            proposer_metrics["proposals_failure"].labels(node_id=NODE_ID).inc()
            del self.active_proposals[instance_id]
            return False, instance_id
            
        # Se algum valor já foi aceito em uma proposta anterior, usar esse valor
        value_to_propose = highest_accepted_value if highest_accepted_value else request_dict
        
        accept_ok = await self._accept_phase(instance_id, value_to_propose)
        
        if not accept_ok:
            self.logger.warning(f"Fase accept falhou para instance_id={instance_id}")
            proposer_metrics["proposals_failure"].labels(node_id=NODE_ID).inc()
            del self.active_proposals[instance_id]
            return False, instance_id
            
        # Após consenso, simular acesso ao recurso na Parte 1
        # Na Parte 2, a lógica será executada pelos learners
        await self._simulate_resource_access()
        
        self.logger.info(f"Proposta bem-sucedida para instance_id={instance_id}")
        proposer_metrics["proposals_success"].labels(node_id=NODE_ID).inc()
        
        # Limpar cache de proposta ativa
        del self.active_proposals[instance_id]
        
        return True, instance_id
        
    async def _prepare_phase(self, instance_id: int, request: Dict[str, Any]) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """
        Executa a fase prepare do Paxos.
        
        Args:
            instance_id: ID da instância.
            request: Requisição do cliente.
            
        Returns:
            Tupla (prepare_ok, highest_accepted_value).
        """
        prepare_start = time.time()
        
        # Gerar número de proposta único
        proposal_number = await self.generate_proposal_number()
        
        # Atualizar cache
        self.active_proposals[instance_id]["proposal_number"] = proposal_number
        
        # Criar requisição prepare
        prepare_request = {
            "type": "PREPARE",
            "proposalNumber": proposal_number,
            "instanceId": instance_id,
            "proposerId": NODE_ID,
            "clientRequest": request
        }
        
        # Enviar prepare para todos os acceptors
        promises_received = 0
        highest_accepted_proposal = -1
        highest_accepted_value = None
        
        for endpoint, client in self.acceptor_clients.items():
            try:
                status, response = await client.post("/prepare", prepare_request)
                
                if status == 200 and response.get("accepted", False):
                    promises_received += 1
                    
                    # Verificar se este acceptor já aceitou alguma proposta
                    acceptor_highest = response.get("highestAccepted", -1)
                    if acceptor_highest > highest_accepted_proposal and response.get("acceptedValue"):
                        highest_accepted_proposal = acceptor_highest
                        highest_accepted_value = response.get("acceptedValue")
                        
            except Exception as e:
                self.logger.warning(f"Erro ao enviar prepare para {endpoint}: {e}")
                
        # Calcular duração da fase prepare
        prepare_duration = time.time() - prepare_start
        proposer_metrics["prepare_phase_duration"].labels(node_id=NODE_ID).observe(prepare_duration)
        
        # Verificar se recebemos promises da maioria
        prepare_ok = promises_received >= QUORUM_SIZE
        
        if prepare_ok:
            self.logger.info(f"Fase prepare bem-sucedida para instance_id={instance_id}: {promises_received} promises")
        else:
            self.logger.info(f"Fase prepare falhou para instance_id={instance_id}: apenas {promises_received} promises")
            
        return prepare_ok, highest_accepted_value
        
    async def _accept_phase(self, instance_id: int, value: Dict[str, Any]) -> bool:
        """
        Executa a fase accept do Paxos.
        
        Args:
            instance_id: ID da instância.
            value: Valor a ser proposto.
            
        Returns:
            True se a fase accept foi bem-sucedida, False caso contrário.
        """
        accept_start = time.time()
        
        # Obter número de proposta do cache
        proposal_number = self.active_proposals[instance_id]["proposal_number"]
        
        # Criar requisição accept
        accept_request = {
            "type": "ACCEPT",
            "proposalNumber": proposal_number,
            "instanceId": instance_id,
            "proposerId": NODE_ID,
            "value": value
        }
        
        # Enviar accept para todos os acceptors
        accepts_received = 0
        
        for endpoint, client in self.acceptor_clients.items():
            try:
                status, response = await client.post("/accept", accept_request)
                
                if status == 200 and response.get("accepted", False):
                    accepts_received += 1
                    
            except Exception as e:
                self.logger.warning(f"Erro ao enviar accept para {endpoint}: {e}")
                
        # Calcular duração da fase accept
        accept_duration = time.time() - accept_start
        proposer_metrics["accept_phase_duration"].labels(node_id=NODE_ID).observe(accept_duration)
        
        # Verificar se recebemos accepts da maioria
        accept_ok = accepts_received >= QUORUM_SIZE
        
        if accept_ok:
            self.logger.info(f"Fase accept bem-sucedida para instance_id={instance_id}: {accepts_received} accepts")
        else:
            self.logger.info(f"Fase accept falhou para instance_id={instance_id}: apenas {accepts_received} accepts")
            
        return accept_ok
        
    async def _simulate_resource_access(self):
        """
        Simula o acesso ao recurso R esperando entre 0.2 e 1.0 segundos.
        Isso será substituído pelo acesso real ao recurso na Parte 2.
        """
        wait_time = get_random_wait_time(RESOURCE_ACCESS_MIN_TIME, RESOURCE_ACCESS_MAX_TIME)
        self.logger.info(f"Simulando acesso ao recurso por {wait_time} segundos")
        await asyncio.sleep(wait_time)
        
    def cleanup_expired_proposals(self):
        """
        Remove propostas expiradas do cache.
        """
        current_time = get_current_timestamp()
        expired_keys = [
            k for k, v in self.active_proposals.items() 
            if v["expiry"] < current_time
        ]
        
        for key in expired_keys:
            self.logger.info(f"Removendo proposta expirada: instance_id={key}")
            del self.active_proposals[key]
            
    def get_status(self) -> Dict[str, Any]:
        """
        Obtém o status atual do proposer.
        
        Returns:
            Status do proposer.
        """
        # Limpar propostas expiradas antes de retornar status
        self.cleanup_expired_proposals()
        
        return {
            "node_id": NODE_ID,
            "role": "proposer",
            "is_leader": self.leader_election.is_leader(),
            "current_leader": self.leader_election.get_current_leader(),
            "last_instance_id": self.last_instance_id,
            "proposal_counter": self.proposal_counter,
            "active_proposals_count": len(self.active_proposals),
            "timestamp": get_current_timestamp()
        }