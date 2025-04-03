"""
Lógica de eleição de líder para o componente Proposer.
"""
import asyncio
import time
import logging
from typing import Dict, Any, List, Optional, Tuple

from common.utils import HttpClient
from common.metrics import proposer_metrics
from proposer import persistence
from proposer.config import (
    NODE_ID,
    PROPOSER_ENDPOINTS,
    ACCEPTOR_ENDPOINTS,
    LEADER_HEARTBEAT_INTERVAL,
    LEADER_TIMEOUT,
    QUORUM_SIZE
)


class LeaderElection:
    """
    Implementação da eleição de líder usando o próprio Paxos.
    """
    def __init__(self, logger: logging.Logger, proposer_instance):
        """
        Inicializa a eleição de líder.
        
        Args:
            logger: Logger configurado.
            proposer_instance: Instância do proposer.
        """
        self.logger = logger
        self.proposer = proposer_instance
        self.state = {
            "is_leader": False,
            "current_leader_id": None,
            "term": 0,
            "last_heartbeat": 0
        }
        self.clients = {
            endpoint: HttpClient(endpoint) for endpoint in PROPOSER_ENDPOINTS
        }
        self.acceptor_clients = {
            endpoint: HttpClient(endpoint) for endpoint in ACCEPTOR_ENDPOINTS
        }
        self.running = False
        self.leader_task = None
        
    async def initialize(self):
        """
        Inicializa o estado de liderança a partir do disco.
        """
        self.state = await persistence.load_leadership_state()
        self.running = True
        
    async def start_background_tasks(self):
        """
        Inicia as tarefas de background para monitoramento e heartbeat.
        """
        if self.leader_task:
            self.leader_task.cancel()
            
        self.leader_task = asyncio.create_task(self.leader_monitor_task())
        
    async def shutdown(self):
        """
        Finaliza as tarefas de background e fecha as conexões.
        """
        self.running = False
        
        if self.leader_task:
            self.leader_task.cancel()
            try:
                await self.leader_task
            except asyncio.CancelledError:
                pass
                
        for client in self.clients.values():
            await client.close()
            
        for client in self.acceptor_clients.values():
            await client.close()
            
        await persistence.save_leadership_state(self.state)
        
    async def leader_monitor_task(self):
        """
        Tarefa de background para monitoramento do líder e heartbeat.
        """
        try:
            while self.running:
                # Se eu sou o líder, enviar heartbeats
                if self.state["is_leader"]:
                    await self._send_leader_heartbeats()
                    await asyncio.sleep(LEADER_HEARTBEAT_INTERVAL)
                else:
                    # Se não sou o líder, verificar se o líder atual está respondendo
                    leader_alive = await self._check_leader_alive()
                    
                    if not leader_alive:
                        # Iniciar eleição se o líder atual não estiver respondendo
                        await self._start_election()
                        
                    await asyncio.sleep(LEADER_TIMEOUT)
        except asyncio.CancelledError:
            # Tarefa cancelada, finalizar graciosamente
            self.logger.info("Tarefa de monitoramento de líder cancelada")
        except Exception as e:
            self.logger.error(f"Erro na tarefa de monitoramento de líder: {e}")
            # Reiniciar a tarefa em caso de erro
            if self.running:
                self.leader_task = asyncio.create_task(self.leader_monitor_task())
                
    async def _send_leader_heartbeats(self):
        """
        Envia heartbeats para todos os outros proposers.
        """
        current_time = int(time.time() * 1000)
        leader_status = {
            "leaderId": NODE_ID,
            "term": self.state["term"],
            "lastInstanceId": await self.proposer.get_last_instance_id()
        }
        
        for endpoint, client in self.clients.items():
            try:
                status, _ = await client.post("/leader-status", leader_status)
                if status != 200:
                    self.logger.warning(f"Falha ao enviar heartbeat para {endpoint}: status {status}")
            except Exception as e:
                self.logger.warning(f"Erro ao enviar heartbeat para {endpoint}: {e}")
                
    async def _check_leader_alive(self) -> bool:
        """
        Verifica se o líder atual está respondendo.
        
        Returns:
            True se o líder estiver respondendo, False caso contrário.
        """
        if self.state["current_leader_id"] is None:
            return False
            
        current_time = int(time.time() * 1000)
        
        # Verificar se recebemos heartbeat recentemente
        if current_time - self.state["last_heartbeat"] > LEADER_TIMEOUT * 1000:
            self.logger.info(f"Líder {self.state['current_leader_id']} considerado inativo: "
                           f"último heartbeat há {(current_time - self.state['last_heartbeat'])/1000} segundos")
            return False
            
        return True
        
    async def _start_election(self):
        """
        Inicia uma eleição de líder usando o próprio Paxos.
        """
        self.logger.info("Iniciando eleição de líder")
        
        # Incrementar o termo
        self.state["term"] += 1
        current_time = int(time.time() * 1000)
        
        # Propor a si mesmo como líder
        election_value = {
            "proposerId": NODE_ID,
            "timestamp": current_time,
            "term": self.state["term"]
        }
        
        # Usar a instância 0 do Paxos para eleição de líder
        instance_id = 0
        
        # Fases do Paxos para eleição de líder
        prepare_ok, highest_accepted_value = await self._paxos_prepare_phase(instance_id)
        
        if not prepare_ok:
            self.logger.info("Fase prepare da eleição falhou")
            return False
            
        # Se algum valor já foi aceito com um proposerId maior, usar esse valor
        if highest_accepted_value and highest_accepted_value.get("proposerId", 0) > NODE_ID:
            election_value = highest_accepted_value
            
        accept_ok = await self._paxos_accept_phase(instance_id, election_value)
        
        if not accept_ok:
            self.logger.info("Fase accept da eleição falhou")
            return False
            
        # Verificar se ganhei a eleição
        if election_value["proposerId"] == NODE_ID:
            old_leader = self.state["current_leader_id"]
            self.state["is_leader"] = True
            self.state["current_leader_id"] = NODE_ID
            self.state["last_heartbeat"] = current_time
            
            if old_leader != NODE_ID:
                self.logger.info(f"Eleito como novo líder com termo {self.state['term']}")
                proposer_metrics["leadership_changes"].labels(node_id=NODE_ID).inc()
                
            await persistence.save_leadership_state(self.state)
            return True
        else:
            # Outro proposer foi eleito
            self.state["is_leader"] = False
            self.state["current_leader_id"] = election_value["proposerId"]
            self.state["last_heartbeat"] = current_time
            
            self.logger.info(f"Proposer {election_value['proposerId']} eleito como líder com termo {self.state['term']}")
            
            await persistence.save_leadership_state(self.state)
            return False
            
    async def _paxos_prepare_phase(self, instance_id: int) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """
        Executa a fase prepare do Paxos para eleição de líder.
        
        Args:
            instance_id: ID da instância Paxos (0 para eleição).
            
        Returns:
            Tupla (prepare_ok, highest_accepted_value).
        """
        prepare_start = time.time()
        
        # Gerar número de proposta único
        proposal_number = await self.proposer.generate_proposal_number()
        
        # Criar requisição prepare
        prepare_request = {
            "type": "PREPARE",
            "proposalNumber": proposal_number,
            "instanceId": instance_id,
            "proposerId": NODE_ID,
            "clientRequest": {
                "type": "LEADER_ELECTION",
                "term": self.state["term"]
            }
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
            self.logger.info(f"Fase prepare da eleição bem-sucedida: {promises_received} promises")
        else:
            self.logger.info(f"Fase prepare da eleição falhou: apenas {promises_received} promises")
            
        return prepare_ok, highest_accepted_value
        
    async def _paxos_accept_phase(self, instance_id: int, election_value: Dict[str, Any]) -> bool:
        """
        Executa a fase accept do Paxos para eleição de líder.
        
        Args:
            instance_id: ID da instância Paxos (0 para eleição).
            election_value: Valor da eleição a ser proposto.
            
        Returns:
            True se a fase accept foi bem-sucedida, False caso contrário.
        """
        accept_start = time.time()
        
        # Gerar número de proposta único
        proposal_number = await self.proposer.generate_proposal_number()
        
        # Criar requisição accept
        accept_request = {
            "type": "ACCEPT",
            "proposalNumber": proposal_number,
            "instanceId": instance_id,
            "proposerId": NODE_ID,
            "value": election_value
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
            self.logger.info(f"Fase accept da eleição bem-sucedida: {accepts_received} accepts")
        else:
            self.logger.info(f"Fase accept da eleição falhou: apenas {accepts_received} accepts")
            
        return accept_ok
        
    async def receive_leader_heartbeat(self, leader_id: int, term: int, last_instance_id: int):
        """
        Recebe um heartbeat do líder atual.
        
        Args:
            leader_id: ID do líder.
            term: Termo do líder.
            last_instance_id: Último ID de instância do líder.
        """
        current_time = int(time.time() * 1000)
        
        # Verificar se o termo é maior que o atual
        if term > self.state["term"]:
            # Atualizar termo e reconhecer o novo líder
            old_leader = self.state["current_leader_id"]
            self.state["term"] = term
            self.state["is_leader"] = False
            self.state["current_leader_id"] = leader_id
            self.state["last_heartbeat"] = current_time
            
            if old_leader != leader_id:
                self.logger.info(f"Novo líder {leader_id} reconhecido com termo {term}")
                
            await persistence.save_leadership_state(self.state)
        elif term == self.state["term"]:
            # Mesmo termo, atualizar heartbeat
            if self.state["current_leader_id"] != leader_id:
                self.logger.warning(f"Conflito de líderes detectado: {self.state['current_leader_id']} vs {leader_id}")
                
            self.state["current_leader_id"] = leader_id
            self.state["last_heartbeat"] = current_time
            
        # Atualizar o último ID de instância conhecido do líder
        if last_instance_id > await self.proposer.get_last_instance_id():
            await self.proposer.set_last_instance_id(last_instance_id)
            
    def is_leader(self) -> bool:
        """
        Verifica se este proposer é o líder atual.
        
        Returns:
            True se este proposer é o líder, False caso contrário.
        """
        return self.state["is_leader"]
        
    def get_current_leader(self) -> Optional[int]:
        """
        Obtém o ID do líder atual.
        
        Returns:
            ID do líder atual ou None se não houver líder.
        """
        return self.state["current_leader_id"]