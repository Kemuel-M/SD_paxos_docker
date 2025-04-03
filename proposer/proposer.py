"""
Implementação principal do Proposer do algoritmo Paxos.
Responsável por iniciar o processo de consenso e coordenar as fases do protocolo.
"""
import os
import time
import logging
import asyncio
import random
from typing import Dict, List, Any, Optional, Tuple
from collections import defaultdict

from common.communication import HttpClient, CircuitBreaker
from common.utils import calculate_backoff, wait_with_backoff, current_timestamp

# Configuração
DEBUG = os.getenv("DEBUG", "false").lower() in ("true", "1", "yes")
USE_CLUSTER_STORE = os.getenv("USE_CLUSTER_STORE", "false").lower() in ("true", "1", "yes")

logger = logging.getLogger("proposer")

class Proposer:
    """
    Implementação do Proposer do algoritmo Paxos.
    
    O Proposer é responsável por:
    1. Receber requisições de clientes
    2. Iniciar o processo de consenso (fases Prepare e Accept)
    3. Coordenar com Acceptors para chegar a um consenso
    4. Gerenciar o estado das propostas em andamento
    """
    
    def __init__(self, node_id: int, acceptors: List[str], learners: List[str], 
                 stores: List[str], proposal_counter: int = 0, last_instance_id: int = 0):
        """
        Inicializa o Proposer.
        
        Args:
            node_id: ID único deste Proposer (1-5)
            acceptors: Lista de endereços dos Acceptors
            learners: Lista de endereços dos Learners
            stores: Lista de endereços dos Cluster Stores
            proposal_counter: Contador inicial de propostas
            last_instance_id: Último ID de instância processado
        """
        self.node_id = node_id
        self.acceptors = acceptors
        self.learners = learners
        self.stores = stores
        self.proposal_counter = proposal_counter
        self.last_instance_id = last_instance_id

        self.instance_id_lock = asyncio.Lock()
        self.proposal_counter_lock = asyncio.Lock()
        
        # Estado das propostas (instanceId -> estado)
        self.proposals = {}
        
        # Fila de propostas pendentes
        self.pending_queue = asyncio.Queue()
        
        # Executor de propostas (background task)
        self.executor_task = None
        
        # Cliente HTTP com circuit breaker
        self.http_client = HttpClient()
        
        # Circuit breakers para cada endpoint (endereço -> circuitbreaker)
        self.circuit_breakers = defaultdict(lambda: CircuitBreaker())
        
        # Estado atual do proposer
        self.state = "initialized"
        
        # Indica se é o líder (será gerenciado pelo LeaderElection)
        self.is_leader = False
        
        # Counter para round-robin dos nós do Cluster Store
        self.store_index = 0
        
        logger.info(f"Proposer {node_id} inicializado com {len(acceptors)} acceptors, "
                    f"{len(learners)} learners e {len(stores)} nós de armazenamento")
    
    async def start(self):
        """Inicia o processador de propostas em background."""
        self.state = "running"
        self.executor_task = asyncio.create_task(self._proposal_executor())
        logger.info(f"Processador de propostas iniciado")
    
    async def stop(self):
        """Para o processador de propostas."""
        if self.executor_task and not self.executor_task.done():
            self.state = "stopping"
            self.executor_task.cancel()
            try:
                await self.executor_task
            except asyncio.CancelledError:
                pass
        self.state = "stopped"
        logger.info(f"Processador de propostas parado")
    
    def set_leader(self, is_leader: bool):
        """Define se este proposer é o líder."""
        self.is_leader = is_leader
        logger.info(f"Status de líder alterado para: {is_leader}")
    
    async def propose(self, client_request: Dict[str, Any]):
        """
        Recebe uma requisição de cliente e a adiciona à fila de processamento.
        
        Args:
            client_request: Requisição do cliente
        """
        # Gera um ID de instância único para esta proposta utilizando lock
        async with self.instance_id_lock:
            instance_id = self.last_instance_id + 1
            self.last_instance_id = instance_id
        
        # Adiciona metadados à proposta
        proposal = {
            "instanceId": instance_id,
            "clientRequest": client_request,
            "status": "pending",
            "created_at": time.time(),
            "attempt": 0
        }
        
        # Armazena a proposta no cache
        self.proposals[instance_id] = proposal
        
        # Adiciona à fila de processamento
        await self.pending_queue.put(instance_id)
        
        logger.info(f"Proposta do cliente {client_request['clientId']} adicionada à fila "
                    f"com instanceId {instance_id}")
    
    async def _proposal_executor(self):
        """Loop de processamento de propostas pendentes."""
        logger.info("Iniciando executor de propostas")
        
        while True:
            try:
                # Obtém próxima proposta da fila
                instance_id = await self.pending_queue.get()
                proposal = self.proposals.get(instance_id)
                
                if not proposal:
                    logger.warning(f"Proposta com instanceId {instance_id} não encontrada")
                    self.pending_queue.task_done()
                    continue
                
                logger.info(f"Processando proposta {instance_id}")
                proposal["status"] = "processing"
                
                # Se somos o líder, podemos pular a fase Prepare (otimização do Multi-Paxos)
                if self.is_leader and instance_id > 1:  # Pula fase Prepare para instâncias após a primeira
                    logger.info(f"Líder otimizando protocolo para instanceId {instance_id}, pulando fase Prepare")
                    accepted = await self._run_accept_phase(instance_id, proposal)
                else:
                    # Executar protocolo Paxos completo
                    accepted = await self._run_paxos(instance_id, proposal)
                
                if accepted:
                    # Na Parte 1, simula acesso ao recurso
                    if not USE_CLUSTER_STORE:
                        await self._simulate_resource_access()
                        # Notifica cliente via learner
                        await self._notify_client_via_learner(instance_id, proposal, "COMMITTED")
                        proposal["status"] = "completed"
                        logger.info(f"Proposta {instance_id} concluída com sucesso (simulação)")
                    else:
                        # Na Parte 2, envia ao Cluster Store via learner
                        success = await self._access_cluster_store(instance_id, proposal)
                        
                        if success:
                            proposal["status"] = "completed"
                            logger.info(f"Proposta {instance_id} concluída com sucesso (acesso real)")
                        else:
                            proposal["status"] = "failed"
                            logger.warning(f"Proposta {instance_id} falhou no acesso ao Cluster Store")
                else:
                    proposal["status"] = "failed"
                    logger.warning(f"Proposta {instance_id} falhou no consenso Paxos")
                    
                    # Se ainda não excedeu o limite de tentativas, coloca de volta na fila com atraso
                    if proposal["attempt"] < 3:
                        proposal["attempt"] += 1
                        logger.info(f"Programando nova tentativa para proposta {instance_id} (tentativa {proposal['attempt']}/3)")
                        # Usa backoff exponencial com jitter entre tentativas
                        delay = (2 ** proposal["attempt"]) * (0.8 + 0.4 * random.random())  # Base * (0.8-1.2 jitter)
                        logger.info(f"Aguardando {delay:.2f}s antes da nova tentativa")
                        await asyncio.sleep(delay)
                        # Atualiza status e coloca de volta na fila
                        proposal["status"] = "pending"
                        await self.pending_queue.put(instance_id)
                    else:
                        # Notifica cliente sobre falha definitiva
                        logger.error(f"Proposta {instance_id} falhou após todas as tentativas")
                        await self._notify_client_via_learner(instance_id, proposal, "NOT_COMMITTED")
                
                # Marca como processada
                self.pending_queue.task_done()
                
                # Limpa propostas antigas (mais de 1 hora)
                self._cleanup_old_proposals()
                
            except asyncio.CancelledError:
                logger.info("Executor de propostas cancelado")
                break
            except Exception as e:
                logger.error(f"Erro no executor de propostas: {e}", exc_info=True)
                await asyncio.sleep(1)  # Evita consumo excessivo de CPU em caso de erros
    
    async def _run_paxos(self, instance_id: int, proposal: Dict[str, Any]) -> bool:
        """
        Executa o protocolo Paxos completo (fases Prepare e Accept).
        
        Args:
            instance_id: ID da instância Paxos
            proposal: Dados da proposta
        
        Returns:
            bool: True se o consenso foi alcançado, False caso contrário
        """
        # Tenta até 3 vezes com números de proposta diferentes
        for attempt in range(3):
            try:
                async with self.proposal_counter_lock:
                    # Incrementa o contador de propostas
                    self.proposal_counter += 1
                    # Gera número de proposta único (contador << 8 | ID)
                    proposal_number = (self.proposal_counter << 8) | self.node_id
                
                logger.info(f"Tentativa {attempt+1} para instância {instance_id} com proposal_number {proposal_number}")
                
                # Fase Prepare
                prepare_result = await self._run_prepare_phase(instance_id, proposal_number, proposal)
                
                if not prepare_result["success"]:
                    logger.warning(f"Fase Prepare falhou para instância {instance_id}, tentativa {attempt+1}")
                    await asyncio.sleep(0.5 * (attempt + 1))  # Backoff exponencial simples
                    continue
                
                # Fase Accept (usando valor aceito anteriormente se necessário)
                highest_value = prepare_result.get("highest_value")
                success = await self._run_accept_phase(instance_id, proposal, 
                                                      proposal_number=proposal_number,
                                                      value_override=highest_value)
                
                if success:
                    return True
                
                logger.warning(f"Fase Accept falhou para instância {instance_id}, tentativa {attempt+1}")
                await asyncio.sleep(0.5 * (attempt + 1))
                
            except Exception as e:
                logger.error(f"Erro durante execução do Paxos para instância {instance_id}: {e}", exc_info=True)
                await asyncio.sleep(0.5 * (attempt + 1))
        
        # Após 3 tentativas sem sucesso
        proposal["attempt"] += 1
        
        # Se ainda não excedeu o limite de tentativas, coloca de volta na fila
        if proposal["attempt"] < 3:
            logger.info(f"Programando nova tentativa para proposta {instance_id} (tentativa {proposal['attempt']+1}/3)")
            await asyncio.sleep(2 ** proposal["attempt"])  # Backoff exponencial entre tentativas
            await self.pending_queue.put(instance_id)
            return False
        
        logger.error(f"Proposta {instance_id} falhou após todas as tentativas")
        return False
    
    async def _run_prepare_phase(self, instance_id: int, proposal_number: int, 
                                proposal: Dict[str, Any]) -> Dict[str, Any]:
        """
        Executa a fase Prepare do Paxos.
        
        Args:
            instance_id: ID da instância
            proposal_number: Número da proposta
            proposal: Dados da proposta
        
        Returns:
            Dict[str, Any]: Resultado da fase Prepare
            {
                "success": bool,  # True se recebeu maioria de promises
                "highest_value": Optional[Dict],  # Valor com maior número de proposta (se houver)
                "promises": int,  # Número de promises recebidas
                "highest_proposal": int  # Maior número de proposta recebido
            }
        """
        logger.info(f"Iniciando fase Prepare para instância {instance_id} com número {proposal_number}")
        
        # Prepara mensagem para enviar aos acceptors
        prepare_message = {
            "type": "PREPARE",
            "proposalNumber": proposal_number,
            "instanceId": instance_id,
            "proposerId": self.node_id,
            "clientRequest": proposal["clientRequest"]
        }
        
        if DEBUG:
            logger.debug(f"Prepare message: {prepare_message}")
        
        # Envia para todos os acceptors em paralelo
        promises = []
        highest_accepted_proposal = -1
        highest_accepted_value = None
        
        # Coleta as respostas
        responses = await self._send_to_acceptors("/prepare", prepare_message)
        
        # Analisa as respostas
        for response in responses:
            if response and response.get("accepted") == True:
                promises.append(response)
                
                # Verifica se tem valor aceito anteriormente
                accepted_proposal = response.get("highestAccepted", -1)
                if accepted_proposal > highest_accepted_proposal and "acceptedValue" in response:
                    highest_accepted_proposal = accepted_proposal
                    highest_accepted_value = response["acceptedValue"]
        
        # Verifica se obteve maioria (3 de 5)
        success = len(promises) >= 3
        
        if success:
            logger.info(f"Fase Prepare bem-sucedida: {len(promises)}/{len(self.acceptors)} promises recebidas")
        else:
            logger.warning(f"Fase Prepare falhou: apenas {len(promises)}/{len(self.acceptors)} promises recebidas")
        
        return {
            "success": success,
            "highest_value": highest_accepted_value,
            "promises": len(promises),
            "highest_proposal": highest_accepted_proposal
        }
    
    async def _run_accept_phase(self, instance_id: int, proposal: Dict[str, Any], 
                               proposal_number: Optional[int] = None,
                               value_override: Optional[Dict[str, Any]] = None) -> bool:
        """
        Executa a fase Accept do Paxos.
        
        Args:
            instance_id: ID da instância
            proposal: Dados da proposta
            proposal_number: Número da proposta (se None, gera um novo)
            value_override: Valor a usar em vez do original (para valores previamente aceitos)
        
        Returns:
            bool: True se maioria aceitou, False caso contrário
        """
        # Se não foi fornecido proposal_number, gera um novo
        if proposal_number is None:
            self.proposal_counter += 1
            proposal_number = (self.proposal_counter << 8) | self.node_id
        
        # Determina o valor a propor
        value = value_override if value_override is not None else proposal["clientRequest"]
        
        logger.info(f"Iniciando fase Accept para instância {instance_id} com número {proposal_number}")
        
        # Prepara mensagem para enviar aos acceptors
        accept_message = {
            "type": "ACCEPT",
            "proposalNumber": proposal_number,
            "instanceId": instance_id,
            "proposerId": self.node_id,
            "value": value
        }
        
        if DEBUG:
            logger.debug(f"Accept message: {accept_message}")
        
        # Envia para todos os acceptors em paralelo
        responses = await self._send_to_acceptors("/accept", accept_message)
        
        # Conta quantos aceitaram
        accepted_count = sum(1 for resp in responses if resp and resp.get("accepted") == True)
        
        # Verifica se obteve maioria (3 de 5)
        success = accepted_count >= 3
        
        if success:
            logger.info(f"Fase Accept bem-sucedida: {accepted_count}/{len(self.acceptors)} aceitaram")
        else:
            logger.warning(f"Fase Accept falhou: apenas {accepted_count}/{len(self.acceptors)} aceitaram")
        
        return success
    
    async def _send_to_acceptors(self, endpoint: str, payload: Dict[str, Any]) -> List[Optional[Dict[str, Any]]]:
        """
        Envia requisições para todos os acceptors em paralelo.
        
        Args:
            endpoint: Endpoint a ser chamado (ex: "/prepare", "/accept")
            payload: Dados a serem enviados
        
        Returns:
            List[Optional[Dict[str, Any]]]: Lista de respostas (None para falhas)
        """
        # Cria tasks para todas as requisições em paralelo
        tasks = []
        for acceptor in self.acceptors:
            url = f"http://{acceptor}{endpoint}"
            cb = self.circuit_breakers[acceptor]
            
            # Adiciona a task com circuit breaker e retentativas
            tasks.append(self._send_with_retry(url, payload, circuit_breaker=cb))
        
        # Executa todas as requisições em paralelo
        return await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _send_with_retry(self, url: str, payload: Dict[str, Any], 
                             circuit_breaker: CircuitBreaker,
                             max_retries: int = 3) -> Optional[Dict[str, Any]]:
        """
        Envia requisição HTTP com retentativas e circuit breaker.
        
        Args:
            url: URL de destino
            payload: Dados a serem enviados
            circuit_breaker: CircuitBreaker para o destino
            max_retries: Número máximo de tentativas
        
        Returns:
            Optional[Dict[str, Any]]: Resposta ou None em caso de falha
        """
        # Verifica se o circuit breaker está aberto
        if not circuit_breaker.allow_request():
            logger.warning(f"Circuit breaker aberto para {url}, ignorando requisição")
            return None
        
        # Tenta enviar a requisição com retentativas
        for attempt in range(max_retries):
            try:
                # Adiciona jitter ao timeout (480-520ms)
                timeout = 0.5 + (random.random() * 0.04 - 0.02)
                
                # Faz a requisição com timeout
                start_time = time.time()
                response = await self.http_client.post(url, json=payload, timeout=timeout)
                elapsed = time.time() - start_time
                
                # Registra sucesso no circuit breaker
                await circuit_breaker.record_success()
                
                if DEBUG:
                    logger.debug(f"Requisição para {url} concluída em {elapsed:.3f}s")
                
                return response
                
            except Exception as e:
                # Registra falha no circuit breaker
                await circuit_breaker.record_failure()
                
                logger.warning(f"Falha ao chamar {url}, tentativa {attempt+1}/{max_retries}: {e}")
                
                # Calcula tempo de espera com backoff exponencial e jitter
                backoff_time = calculate_backoff(attempt)
                
                if attempt < max_retries - 1:
                    logger.info(f"Aguardando {backoff_time:.2f}s antes da próxima tentativa")
                    await asyncio.sleep(backoff_time)
        
        return None
    
    async def _simulate_resource_access(self):
        """
        Simula o acesso ao recurso (apenas na Parte 1).
        Espera entre 0.2 e 1.0 segundos.
        """
        delay = 0.2 + random.random() * 0.8
        logger.info(f"Simulando acesso ao recurso por {delay:.3f} segundos")
        await asyncio.sleep(delay)
    
    async def _access_cluster_store(self, instance_id: int, proposal: Dict[str, Any]) -> bool:
        """
        Acessa o Cluster Store para escrita real (Parte 2).
        
        Args:
            instance_id: ID da instância
            proposal: Dados da proposta
        
        Returns:
            bool: True se acesso bem-sucedido, False caso contrário
        """
        # Seleciona um nó do Cluster Store via round-robin
        if not self.stores:
            logger.error("Nenhum nó do Cluster Store configurado")
            return False
        
        # Seleciona próximo nó via round-robin
        self.store_index = (self.store_index + 1) % len(self.stores)
        selected_store = self.stores[self.store_index]
        
        logger.info(f"Acessando Cluster Store {selected_store} para instância {instance_id}")
        
        try:
            # Em vez de acessar diretamente, delegamos ao Learner
            # que implementa o protocolo ROWA para replicação
            await self._notify_learner_for_cluster_access(instance_id, proposal, selected_store)
            return True
        except Exception as e:
            logger.error(f"Erro ao acessar Cluster Store: {e}", exc_info=True)
            return False
    
    async def _notify_client_via_learner(self, instance_id: int, proposal: Dict[str, Any], status: str):
        """
        Notifica o cliente via Learner sobre o resultado da operação.
        
        Args:
            instance_id: ID da instância
            proposal: Dados da proposta
            status: Status da operação (COMMITTED/NOT_COMMITTED)
        """
        client_id = proposal["clientRequest"].get("clientId", "unknown")
        
        # Determina qual learner deve notificar este cliente
        # Usa hash do clientId para distribuição consistente
        learner_index = hash(client_id) % len(self.learners)
        learner = self.learners[learner_index]
        
        logger.info(f"Notificando cliente {client_id} via learner {learner} com status {status}")
        
        notification = {
            "type": "NOTIFY_CLIENT",
            "instanceId": instance_id,
            "clientId": client_id,
            "status": status,
            "timestamp": current_timestamp(),
            "resource": proposal["clientRequest"].get("resource")
        }
        
        try:
            url = f"http://{learner}/notify-client"
            await self.http_client.post(url, json=notification, timeout=1.0)
        except Exception as e:
            logger.error(f"Erro ao notificar cliente via learner: {e}")
    
    async def _notify_learner_for_cluster_access(self, instance_id: int, proposal: Dict[str, Any], selected_store: str):
        """
        Notifica o Learner para acessar o Cluster Store.
        
        Args:
            instance_id: ID da instância
            proposal: Dados da proposta
            selected_store: Nó do Cluster Store selecionado
        """
        client_id = proposal["clientRequest"].get("clientId", "unknown")
        
        # Determina qual learner deve processar esta operação
        # Usa hash do instanceId para distribuição equilibrada
        learner_index = hash(str(instance_id)) % len(self.learners)
        learner = self.learners[learner_index]
        
        logger.info(f"Delegando acesso ao Cluster Store {selected_store} para learner {learner}")
        
        access_request = {
            "type": "CLUSTER_ACCESS",
            "instanceId": instance_id,
            "clientId": client_id,
            "operation": proposal["clientRequest"].get("operation", "WRITE"),
            "resource": proposal["clientRequest"].get("resource"),
            "data": proposal["clientRequest"].get("data"),
            "timestamp": current_timestamp(),
            "selectedStore": selected_store
        }
        
        try:
            url = f"http://{learner}/cluster-access"
            response = await self.http_client.post(url, json=access_request, timeout=2.0)
            
            # Verifica resultado
            if response.get("status") == "success":
                logger.info(f"Acesso ao Cluster Store bem-sucedido via learner {learner}")
                return True
            else:
                logger.warning(f"Acesso ao Cluster Store falhou: {response.get('message', 'Sem detalhes')}")
                return False
        except Exception as e:
            logger.error(f"Erro ao delegar acesso ao Cluster Store para learner: {e}")
            return False
    
    def _cleanup_old_proposals(self, max_age: float = 3600):
        """
        Remove propostas antigas do cache.
        
        Args:
            max_age: Idade máxima em segundos
        """
        now = time.time()
        to_remove = []
        
        for instance_id, proposal in self.proposals.items():
            age = now - proposal.get("created_at", now)
            if age > max_age:
                to_remove.append(instance_id)
        
        for instance_id in to_remove:
            del self.proposals[instance_id]
        
        if to_remove and DEBUG:
            logger.debug(f"Removidas {len(to_remove)} propostas antigas do cache")