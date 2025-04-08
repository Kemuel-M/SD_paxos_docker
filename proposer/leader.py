"""
File: proposer/leader.py
Implementação da lógica de eleição de líder para o algoritmo Multi-Paxos com melhorias nos logs e tratamentos de casos de borda.
"""
import os
import time
import asyncio
import logging
import random
from typing import Dict, List, Any, Optional

from common.communication import HttpClient, CircuitBreaker
from common.heartbeat import HeartbeatMonitor

# Configuração de debug aprimorada
DEBUG = os.getenv("DEBUG", "false").lower() in ("true", "1", "yes")
DEBUG_LEVEL = os.getenv("DEBUG_LEVEL", "basic").lower()  # Níveis: basic, advanced, trace

logger = logging.getLogger("proposer")

class LeaderElection:
    """
    Implementação da eleição de líder para Multi-Paxos.
    
    Responsável por:
    1. Detectar ausência do líder atual
    2. Iniciar nova eleição quando necessário
    3. Enviar heartbeats se for o líder
    4. Receber heartbeats do líder
    """
    
    def __init__(self, node_id: int, proposers: List[str], proposer, 
                 current_leader: Optional[int] = None, current_term: int = 0):
        """
        Inicializa o gerenciador de eleição de líder.
        
        Args:
            node_id: ID deste nó
            proposers: Lista de endereços de todos os proposers
            proposer: Referência para o objeto Proposer
            current_leader: ID do líder atual, se conhecido
            current_term: Termo atual da eleição
        """
        self.node_id = node_id
        self.proposers = proposers if proposers else []
        self.proposer = proposer
        self.current_leader = current_leader
        self.current_term = current_term
        
        # Cliente HTTP para comunicação
        self.http_client = HttpClient()
        
        # Circuit breakers para proposers
        self.circuit_breakers = {p: CircuitBreaker() for p in self.proposers}
        
        # Monitor de heartbeat para o líder
        leader_desc = f"Líder (proposer-{current_leader})" if current_leader else "Líder desconhecido"
        self.leader_monitor = HeartbeatMonitor(
            target_description=leader_desc,
            failure_threshold=5,  # Considera líder como falho após 5 segundos sem heartbeat
            on_failure=self._handle_leader_failure
        )
        
        # Task de heartbeat (se for líder)
        self.heartbeat_task = None
        
        # Flag para controle da execução
        self.running = False
        
        # Tempo da última eleição iniciada
        self.last_election_time = 0
        
        # Tempo da última vez que vimos o líder
        self.last_leader_seen = 0 if current_leader is None else time.time()
        
        # Contador de chamadas para is_leader (debug avançado)
        self.is_leader_call_count = 0
        
        logger.info(f"Gerenciador de eleição inicializado. Líder atual: {current_leader}, Termo: {current_term}")
        if DEBUG and DEBUG_LEVEL in ("advanced", "trace"):
            logger.debug(f"Proposers conhecidos: {self.proposers}")
    
    async def start(self):
        """Inicia o gerenciador de eleição de líder."""
        if self.running:
            logger.debug("start() chamado, mas o gerenciador já está em execução")
            return
        
        self.running = True
        
        # Inicia o monitor de heartbeat
        if self.current_leader is not None and self.current_leader != self.node_id:
            self.leader_monitor.start()
            logger.debug(f"Monitor de heartbeat iniciado para líder {self.current_leader}")
        
        # Se formos o líder, inicia envio de heartbeats
        if self.current_leader == self.node_id:
            logger.important(f"Nó {self.node_id} iniciando como líder no termo {self.current_term}")
            await self._become_leader()
        else:
            logger.debug(f"Nó {self.node_id} iniciando como seguidor, líder atual: {self.current_leader}")
        
        # Inicia o detector de líder
        asyncio.create_task(self._leader_detector_loop())
        
        logger.info(f"Gerenciador de eleição iniciado")
    
    async def stop(self):
        """Para o gerenciador de eleição de líder."""
        if not self.running:
            logger.debug("stop() chamado, mas o gerenciador já está parado")
            return
        
        self.running = False
        
        # Para o monitor de heartbeat
        self.leader_monitor.stop()
        
        # Cancela a task de heartbeat se existir
        if self.heartbeat_task and not self.heartbeat_task.done():
            self.heartbeat_task.cancel()
            try:
                await self.heartbeat_task
            except asyncio.CancelledError:
                pass
        
        logger.info(f"Gerenciador de eleição parado")
    
    def is_leader(self) -> bool:
        """Retorna se este nó é o líder atual."""
        self.is_leader_call_count += 1
        result = self.current_leader == self.node_id
        
        # Log especial para debugging avançado
        if DEBUG and DEBUG_LEVEL == "trace":
            logger.debug(f"is_leader() chamado (#{self.is_leader_call_count}) → retornando {result}")
        elif DEBUG and DEBUG_LEVEL == "advanced" and self.is_leader_call_count % 100 == 0:
            # Log a cada 100 chamadas para não sobrecarregar os logs
            logger.debug(f"is_leader() chamado {self.is_leader_call_count} vezes, valor atual: {result}")
            
        return result
    
    async def receive_heartbeat(self, data: Dict[str, Any]):
        """
        Recebe heartbeat do líder.
        
        Args:
            data: Dados do heartbeat
            {
                "leaderId": int,
                "term": int,
                "lastInstanceId": int
            }
        """
        # Validação e valores padrão para dados recebidos
        leader_id = data.get("leaderId", -1)
        term = data.get("term", 0)
        last_instance_id = data.get("lastInstanceId", 0)
        
        if leader_id < 0:
            logger.warning(f"Recebido heartbeat com leaderId inválido: {leader_id}. Ignorando.")
            return
        
        # Ignora heartbeats de termos anteriores
        if term < self.current_term:
            logger.warning(f"Recebido heartbeat com termo {term} menor que o atual {self.current_term}. Ignorando.")
            return
        
        # Se o termo é maior, reconhece o novo líder
        if term > self.current_term or self.current_leader != leader_id:
            logger.important(f"Reconhecendo novo líder: {leader_id} (termo {term})")
            self.current_term = term
            
            # Atualiza o líder
            old_leader = self.current_leader
            self.current_leader = leader_id
            
            # Para de ser líder se era antes
            if old_leader == self.node_id:
                logger.important(f"Nó {self.node_id} deixou de ser líder, novo líder: {leader_id}")
                await self._stop_being_leader()
            
            # Atualiza o monitor
            if leader_id != self.node_id:
                self.leader_monitor.set_target(f"Líder (proposer-{leader_id})")
                self.leader_monitor.start()
                logger.debug(f"Monitor de heartbeat atualizado para novo líder: {leader_id}")
        
        # Atualiza o último instanceId se necessário
        if last_instance_id > self.proposer.last_instance_id:
            logger.info(f"Atualizando último instanceId de {self.proposer.last_instance_id} para {last_instance_id}")
            self.proposer.last_instance_id = last_instance_id
        
        # Marca que vimos o líder
        self.last_leader_seen = time.time()
        self.leader_monitor.record_heartbeat()
        
        if DEBUG and DEBUG_LEVEL in ("advanced", "trace"):
            logger.debug(f"Processado heartbeat do líder {leader_id} (termo {term}, instanceId {last_instance_id})")
    
    async def _leader_detector_loop(self):
        """Loop para detectar ausência de líder e iniciar eleição quando necessário."""
        logger.info(f"Iniciando detector de líder")
        
        while self.running:
            try:
                # Se não temos líder ou o líder está com problemas
                if self.current_leader is None or (
                    self.current_leader != self.node_id and 
                    time.time() - self.last_leader_seen > 5  # 5 segundos sem ver o líder
                ):
                    # Verifica se podemos iniciar eleição (evita tempestade de eleições)
                    if time.time() - self.last_election_time > 5:  # No máximo uma eleição a cada 5 segundos
                        leader_status = "ausente" if self.current_leader is None else "inativo"
                        logger.warning(f"Líder {leader_status} por mais de 5 segundos. Iniciando eleição.")
                        await self._start_election()
                
                await asyncio.sleep(1)  # Verifica a cada segundo
                
            except asyncio.CancelledError:
                logger.info("Detector de líder cancelado")
                break
            except Exception as e:
                logger.error(f"Erro no detector de líder: {e}", exc_info=True)
                await asyncio.sleep(1)
    
    async def _start_election(self):
        """Inicia uma nova eleição."""
        # Marca o tempo da eleição
        self.last_election_time = time.time()
        
        # Incrementa o termo
        self.current_term += 1
        term = self.current_term
        
        logger.important(f"Iniciando eleição para o termo {term}")
        
        # Usa a instância 0 especial para eleição
        instance_id = 0
        
        # Valor da proposta é o próprio ID com timestamp
        value = {
            "proposerId": self.node_id,
            "timestamp": int(time.time() * 1000)
        }
        
        # Gera número de proposta único para esta eleição
        self.proposer.proposal_counter += 1
        proposal_number = (self.proposer.proposal_counter << 8) | self.node_id
        
        # Prepara mensagem para fase Prepare
        prepare_message = {
            "type": "PREPARE",
            "proposalNumber": proposal_number,
            "instanceId": instance_id,
            "proposerId": self.node_id,
            "term": term
        }
        
        # Envia Prepare para todos os acceptors
        logger.debug(f"Enviando PREPARE para todos os acceptors (eleição, termo {term})")
        responses = await self._send_to_acceptors("/prepare", prepare_message)
        
        # Analisa as respostas
        promises = []
        highest_accepted_proposal = -1
        highest_accepted_value = None
        
        for response in responses:
            if response and response.get("accepted") == True:
                promises.append(response)
                
                # Verifica se tem valor aceito anteriormente
                accepted_proposal = response.get("highestAccepted", -1)
                if accepted_proposal > highest_accepted_proposal and "acceptedValue" in response:
                    highest_accepted_proposal = accepted_proposal
                    highest_accepted_value = response["acceptedValue"]
        
        # Verifica se obteve maioria (3 de 5)
        if len(promises) < 3:
            logger.warning(f"Eleição falhou: apenas {len(promises)}/{len(self.proposer.acceptors)} promises recebidas")
            return
        
        # Determina o valor a propor (usa o valor aceito com maior número se existir)
        if highest_accepted_value is not None:
            # Se já existe valor aceito, usa-o (mantém o líder já eleito)
            value = highest_accepted_value
            logger.info(f"Usando valor previamente aceito: proposer-{value.get('proposerId', 'desconhecido')}")
        
        # Prepara mensagem para fase Accept
        accept_message = {
            "type": "ACCEPT",
            "proposalNumber": proposal_number,
            "instanceId": instance_id,
            "proposerId": self.node_id,
            "value": value,
            "term": term
        }
        
        # Envia Accept para todos os acceptors
        logger.debug(f"Enviando ACCEPT para todos os acceptors (eleição, termo {term})")
        responses = await self._send_to_acceptors("/accept", accept_message)
        
        # Conta quantos aceitaram
        accepted_count = sum(1 for resp in responses if resp and resp.get("accepted") == True)
        
        # Verifica se obteve maioria (3 de 5)
        if accepted_count < 3:
            logger.warning(f"Eleição falhou: apenas {accepted_count}/{len(self.proposer.acceptors)} aceitaram")
            return
        
        # Determina o vencedor da eleição
        winner_id = value.get("proposerId", -1)
        
        logger.important(f"Eleição para o termo {term} concluída. Vencedor: proposer-{winner_id}")
        
        # Atualiza o líder
        old_leader = self.current_leader
        self.current_leader = winner_id
        
        # Se éramos o líder e não somos mais
        if old_leader == self.node_id and winner_id != self.node_id:
            logger.important(f"Nó {self.node_id} perdeu liderança para proposer-{winner_id}")
            await self._stop_being_leader()
        
        # Se não éramos o líder e agora somos
        if old_leader != self.node_id and winner_id == self.node_id:
            logger.important(f"Nó {self.node_id} ganhou eleição e se tornou líder no termo {term}")
            await self._become_leader()
        
        # Atualiza o Proposer
        await self.proposer.set_leader(self.is_leader())
        
        # Atualiza o monitor de heartbeat
        if winner_id != self.node_id:
            self.leader_monitor.set_target(f"Líder (proposer-{winner_id})")
            self.leader_monitor.start()
            logger.debug(f"Monitor de heartbeat atualizado para novo líder: {winner_id}")
        else:
            self.leader_monitor.stop()
            logger.debug("Monitor de heartbeat parado (somos o líder)")
    
    async def _become_leader(self):
        """Ações a serem tomadas quando este nó se torna líder."""
        logger.important(f"Este nó agora é o líder para o termo {self.current_term}")
        
        # Atualiza o proposer
        await self.proposer.set_leader(True)
        
        # Para o monitor de heartbeat (não precisamos monitorar a nós mesmos)
        self.leader_monitor.stop()

        # Sincroniza estado com outros proposers para garantir continuidade
        await self._sync_state_as_leader()
        
        # Inicia envio de heartbeats periódicos
        self.heartbeat_task = asyncio.create_task(self._send_heartbeats_loop())
        logger.debug("Iniciada task de envio de heartbeats periódicos")

    async def _sync_state_as_leader(self):
        """Sincroniza estado com outros proposers ao se tornar líder."""
        logger.info("Sincronizando estado como novo líder")
        
        try:
            # Prepara dados para sincronização
            sync_data = {
                "leaderId": self.node_id,
                "term": self.current_term,
                "lastInstanceId": self.proposer.last_instance_id,
                "isNewLeader": True  # Indica que é um novo líder assumindo
            }
            
            # Envia notificação a todos os outros proposers
            tasks = []
            for i, proposer_addr in enumerate(self.proposers):
                # Ignorar proposers com endereço vazio
                if not proposer_addr:
                    continue
                    
                proposer_id = i + 1  # IDs são 1-based
                
                # Não envia para si mesmo
                if proposer_id == self.node_id:
                    continue
                
                url = f"http://{proposer_addr}/leader-heartbeat"
                # Usa timeout maior para sincronização inicial (1 segundo)
                tasks.append(self.http_client.post(url, json=sync_data, timeout=1.0))
            
            # Aguarda resultados com tratamento de exceções
            responses = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Analisa resultados para verificar se algum proposer tem instanceId maior
            max_instance_id = self.proposer.last_instance_id
            
            for i, response in enumerate(responses):
                if isinstance(response, Exception):
                    # Ignora erros, apenas loga
                    if DEBUG:
                        logger.debug(f"Erro durante sincronização com proposer-{i+1}: {str(response)}")
                    continue
                    
                if isinstance(response, dict) and "lastInstanceId" in response:
                    last_id = response.get("lastInstanceId", 0)
                    if last_id > max_instance_id:
                        max_instance_id = last_id
            
            # Se descobriu instanceId maior, atualiza
            if max_instance_id > self.proposer.last_instance_id:
                logger.info(f"Atualizando lastInstanceId para {max_instance_id} com base na sincronização")
                async with self.proposer.instance_id_lock:
                    self.proposer.last_instance_id = max_instance_id
        
        except Exception as e:
            logger.error(f"Erro durante sincronização como líder: {e}", exc_info=True)
    
    async def _stop_being_leader(self):
        """Ações a serem tomadas quando este nó deixa de ser líder."""
        logger.important(f"Este nó não é mais o líder")
        
        # Atualiza o proposer
        await self.proposer.set_leader(False)
        
        # Cancela a task de heartbeat
        if self.heartbeat_task and not self.heartbeat_task.done():
            self.heartbeat_task.cancel()
            try:
                await self.heartbeat_task
            except asyncio.CancelledError:
                logger.debug("Task de heartbeat cancelada")
            self.heartbeat_task = None
    
    async def _send_heartbeats_loop(self):
        """Loop para enviar heartbeats periódicos para outros proposers."""
        logger.info(f"Iniciando envio de heartbeats como líder")
        
        while self.running and self.is_leader():
            try:
                # Prepara dados do heartbeat
                heartbeat_data = {
                    "leaderId": self.node_id,
                    "term": self.current_term,
                    "lastInstanceId": self.proposer.last_instance_id
                }
                
                # Contabiliza quantos heartbeats foram enviados com sucesso
                sent_count = 0
                total_count = 0
                
                # Envia para todos os outros proposers
                for i, proposer_addr in enumerate(self.proposers):
                    # Ignora proposers com endereço vazio
                    if not proposer_addr:
                        continue
                        
                    proposer_id = i + 1  # IDs são 1-based
                    
                    # Não envia para si mesmo
                    if proposer_id == self.node_id:
                        continue
                    
                    total_count += 1
                    # Envia heartbeat assíncrono (não bloqueia em caso de falha)
                    success = await self._send_heartbeat(proposer_addr, heartbeat_data)
                    if success:
                        sent_count += 1
                
                if DEBUG and DEBUG_LEVEL in ("advanced", "trace"):
                    logger.debug(f"Heartbeats enviados: {sent_count}/{total_count} bem-sucedidos")
                
                # Espera 1 segundo antes do próximo heartbeat
                await asyncio.sleep(1)
                
            except asyncio.CancelledError:
                logger.info(f"Envio de heartbeats cancelado")
                break
            except Exception as e:
                logger.error(f"Erro ao enviar heartbeats: {e}", exc_info=True)
                await asyncio.sleep(1)
    
    async def _send_heartbeat(self, proposer_addr: str, data: Dict[str, Any]) -> bool:
        """
        Envia heartbeat para um proposer específico.
        
        Args:
            proposer_addr: Endereço do proposer
            data: Dados do heartbeat
            
        Returns:
            bool: True se enviado com sucesso, False caso contrário
        """
        if not proposer_addr:
            logger.warning(f"Tentativa de enviar heartbeat para endereço vazio")
            return False
            
        url = f"http://{proposer_addr}/leader-heartbeat"
        cb = self.circuit_breakers.get(proposer_addr)
        
        # Verifica se o circuit breaker permite requisição
        if cb and not cb.allow_request():
            if DEBUG:
                logger.debug(f"Circuit breaker aberto para {proposer_addr}, ignorando heartbeat")
            return False
        
        try:
            # Timeout mais curto para heartbeats (300ms)
            response = await self.http_client.post(url, json=data, timeout=0.3)
            
            # Registra sucesso no circuit breaker
            if cb:
                await cb.record_success()
            
            if DEBUG and DEBUG_LEVEL == "trace":
                logger.debug(f"Heartbeat enviado com sucesso para {proposer_addr}")
            
            return True
            
        except Exception as e:
            # Registra falha no circuit breaker
            if cb:
                await cb.record_failure()
            
            if DEBUG:
                logger.debug(f"Falha ao enviar heartbeat para {proposer_addr}: {e}")
            
            return False
    
    async def _send_to_acceptors(self, endpoint: str, payload: Dict[str, Any]) -> List[Optional[Dict[str, Any]]]:
        """
        Envia requisições para todos os acceptors em paralelo.
        
        Args:
            endpoint: Endpoint a ser chamado (ex: "/prepare", "/accept")
            payload: Dados a serem enviados
        
        Returns:
            List[Optional[Dict[str, Any]]]: Lista de respostas (None para falhas)
        """
        # Validação de entrada
        if not self.proposer.acceptors:
            logger.warning(f"Nenhum acceptor configurado para envio de {endpoint}")
            return []
            
        # Cria tasks para todas as requisições em paralelo
        tasks = []
        for acceptor in self.proposer.acceptors:
            if not acceptor:  # Ignora acceptors com endereço vazio
                continue
                
            url = f"http://{acceptor}{endpoint}"
            
            # Adiciona a task com circuit breaker e retentativas
            tasks.append(self._send_with_retry(url, payload))
        
        # Executa todas as requisições em paralelo
        return await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _send_with_retry(self, url: str, payload: Dict[str, Any], 
                             max_retries: int = 3) -> Optional[Dict[str, Any]]:
        """
        Envia requisição HTTP com retentativas.
        
        Args:
            url: URL de destino
            payload: Dados a serem enviados
            max_retries: Número máximo de tentativas
        
        Returns:
            Optional[Dict[str, Any]]: Resposta ou None em caso de falha
        """
        # Validação de entrada
        if not url:
            logger.warning("Tentativa de enviar requisição para URL vazia")
            return None
            
        # Tenta enviar a requisição com retentativas
        for attempt in range(max_retries):
            try:
                # Adiciona jitter ao timeout (480-520ms)
                timeout = 0.5 + (random.random() * 0.04 - 0.02)
                
                # Faz a requisição com timeout
                response = await self.http_client.post(url, json=payload, timeout=timeout)
                
                return response
                
            except Exception as e:
                logger.warning(f"Falha ao chamar {url}, tentativa {attempt+1}/{max_retries}: {e}")
                
                # Calcula tempo de espera com backoff exponencial e jitter
                backoff_time = 0.5 * (2 ** attempt)  # 0.5s, 1s, 2s
                jitter = backoff_time * 0.2 * random.random()  # ±20% de jitter
                wait_time = backoff_time + jitter
                
                if attempt < max_retries - 1:
                    await asyncio.sleep(wait_time)
        
        return None
    
    async def _handle_leader_failure(self):
        """Callback chamado quando o monitor de heartbeat detecta falha do líder."""
        if not self.running:
            return
        
        failed_leader = self.current_leader
        logger.warning(f"Falha do líder {failed_leader} detectada pelo monitor de heartbeat")
        
        # Limpa o líder atual
        self.current_leader = None
        
        # Atualiza o proposer
        await self.proposer.set_leader(False)
        
        # Inicia nova eleição
        if time.time() - self.last_election_time > 5:  # Evita tempestade de eleições
            logger.important(f"Iniciando nova eleição após falha do líder {failed_leader}")
            await self._start_election()