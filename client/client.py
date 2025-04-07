"""
Implementação do cliente Paxos.
Responsável por enviar requisições ao proposer e processar notificações dos learners.
"""
import os
import time
import random
import logging
import asyncio
from typing import Dict, Any, List, Optional
import httpx

logger = logging.getLogger("client")

class PaxosClient:
    """
    Cliente para o sistema Paxos.
    Envia requisições ao proposer e processa notificações dos learners.
    """
    def __init__(self, client_id: str, proposer_url: str, callback_url: str, 
                num_operations: Optional[int] = None, resource_id: str = "R"):
        """
        Inicializa o cliente Paxos.
        
        Args:
            client_id: ID do cliente (formato client-X)
            proposer_url: URL do proposer designado
            callback_url: URL para receber notificações dos learners
            num_operations: Número de operações a serem realizadas (aleatório entre 10-50 se None)
        """
        self.client_id = client_id
        self.proposer_url = proposer_url
        self.callback_url = callback_url
        self.num_operations = num_operations or random.randint(10, 50)
        
        # Estado do cliente
        self.operations_completed = 0
        self.operations_failed = 0
        self.operations_in_progress = {}  # instanceId -> operation_info
        self.next_operation_id = 1
        self.history = []  # Histórico de operações
        self.running = False
        self.http_client = httpx.AsyncClient(timeout=10.0)

        self.request_ids = {}  # Para rastrear requisições duplicadas: request_id -> operation_id
        self.request_id_ttl = 300  # TTL em segundos para IDs de requisição
        self.resource_id = resource_id
        logger.info(f"Cliente {client_id} inicializado para recurso {resource_id}")
        
        # Métricas
        self.latencies = []
        self.start_time = time.time()
        
        logger.info(f"Cliente {client_id} inicializado com {self.num_operations} operações para realizar")
        logger.info(f"Proposer designado: {proposer_url}")
        logger.info(f"URL de callback: {callback_url}")
    
    async def start(self):
        """Inicia o ciclo de operações do cliente."""
        if self.running:
            logger.warning("Cliente já está em execução")
            return
        
        self.running = True
        logger.info(f"Iniciando ciclo de operações. Total planejado: {self.num_operations}")
        
        # Agora retornamos a task criada em vez de apenas criá-la
        loop_task = asyncio.create_task(self._operation_loop())
        
        # Em produção, isso funciona normalmente
        # Em testes, podemos aguardar ou cancelar esta task conforme necessário
        return loop_task
        
    async def stop(self):
        """Para o ciclo de operações do cliente."""
        logger.info("Parando cliente...")
        self.running = False

        # Limpa todas as tasks pendentes
        await self.cleanup_pending_tasks()
        
        await self.http_client.aclose()
        logger.info("Cliente parado")
        
    async def _operation_loop(self):
        """Loop principal que executa as operações planejadas."""
        try:
            while self.running and (self.operations_completed + self.operations_failed) < self.num_operations:
                # Verifica se há muitas operações em andamento
                if len(self.operations_in_progress) > 5:
                    logger.debug(f"Aguardando conclusão de operações em andamento: {len(self.operations_in_progress)}")
                    await asyncio.sleep(0.5)
                    continue
                
                # Cria e envia nova operação
                operation_id = self.next_operation_id
                self.next_operation_id += 1
                
                logger.info(f"Iniciando operação #{operation_id}")
                asyncio.create_task(self._send_operation(operation_id))
                
                # Espera tempo aleatório entre 1.0 e 5.0 segundos
                wait_time = random.uniform(1.0, 5.0)
                logger.debug(f"Aguardando {wait_time:.2f}s antes da próxima operação")
                await asyncio.sleep(wait_time)
                
            logger.info(f"Ciclo de operações concluído. Completadas: {self.operations_completed}, Falhas: {self.operations_failed}")
            
        except Exception as e:
            logger.error(f"Erro no loop de operações: {e}", exc_info=True)
            self.running = False
    
    async def _send_operation(self, operation_id: int, retries: int = 0, timestamp_override: Optional[int] = None):
        """
        Envia uma operação de escrita para o proposer.
        
        Args:
            operation_id: ID da operação (usado apenas localmente)
            retries: Número de tentativas realizadas
        """
        try:
            # Gera dados da operação
            timestamp = timestamp_override or int(time.time() * 1000)
            data = f"Data {self.client_id}-{operation_id} at {timestamp}"

            # Cria ID de requisição para desduplicação
            request_id = f"{self.client_id}-{operation_id}"
            
            # Verifica se é uma requisição duplicada
            if request_id in self.request_ids:
                logger.info(f"Requisição duplicada detectada: {request_id}, ignorando.")
                return
                
            # Armazena ID de requisição
            self.request_ids[request_id] = operation_id
            
            # Agenda limpeza do ID de requisição após TTL
            cleanup_task = asyncio.create_task(self._cleanup_request_id(request_id))
            # Armazenar referência da task para cancelamento posterior
            if not hasattr(self, '_cleanup_tasks'):
                self._cleanup_tasks = []
            self._cleanup_tasks.append(cleanup_task)
            
            # Cria payload da requisição
            payload = {
                "clientId": self.client_id,
                "timestamp": timestamp,
                "operation": "WRITE",
                "resource": self.resource_id,
                "data": data
            }
            
            # Registra início da operação
            start_time = time.time()
            operation_info = {
                "id": operation_id,
                "start_time": start_time,
                "payload": payload,
                "retries": retries,
                "status": "in_progress"
            }
            
            logger.info(f"Enviando operação #{operation_id} para o proposer: WRITE em R")
            if retries > 0:
                logger.info(f"Esta é a tentativa #{retries+1} para operação #{operation_id}")
            
            # Envia para o proposer
            response = await self.http_client.post(
                f"{self.proposer_url}/propose",
                json=payload,
                timeout=10.0  # Timeout mais longo para proposer
            )
            
            # Processa resposta
            if response.status_code == 202:  # Accepted
                data = response.json()
                instance_id = data.get("instanceId")
                
                if not instance_id:
                    logger.warning(f"Resposta do proposer não contém instanceId: {data}")
                    operation_info["status"] = "failed"
                    operation_info["error"] = "Missing instanceId in response"
                    self.operations_failed += 1
                    self.history.append(operation_info)
                    return
                
                logger.info(f"Operação #{operation_id} aceita pelo proposer com instanceId {instance_id}")
                
                # Registra operação em andamento
                operation_info["instance_id"] = instance_id
                self.operations_in_progress[instance_id] = operation_info
                
                # Configura timeout para o caso de não receber notificação
                asyncio.create_task(self._handle_operation_timeout(instance_id))
                
            elif response.status_code == 307:  # Redirect
                # Redirecionamento para outro proposer (provavelmente o líder)
                redirect_url = response.headers.get("Location")
                if not redirect_url:
                    logger.warning(f"Redirecionamento sem Location header: {response.headers}")
                    operation_info["status"] = "failed"
                    operation_info["error"] = "Redirect without Location header"
                    self.operations_failed += 1
                    self.history.append(operation_info)
                    return
                
                logger.info(f"Redirecionando operação #{operation_id} para {redirect_url}")
                
                # Atualiza o proposer para esta operação específica
                from urllib.parse import urlparse
                parsed_url = urlparse(redirect_url)
                temp_proposer_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
                
                # Reenviar para o novo proposer após um pequeno delay
                await asyncio.sleep(0.1)
                
                # Cria payload da requisição
                new_payload = {
                    "clientId": self.client_id,
                    "timestamp": int(time.time() * 1000),  # Atualiza timestamp
                    "operation": operation_info["payload"]["operation"],
                    "resource": operation_info["payload"]["resource"],
                    "data": operation_info["payload"]["data"]  # Usa os dados do payload original
                }
                
                # Envia para o novo proposer
                response = await self.http_client.post(
                    f"{temp_proposer_url}/propose",
                    json=new_payload,
                    timeout=10.0
                )
                
                # Processa resposta do novo proposer
                if response.status_code == 202:  # Accepted
                    data = response.json()
                    instance_id = data.get("instanceId")
                    
                    if not instance_id:
                        logger.warning(f"Resposta do proposer redirecionado não contém instanceId: {data}")
                        operation_info["status"] = "failed"
                        operation_info["error"] = "Missing instanceId in response from redirected proposer"
                        self.operations_failed += 1
                        self.history.append(operation_info)
                        return
                    
                    logger.info(f"Operação #{operation_id} aceita pelo proposer redirecionado com instanceId {instance_id}")
                    
                    # Registra operação em andamento
                    operation_info["instance_id"] = instance_id
                    self.operations_in_progress[instance_id] = operation_info
                    
                    # Configura timeout para o caso de não receber notificação
                    asyncio.create_task(self._handle_operation_timeout(instance_id))
                else:
                    logger.error(f"Falha após redirecionamento para operação #{operation_id}: {response.status_code}")
                    logger.debug(f"Resposta: {response.text}")
                    
                    # Trata como falha se exceder limite de tentativas
                    if retries >= 2:
                        logger.warning(f"Número máximo de tentativas excedido para operação #{operation_id}")
                        operation_info["status"] = "failed"
                        operation_info["error"] = f"Failed after redirect: {response.status_code}"
                        self.operations_failed += 1
                        self.history.append(operation_info)
                    else:
                        # Tenta novamente após um delay
                        retry_delay = random.uniform(1.0, 3.0)
                        logger.info(f"Tentando novamente operação #{operation_id} após {retry_delay:.2f}s")
                        await asyncio.sleep(retry_delay)
                        await self._send_operation(operation_id, retries + 1)
            
            else:
                logger.error(f"Erro ao enviar operação #{operation_id}: {response.status_code}")
                logger.debug(f"Resposta: {response.text}")
                
                # Trata como falha se exceder limite de tentativas
                if retries >= 2:
                    logger.warning(f"Número máximo de tentativas excedido para operação #{operation_id}")
                    operation_info["status"] = "failed"
                    operation_info["error"] = f"Failed with status: {response.status_code}"
                    self.operations_failed += 1
                    self.history.append(operation_info)
                else:
                    # Tenta novamente após um delay
                    retry_delay = random.uniform(1.0, 3.0)
                    logger.info(f"Tentando novamente operação #{operation_id} após {retry_delay:.2f}s")
                    await asyncio.sleep(retry_delay)
                    await self._send_operation(operation_id, retries + 1)
        
        except Exception as e:
            logger.error(f"Exceção ao enviar operação #{operation_id}: {e}", exc_info=True)
            
            # Trata como falha se exceder limite de tentativas
            if retries >= 2:
                logger.warning(f"Número máximo de tentativas excedido para operação #{operation_id}")
                operation_info = {
                    "id": operation_id,
                    "start_time": time.time(),
                    "status": "failed",
                    "error": f"{type(e).__name__}: {str(e)}",
                    "retries": retries
                }
                self.operations_failed += 1
                self.history.append(operation_info)
            else:
                # Tenta novamente após um delay
                retry_delay = random.uniform(1.0, 3.0)
                logger.info(f"Tentando novamente operação #{operation_id} após {retry_delay:.2f}s")
                await asyncio.sleep(retry_delay)
                await self._send_operation(operation_id, retries + 1)

    async def cleanup_pending_tasks(self):
        """Limpa todas as tasks pendentes criadas pelo cliente."""
        if hasattr(self, '_cleanup_tasks'):
            for task in self._cleanup_tasks:
                if not task.done():
                    task.cancel()
            
            # Aguarda o cancelamento de todas as tasks
            await asyncio.gather(*[t for t in self._cleanup_tasks], return_exceptions=True)
            self._cleanup_tasks = []
        
        # Também limpa tasks de timeout
        if hasattr(self, '_timeout_tasks'):
            for task in self._timeout_tasks:
                if not task.done():
                    task.cancel()
            
            # Aguarda o cancelamento de todas as tasks
            await asyncio.gather(*[t for t in self._timeout_tasks], return_exceptions=True)
            self._timeout_tasks = []

    # Método para limpar IDs de requisição antigos
    async def _cleanup_request_id(self, request_id: str):
        await asyncio.sleep(self.request_id_ttl)
        if request_id in self.request_ids:
            self.request_ids.pop(request_id)
            logger.debug(f"ID de requisição expirado removido: {request_id}")
    
    async def _handle_operation_timeout(self, instance_id: str):
        """
        Gerencia timeout para uma operação pendente.
        
        Args:
            instance_id: ID da instância Paxos para a operação
        """
        try:
            # Espera 30 segundos
            await asyncio.sleep(30)
            
            # Verifica se a operação ainda está pendente
            if instance_id in self.operations_in_progress:
                operation_info = self.operations_in_progress.pop(instance_id)
                operation_id = operation_info["id"]
                
                logger.warning(f"Timeout para operação #{operation_id} com instanceId {instance_id}")
                
                # Atualiza informações da operação
                operation_info["status"] = "timeout"
                operation_info["end_time"] = time.time()
                operation_info["latency"] = operation_info["end_time"] - operation_info["start_time"]
                
                # Incrementa contador de falhas
                self.operations_failed += 1
                
                # Adiciona ao histórico
                self.history.append(operation_info)
        except asyncio.CancelledError:
            # Tratamento limpo de cancelamento
            logger.debug(f"Task de timeout para {instance_id} cancelada")
            raise
    
    def process_notification(self, notification: Dict[str, Any]) -> Dict[str, Any]:
        """
        Processa notificação do learner.
        
        Args:
            notification: Notificação recebida do learner
            
        Returns:
            Dict[str, Any]: Resposta para o learner
        """
        instance_id = notification.get("instanceId")
        status = notification.get("status")
        timestamp = notification.get("timestamp")
        resource = notification.get("resource")
        
        logger.info(f"Recebida notificação do learner: instanceId={instance_id}, status={status}")
        
        # Verifica se temos uma operação correspondente
        if instance_id not in self.operations_in_progress:
            logger.warning(f"Notificação recebida para instanceId desconhecido: {instance_id}")
            return {"status": "acknowledged", "known": False}
        
        # Obtém informações da operação
        operation_info = self.operations_in_progress.pop(instance_id)
        operation_id = operation_info["id"]
        start_time = operation_info["start_time"]
        
        # Calcula latência
        end_time = time.time()
        latency = end_time - start_time
        
        # Atualiza informações da operação
        operation_info["status"] = status
        operation_info["end_time"] = end_time
        operation_info["latency"] = latency
        operation_info["notification"] = notification
        
        # Registra no histórico
        self.history.append(operation_info)
        
        # Atualiza contadores
        if status == "COMMITTED":
            self.operations_completed += 1
            self.latencies.append(latency)
            logger.info(f"Operação #{operation_id} concluída com sucesso em {latency:.2f}s")
        else:
            self.operations_failed += 1
            logger.warning(f"Operação #{operation_id} falhou com status {status}")
        
        return {
            "status": "acknowledged",
            "known": True,
            "operation_id": operation_id
        }
    
    def get_status(self) -> Dict[str, Any]:
        """
        Obtém status atual do cliente.
        
        Returns:
            Dict[str, Any]: Status do cliente
        """
        runtime = time.time() - self.start_time
        
        return {
            "client_id": self.client_id,
            "proposer_url": self.proposer_url,
            "total_operations": self.num_operations,
            "completed": self.operations_completed,
            "failed": self.operations_failed,
            "in_progress": len(self.operations_in_progress),
            "avg_latency": sum(self.latencies) / len(self.latencies) if self.latencies else 0,
            "runtime": runtime,
            "running": self.running
        }
    
    def get_history(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Obtém histórico de operações.
        
        Args:
            limit: Número máximo de operações a retornar
            
        Returns:
            List[Dict[str, Any]]: Histórico de operações
        """
        # Retorna operações mais recentes primeiro
        sorted_history = sorted(self.history, key=lambda x: x.get("start_time", 0), reverse=True)
        return sorted_history[:limit]
    
    def get_operation(self, operation_id: int) -> Optional[Dict[str, Any]]:
        """
        Obtém detalhes de uma operação específica.
        
        Args:
            operation_id: ID da operação
            
        Returns:
            Optional[Dict[str, Any]]: Detalhes da operação ou None se não encontrada
        """
        for op in self.history:
            if op.get("id") == operation_id:
                return op
        
        # Verifica operações em andamento
        for op in self.operations_in_progress.values():
            if op.get("id") == operation_id:
                return op
        
        return None