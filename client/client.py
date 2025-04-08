import asyncio
import logging
import random
import time
from typing import Dict, List, Optional, Union

from common.utils import current_timestamp, generate_id, http_request

logger = logging.getLogger(__name__)

class Client:
    """
    Implementação do cliente do sistema Paxos.
    Responsável por enviar pedidos de acesso ao recurso R.
    """
    
    def __init__(self, client_id: str, debug: bool = False):
        """
        Inicializa o cliente.
        
        Args:
            client_id: ID único do cliente
            debug: Flag para ativar modo de depuração
        """
        self.id = client_id
        self.debug = debug
        
        # URL do proposer que este cliente conhece
        self.proposer_url: Optional[str] = None
        
        # Histórico de pedidos
        self.requests: List[Dict] = []
        
        # Estado do cliente
        self.running = False
        self.total_requests = 0
        self.successful_requests = 0
        self.failed_requests = 0
        
        # Configurações para os pedidos aleatórios
        self.min_wait = 1.0  # Tempo mínimo de espera entre pedidos (segundos)
        self.max_wait = 5.0  # Tempo máximo de espera entre pedidos (segundos)
        self.min_requests = 10  # Número mínimo de pedidos a serem enviados
        self.max_requests = 50  # Número máximo de pedidos a serem enviados
        
        # Lista de ouvintes para notificações
        self.listeners: List[callable] = []
        
        # Task para o loop de pedidos
        self._task: Optional[asyncio.Task] = None
        
        logger.debug(f"Cliente {client_id} inicializado (debug={debug})")
    
    def set_proposer(self, url: str) -> None:
        """
        Define o proposer que este cliente conhece.
        
        Args:
            url: URL base do proposer
        """
        self.proposer_url = url
        logger.debug(f"Proposer definido: {url}")
    
    def add_listener(self, listener: callable) -> None:
        """
        Adiciona um ouvinte para receber notificações de eventos.
        
        Args:
            listener: Função a ser chamada quando ocorrer um evento
        """
        self.listeners.append(listener)
        logger.debug(f"Ouvinte adicionado: {listener}")
    
    def _notify_listeners(self, event: str, data: Dict) -> None:
        """
        Notifica todos os ouvintes sobre um evento.
        
        Args:
            event: Tipo de evento
            data: Dados do evento
        """
        for listener in self.listeners:
            try:
                listener(event, data)
            except Exception as e:
                logger.error(f"Erro ao notificar ouvinte sobre evento {event}: {str(e)}")
    
    async def send_request(self, resource_data: Dict = None) -> Dict:
        """
        Envia um pedido de acesso ao recurso R.
        
        Args:
            resource_data: Dados do recurso a serem enviados
        
        Returns:
            Dict: Resultado do pedido
        """
        if not self.proposer_url:
            error_msg = "Proposer não definido"
            logger.error(error_msg)
            return {
                "success": False,
                "error": error_msg
            }
        
        # Se não foram fornecidos dados, gera alguns dados aleatórios
        if not resource_data:
            resource_data = self._generate_random_data()
        
        # Gera um timestamp para identificar o pedido
        timestamp = current_timestamp()
        
        # Cria o pedido
        request_data = {
            "client_id": self.id,
            "resource_data": resource_data
        }
        
        # URL do endpoint propose do proposer
        propose_url = f"{self.proposer_url}/propose"
        
        # Envia o pedido
        try:
            logger.info(f"Enviando pedido de acesso ao recurso R: timestamp={timestamp}")
            
            # Notifica os ouvintes sobre o início do pedido
            self._notify_listeners("request_start", {
                "timestamp": timestamp,
                "client_id": self.id,
                "resource_data": resource_data
            })
            
            # Registra o pedido no histórico
            request_entry = {
                "timestamp": timestamp,
                "resource_data": resource_data,
                "status": "pending",
                "start_time": time.time()
            }
            self.requests.append(request_entry)
            
            # Incrementa o contador de pedidos
            self.total_requests += 1
            
            # Envia o pedido para o proposer
            response = await http_request("POST", propose_url, data=request_data)
            
            # Atualiza o pedido no histórico
            request_entry["response"] = response
            request_entry["end_time"] = time.time()
            
            if response.get("success", False):
                # Pedido bem-sucedido (pelo menos iniciou o processo de consenso)
                request_entry["status"] = "accepted"
                request_entry["proposal_id"] = response.get("proposal_id")
                
                logger.info(f"Pedido aceito pelo proposer: proposal_id={response.get('proposal_id')}")
                
                # Notifica os ouvintes sobre o pedido aceito
                self._notify_listeners("request_accepted", {
                    "timestamp": timestamp,
                    "client_id": self.id,
                    "proposal_id": response.get("proposal_id"),
                    "resource_data": resource_data
                })
                
                # Incrementa o contador de pedidos bem-sucedidos
                self.successful_requests += 1
            else:
                # Pedido falhou
                request_entry["status"] = "failed"
                request_entry["error"] = response.get("error", "Erro desconhecido")
                
                logger.warning(f"Pedido rejeitado pelo proposer: {response.get('error', 'Erro desconhecido')}")
                
                # Notifica os ouvintes sobre o pedido rejeitado
                self._notify_listeners("request_rejected", {
                    "timestamp": timestamp,
                    "client_id": self.id,
                    "error": response.get("error", "Erro desconhecido"),
                    "resource_data": resource_data
                })
                
                # Incrementa o contador de pedidos falhos
                self.failed_requests += 1
            
            return response
        except Exception as e:
            error_str = str(e)
            logger.error(f"Erro ao enviar pedido: {error_str}")
            
            # Atualiza o pedido no histórico
            if len(self.requests) > 0 and self.requests[-1].get("timestamp") == timestamp:
                self.requests[-1]["status"] = "error"
                self.requests[-1]["error"] = error_str
                self.requests[-1]["end_time"] = time.time()
            
            # Notifica os ouvintes sobre o erro
            self._notify_listeners("request_error", {
                "timestamp": timestamp,
                "client_id": self.id,
                "error": error_str,
                "resource_data": resource_data
            })
            
            # Incrementa o contador de pedidos falhos
            self.failed_requests += 1
            
            return {
                "success": False,
                "error": error_str
            }
    
    def _generate_random_data(self) -> Dict:
        """
        Gera dados aleatórios para o recurso R.
        
        Returns:
            Dict: Dados aleatórios
        """
        # Simula um documento JSON com dados aleatórios
        return {
            "name": f"Resource_{random.randint(1, 1000)}",
            "value": random.randint(1, 100),
            "created_by": self.id,
            "timestamp": time.time(),
            "random_data": {
                "field1": random.choice(["foo", "bar", "baz"]),
                "field2": random.random(),
                "field3": bool(random.getrandbits(1))
            }
        }
    
    async def start_random_requests(self, num_requests: int = None) -> None:
        """
        Inicia o loop de envio de pedidos aleatórios.
        
        Args:
            num_requests: Número de pedidos a serem enviados (se None, usa um valor aleatório)
        """
        if self.running:
            logger.warning("Cliente já está enviando pedidos")
            return
        
        self.running = True
        
        # Define o número de pedidos a serem enviados
        if num_requests is None:
            num_requests = random.randint(self.min_requests, self.max_requests)
        
        logger.info(f"Iniciando loop de {num_requests} pedidos aleatórios")
        
        # Notifica os ouvintes sobre o início dos pedidos
        self._notify_listeners("requests_started", {
            "client_id": self.id,
            "num_requests": num_requests
        })
        
        # Inicia o loop de pedidos em uma nova task
        self._task = asyncio.create_task(self._request_loop(num_requests))
    
    async def _request_loop(self, num_requests: int) -> None:
        """
        Loop para envio de pedidos aleatórios.
        
        Args:
            num_requests: Número de pedidos a serem enviados
        """
        try:
            for i in range(num_requests):
                if not self.running:
                    logger.info("Loop de pedidos interrompido")
                    break
                
                # Envia um pedido
                await self.send_request()
                
                # Espera um tempo aleatório antes do próximo pedido
                if i < num_requests - 1:  # Não espera após o último pedido
                    wait_time = random.uniform(self.min_wait, self.max_wait)
                    logger.debug(f"Aguardando {wait_time:.2f}s antes do próximo pedido ({i+1}/{num_requests})")
                    
                    # Notifica os ouvintes sobre a espera
                    self._notify_listeners("request_wait", {
                        "client_id": self.id,
                        "wait_time": wait_time,
                        "current_request": i + 1,
                        "total_requests": num_requests
                    })
                    
                    await asyncio.sleep(wait_time)
            
            logger.info(f"Loop de pedidos concluído: {num_requests} pedidos enviados")
            
            # Notifica os ouvintes sobre o fim dos pedidos
            self._notify_listeners("requests_completed", {
                "client_id": self.id,
                "total_requests": num_requests,
                "successful_requests": self.successful_requests,
                "failed_requests": self.failed_requests
            })
        except Exception as e:
            logger.error(f"Erro no loop de pedidos: {str(e)}")
            
            # Notifica os ouvintes sobre o erro
            self._notify_listeners("requests_error", {
                "client_id": self.id,
                "error": str(e)
            })
        finally:
            self.running = False
    
    def stop_requests(self) -> None:
        """
        Para o loop de envio de pedidos.
        """
        if not self.running:
            logger.warning("Cliente não está enviando pedidos")
            return
        
        self.running = False
        
        if self._task:
            self._task.cancel()
        
        logger.info("Loop de pedidos interrompido")
        
        # Notifica os ouvintes sobre a interrupção
        self._notify_listeners("requests_stopped", {
            "client_id": self.id
        })
    
    async def handle_notification(self, proposal_id: str, tid: int, status: str, 
                               resource_data: Dict, learner_id: str, timestamp: float,
                               protocol: str = None) -> Dict:
        """
        Processa uma notificação recebida de um learner.
        
        Args:
            proposal_id: ID da proposta
            tid: TID da proposta
            status: Status da proposta (COMMITTED ou NOT_COMMITTED)
            resource_data: Dados do recurso
            learner_id: ID do learner que enviou a notificação
            timestamp: Timestamp da notificação
            protocol: Protocolo usado (opcional)
        
        Returns:
            Dict: Resultado do processamento
        """
        # Procura o pedido correspondente no histórico
        for request in self.requests:
            if request.get("proposal_id") == proposal_id:
                # Atualiza o pedido no histórico
                request["status"] = status.lower()
                request["notification"] = {
                    "learner_id": learner_id,
                    "timestamp": timestamp,
                    "protocol": protocol
                }
                
                if status == "COMMITTED":
                    logger.info(f"Proposta {proposal_id} commitada: TID={tid}, learner={learner_id}")
                    
                    # Notifica os ouvintes sobre o commit
                    self._notify_listeners("proposal_committed", {
                        "client_id": self.id,
                        "proposal_id": proposal_id,
                        "tid": tid,
                        "learner_id": learner_id,
                        "resource_data": resource_data,
                        "protocol": protocol
                    })
                else:
                    logger.warning(f"Proposta {proposal_id} não commitada: status={status}, learner={learner_id}")
                    
                    # Notifica os ouvintes sobre o não-commit
                    self._notify_listeners("proposal_not_committed", {
                        "client_id": self.id,
                        "proposal_id": proposal_id,
                        "tid": tid,
                        "learner_id": learner_id,
                        "status": status,
                        "protocol": protocol
                    })
                
                break
        else:
            # Não encontrou o pedido no histórico
            logger.warning(f"Recebeu notificação para proposta desconhecida: {proposal_id}")
        
        return {
            "success": True,
            "proposal_id": proposal_id,
            "client_id": self.id
        }
    
    def get_status(self) -> Dict:
        """
        Obtém o status do cliente.
        
        Returns:
            Dict: Status do cliente
        """
        return {
            "id": self.id,
            "running": self.running,
            "total_requests": self.total_requests,
            "successful_requests": self.successful_requests,
            "failed_requests": self.failed_requests,
            "proposer_url": self.proposer_url,
            "min_wait": self.min_wait,
            "max_wait": self.max_wait,
            "min_requests": self.min_requests,
            "max_requests": self.max_requests
        }