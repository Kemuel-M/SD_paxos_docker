"""
Sistema de heartbeat para detecção de falhas entre componentes.
"""
import time
import logging
import asyncio
from typing import Callable, Optional, Dict, Any

logger = logging.getLogger("heartbeat")

class HeartbeatMonitor:
    """
    Monitor de heartbeat para detecção de falhas.
    
    Mantém controle de quando um componente foi visto pela última vez
    e executa callback quando o tempo sem heartbeat exceder o limite.
    """
    
    def __init__(self, target_description: str, failure_threshold: int = 6, 
                on_failure: Optional[Callable] = None):
        """
        Inicializa o monitor de heartbeat.
        
        Args:
            target_description: Descrição do alvo monitorado (para logs)
            failure_threshold: Tempo em segundos após o qual o alvo é considerado falho
            on_failure: Callback a ser executado quando uma falha for detectada
        """
        self.target_description = target_description
        self.failure_threshold = failure_threshold
        self.on_failure = on_failure
        
        self.last_heartbeat = 0
        self.running = False
        self.monitor_task = None
        self.failures_detected = 0
        
        logger.info(f"Monitor de heartbeat inicializado para {target_description}")
    
    def start(self):
        """Inicia o monitor de heartbeat."""
        if self.running:
            return
        
        self.running = True
        self.last_heartbeat = time.time()
        self.failures_detected = 0
        
        # Inicia task de monitoramento
        self.monitor_task = asyncio.create_task(self._monitor_loop())
        
        logger.info(f"Monitor de heartbeat iniciado para {self.target_description}")
    
    def stop(self):
        """Para o monitor de heartbeat."""
        if not self.running:
            return
        
        self.running = False
        
        # Cancela task de monitoramento
        if self.monitor_task and not self.monitor_task.done():
            self.monitor_task.cancel()
        
        logger.info(f"Monitor de heartbeat parado para {self.target_description}")
    
    def set_target(self, target_description: str):
        """
        Atualiza a descrição do alvo monitorado.
        
        Args:
            target_description: Nova descrição do alvo
        """
        self.target_description = target_description
        logger.info(f"Alvo do monitor de heartbeat atualizado para {target_description}")
    
    def record_heartbeat(self):
        """Registra um heartbeat recebido."""
        self.last_heartbeat = time.time()
        
        # Se já tínhamos detectado falha, registra recuperação
        if self.failures_detected > 0:
            logger.info(f"{self.target_description} recuperado após {self.failures_detected} falhas detectadas")
            self.failures_detected = 0
    
    async def _monitor_loop(self):
        """Loop de monitoramento que verifica heartbeat periodicamente."""
        logger.info(f"Iniciando loop de monitoramento para {self.target_description}")
        
        while self.running:
            try:
                # Verifica tempo desde último heartbeat
                now = time.time()
                elapsed = now - self.last_heartbeat
                
                # Verifica se excedeu limiar
                if elapsed > self.failure_threshold:
                    self.failures_detected += 1
                    logger.warning(f"Falha de heartbeat detectada para {self.target_description}. "
                                  f"Último heartbeat há {elapsed:.1f}s. "
                                  f"Falhas consecutivas: {self.failures_detected}")
                    
                    # Executa callback de falha se definido
                    if self.on_failure:
                        try:
                            await self.on_failure()
                        except Exception as e:
                            logger.error(f"Erro no callback de falha: {e}", exc_info=True)
                
                # Dorme por 1 segundo antes da próxima verificação
                await asyncio.sleep(1)
                
            except asyncio.CancelledError:
                logger.info(f"Loop de monitoramento cancelado para {self.target_description}")
                break
            except Exception as e:
                logger.error(f"Erro no loop de monitoramento: {e}", exc_info=True)
                await asyncio.sleep(1)  # Evita loop infinito de erros

class HeartbeatSender:
    """
    Sender de heartbeat para indicar que um componente está ativo.
    
    Envia heartbeats periódicos para um ou mais alvos.
    """
    
    def __init__(self, component_name: str, heartbeat_interval: float = 2.0):
        """
        Inicializa o sender de heartbeat.
        
        Args:
            component_name: Nome do componente que envia heartbeats
            heartbeat_interval: Intervalo entre heartbeats em segundos
        """
        self.component_name = component_name
        self.heartbeat_interval = heartbeat_interval
        
        self.targets = {}  # endpoint -> { url, callback }
        self.running = False
        self.sender_task = None
        
        logger.info(f"Sender de heartbeat inicializado para {component_name} "
                   f"com intervalo de {heartbeat_interval}s")
    
    def add_target(self, target_id: str, callback: Callable):
        """
        Adiciona um alvo para receber heartbeats.
        
        Args:
            target_id: Identificador do alvo
            callback: Função a ser chamada para enviar heartbeat
        """
        self.targets[target_id] = {
            "callback": callback,
            "last_success": 0,
            "failures": 0
        }
        logger.info(f"Adicionado alvo de heartbeat: {target_id}")
    
    def remove_target(self, target_id: str):
        """
        Remove um alvo.
        
        Args:
            target_id: Identificador do alvo
        """
        if target_id in self.targets:
            del self.targets[target_id]
            logger.info(f"Removido alvo de heartbeat: {target_id}")
    
    def start(self):
        """Inicia o envio de heartbeats."""
        if self.running:
            return
        
        self.running = True
        
        # Inicia task de envio
        self.sender_task = asyncio.create_task(self._sender_loop())
        
        logger.info(f"Sender de heartbeat iniciado para {self.component_name}")
    
    def stop(self):
        """Para o envio de heartbeats."""
        if not self.running:
            return
        
        self.running = False
        
        # Cancela task de envio
        if self.sender_task and not self.sender_task.done():
            self.sender_task.cancel()
        
        logger.info(f"Sender de heartbeat parado para {self.component_name}")
    
    async def _sender_loop(self):
        """Loop de envio que envia heartbeats periodicamente."""
        logger.info(f"Iniciando loop de envio de heartbeat para {self.component_name}")
        
        while self.running:
            try:
                # Envia heartbeat para todos os alvos
                send_tasks = []
                
                for target_id, target in self.targets.items():
                    send_tasks.append(self._send_heartbeat(target_id, target))
                
                # Executa envios em paralelo
                await asyncio.gather(*send_tasks, return_exceptions=True)
                
                # Dorme até o próximo intervalo
                await asyncio.sleep(self.heartbeat_interval)
                
            except asyncio.CancelledError:
                logger.info(f"Loop de envio de heartbeat cancelado para {self.component_name}")
                break
            except Exception as e:
                logger.error(f"Erro no loop de envio de heartbeat: {e}", exc_info=True)
                await asyncio.sleep(1)  # Evita loop infinito de erros
    
    async def _send_heartbeat(self, target_id: str, target: Dict[str, Any]):
        """
        Envia um heartbeat para um alvo específico.
        
        Args:
            target_id: Identificador do alvo
            target: Informações do alvo
        """
        try:
            # Chama o callback para enviar heartbeat
            await target["callback"]()
            
            # Atualiza status
            target["last_success"] = time.time()
            
            # Registra recuperação se necessário
            if target["failures"] > 0:
                logger.info(f"Heartbeat para {target_id} recuperado após {target['failures']} falhas")
                target["failures"] = 0
            
        except Exception as e:
            # Incrementa contador de falhas
            target["failures"] += 1
            
            # Loga apenas na primeira falha e a cada 5 falhas para evitar spam
            if target["failures"] == 1 or target["failures"] % 5 == 0:
                logger.warning(f"Falha ao enviar heartbeat para {target_id}: {e}. "
                              f"Falhas consecutivas: {target['failures']}")