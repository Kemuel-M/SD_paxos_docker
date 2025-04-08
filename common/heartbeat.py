import asyncio
import logging
import time
from typing import Callable, Dict, List, Optional

logger = logging.getLogger(__name__)

class HeartbeatSystem:
    """
    Sistema de heartbeat para monitoramento de componentes.
    Envia heartbeats a cada 5 segundos exatos para os componentes configurados.
    """
    
    def __init__(self, component_id: str, debug: bool = False):
        """
        Inicializa o sistema de heartbeat.
        
        Args:
            component_id: ID único do componente
            debug: Flag para ativar o modo de depuração
        """
        self.component_id = component_id
        self.debug = debug
        self.targets: Dict[str, dict] = {}
        self.last_heartbeats: Dict[str, float] = {}
        self.callbacks: Dict[str, List[Callable]] = {}
        self.running = False
        self.heartbeat_interval = 5.0  # Exatamente 5 segundos, conforme especificação
        self._task: Optional[asyncio.Task] = None
        
        # Configura logs mais detalhados se debug estiver ativado
        if debug:
            log_level = logging.DEBUG
        else:
            log_level = logging.INFO
            
        logging.basicConfig(
            level=log_level,
            format=f'[{component_id}] %(asctime)s - %(levelname)s - %(message)s'
        )
        
        logger.debug("Sistema de heartbeat inicializado")
    
    def add_target(self, target_id: str, url: str, timeout: float = 2.0) -> None:
        """
        Adiciona um alvo para envio de heartbeats.
        
        Args:
            target_id: ID único do alvo
            url: URL completo do endpoint de heartbeat do alvo
            timeout: Tempo máximo de espera pela resposta
        """
        self.targets[target_id] = {
            "url": url,
            "timeout": timeout,
            "failures": 0,
            "status": "unknown"
        }
        self.last_heartbeats[target_id] = 0
        self.callbacks[target_id] = []
        logger.debug(f"Alvo adicionado: {target_id} -> {url}")
    
    def remove_target(self, target_id: str) -> None:
        """
        Remove um alvo do sistema de heartbeat.
        
        Args:
            target_id: ID do alvo a ser removido
        """
        if target_id in self.targets:
            del self.targets[target_id]
            del self.last_heartbeats[target_id]
            del self.callbacks[target_id]
            logger.debug(f"Alvo removido: {target_id}")
    
    def register_failure_callback(self, target_id: str, callback: Callable) -> None:
        """
        Registra uma função de callback para ser chamada quando um alvo falhar.
        
        Args:
            target_id: ID do alvo
            callback: Função a ser chamada em caso de falha
        """
        if target_id in self.callbacks:
            self.callbacks[target_id].append(callback)
            logger.debug(f"Callback registrado para {target_id}")
    
    async def _send_heartbeat(self, target_id: str, target_info: dict) -> bool:
        """
        Envia um heartbeat para um alvo específico.
        
        Args:
            target_id: ID do alvo
            target_info: Informações do alvo
            
        Returns:
            bool: True se o heartbeat foi bem-sucedido, False caso contrário
        """
        import aiohttp
        
        url = target_info["url"]
        timeout = target_info["timeout"]
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=timeout) as response:
                    if response.status == 200:
                        if self.debug:
                            logger.debug(f"Heartbeat bem-sucedido para {target_id}")
                        return True
                    else:
                        logger.warning(f"Heartbeat falhou para {target_id}: status {response.status}")
                        return False
        except Exception as e:
            logger.warning(f"Heartbeat falhou para {target_id}: {str(e)}")
            return False
    
    async def _heartbeat_loop(self) -> None:
        """
        Loop principal que envia heartbeats para todos os alvos a cada 5 segundos.
        """
        logger.info("Iniciando loop de heartbeat")
        
        while self.running:
            start_time = time.time()
            
            # Envia heartbeats para todos os alvos
            for target_id, target_info in self.targets.items():
                result = await self._send_heartbeat(target_id, target_info)
                
                if result:
                    # Heartbeat bem-sucedido
                    self.last_heartbeats[target_id] = start_time
                    if target_info["status"] == "down":
                        logger.info(f"Alvo {target_id} está online novamente")
                    target_info["status"] = "up"
                    target_info["failures"] = 0
                else:
                    # Heartbeat falhou
                    target_info["failures"] += 1
                    
                    if target_info["failures"] >= 3 and target_info["status"] != "down":
                        # Consideramos um componente como caído após 3 falhas consecutivas
                        logger.warning(f"Alvo {target_id} está fora do ar após {target_info['failures']} falhas")
                        target_info["status"] = "down"
                        
                        # Chama os callbacks registrados
                        for callback in self.callbacks[target_id]:
                            try:
                                callback(target_id)
                            except Exception as e:
                                logger.error(f"Erro no callback para {target_id}: {str(e)}")
            
            # Calcula o tempo para esperar até o próximo ciclo
            elapsed = time.time() - start_time
            wait_time = max(0, self.heartbeat_interval - elapsed)
            
            if self.debug:
                logger.debug(f"Ciclo de heartbeat completado em {elapsed:.3f}s, aguardando {wait_time:.3f}s")
            
            # Espera exatamente o tempo necessário para manter o intervalo de 5 segundos
            await asyncio.sleep(wait_time)
    
    def start(self) -> None:
        """
        Inicia o sistema de heartbeat.
        """
        if not self.running:
            self.running = True
            self._task = asyncio.create_task(self._heartbeat_loop())
            logger.info("Sistema de heartbeat iniciado")
    
    def stop(self) -> None:
        """
        Para o sistema de heartbeat.
        """
        if self.running:
            self.running = False
            if self._task:
                self._task.cancel()
            logger.info("Sistema de heartbeat parado")
    
    def get_status(self, target_id: str = None) -> dict:
        """
        Obtém o status de um alvo específico ou de todos os alvos.
        
        Args:
            target_id: ID do alvo (opcional)
            
        Returns:
            dict: Status do alvo ou de todos os alvos
        """
        if target_id:
            if target_id in self.targets:
                return {
                    "status": self.targets[target_id]["status"],
                    "last_heartbeat": self.last_heartbeats[target_id],
                    "failures": self.targets[target_id]["failures"]
                }
            return {"error": f"Alvo {target_id} não encontrado"}
        
        # Retorna o status de todos os alvos
        result = {}
        for tid, info in self.targets.items():
            result[tid] = {
                "status": info["status"],
                "last_heartbeat": self.last_heartbeats[tid],
                "failures": info["failures"]
            }
        return result