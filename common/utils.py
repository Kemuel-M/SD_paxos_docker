import asyncio
import json
import logging
import os
import time
import uuid
from typing import Any, Dict, List, Optional, Union

logger = logging.getLogger(__name__)

def get_env_var(var_name: str, default: Any = None) -> Any:
    """
    Obtém uma variável de ambiente, com valor padrão opcional.
    
    Args:
        var_name: Nome da variável de ambiente
        default: Valor padrão caso a variável não exista
        
    Returns:
        Valor da variável de ambiente ou o valor padrão
    """
    return os.environ.get(var_name, default)

def get_debug_mode() -> bool:
    """
    Verifica se o modo de depuração está ativado.
    
    Returns:
        bool: True se o modo de depuração estiver ativado, False caso contrário
    """
    debug_env = get_env_var("DEBUG", "false").lower()
    return debug_env in ("true", "1", "yes")

def setup_logging(component_id: str, debug: bool = None) -> None:
    """
    Configura o sistema de logging para o componente.
    
    Args:
        component_id: ID do componente
        debug: Flag para ativar o modo de depuração (se None, usa o valor da variável DEBUG)
    """
    if debug is None:
        debug = get_debug_mode()
    
    log_level = logging.DEBUG if debug else logging.INFO
    
    logging.basicConfig(
        level=log_level,
        format=f'[{component_id}] %(asctime)s - %(levelname)s - %(message)s'
    )
    
    logger.debug(f"Logging configurado para {component_id} (debug={debug})")

def generate_id() -> str:
    """
    Gera um ID único usando UUID4.
    
    Returns:
        str: ID único gerado
    """
    return str(uuid.uuid4())

def current_timestamp() -> int:
    """
    Obtém o timestamp atual em milissegundos.
    
    Returns:
        int: Timestamp atual em milissegundos
    """
    return int(time.time() * 1000)

async def http_request(method: str, url: str, data: Dict = None, 
                       timeout: float = 5.0, retries: int = 3, 
                       backoff_factor: float = 0.5) -> Dict:
    """
    Executa uma requisição HTTP com retry e backoff exponencial.
    
    Args:
        method: Método HTTP (GET, POST, etc.)
        url: URL do endpoint
        data: Dados a serem enviados (para POST, PUT, etc.)
        timeout: Tempo máximo de espera pela resposta
        retries: Número máximo de tentativas
        backoff_factor: Fator para o cálculo do tempo de espera entre tentativas
    
    Returns:
        Dict: Resposta da requisição como dicionário ou erro
    """
    import aiohttp
    
    attempt = 0
    last_exception = None
    
    while attempt < retries:
        try:
            async with aiohttp.ClientSession() as session:
                if method.upper() == 'GET':
                    async with session.get(url, timeout=timeout) as response:
                        if response.status == 200:
                            return await response.json()
                        else:
                            return {
                                "error": f"HTTP error: {response.status}",
                                "status": response.status
                            }
                elif method.upper() == 'POST':
                    async with session.post(url, json=data, timeout=timeout) as response:
                        if response.status in (200, 201):
                            return await response.json()
                        else:
                            return {
                                "error": f"HTTP error: {response.status}",
                                "status": response.status
                            }
                else:
                    return {"error": f"Método HTTP não suportado: {method}"}
        except Exception as e:
            last_exception = e
            attempt += 1
            
            if attempt < retries:
                # Calcula o tempo de espera com backoff exponencial
                wait_time = backoff_factor * (2 ** (attempt - 1))
                logger.warning(f"Tentativa {attempt} falhou: {str(e)}. Tentando novamente em {wait_time:.1f}s")
                await asyncio.sleep(wait_time)
            else:
                logger.error(f"Todas as {retries} tentativas falharam: {str(e)}")
    
    return {
        "error": f"Todas as tentativas falharam: {str(last_exception)}",
        "status": 0
    }

def parse_json(data: str) -> Dict:
    """
    Faz o parse de uma string JSON com tratamento de erros.
    
    Args:
        data: String JSON
    
    Returns:
        Dict: Dados parseados ou dicionário de erro
    """
    try:
        return json.loads(data)
    except json.JSONDecodeError as e:
        logger.error(f"Erro ao fazer parse do JSON: {str(e)}")
        return {"error": f"Erro ao fazer parse do JSON: {str(e)}"}

def validate_resource_data(data: Dict) -> bool:
    """
    Valida se os dados do recurso R estão no formato correto.
    
    Args:
        data: Dados do recurso R
    
    Returns:
        bool: True se os dados são válidos, False caso contrário
    """
    # Implementação simples para validação de dados do recurso
    # Pode ser expandida conforme necessário
    if not isinstance(data, dict):
        return False
    
    # Adicione validações específicas para o formato do recurso R aqui
    
    return True

class DistributedCounter:
    """
    Implementação de um contador distribuído seguro para geração de TIDs.
    """
    
    def __init__(self, initial_value: int = 0):
        """
        Inicializa o contador distribuído.
        
        Args:
            initial_value: Valor inicial do contador
        """
        self.value = initial_value
        self.lock = asyncio.Lock()
    
    async def get_next(self) -> int:
        """
        Obtém o próximo valor do contador de forma thread-safe.
        
        Returns:
            int: Próximo valor do contador
        """
        async with self.lock:
            self.value += 1
            return self.value
    
    async def update_if_greater(self, new_value: int) -> int:
        """
        Atualiza o contador se o novo valor for maior que o valor atual.
        
        Args:
            new_value: Novo valor proposto para o contador
        
        Returns:
            int: Valor atual do contador após a possível atualização
        """
        async with self.lock:
            if new_value > self.value:
                self.value = new_value
            return self.value
    
    def get_current(self) -> int:
        """
        Obtém o valor atual do contador.
        
        Returns:
            int: Valor atual do contador
        """
        return self.value