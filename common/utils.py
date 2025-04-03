"""
Utilidades comuns para todos os componentes.
"""
import os
import time
import random
import asyncio
import aiohttp
from typing import Dict, Any, Optional, List, Tuple, Union, Callable
import json
import logging
import zlib


def get_env_int(name: str, default: int) -> int:
    """
    Obtém um valor inteiro de uma variável de ambiente.
    
    Args:
        name: Nome da variável de ambiente.
        default: Valor padrão caso a variável não exista.
        
    Returns:
        Valor inteiro da variável de ambiente ou o valor padrão.
    """
    value = os.environ.get(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def get_env_float(name: str, default: float) -> float:
    """
    Obtém um valor float de uma variável de ambiente.
    
    Args:
        name: Nome da variável de ambiente.
        default: Valor padrão caso a variável não exista.
        
    Returns:
        Valor float da variável de ambiente ou o valor padrão.
    """
    value = os.environ.get(name)
    if value is None:
        return default
    try:
        return float(value)
    except ValueError:
        return default


def get_env_str(name: str, default: str) -> str:
    """
    Obtém um valor string de uma variável de ambiente.
    
    Args:
        name: Nome da variável de ambiente.
        default: Valor padrão caso a variável não exista.
        
    Returns:
        Valor string da variável de ambiente ou o valor padrão.
    """
    return os.environ.get(name, default)


def get_current_timestamp() -> int:
    """
    Obtém o timestamp atual em milissegundos.
    
    Returns:
        Timestamp atual em milissegundos.
    """
    return int(time.time() * 1000)


def get_random_wait_time(min_seconds: float, max_seconds: float) -> float:
    """
    Gera um tempo de espera aleatório entre min_seconds e max_seconds.
    
    Args:
        min_seconds: Tempo mínimo em segundos.
        max_seconds: Tempo máximo em segundos.
        
    Returns:
        Tempo de espera aleatório em segundos.
    """
    return round(random.uniform(min_seconds, max_seconds), 3)


async def retry_with_backoff(
    func: Callable, 
    max_retries: int = 3, 
    base_delay: float = 0.5, 
    max_delay: float = 4.0,
    backoff_factor: float = 2.0,
    jitter: float = 0.2
) -> Any:
    """
    Executa uma função com retentativas e backoff exponencial.
    
    Args:
        func: Função a ser executada.
        max_retries: Número máximo de retentativas.
        base_delay: Delay base em segundos.
        max_delay: Delay máximo em segundos.
        backoff_factor: Fator de backoff exponencial.
        jitter: Fator de aleatoriedade para o delay.
        
    Returns:
        Resultado da função.
        
    Raises:
        Exception: Caso todas as retentativas falhem.
    """
    retries = 0
    last_exception = None
    
    while retries <= max_retries:
        try:
            return await func()
        except Exception as e:
            last_exception = e
            retries += 1
            if retries > max_retries:
                break
                
            # Calcular delay com backoff exponencial
            delay = min(base_delay * (backoff_factor ** (retries - 1)), max_delay)
            
            # Adicionar jitter (±jitter%)
            jitter_amount = delay * jitter
            delay = delay + random.uniform(-jitter_amount, jitter_amount)
            
            # Esperar
            await asyncio.sleep(delay)
    
    raise last_exception


class CircuitBreaker:
    """
    Implementação de um circuit breaker para proteger contra falhas em serviços externos.
    """
    # Estados possíveis
    CLOSED = 'closed'  # Normal operation, requests flow through
    OPEN = 'open'      # Circuit is open, requests fail fast
    HALF_OPEN = 'half_open'  # Testing if service is back
    
    def __init__(
        self, 
        failure_threshold: int = 5, 
        recovery_timeout: float = 30.0, 
        recovery_success_threshold: int = 3
    ):
        """
        Inicializa o circuit breaker.
        
        Args:
            failure_threshold: Número de falhas consecutivas para abrir o circuito.
            recovery_timeout: Tempo em segundos para passar para o estado HALF_OPEN.
            recovery_success_threshold: Número de sucessos consecutivos para fechar o circuito.
        """
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.recovery_success_threshold = recovery_success_threshold
        
        self.state = self.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = 0
        
    async def execute(self, func: Callable) -> Any:
        """
        Executa uma função com proteção do circuit breaker.
        
        Args:
            func: Função a ser executada.
            
        Returns:
            Resultado da função.
            
        Raises:
            Exception: Caso o circuito esteja aberto ou a função falhe.
        """
        # Verificar estado atual
        if self.state == self.OPEN:
            if time.time() - self.last_failure_time >= self.recovery_timeout:
                # Passar para HALF_OPEN após o timeout
                self.state = self.HALF_OPEN
                self.success_count = 0
            else:
                # Falha rápida se o circuito estiver aberto
                raise Exception("Circuit breaker is open")
                
        try:
            result = await func()
            
            # Registrar sucesso
            if self.state == self.HALF_OPEN:
                self.success_count += 1
                if self.success_count >= self.recovery_success_threshold:
                    # Fechar o circuito após sucessos consecutivos
                    self.state = self.CLOSED
                    self.failure_count = 0
            elif self.state == self.CLOSED:
                # Resetar contagem de falhas após sucesso
                self.failure_count = 0
                
            return result
            
        except Exception as e:
            # Registrar falha
            self.failure_count += 1
            self.last_failure_time = time.time()
            
            if self.state == self.CLOSED and self.failure_count >= self.failure_threshold:
                # Abrir o circuito após falhas consecutivas
                self.state = self.OPEN
                
            if self.state == self.HALF_OPEN:
                # Voltar para OPEN em caso de falha durante teste
                self.state = self.OPEN
                
            raise e


class HttpClient:
    """
    Cliente HTTP com suporte a circuit breaker, retentativas e compressão.
    """
    def __init__(
        self, 
        base_url: str,
        timeout: float = 0.5,  # 500ms default timeout
        max_retries: int = 3,
        circuit_breaker_threshold: int = 5,
        circuit_breaker_timeout: float = 30.0
    ):
        """
        Inicializa o cliente HTTP.
        
        Args:
            base_url: URL base para requisições.
            timeout: Timeout para requisições em segundos.
            max_retries: Número máximo de retentativas.
            circuit_breaker_threshold: Threshold para o circuit breaker.
            circuit_breaker_timeout: Timeout para o circuit breaker.
        """
        self.base_url = base_url
        self.timeout = timeout
        self.max_retries = max_retries
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=circuit_breaker_threshold,
            recovery_timeout=circuit_breaker_timeout
        )
        self.session = None
        
    async def ensure_session(self):
        """
        Garante que a sessão HTTP esteja inicializada.
        """
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self.timeout),
                headers={"Connection": "keep-alive"}
            )
            
    async def close(self):
        """
        Fecha a sessão HTTP.
        """
        if self.session and not self.session.closed:
            await self.session.close()
            self.session = None
            
    async def request(
        self, 
        method: str, 
        endpoint: str, 
        data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, Any]] = None
    ) -> Tuple[int, Dict[str, Any]]:
        """
        Realiza uma requisição HTTP com circuit breaker e retentativas.
        
        Args:
            method: Método HTTP (GET, POST, etc).
            endpoint: Endpoint da requisição.
            data: Dados para enviar no corpo da requisição.
            params: Parâmetros para a URL.
            headers: Cabeçalhos adicionais.
            
        Returns:
            Tupla (status, json_response).
            
        Raises:
            Exception: Em caso de falha após retentativas.
        """
        await self.ensure_session()
        
        url = f"{self.base_url}{endpoint}"
        _headers = headers or {}
        
        # Aplicar compressão gzip para mensagens maiores que 1KB
        if data:
            json_data = json.dumps(data)
            if len(json_data) > 1024:  # 1KB
                _headers["Content-Encoding"] = "gzip"
                compressed_data = zlib.compress(json_data.encode())
                _headers["Content-Type"] = "application/json"
                _data = compressed_data
            else:
                _headers["Content-Type"] = "application/json"
                _data = json_data
        else:
            _data = None
                
        async def _do_request():
            # Implementação da requisição HTTP
            async with getattr(self.session, method.lower())(
                url, 
                data=_data, 
                params=params, 
                headers=_headers
            ) as response:
                if response.status == 429:  # Too Many Requests
                    retry_after = int(response.headers.get("Retry-After", "1"))
                    await asyncio.sleep(retry_after)
                    # Raise para que a retentativa seja tratada
                    raise Exception(f"Rate limited. Retry after {retry_after}s")
                    
                try:
                    json_response = await response.json()
                except:
                    json_response = {}
                    
                return response.status, json_response
                
        # Executar com circuit breaker e retentativas
        return await self.circuit_breaker.execute(
            lambda: retry_with_backoff(_do_request, max_retries=self.max_retries)
        )
        
    async def get(
        self, 
        endpoint: str, 
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, Any]] = None
    ) -> Tuple[int, Dict[str, Any]]:
        """
        Realiza uma requisição GET.
        """
        return await self.request("GET", endpoint, params=params, headers=headers)
        
    async def post(
        self, 
        endpoint: str, 
        data: Dict[str, Any],
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, Any]] = None
    ) -> Tuple[int, Dict[str, Any]]:
        """
        Realiza uma requisição POST.
        """
        return await self.request("POST", endpoint, data=data, params=params, headers=headers)


def compute_client_hash(client_id: str) -> int:
    """
    Calcula o hash do ID do cliente para determinar o learner responsável.
    
    Args:
        client_id: ID do cliente.
        
    Returns:
        Hash CRC32 do ID do cliente.
    """
    return zlib.crc32(client_id.encode()) & 0xffffffff