"""
Módulo de comunicação compartilhado entre componentes.
Fornece classes para comunicação HTTP com retry e circuit breaker.
"""
import time
import asyncio
import logging
from typing import Dict, Any, Optional, List, Tuple
import httpx

logger = logging.getLogger("communication")

class HttpClient:
    """
    Cliente HTTP para comunicação entre componentes.
    
    Características:
    1. Suporte a timeouts configuráveis
    2. Gerenciamento de conexões keep-alive
    3. Serialização/desserialização automática de JSON
    """
    
    def __init__(self, timeout: float = 30.0, max_connections: int = 100):
        """
        Inicializa o cliente HTTP.
        
        Args:
            timeout: Timeout padrão para requisições em segundos
            max_connections: Número máximo de conexões concorrentes
        """
        self.timeout = timeout
        self.limits = httpx.Limits(max_connections=max_connections)
        self._client = None
    
    @property
    def client(self) -> httpx.AsyncClient:
        """
        Obtém o cliente HTTP assíncrono, criando-o se necessário.
        
        Returns:
            httpx.AsyncClient: Cliente HTTP assíncrono
        """
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=self.timeout,
                limits=self.limits,
                follow_redirects=True
            )
        return self._client
    
    async def close(self):
        """Fecha o cliente HTTP."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None
    
    async def get(self, url: str, params: Optional[Dict[str, Any]] = None, 
                  headers: Optional[Dict[str, str]] = None, timeout: Optional[float] = None) -> Dict[str, Any]:
        """
        Faz uma requisição GET.
        
        Args:
            url: URL para requisição
            params: Parâmetros da query string
            headers: Cabeçalhos HTTP
            timeout: Timeout para esta requisição específica (sobrescreve o padrão)
        
        Returns:
            Dict[str, Any]: Resposta JSON convertida para dicionário
        
        Raises:
            httpx.HTTPError: Se ocorrer erro na requisição
        """
        response = await self.client.get(
            url, 
            params=params, 
            headers=headers, 
            timeout=timeout or self.timeout
        )
        response.raise_for_status()
        return response.json() if response.content else {}
    
    async def post(self, url: str, json: Optional[Dict[str, Any]] = None,
                  headers: Optional[Dict[str, str]] = None, timeout: Optional[float] = None) -> Dict[str, Any]:
        """
        Faz uma requisição POST.
        
        Args:
            url: URL para requisição
            json: Dados a serem enviados como JSON
            headers: Cabeçalhos HTTP
            timeout: Timeout para esta requisição específica (sobrescreve o padrão)
        
        Returns:
            Dict[str, Any]: Resposta JSON convertida para dicionário
        
        Raises:
            httpx.HTTPError: Se ocorrer erro na requisição
        """
        response = await self.client.post(
            url,
            json=json,
            headers=headers,
            timeout=timeout or self.timeout
        )
        response.raise_for_status()
        return response.json() if response.content else {}


class CircuitBreaker:
    """
    Implementa o padrão Circuit Breaker para proteção contra falhas em cascata.
    
    Estados:
    - CLOSED: Operação normal, requisições são encaminhadas
    - OPEN: Circuito aberto, requisições são rejeitadas imediatamente
    - HALF_OPEN: Estado intermediário, permite uma requisição de teste
    
    Regras:
    - Abre após 5 falhas consecutivas
    - Meio-aberto após 30 segundos
    - Fecha completamente após 3 requisições bem-sucedidas
    """
    
    # Estados do circuit breaker
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half-open"
    
    def __init__(self, failure_threshold: int = 5, recovery_timeout: int = 30, 
                 success_threshold: int = 3):
        """
        Inicializa o circuit breaker.
        
        Args:
            failure_threshold: Número de falhas consecutivas para abrir o circuito
            recovery_timeout: Tempo em segundos antes de tentar requisição de teste
            success_threshold: Número de sucessos consecutivos para fechar o circuito
        """
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.success_threshold = success_threshold
        
        self.state = self.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = 0
        self.lock = asyncio.Lock()
    
    def allow_request(self) -> bool:
        """
        Verifica se uma requisição deve ser permitida.
        
        Returns:
            bool: True se a requisição deve ser permitida, False caso contrário
        """
        # Se fechado, sempre permite
        if self.state == self.CLOSED:
            return True
        
        # Se aberto, verifica se está na hora de tentar novamente
        if self.state == self.OPEN:
            if time.time() - self.last_failure_time > self.recovery_timeout:
                self.state = self.HALF_OPEN
                return True
            return False
        
        # Se meio-aberto, permite apenas uma requisição por vez
        return True
    
    async def record_success(self):
        """Registra uma requisição bem-sucedida."""
        async with self.lock:
            self.failure_count = 0
            
            if self.state == self.HALF_OPEN:
                self.success_count += 1
                if self.success_count >= self.success_threshold:
                    self.state = self.CLOSED
                    self.success_count = 0
                    logger.info("Circuit breaker fechado após requisições bem-sucedidas")
    
    async def record_failure(self):
        """Registra uma requisição falha."""
        async with self.lock:
            self.last_failure_time = time.time()
            
            if self.state == self.CLOSED:
                self.failure_count += 1
                if self.failure_count >= self.failure_threshold:
                    self.state = self.OPEN
                    logger.warning(f"Circuit breaker aberto após {self.failure_count} falhas consecutivas")
            
            elif self.state == self.HALF_OPEN:
                self.state = self.OPEN
                self.success_count = 0
                logger.warning("Circuit breaker reaberto após falha em estado meio-aberto")


class RateLimiter:
    """
    Implementa limitação de taxa de requisições.
    
    Permite definir um número máximo de requisições por segundo.
    Requisições que excederem o limite são bloqueadas.
    """
    
    def __init__(self, max_rate: int = 100, window_size: float = 1.0):
        """
        Inicializa o limitador de taxa.
        
        Args:
            max_rate: Número máximo de requisições permitidas por janela de tempo
            window_size: Tamanho da janela de tempo em segundos
        """
        self.max_rate = max_rate
        self.window_size = window_size
        self.tokens = max_rate
        self.last_refill = time.time()
        self.lock = asyncio.Lock()
    
    async def acquire(self, tokens: int = 1) -> bool:
        """
        Tenta adquirir tokens para uma requisição.
        
        Args:
            tokens: Número de tokens a adquirir
        
        Returns:
            bool: True se adquiriu os tokens, False caso contrário
        """
        async with self.lock:
            # Reabastece tokens conforme o tempo passa
            now = time.time()
            elapsed = now - self.last_refill
            
            # Calcula quantos tokens adicionar
            to_add = elapsed * (self.max_rate / self.window_size)
            self.tokens = min(self.max_rate, self.tokens + to_add)
            self.last_refill = now
            
            # Verifica se há tokens suficientes
            if self.tokens >= tokens:
                self.tokens -= tokens
                return True
            else:
                return False
    
    async def wait_for_token(self, tokens: int = 1, timeout: Optional[float] = None) -> bool:
        """
        Aguarda até que tokens estejam disponíveis ou timeout expire.
        
        Args:
            tokens: Número de tokens a adquirir
            timeout: Tempo máximo de espera em segundos
        
        Returns:
            bool: True se adquiriu os tokens, False caso contrário
        """
        start_time = time.time()
        
        while True:
            # Verifica timeout
            if timeout is not None and time.time() - start_time > timeout:
                return False
            
            # Tenta adquirir tokens
            if await self.acquire(tokens):
                return True
            
            # Espera um pouco antes da próxima tentativa
            await asyncio.sleep(0.01)