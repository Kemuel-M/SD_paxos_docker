"""
Implementação do Cliente para o sistema de consenso distribuído Paxos.
"""
import asyncio
import aiohttp
import json
import sys
import random
import time
from typing import Dict, Any, Optional

from client.config import (
    CLIENT_ID,
    PROPOSER_ENDPOINT,
    TOTAL_REQUESTS,
    MIN_WAIT_TIME,
    MAX_WAIT_TIME,
    MAX_RETRIES,
    REQUEST_TIMEOUT
)
from client.utils import (
    generate_random_content,
    get_current_timestamp,
    get_random_wait_time,
    log_message
)


class Client:
    """
    Cliente para o sistema de consenso distribuído Paxos.
    """
    def __init__(self):
        """
        Inicializa o cliente.
        """
        self.client_id = CLIENT_ID
        self.proposer_endpoint = PROPOSER_ENDPOINT
        self.total_requests = TOTAL_REQUESTS
        self.completed_requests = 0
        self.failed_requests = 0
        self.session = None
        
    async def start(self):
        """
        Inicia o cliente.
        """
        log_message(f"Cliente {self.client_id} iniciado", {
            "proposer_endpoint": self.proposer_endpoint,
            "total_requests": self.total_requests
        })
        
        # Criar sessão HTTP
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        )
        
        try:
            # Executar ciclo de requisições
            await self.request_cycle()
        finally:
            # Fechar sessão HTTP
            await self.session.close()
            
        log_message(f"Cliente {self.client_id} finalizado", {
            "completed_requests": self.completed_requests,
            "failed_requests": self.failed_requests
        })
        
    async def request_cycle(self):
        """
        Executa o ciclo de requisições.
        """
        while self.completed_requests < self.total_requests:
            # Gerar conteúdo aleatório
            content = generate_random_content()
            
            # Enviar requisição
            success = await self.send_request(content)
            
            if success:
                self.completed_requests += 1
                log_message(f"Requisição {self.completed_requests}/{self.total_requests} completada")
                
                # Esperar tempo aleatório antes da próxima requisição
                wait_time = get_random_wait_time(MIN_WAIT_TIME, MAX_WAIT_TIME)
                log_message(f"Aguardando {wait_time} segundos antes da próxima requisição")
                await asyncio.sleep(wait_time)
            else:
                self.failed_requests += 1
                log_message(f"Requisição falhou ({self.failed_requests} falhas)")
                
                # Esperar um pouco antes de tentar novamente
                await asyncio.sleep(1.0)
        
    async def send_request(self, content: str) -> bool:
        """
        Envia uma requisição ao Proposer.
        
        Args:
            content: Conteúdo a ser enviado.
            
        Returns:
            True se a requisição foi bem-sucedida, False caso contrário.
        """
        for attempt in range(MAX_RETRIES):
            try:
                # Preparar requisição
                timestamp = get_current_timestamp()
                request_data = {
                    "clientId": self.client_id,
                    "timestamp": timestamp,
                    "operation": "WRITE",
                    "resource": "R",
                    "data": content
                }
                
                log_message(f"Enviando requisição (tentativa {attempt+1}/{MAX_RETRIES})", {
                    "timestamp": timestamp,
                    "data_length": len(content)
                })
                
                # Enviar requisição
                async with self.session.post(
                    f"{self.proposer_endpoint}/propose",
                    json=request_data
                ) as response:
                    status = response.status
                    response_data = await response.json()
                    
                    if status == 200 and response_data.get("success", False):
                        instance_id = response_data.get("instanceId")
                        log_message(f"Requisição aceita com instance_id={instance_id}")
                        
                        # Aguardar notificação COMMITTED
                        # Na implementação real, o cliente receberia uma notificação assíncrona
                        # Para simplificar, consideramos que a aceitação já é suficiente
                        return True
                    else:
                        log_message(f"Requisição rejeitada: status={status}", response_data)
                        
            except aiohttp.ClientError as e:
                log_message(f"Erro na requisição: {str(e)}")
                
            except asyncio.TimeoutError:
                log_message("Timeout na requisição")
                
            # Esperar antes de tentar novamente
            await asyncio.sleep(2 ** attempt)  # Backoff exponencial
            
        # Todas as tentativas falharam
        return False


async def main():
    """
    Função principal.
    """
    client = Client()
    await client.start()


if __name__ == "__main__":
    asyncio.run(main())