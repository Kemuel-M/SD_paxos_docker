"""
Cliente para interação com o Cluster Store (Parte 2).
"""
import asyncio
import logging
import time
import random
from typing import Dict, Any, List, Tuple, Optional

from common.utils import HttpClient, get_current_timestamp, retry_with_backoff
from common.metrics import learner_metrics
from common.models import (
    TwoPhaseCommitPrepareRequest,
    TwoPhaseCommitPrepareResponse,
    TwoPhaseCommitCommitRequest,
    TwoPhaseCommitCommitResponse,
    ResourceData
)
from learner.config import (
    NODE_ID,
    STORE_ENDPOINTS,
    RETRY_BACKOFF_BASE,
    RETRY_BACKOFF_FACTOR,
    RETRY_MAX_ATTEMPTS
)


class StoreClient:
    """
    Cliente para interação com o Cluster Store.
    """
    def __init__(self, logger: logging.Logger):
        """
        Inicializa o cliente do Cluster Store.
        
        Args:
            logger: Logger configurado.
        """
        self.logger = logger
        
        # Clientes HTTP para o Cluster Store
        self.store_clients = {
            endpoint: HttpClient(endpoint) for endpoint in STORE_ENDPOINTS
        }
        
        # Índice para round-robin
        self.current_store_index = 0
        
    async def close(self):
        """
        Fecha as conexões HTTP.
        """
        for client in self.store_clients.values():
            await client.close()
            
    async def _get_next_store_endpoint(self) -> str:
        """
        Obtém o próximo endpoint do Cluster Store seguindo round-robin.
        
        Returns:
            Endpoint do Cluster Store.
        """
        # Implementação simples de round-robin
        endpoints = list(self.store_clients.keys())
        endpoint = endpoints[self.current_store_index % len(endpoints)]
        self.current_store_index += 1
        return endpoint
        
    async def write_resource(
        self, 
        instance_id: int, 
        resource_id: str, 
        data: str, 
        client_id: str, 
        client_timestamp: int
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        Escreve um recurso no Cluster Store usando protocolo ROWA.
        
        Args:
            instance_id: ID da instância.
            resource_id: ID do recurso.
            data: Dados a serem escritos.
            client_id: ID do cliente.
            client_timestamp: Timestamp do cliente.
            
        Returns:
            Tupla (success, response).
        """
        self.logger.info(f"Iniciando escrita do recurso {resource_id} para instance_id={instance_id}")
        
        # Executar protocolo 2PC
        prepare_ok, prepare_responses = await self._prepare_phase(
            instance_id, 
            resource_id, 
            data, 
            client_id, 
            client_timestamp
        )
        
        if not prepare_ok:
            self.logger.warning(f"Fase prepare do 2PC falhou para instance_id={instance_id}")
            learner_metrics["store_operations"].labels(
                node_id=NODE_ID, 
                operation="write", 
                status="failure"
            ).inc()
            return False, {"status": "FAILED", "reason": "2PC prepare phase failed"}
            
        # Obter a versão atual mais alta
        current_version = max([resp.get("currentVersion", 0) for resp in prepare_responses])
        new_version = current_version + 1
        
        commit_ok, commit_response = await self._commit_phase(
            instance_id, 
            resource_id, 
            data, 
            new_version, 
            client_id, 
            client_timestamp
        )
        
        if not commit_ok:
            self.logger.warning(f"Fase commit do 2PC falhou para instance_id={instance_id}")
            learner_metrics["store_operations"].labels(
                node_id=NODE_ID, 
                operation="write", 
                status="failure"
            ).inc()
            return False, {"status": "FAILED", "reason": "2PC commit phase failed"}
            
        self.logger.info(f"Escrita do recurso {resource_id} bem-sucedida para instance_id={instance_id}")
        learner_metrics["store_operations"].labels(
            node_id=NODE_ID, 
            operation="write", 
            status="success"
        ).inc()
        
        return True, commit_response
        
    async def read_resource(self, resource_id: str) -> Tuple[bool, Optional[ResourceData]]:
        """
        Lê um recurso do Cluster Store usando protocolo ROWA.
        
        Args:
            resource_id: ID do recurso.
            
        Returns:
            Tupla (success, resource_data).
        """
        self.logger.info(f"Iniciando leitura do recurso {resource_id}")
        
        # No protocolo ROWA, podemos ler de qualquer nó (Nr=1)
        store_endpoint = await self._get_next_store_endpoint()
        store_client = self.store_clients[store_endpoint]
        
        try:
            # Tentar ler o recurso
            async def read_attempt():
                status, response = await store_client.get(f"/resource/{resource_id}")
                if status != 200:
                    raise Exception(f"Falha ao ler recurso {resource_id}: status {status}")
                return response
                
            # Tentar com retentativas
            response = await retry_with_backoff(
                read_attempt,
                max_retries=RETRY_MAX_ATTEMPTS,
                base_delay=RETRY_BACKOFF_BASE,
                backoff_factor=RETRY_BACKOFF_FACTOR
            )
            
            # Incrementar contador de operações de leitura
            learner_metrics["store_operations"].labels(
                node_id=NODE_ID, 
                operation="read", 
                status="success"
            ).inc()
            
            # Converter para ResourceData
            return True, ResourceData(**response)
            
        except Exception as e:
            self.logger.warning(f"Erro ao ler recurso {resource_id}: {e}")
            
            # Incrementar contador de operações de leitura com falha
            learner_metrics["store_operations"].labels(
                node_id=NODE_ID, 
                operation="read", 
                status="failure"
            ).inc()
            
            # Tentar outros nós antes de desistir
            for alt_endpoint, alt_client in self.store_clients.items():
                if alt_endpoint == store_endpoint:
                    continue
                    
                try:
                    status, response = await alt_client.get(f"/resource/{resource_id}")
                    if status == 200:
                        # Incrementar contador de operações de leitura bem-sucedidas
                        learner_metrics["store_operations"].labels(
                            node_id=NODE_ID, 
                            operation="read", 
                            status="success"
                        ).inc()
                        
                        # Converter para ResourceData
                        return True, ResourceData(**response)
                except Exception as alt_e:
                    self.logger.warning(f"Erro ao ler recurso {resource_id} do nó alternativo {alt_endpoint}: {alt_e}")
                    
            # Se chegamos aqui, todas as tentativas falharam
            return False, None
            
    async def _prepare_phase(
        self, 
        instance_id: int, 
        resource_id: str, 
        data: str, 
        client_id: str, 
        client_timestamp: int
    ) -> Tuple[bool, List[Dict[str, Any]]]:
        """
        Executa a fase prepare do protocolo 2PC.
        
        Args:
            instance_id: ID da instância.
            resource_id: ID do recurso.
            data: Dados a serem escritos.
            client_id: ID do cliente.
            client_timestamp: Timestamp do cliente.
            
        Returns:
            Tupla (success, responses).
        """
        self.logger.info(f"Iniciando fase prepare do 2PC para instance_id={instance_id}")
        
        # Criar requisição prepare
        prepare_request = TwoPhaseCommitPrepareRequest(
            instanceId=instance_id,
            resource=resource_id,
            data=data,
            clientId=client_id,
            timestamp=client_timestamp
        )
        
        prepare_req_dict = prepare_request.dict()
        responses = []
        success = True
        
        # Enviar para todos os nós (Nw=3)
        for endpoint, client in self.store_clients.items():
            try:
                # Enviar requisição prepare
                status, response = await client.post("/prepare", prepare_req_dict)
                
                # Verificar se o nó está pronto
                if status == 200 and response.get("ready", False):
                    responses.append(response)
                else:
                    self.logger.warning(f"Nó {endpoint} não está pronto: {response}")
                    success = False
                    break
                    
            except Exception as e:
                self.logger.warning(f"Erro ao enviar prepare para {endpoint}: {e}")
                success = False
                break
                
        # No protocolo ROWA, precisamos de todos os nós (Nw=3)
        if not success or len(responses) != len(self.store_clients):
            # Se qualquer nó falhar, abortar a transação
            await self._abort_transaction(instance_id)
            return False, []
            
        return True, responses
        
    async def _commit_phase(
        self, 
        instance_id: int, 
        resource_id: str, 
        data: str, 
        version: int, 
        client_id: str, 
        client_timestamp: int
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        Executa a fase commit do protocolo 2PC.
        
        Args:
            instance_id: ID da instância.
            resource_id: ID do recurso.
            data: Dados a serem escritos.
            version: Nova versão do recurso.
            client_id: ID do cliente.
            client_timestamp: Timestamp do cliente.
            
        Returns:
            Tupla (success, response).
        """
        self.logger.info(f"Iniciando fase commit do 2PC para instance_id={instance_id}")
        
        # Criar requisição commit
        commit_request = TwoPhaseCommitCommitRequest(
            instanceId=instance_id,
            resource=resource_id,
            data=data,
            version=version,
            clientId=client_id,
            timestamp=client_timestamp
        )
        
        commit_req_dict = commit_request.dict()
        success = True
        responses = []
        
        # Enviar para todos os nós (Nw=3)
        for endpoint, client in self.store_clients.items():
            try:
                # Enviar requisição commit
                status, response = await client.post("/commit", commit_req_dict)
                
                # Verificar se o commit foi bem-sucedido
                if status == 200 and response.get("success", False):
                    responses.append(response)
                else:
                    self.logger.warning(f"Commit falhou no nó {endpoint}: {response}")
                    success = False
                    break
                    
            except Exception as e:
                self.logger.warning(f"Erro ao enviar commit para {endpoint}: {e}")
                success = False
                break
                
        # No protocolo ROWA, precisamos de todos os nós (Nw=3)
        if not success or len(responses) != len(self.store_clients):
            # Se qualquer nó falhar, abortar a transação
            await self._abort_transaction(instance_id)
            return False, {}
            
        # Retornar o último response (todos devem ser idênticos em caso de sucesso)
        return True, responses[-1]
        
    async def _abort_transaction(self, instance_id: int):
        """
        Aborta uma transação enviando um comando abort para todos os nós do Cluster Store.
        
        Args:
            instance_id: ID da instância.
        """
        self.logger.info(f"Abortando transação para instance_id={instance_id}")
        
        # Criar requisição abort
        abort_request = {"type": "ABORT", "instanceId": instance_id}
        
        for endpoint, client in self.store_clients.items():
            try:
                # Enviar requisição abort
                await client.post("/abort", abort_request)
            except Exception as e:
                self.logger.warning(f"Erro ao enviar abort para {endpoint}: {e}")