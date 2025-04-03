"""
Implementação do componente Learner do protocolo Paxos.
"""
import asyncio
import time
import json
import logging
import zlib
from typing import Dict, Any, List, Tuple, Optional, Set

from common.utils import HttpClient, get_current_timestamp, compute_client_hash, retry_with_backoff
from common.metrics import learner_metrics
from common.models import (
    LearnMessage,
    StatusResponse,
    ClientNotification
)
from learner import persistence
from learner.config import (
    NODE_ID,
    QUORUM_SIZE,
    CLIENT_NOTIFICATION_TIMEOUT,
    BACKUP_LEARNER_DELAY
)
from learner.store_client import StoreClient


class Learner:
    """
    Implementação do componente Learner do protocolo Paxos.
    """
    def __init__(self, logger: logging.Logger):
        """
        Inicializa o learner.
        
        Args:
            logger: Logger configurado.
        """
        self.logger = logger
        
        # Estado do learner
        self.learned_values = {}  # {instance_id: {proposal_number, value, timestamp}}
        self.current_votes = {}  # {instance_id: {value_hash: {acceptors, proposal_number, value}}}
        self.notification_tasks = {}  # {instance_id: asyncio.Task}
        
        # Cliente para o Cluster Store
        self.store_client = None
        
    async def initialize(self):
        """
        Inicializa o learner carregando dados persistentes.
        """
        # Carregar valores aprendidos
        self.learned_values = await persistence.load_learned_values()
        
        # Inicializar cliente do Cluster Store
        self.store_client = StoreClient(self.logger)
        
        self.logger.info(f"Learner inicializado: learned_values={len(self.learned_values)}")
        
    async def shutdown(self):
        """
        Finaliza o learner, salvando dados persistentes.
        """
        # Salvar valores aprendidos
        await persistence.save_learned_values(self.learned_values)
        
        # Cancelar tarefas de notificação pendentes
        for task in self.notification_tasks.values():
            if not task.done():
                task.cancel()
                
        # Fechar cliente do Cluster Store
        if self.store_client:
            await self.store_client.close()
        
        self.logger.info("Learner finalizado")
        
    async def learn(self, message: LearnMessage):
        """
        Processa uma mensagem learn do protocolo Paxos.
        
        Args:
            message: Mensagem learn.
        """
        # Incrementar contador de mensagens learn recebidas
        learner_metrics["learn_messages"].labels(node_id=NODE_ID).inc()
        
        # Extrair dados da mensagem
        proposal_number = message.proposalNumber
        instance_id = message.instanceId
        acceptor_id = message.acceptorId
        accepted = message.accepted
        value = message.value
        
        if not accepted:
            # Ignorar mensagens não aceitas
            return
            
        # Converter instance_id para string para usar como chave no dicionário
        instance_id_str = str(instance_id)
        
        # Verificar se já aprendemos um valor para esta instância
        if instance_id_str in self.learned_values:
            # Já aprendemos um valor, ignorar
            self.logger.info(f"Valor já aprendido para instance_id={instance_id}")
            return
            
        # Calcular hash do valor para usar como chave
        value_str = json.dumps(value, sort_keys=True)
        value_hash = str(zlib.crc32(value_str.encode()))
        
        # Inicializar estrutura de votes se necessário
        if instance_id_str not in self.current_votes:
            self.current_votes[instance_id_str] = {}
            
        if value_hash not in self.current_votes[instance_id_str]:
            self.current_votes[instance_id_str][value_hash] = {
                "acceptors": set(),
                "proposal_number": proposal_number,
                "value": value
            }
            
        # Registrar voto deste acceptor
        self.current_votes[instance_id_str][value_hash]["acceptors"].add(acceptor_id)
        
        # Verificar se atingimos o quórum
        if len(self.current_votes[instance_id_str][value_hash]["acceptors"]) >= QUORUM_SIZE:
            # Valor decidido pela maioria dos acceptors
            await self._process_decided_value(instance_id, proposal_number, value)
            
    async def _process_decided_value(self, instance_id: int, proposal_number: int, value: Dict[str, Any]):
        """
        Processa um valor decidido pelo consenso.
        
        Args:
            instance_id: ID da instância.
            proposal_number: Número da proposta.
            value: Valor decidido.
        """
        instance_id_str = str(instance_id)
        
        # Verificar se já aprendemos um valor para esta instância
        if instance_id_str in self.learned_values:
            # Já aprendemos um valor, ignorar
            return
            
        # Registrar valor aprendido
        self.learned_values[instance_id_str] = {
            "proposal_number": proposal_number,
            "value": value,
            "timestamp": get_current_timestamp()
        }
        
        # Persistir valores aprendidos
        await persistence.save_learned_values(self.learned_values)
        
        # Incrementar contador de consensos alcançados
        learner_metrics["consensus_reached"].labels(node_id=NODE_ID).inc()
        
        self.logger.info(f"Consenso alcançado para instance_id={instance_id}")
        
        # Verificar se sou o learner responsável por notificar o cliente
        client_id = value.get("clientId", "")
        learner_hash = 1 + (compute_client_hash(client_id) % 2)  # Mapear para 1 ou 2
        
        if learner_hash == NODE_ID:
            # Sou o learner responsável, iniciar tarefa de notificação
            self.logger.info(f"Learner {NODE_ID} é responsável por notificar o cliente {client_id}")
            self._start_notification_task(instance_id, value)
        else:
            # Não sou o learner responsável, aguardar como backup
            self.logger.info(f"Learner {NODE_ID} é backup para notificar o cliente {client_id}")
            self._start_backup_notification_task(instance_id, value)
            
        # Limpar votos desta instância para economizar memória
        if instance_id_str in self.current_votes:
            del self.current_votes[instance_id_str]
            
    def _start_notification_task(self, instance_id: int, value: Dict[str, Any]):
        """
        Inicia uma tarefa assíncrona para notificar o cliente.
        
        Args:
            instance_id: ID da instância.
            value: Valor decidido.
        """
        instance_id_str = str(instance_id)
        
        # Cancelar tarefa anterior se existir
        if instance_id_str in self.notification_tasks and not self.notification_tasks[instance_id_str].done():
            self.notification_tasks[instance_id_str].cancel()
            
        # Criar nova tarefa
        self.notification_tasks[instance_id_str] = asyncio.create_task(
            self._notify_client(instance_id, value)
        )
        
    def _start_backup_notification_task(self, instance_id: int, value: Dict[str, Any]):
        """
        Inicia uma tarefa assíncrona para notificar o cliente como backup.
        
        Args:
            instance_id: ID da instância.
            value: Valor decidido.
        """
        instance_id_str = str(instance_id)
        
        # Cancelar tarefa anterior se existir
        if instance_id_str in self.notification_tasks and not self.notification_tasks[instance_id_str].done():
            self.notification_tasks[instance_id_str].cancel()
            
        # Criar nova tarefa com delay
        self.notification_tasks[instance_id_str] = asyncio.create_task(
            self._notify_client_backup(instance_id, value)
        )
        
    async def _notify_client(self, instance_id: int, value: Dict[str, Any]):
        """
        Notifica o cliente sobre o resultado da proposta.
        
        Args:
            instance_id: ID da instância.
            value: Valor decidido.
        """
        client_id = value.get("clientId", "")
        resource = value.get("resource", "R")
        operation = value.get("operation", "WRITE")
        data = value.get("data", "")
        timestamp = value.get("timestamp", 0)
        
        self.logger.info(f"Notificando cliente {client_id} sobre instance_id={instance_id}")
        
        # Na Parte 1, simular acesso ao recurso
        # Na Parte 2, realizar acesso real ao recurso
        if self.store_client is None:
            # Parte 1 - Simular acesso bem-sucedido
            success = True
            response = {}
        else:
            # Parte 2 - Realizar acesso ao recurso via Cluster Store
            if operation == "WRITE":
                success, response = await self.store_client.write_resource(
                    instance_id, 
                    resource, 
                    data, 
                    client_id, 
                    timestamp
                )
            else:
                # Operação não suportada
                success = False
                response = {"error": "Unsupported operation"}
                
        # Preparar notificação para o cliente
        notification = ClientNotification(
            status=StatusResponse.COMMITTED if success else StatusResponse.NOT_COMMITTED,
            instanceId=instance_id,
            resource=resource,
            timestamp=get_current_timestamp()
        )
        
        # Incrementar contador de notificações enviadas
        learner_metrics["client_notifications"].labels(
            node_id=NODE_ID,
            status=notification.status
        ).inc()
        
        # TODO: Enviar notificação ao cliente
        # Na implementação real, enviaríamos uma requisição HTTP para o cliente
        # usando um endpoint de callback fornecido na requisição original
        # Por simplicidade, apenas registramos que a notificação foi "enviada"
        self.logger.info(f"Notificação enviada para o cliente {client_id}: {notification.dict()}")
        
    async def _notify_client_backup(self, instance_id: int, value: Dict[str, Any]):
        """
        Notifica o cliente como backup após um delay.
        
        Args:
            instance_id: ID da instância.
            value: Valor decidido.
        """
        # Esperar o delay para dar tempo ao learner principal
        await asyncio.sleep(BACKUP_LEARNER_DELAY)
        
        # Verificar se a instância ainda está na lista de valores aprendidos
        # Se sim, o learner principal provavelmente falhou
        instance_id_str = str(instance_id)
        if instance_id_str in self.learned_values:
            self.logger.info(f"Learner backup iniciando notificação para instance_id={instance_id}")
            await self._notify_client(instance_id, value)
        
    def _get_value_hash(self, value: Dict[str, Any]) -> str:
        """
        Calcula o hash de um valor.
        
        Args:
            value: Valor a ser hasheado.
            
        Returns:
            Hash do valor.
        """
        value_str = json.dumps(value, sort_keys=True)
        return str(zlib.crc32(value_str.encode()))
        
    def get_status(self) -> Dict[str, Any]:
        """
        Obtém o status atual do learner.
        
        Returns:
            Status do learner.
        """
        return {
            "node_id": NODE_ID,
            "role": "learner",
            "learned_values_count": len(self.learned_values),
            "current_votes_count": len(self.current_votes),
            "active_notification_tasks": sum(1 for task in self.notification_tasks.values() if not task.done()),
            "timestamp": get_current_timestamp()
        }