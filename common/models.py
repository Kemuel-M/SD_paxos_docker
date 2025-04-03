"""
Modelos de dados comuns para todos os componentes do sistema de consenso distribuído Paxos.
"""
from typing import Dict, Any, Optional, List, Union
from enum import Enum
from pydantic import BaseModel, Field


class OperationType(str, Enum):
    """Tipos de operação suportados."""
    READ = "READ"
    WRITE = "WRITE"


class MessageType(str, Enum):
    """Tipos de mensagens do protocolo Paxos."""
    PREPARE = "PREPARE"
    PROMISE = "PROMISE"
    ACCEPT = "ACCEPT"
    ACCEPTED = "ACCEPTED"
    LEARN = "LEARN"
    COMMIT = "COMMIT"
    ABORT = "ABORT"


class StatusResponse(str, Enum):
    """Status possíveis para respostas ao cliente."""
    COMMITTED = "COMMITTED"
    NOT_COMMITTED = "NOT_COMMITTED"


class ClientRequest(BaseModel):
    """Modelo para requisição de cliente."""
    clientId: str
    timestamp: int  # Unix timestamp em milissegundos
    operation: OperationType = OperationType.WRITE  # Sempre WRITE neste projeto
    resource: str = "R"  # Sempre "R" neste projeto
    data: str


class PrepareRequest(BaseModel):
    """Modelo para requisição prepare do Paxos."""
    type: MessageType = MessageType.PREPARE
    proposalNumber: int
    instanceId: int
    proposerId: int
    clientRequest: Dict[str, Any]


class PromiseResponse(BaseModel):
    """Modelo para resposta promise do Paxos."""
    type: MessageType = MessageType.PROMISE
    accepted: bool  # true: promise, false: not promise
    proposalNumber: int
    instanceId: int
    acceptorId: int
    highestAccepted: int = -1  # -1 se nenhum
    acceptedValue: Optional[Dict[str, Any]] = None  # null se nenhum


class AcceptRequest(BaseModel):
    """Modelo para requisição accept do Paxos."""
    type: MessageType = MessageType.ACCEPT
    proposalNumber: int
    instanceId: int
    proposerId: int
    value: Dict[str, Any]


class AcceptedResponse(BaseModel):
    """Modelo para resposta accepted do Paxos."""
    type: MessageType = MessageType.ACCEPTED
    accepted: bool  # true: accepted, false: not accepted
    proposalNumber: int
    instanceId: int
    acceptorId: int


class LearnMessage(BaseModel):
    """Modelo para mensagem learn do Paxos."""
    type: MessageType = MessageType.LEARN
    proposalNumber: int
    instanceId: int
    acceptorId: int
    accepted: bool
    value: Dict[str, Any]


class TwoPhaseCommitPrepareRequest(BaseModel):
    """Modelo para requisição prepare do 2PC."""
    type: MessageType = MessageType.PREPARE
    instanceId: int
    resource: str
    data: str
    clientId: str
    timestamp: int


class TwoPhaseCommitPrepareResponse(BaseModel):
    """Modelo para resposta prepare do 2PC."""
    ready: bool
    currentVersion: int


class TwoPhaseCommitCommitRequest(BaseModel):
    """Modelo para requisição commit do 2PC."""
    type: MessageType = MessageType.COMMIT
    instanceId: int
    resource: str
    data: str
    version: int
    clientId: str
    timestamp: int


class TwoPhaseCommitCommitResponse(BaseModel):
    """Modelo para resposta commit do 2PC."""
    success: bool
    resource: str
    version: int
    timestamp: int


class ClientNotification(BaseModel):
    """Modelo para notificação ao cliente."""
    status: StatusResponse
    instanceId: int
    resource: str
    timestamp: int


class HealthResponse(BaseModel):
    """Modelo para resposta de verificação de saúde."""
    status: str = "healthy"


class ResourceData(BaseModel):
    """Modelo para o recurso R."""
    data: str
    version: int = 0
    timestamp: int  # Unix timestamp em milissegundos
    node_id: int