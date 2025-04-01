import os
import yaml
import json
import time
import asyncio
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import structlog
from prometheus_client import Counter, Gauge, generate_latest
import aiohttp
from typing import Dict, Optional, Any
from tinydb import TinyDB, Query
import aiofiles

# Configurar logging estruturado
log = structlog.get_logger()

# Carregar configuração
def load_config():
    with open("config/config.yaml", "r") as f:
        config = yaml.safe_load(f)
    
    # Substituir variáveis de ambiente
    config["node"]["id"] = int(os.environ.get("NODE_ID", "1"))
    
    return config

config = load_config()

# Inicializar a aplicação FastAPI
app = FastAPI(title="Paxos Acceptor")

# Configurar CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Métricas Prometheus
promises_total = Counter("paxos_acceptor_promises_total", "Total number of promises made")
accepts_total = Counter("paxos_acceptor_accepts_total", "Total number of proposals accepted")
rejects_total = Counter("paxos_acceptor_rejects_total", "Total number of proposals rejected")
disk_operations = Counter("paxos_acceptor_disk_operations_total", "Total number of persistence operations")
active_promises = Gauge("paxos_acceptor_active_promises", "Number of active promises")
active_accepts = Gauge("paxos_acceptor_active_accepts", "Number of active accepts")

# Modelos de dados Pydantic
class PrepareRequest(BaseModel):
    """Modelo para solicitações de fase 'prepare' do protocolo Paxos"""
    proposal_number: int
    instance_id: int
    proposer_id: int

class AcceptRequest(BaseModel):
    """Modelo para solicitações de fase 'accept' do protocolo Paxos"""
    proposal_number: int
    instance_id: int
    proposer_id: int
    value: Dict[str, Any]

# Estado do acceptor
class AcceptorState:
    def __init__(self, node_id):
        self.node_id = node_id
        self.promises = {}  # instanceId -> {highestPromised, lastPromiseTimestamp}
        self.accepted_values = {}  # instanceId -> {acceptedProposalNumber, acceptedValue, acceptTimestamp}
        
        # Garantir que o diretório de dados existe
        os.makedirs("/app/data", exist_ok=True)
        
        self.db = TinyDB("/app/data/acceptor.json")
        self.load_state()
        
        # Atualizar métricas
        active_promises.set(len(self.promises))
        active_accepts.set(len(self.accepted_values))
    
    def load_state(self):
        """Carrega o estado persistido do disco"""
        try:
            log.info("Loading state from disk")
            promises_table = self.db.table("promises")
            for promise in promises_table.all():
                self.promises[promise["instance_id"]] = {
                    "highestPromised": promise["highestPromised"],
                    "lastPromiseTimestamp": promise["lastPromiseTimestamp"]
                }
                
            accepted_table = self.db.table("accepted")
            for accepted in accepted_table.all():
                self.accepted_values[accepted["instance_id"]] = {
                    "acceptedProposalNumber": accepted["acceptedProposalNumber"],
                    "acceptedValue": accepted["acceptedValue"],
                    "acceptTimestamp": accepted["acceptTimestamp"]
                }
            log.info("State loaded successfully", 
                    promises_count=len(self.promises), 
                    accepted_count=len(self.accepted_values))
        except Exception as e:
            log.error("Error loading state", error=str(e))
    
    async def save_promise(self, instance_id, proposal_number):
        """Persiste uma promessa para um número de proposta"""
        try:
            log.debug("Saving promise", 
                     instance_id=instance_id, 
                     proposal_number=proposal_number)
            promises_table = self.db.table("promises")
            Promise = Query()
            promises_table.upsert(
                {
                    "instance_id": instance_id,
                    "highestPromised": proposal_number,
                    "lastPromiseTimestamp": time.time()
                },
                Promise.instance_id == instance_id
            )
            disk_operations.inc()
            
            # Atualizar métrica
            active_promises.set(len(self.promises))
        except Exception as e:
            log.error("Error saving promise", 
                     error=str(e), 
                     instance_id=instance_id)
    
    async def save_accepted(self, instance_id, proposal_number, value):
        """Persiste um valor aceito para uma instância"""
        try:
            log.debug("Saving accepted value", 
                     instance_id=instance_id, 
                     proposal_number=proposal_number)
            accepted_table = self.db.table("accepted")
            Accepted = Query()
            accepted_table.upsert(
                {
                    "instance_id": instance_id,
                    "acceptedProposalNumber": proposal_number,
                    "acceptedValue": value,
                    "acceptTimestamp": time.time()
                },
                Accepted.instance_id == instance_id
            )
            disk_operations.inc()
            
            # Atualizar métrica
            active_accepts.set(len(self.accepted_values))
        except Exception as e:
            log.error("Error saving accepted value", 
                     error=str(e), 
                     instance_id=instance_id)

# Inicializar o estado do acceptor
state = AcceptorState(config["node"]["id"])

# Rotas da API
@app.post("/prepare")
async def prepare(request: PrepareRequest):
    """
    Endpoint para a fase 'prepare' do protocolo Paxos.
    O proposer envia um número de proposta, e o acceptor decide se promete
    não aceitar propostas com números menores.
    """
    instance_id = request.instance_id
    proposal_number = request.proposal_number
    proposer_id = request.proposer_id
    
    log.info("Received prepare request", 
            instance_id=instance_id, 
            proposal_number=proposal_number, 
            proposer_id=proposer_id)
    
    # Verificar se já existe uma promessa para esta instância
    if instance_id in state.promises:
        highestPromised = state.promises[instance_id]["highestPromised"]
        
        # Se já prometemos para um número maior, rejeitar
        if highestPromised > proposal_number:
            log.info("Rejecting prepare request", 
                    instance_id=instance_id, 
                    proposal_number=proposal_number, 
                    highest_promised=highest_promised)
            rejects_total.inc()
            return {
                "accepted": False,
                "highestPromised": highestPromised
            }
    
    # Prometer não aceitar propostas com número menor
    state.promises[instance_id] = {
        "highestPromised": proposal_number,
        "lastPromiseTimestamp": time.time()
    }
    
    # Persistir promessa
    await state.save_promise(instance_id, proposal_number)
    promises_total.inc()
    
    # Verificar se já aceitamos algum valor para esta instância
    accepted_value = None
    accepted_proposal_number = None
    
    if instance_id in state.accepted_values:
        accepted_proposal_number = state.accepted_values[instance_id]["acceptedProposalNumber"]
        accepted_value = state.accepted_values[instance_id]["acceptedValue"]
        
        log.info("Promising with previously accepted value", 
                instance_id=instance_id, 
                accepted_proposal_number=accepted_proposal_number)
    else:
        log.info("Promising with no previous value", 
                instance_id=instance_id)
    
    return {
        "accepted": True,
        "highest_accepted": accepted_proposal_number,
        "accepted_value": accepted_value
    }

@app.post("/accept")
async def accept(request: AcceptRequest):
    """
    Endpoint para a fase 'accept' do protocolo Paxos.
    O proposer envia um número de proposta e um valor, e o acceptor
    decide se aceita a proposta.
    """
    instance_id = request.instance_id
    proposal_number = request.proposal_number
    proposer_id = request.proposer_id
    value = request.value
    
    log.info("Received accept request", 
            instance_id=instance_id, 
            proposal_number=proposal_number, 
            proposer_id=proposer_id)
    
    # Verificar se já prometemos para um número maior
    if instance_id in state.promises:
        highestPromised = state.promises[instance_id]["highestPromised"]
        
        # Se já prometemos para um número maior, rejeitar
        if highestPromised > proposal_number:
            log.info("Rejecting accept request", 
                    instance_id=instance_id, 
                    proposal_number=proposal_number, 
                    highest_promised=highest_promised)
            rejects_total.inc()
            return {
                "accepted": False,
                "highestPromised": highestPromised
            }
    
    # Aceitar a proposta
    state.accepted_values[instance_id] = {
        "acceptedProposalNumber": proposal_number,
        "acceptedValue": value,
        "acceptTimestamp": time.time()
    }
    
    # Persistir valor aceito
    await state.save_accepted(instance_id, proposal_number, value)
    accepts_total.inc()
    
    log.info("Accepted proposal", 
            instance_id=instance_id, 
            proposal_number=proposal_number)
    
    # Notificar learners em background
    asyncio.create_task(notify_learners(instance_id, proposal_number, value))
    
    return {
        "accepted": True
    }

async def notify_learners(instance_id, proposal_number, value):
    """
    Notifica os learners sobre um valor aceito.
    Este método é executado em segundo plano.
    """
    log.info("Notifying learners", 
            instance_id=instance_id, 
            proposal_number=proposal_number, 
            learners=config["networking"]["learners"])
    
    for learner_url in config["networking"]["learners"]:
        try:
            async with aiohttp.ClientSession() as session:
                log.debug("Sending notification to learner", 
                        learner=learner_url, 
                        instance_id=instance_id)
                
                response = await session.post(
                    f"http://{learner_url}/learn",
                    json={
                        "instance_id": instance_id,
                        "proposal_number": proposal_number,
                        "acceptor_id": state.node_id,
                        "value": value
                    }
                )
                
                if response.status != 200:
                    log.warning("Unexpected status from learner", 
                               learner=learner_url, 
                               status=response.status)
        except Exception as e:
            log.error("Error notifying learner", 
                     learner=learner_url, 
                     error=str(e))

@app.get("/status")
async def get_status():
    """
    Retorna informações sobre o estado atual do acceptor.
    Útil para monitoramento e depuração.
    """
    return {
        "node_id": state.node_id,
        "role": "acceptor",
        "promises_count": len(state.promises),
        "accepted_count": len(state.accepted_values),
        "uptime_seconds": time.time() - app.state.start_time
    }

# Configuração de métricas Prometheus
@app.get("/metrics")
async def metrics():
    """
    Expõe métricas no formato Prometheus.
    """
    return generate_latest()

# Eventos do ciclo de vida da aplicação
@app.on_event("startup")
async def startup_event():
    """
    Evento executado ao iniciar a aplicação.
    """
    app.state.start_time = time.time()
    log.info("Acceptor starting up", 
            node_id=state.node_id, 
            config=config)

@app.on_event("shutdown")
async def shutdown_event():
    """
    Evento executado ao encerrar a aplicação.
    """
    log.info("Acceptor shutting down", 
            node_id=state.node_id)