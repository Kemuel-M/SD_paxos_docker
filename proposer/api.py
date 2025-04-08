import asyncio
import json
import logging
from typing import Dict, List, Optional, Set

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from common.utils import get_debug_mode, setup_logging
from proposer.leader import LeaderElection
from proposer.proposer import Proposer

# Configura o logger
logger = logging.getLogger(__name__)

# Modelos de dados para as requisições e respostas
class ProposeRequest(BaseModel):
    client_id: str
    resource_data: Dict

class ProposeResponse(BaseModel):
    success: bool
    proposal_id: Optional[str] = None
    error: Optional[str] = None
    leader: Optional[str] = None

class VoteRequest(BaseModel):
    candidate_id: str
    election_id: str
    term: int

class VoteResponse(BaseModel):
    vote_granted: bool
    reason: Optional[str] = None
    term: int

class LeaderRequest(BaseModel):
    leader_id: str
    term: int
    election_id: str

class LeaderResponse(BaseModel):
    accepted: bool
    reason: Optional[str] = None
    term: int

class HeartbeatRequest(BaseModel):
    leader_id: str
    term: int
    timestamp: float

class HeartbeatResponse(BaseModel):
    accepted: bool
    reason: Optional[str] = None
    term: int

class StatusResponse(BaseModel):
    id: str
    is_leader: bool
    current_leader: Optional[str]
    tid_counter: int
    term: int
    acceptors_count: int
    learners_count: int
    stores_count: int
    active_proposals: int
    proposers_count: int

class HealthResponse(BaseModel):
    status: str
    timestamp: float

# Variáveis globais para instâncias de Proposer e LeaderElection
proposer: Optional[Proposer] = None
leader_election: Optional[LeaderElection] = None

# Cria a aplicação FastAPI
app = FastAPI(title="Proposer API")

# Middleware para logging de requisições
@app.middleware("http")
async def log_requests(request: Request, call_next):
    # Obtém o corpo da requisição
    body = await request.body()
    
    # Reconstrói o stream de requisição para ser consumido novamente
    request._body = body
    
    # Log da requisição
    method = request.method
    url = request.url
    logger.debug(f"Requisição recebida: {method} {url}")
    
    if get_debug_mode() and body:
        try:
            body_str = body.decode('utf-8')
            if body_str:
                body_json = json.loads(body_str)
                logger.debug(f"Corpo da requisição: {json.dumps(body_json, indent=2)}")
        except Exception as e:
            logger.debug(f"Corpo da requisição não é JSON válido: {body}")
    
    # Processa a requisição
    response = await call_next(request)
    
    # Log da resposta
    status_code = response.status_code
    logger.debug(f"Resposta enviada: {status_code}")
    
    return response

# Rotas da API
@app.post("/propose", response_model=ProposeResponse)
async def propose_endpoint(request: ProposeRequest):
    """
    Endpoint para receber uma proposta de um cliente.
    """
    if not proposer:
        raise HTTPException(status_code=500, detail="Proposer não inicializado")
    
    # Executa a proposta
    result = await proposer.propose(request.client_id, request.resource_data)
    
    # Retorna o resultado
    return result

@app.post("/vote", response_model=VoteResponse)
async def vote_endpoint(request: VoteRequest):
    """
    Endpoint para receber uma solicitação de voto para eleição de líder.
    """
    if not leader_election:
        raise HTTPException(status_code=500, detail="Leader Election não inicializado")
    
    # Processa a solicitação de voto
    result = await leader_election.handle_vote_request(
        request.candidate_id, request.election_id, request.term
    )
    
    # Retorna o resultado
    return result

@app.post("/leader", response_model=LeaderResponse)
async def leader_endpoint(request: LeaderRequest):
    """
    Endpoint para receber um anúncio de líder.
    """
    if not leader_election:
        raise HTTPException(status_code=500, detail="Leader Election não inicializado")
    
    # Processa o anúncio de líder
    result = await leader_election.handle_leader_announcement(
        request.leader_id, request.term, request.election_id
    )
    
    # Se o anúncio for aceito, atualiza o proposer
    if result.get("accepted") and proposer:
        await proposer.set_leader(request.leader_id)
    
    # Retorna o resultado
    return result

@app.post("/heartbeat", response_model=HeartbeatResponse)
async def heartbeat_endpoint(request: HeartbeatRequest):
    """
    Endpoint para receber um heartbeat do líder.
    """
    if not leader_election:
        raise HTTPException(status_code=500, detail="Leader Election não inicializado")
    
    # Processa o heartbeat
    result = await leader_election.handle_heartbeat(
        request.leader_id, request.term, request.timestamp
    )
    
    # Retorna o resultado
    return result

@app.get("/status", response_model=StatusResponse)
async def status_endpoint():
    """
    Endpoint para obter o status do proposer.
    """
    if not proposer or not leader_election:
        raise HTTPException(status_code=500, detail="Proposer ou Leader Election não inicializado")
    
    # Obtém o status do proposer e do leader election
    proposer_status = proposer.get_status()
    leader_status = leader_election.get_status()
    
    # Combina os status
    combined_status = {
        **proposer_status,
        "term": leader_status.get("term", 0),
        "proposers_count": leader_status.get("proposers_count", 0)
    }
    
    # Retorna o status combinado
    return combined_status

@app.get("/health", response_model=HealthResponse)
async def health_endpoint():
    """
    Endpoint para verificar a saúde do proposer.
    """
    import time
    
    # Retorna sempre saudável com o timestamp atual
    return {
        "status": "healthy",
        "timestamp": time.time()
    }

@app.get("/logs", response_class=Response)
async def logs_endpoint(lines: int = 100):
    """
    Endpoint para obter os logs do proposer.
    """
    try:
        # Obtém os N últimos logs do arquivo
        log_file = "logs/proposer.log"
        with open(log_file, "r") as f:
            logs = f.readlines()
            last_logs = logs[-lines:] if len(logs) >= lines else logs
        
        # Formata os logs
        formatted_logs = "".join(last_logs)
        
        # Retorna os logs como texto
        return Response(content=formatted_logs, media_type="text/plain")
    except Exception as e:
        logger.error(f"Erro ao obter logs: {str(e)}")
        return Response(content=f"Erro ao obter logs: {str(e)}", media_type="text/plain", status_code=500)

@app.get("/proposals/{proposal_id}")
async def get_proposal_endpoint(proposal_id: str):
    """
    Endpoint para obter o status de uma proposta específica.
    """
    if not proposer:
        raise HTTPException(status_code=500, detail="Proposer não inicializado")
    
    # Obtém o status da proposta
    proposal_status = proposer.get_proposal_status(proposal_id)
    
    # Se a proposta não foi encontrada, retorna um erro
    if "error" in proposal_status:
        raise HTTPException(status_code=404, detail=proposal_status["error"])
    
    # Retorna o status da proposta
    return proposal_status

@app.get("/proposals")
async def get_all_proposals_endpoint():
    """
    Endpoint para obter o status de todas as propostas.
    """
    if not proposer:
        raise HTTPException(status_code=500, detail="Proposer não inicializado")
    
    # Obtém o status de todas as propostas
    proposals = proposer.get_all_proposals()
    
    # Retorna a lista de propostas
    return proposals

# Funções para inicialização
def initialize(proposer_id: str, debug: bool = None):
    """
    Inicializa o proposer e o sistema de eleição de líder.
    
    Args:
        proposer_id: ID do proposer
        debug: Flag para ativar modo de depuração
    """
    global proposer, leader_election
    
    # Configura o logging
    if debug is None:
        debug = get_debug_mode()
    
    setup_logging(f"proposer_{proposer_id}", debug)
    
    # Cria as instâncias
    proposer = Proposer(proposer_id, debug)
    leader_election = LeaderElection(proposer_id, debug)
    
    logger.info(f"Proposer {proposer_id} inicializado (debug={debug})")

def add_acceptor(acceptor_id: str, url: str):
    """
    Adiciona um acceptor ao proposer.
    
    Args:
        acceptor_id: ID do acceptor
        url: URL base do acceptor
    """
    if proposer:
        proposer.add_acceptor(acceptor_id, url)

def add_learner(learner_id: str, url: str):
    """
    Adiciona um learner ao proposer.
    
    Args:
        learner_id: ID do learner
        url: URL base do learner
    """
    if proposer:
        proposer.add_learner(learner_id, url)

def add_store(store_id: str, url: str):
    """
    Adiciona um store ao proposer.
    
    Args:
        store_id: ID do store
        url: URL base do store
    """
    if proposer:
        proposer.add_store(store_id, url)

def add_proposer(proposer_id: str, url: str):
    """
    Adiciona um proposer ao sistema de eleição de líder.
    
    Args:
        proposer_id: ID do proposer
        url: URL base do proposer
    """
    if leader_election:
        leader_election.add_proposer(proposer_id, url)

async def start_leader_election_check():
    """
    Inicia um loop para verificar periodicamente se o líder está saudável
    e iniciar uma eleição se necessário.
    """
    if not leader_election:
        logger.error("Leader Election não inicializado")
        return
    
    # Intervalo para verificação do líder
    check_interval = 5.0  # segundos
    
    while True:
        # Verifica se o líder está saudável
        leader_healthy = await leader_election.check_leader_health()
        
        # Se não temos líder ou o líder atual não está saudável, iniciamos uma eleição
        if not leader_election.current_leader or not leader_healthy:
            logger.info("Iniciando eleição de líder")
            await leader_election.start_election()
        
        # Espera até a próxima verificação
        await asyncio.sleep(check_interval)