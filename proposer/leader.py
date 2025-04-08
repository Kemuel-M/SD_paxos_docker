import asyncio
import logging
import random
import time
from typing import Dict, List, Optional, Set

from common.utils import generate_id, http_request

logger = logging.getLogger(__name__)

class LeaderElection:
    """
    Implementação do algoritmo de eleição de líder para Multi-Paxos.
    Usa uma variação do algoritmo Paxos para eleger um líder entre os proposers.
    """
    
    def __init__(self, proposer_id: str, debug: bool = False):
        """
        Inicializa o sistema de eleição de líder.
        
        Args:
            proposer_id: ID do proposer local
            debug: Flag para ativar modo de depuração
        """
        self.proposer_id = proposer_id
        self.debug = debug
        self.current_leader: Optional[str] = None
        self.election_in_progress = False
        self.election_id: Optional[str] = None
        self.term = 0  # Termo atual (aumenta a cada nova eleição)
        
        # Lista de outros proposers
        self.other_proposers: Dict[str, str] = {}  # {proposer_id: url}
        
        # Para controle de eleição
        self.votes: Dict[str, Set[str]] = {}  # {candidate_id: {voter_id, ...}}
        
        # Locks para operações concorrentes
        self.election_lock = asyncio.Lock()
        
        # Timeout para detectar líder inativo
        self.leader_timeout = 10.0  # segundos
        self.last_leader_heartbeat = 0
        
        logger.debug(f"Sistema de eleição de líder inicializado para {proposer_id}")
    
    def add_proposer(self, proposer_id: str, url: str) -> None:
        """
        Adiciona um proposer à lista de proposers conhecidos.
        
        Args:
            proposer_id: ID do proposer
            url: URL base do proposer
        """
        if proposer_id != self.proposer_id:
            self.other_proposers[proposer_id] = url
            logger.debug(f"Proposer adicionado: {proposer_id} -> {url}")
    
    def remove_proposer(self, proposer_id: str) -> None:
        """
        Remove um proposer da lista de proposers conhecidos.
        
        Args:
            proposer_id: ID do proposer a ser removido
        """
        if proposer_id in self.other_proposers:
            del self.other_proposers[proposer_id]
            logger.debug(f"Proposer removido: {proposer_id}")
    
    def get_all_proposers(self) -> List[str]:
        """
        Obtém a lista de todos os proposers conhecidos, incluindo o local.
        
        Returns:
            List[str]: Lista de IDs de todos os proposers
        """
        return [self.proposer_id] + list(self.other_proposers.keys())
    
    async def check_leader_health(self) -> bool:
        """
        Verifica se o líder atual está ativo.
        
        Returns:
            bool: True se o líder está ativo, False caso contrário
        """
        if not self.current_leader or self.current_leader == self.proposer_id:
            return True
        
        # Se já passou muito tempo desde o último heartbeat do líder
        current_time = time.time()
        if current_time - self.last_leader_heartbeat > self.leader_timeout:
            logger.warning(f"Líder {self.current_leader} pode estar inativo. Último heartbeat: {self.last_leader_heartbeat:.1f}s atrás")
            
            # Tenta contactar o líder diretamente
            leader_url = self.other_proposers.get(self.current_leader)
            if not leader_url:
                logger.error(f"URL do líder {self.current_leader} não encontrada")
                return False
            
            health_url = f"{leader_url}/health"
            
            try:
                response = await http_request("GET", health_url, timeout=2.0)
                if response.get("status") == "healthy":
                    # Atualiza o timestamp do último heartbeat
                    self.last_leader_heartbeat = current_time
                    return True
                else:
                    logger.warning(f"Líder {self.current_leader} respondeu, mas não está saudável: {response}")
                    return False
            except Exception as e:
                logger.error(f"Erro ao verificar saúde do líder {self.current_leader}: {str(e)}")
                return False
        
        return True
    
    async def start_election(self) -> None:
        """
        Inicia uma nova eleição de líder.
        """
        async with self.election_lock:
            if self.election_in_progress:
                logger.debug("Eleição já em andamento. Ignorando solicitação.")
                return
            
            # Incrementa o termo e gera um novo ID de eleição
            self.term += 1
            self.election_id = generate_id()
            self.election_in_progress = True
            
            # Limpa os votos anteriores
            self.votes = {}
            
            # Adiciona nosso próprio voto
            self.votes[self.proposer_id] = {self.proposer_id}
            
            logger.info(f"Iniciando eleição de líder {self.election_id} (termo {self.term})")
        
        # Solicita votos de outros proposers
        await self._request_votes()
    
    async def _request_votes(self) -> None:
        """
        Solicita votos dos outros proposers.
        """
        vote_tasks = []
        
        for proposer_id, url in self.other_proposers.items():
            vote_url = f"{url}/vote"
            vote_data = {
                "candidate_id": self.proposer_id,
                "election_id": self.election_id,
                "term": self.term
            }
            
            task = asyncio.create_task(
                self._send_vote_request(proposer_id, vote_url, vote_data)
            )
            vote_tasks.append(task)
        
        # Espera todas as solicitações de voto serem concluídas
        await asyncio.gather(*vote_tasks, return_exceptions=True)
        
        # Verifica se temos votos suficientes para ganhar a eleição
        await self._count_votes()
    
    async def _send_vote_request(self, proposer_id: str, url: str, data: Dict) -> Dict:
        """
        Envia uma solicitação de voto para um proposer.
        
        Args:
            proposer_id: ID do proposer
            url: URL do endpoint de voto
            data: Dados da solicitação
        
        Returns:
            Dict: Resposta do proposer
        """
        try:
            response = await http_request("POST", url, data=data)
            
            vote_granted = response.get("vote_granted", False)
            
            if vote_granted:
                logger.debug(f"Proposer {proposer_id} votou em nós")
                async with self.election_lock:
                    if self.proposer_id not in self.votes:
                        self.votes[self.proposer_id] = set()
                    self.votes[self.proposer_id].add(proposer_id)
            else:
                logger.debug(f"Proposer {proposer_id} não votou em nós. Motivo: {response.get('reason', 'desconhecido')}")
            
            return response
        except Exception as e:
            logger.error(f"Erro ao solicitar voto do proposer {proposer_id}: {str(e)}")
            return {
                "vote_granted": False,
                "reason": str(e)
            }
    
    async def handle_vote_request(self, candidate_id: str, election_id: str, term: int) -> Dict:
        """
        Processa uma solicitação de voto de outro proposer.
        
        Args:
            candidate_id: ID do proposer candidato
            election_id: ID da eleição
            term: Termo da eleição
        
        Returns:
            Dict: Resposta à solicitação
        """
        async with self.election_lock:
            # Se o termo da solicitação for maior que o nosso, atualizamos
            if term > self.term:
                self.term = term
                self.election_id = election_id
                self.election_in_progress = True
                self.votes = {}
                
                # Votamos no candidato
                if candidate_id not in self.votes:
                    self.votes[candidate_id] = set()
                self.votes[candidate_id].add(self.proposer_id)
                
                logger.info(f"Votando em {candidate_id} para eleição {election_id} (termo {term})")
                
                return {
                    "vote_granted": True,
                    "term": self.term
                }
            # Se o termo for igual, verificamos se já votamos
            elif term == self.term:
                # Se já votamos em outro candidato, não podemos votar novamente
                for candidate, voters in self.votes.items():
                    if self.proposer_id in voters and candidate != candidate_id:
                        logger.debug(f"Já votamos em {candidate} nesta eleição. Rejeitando voto para {candidate_id}")
                        return {
                            "vote_granted": False,
                            "reason": "already_voted",
                            "term": self.term
                        }
                
                # Se ainda não votamos, votamos no candidato
                if candidate_id not in self.votes:
                    self.votes[candidate_id] = set()
                self.votes[candidate_id].add(self.proposer_id)
                
                logger.debug(f"Votando em {candidate_id} para eleição {election_id} (termo {term})")
                
                return {
                    "vote_granted": True,
                    "term": self.term
                }
            # Se o termo for menor, rejeitamos o voto
            else:
                logger.debug(f"Rejeitando voto para {candidate_id} (termo {term} < nosso termo {self.term})")
                return {
                    "vote_granted": False,
                    "reason": "term_too_low",
                    "term": self.term
                }
    
    async def _count_votes(self) -> None:
        """
        Conta os votos e determina se houve um vencedor.
        """
        async with self.election_lock:
            if not self.election_in_progress:
                return
            
            total_proposers = len(self.get_all_proposers())
            majority = total_proposers // 2 + 1
            
            # Conta os votos para cada candidato
            for candidate_id, voters in self.votes.items():
                vote_count = len(voters)
                
                logger.debug(f"Candidato {candidate_id}: {vote_count} votos (maioria: {majority})")
                
                # Se algum candidato tiver maioria dos votos, ele é o líder
                if vote_count >= majority:
                    await self._declare_leader(candidate_id)
                    return
            
            # Se ninguém tem maioria, a eleição continua
            logger.debug("Nenhum candidato tem maioria. Eleição continua.")
    
    async def _declare_leader(self, leader_id: str) -> None:
        """
        Declara um proposer como líder.
        
        Args:
            leader_id: ID do proposer eleito como líder
        """
        async with self.election_lock:
            self.current_leader = leader_id
            self.election_in_progress = False
            
            logger.info(f"Líder eleito: {leader_id} (termo {self.term})")
            
            # Se formos o líder, começamos a enviar heartbeats
            if leader_id == self.proposer_id:
                logger.info("Nós somos o líder. Iniciando heartbeats.")
                # Inicia o envio de heartbeats em uma nova task
                asyncio.create_task(self._send_leader_heartbeats())
            else:
                # Atualiza o timestamp do último heartbeat do líder
                self.last_leader_heartbeat = time.time()
        
        # Notifica outros proposers sobre o novo líder
        await self._announce_leader(leader_id)
    
    async def _announce_leader(self, leader_id: str) -> None:
        """
        Anuncia o novo líder para todos os outros proposers.
        
        Args:
            leader_id: ID do proposer eleito como líder
        """
        announce_tasks = []
        
        for proposer_id, url in self.other_proposers.items():
            leader_url = f"{url}/leader"
            leader_data = {
                "leader_id": leader_id,
                "term": self.term,
                "election_id": self.election_id
            }
            
            task = asyncio.create_task(
                http_request("POST", leader_url, data=leader_data)
            )
            announce_tasks.append(task)
        
        # Espera todos os anúncios serem concluídos
        await asyncio.gather(*announce_tasks, return_exceptions=True)
    
    async def handle_leader_announcement(self, leader_id: str, term: int, election_id: str) -> Dict:
        """
        Processa um anúncio de líder de outro proposer.
        
        Args:
            leader_id: ID do proposer eleito como líder
            term: Termo da eleição
            election_id: ID da eleição
        
        Returns:
            Dict: Resposta ao anúncio
        """
        async with self.election_lock:
            # Só aceitamos o anúncio se o termo for pelo menos igual ao nosso
            if term >= self.term:
                # Atualiza nosso termo se necessário
                if term > self.term:
                    self.term = term
                
                self.current_leader = leader_id
                self.election_id = election_id
                self.election_in_progress = False
                
                # Atualiza o timestamp do último heartbeat do líder
                self.last_leader_heartbeat = time.time()
                
                logger.info(f"Aceitando {leader_id} como líder (termo {term})")
                
                return {
                    "accepted": True,
                    "term": self.term
                }
            else:
                logger.warning(f"Rejeitando {leader_id} como líder (termo {term} < nosso termo {self.term})")
                return {
                    "accepted": False,
                    "reason": "term_too_low",
                    "term": self.term
                }
    
    async def _send_leader_heartbeats(self) -> None:
        """
        Envia heartbeats periódicos para todos os outros proposers.
        Só é executado pelo líder atual.
        """
        # Intervalo entre heartbeats
        heartbeat_interval = 2.0  # segundos
        
        while self.current_leader == self.proposer_id:
            heartbeat_tasks = []
            
            for proposer_id, url in self.other_proposers.items():
                heartbeat_url = f"{url}/heartbeat"
                heartbeat_data = {
                    "leader_id": self.proposer_id,
                    "term": self.term,
                    "timestamp": time.time()
                }
                
                task = asyncio.create_task(
                    http_request("POST", heartbeat_url, data=heartbeat_data)
                )
                heartbeat_tasks.append(task)
            
            # Espera todos os heartbeats serem concluídos
            await asyncio.gather(*heartbeat_tasks, return_exceptions=True)
            
            # Espera até o próximo ciclo de heartbeat
            await asyncio.sleep(heartbeat_interval)
    
    async def handle_heartbeat(self, leader_id: str, term: int, timestamp: float) -> Dict:
        """
        Processa um heartbeat do líder.
        
        Args:
            leader_id: ID do líder
            term: Termo atual
            timestamp: Timestamp do heartbeat
        
        Returns:
            Dict: Resposta ao heartbeat
        """
        async with self.election_lock:
            # Verifica se o termo do heartbeat é válido
            if term < self.term:
                logger.warning(f"Heartbeat de {leader_id} tem termo {term} < nosso termo {self.term}")
                return {
                    "accepted": False,
                    "reason": "term_too_low",
                    "term": self.term
                }
            
            # Atualiza o termo se necessário
            if term > self.term:
                self.term = term
            
            # Atualiza o líder atual
            self.current_leader = leader_id
            
            # Atualiza o timestamp do último heartbeat
            self.last_leader_heartbeat = time.time()
            
            if self.debug:
                logger.debug(f"Heartbeat recebido de {leader_id} (termo {term})")
            
            return {
                "accepted": True,
                "term": self.term
            }
    
    def get_status(self) -> Dict:
        """
        Obtém o status atual do sistema de eleição.
        
        Returns:
            Dict: Status do sistema de eleição
        """
        return {
            "current_leader": self.current_leader,
            "term": self.term,
            "election_in_progress": self.election_in_progress,
            "election_id": self.election_id,
            "last_leader_heartbeat": self.last_leader_heartbeat,
            "proposers_count": len(self.other_proposers) + 1
        }