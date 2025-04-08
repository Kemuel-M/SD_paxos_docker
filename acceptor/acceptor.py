"""
File: acceptor/acceptor.py
Implementation of the Acceptor component for the Paxos consensus algorithm.
Responsible for processing prepare and accept requests from proposers and notifying learners.
"""
import os
import time
import logging
import asyncio
from typing import Dict, List, Any, Optional, Tuple
from collections import defaultdict

from common.communication import HttpClient, CircuitBreaker
from common.utils import current_timestamp

# Configure debug settings
DEBUG = os.getenv("DEBUG", "false").lower() in ("true", "1", "yes")
DEBUG_LEVEL = os.getenv("DEBUG_LEVEL", "basic").lower()  # Levels: basic, advanced, trace

logger = logging.getLogger("acceptor")

class Acceptor:
    """
    Implementation of the Acceptor in the Paxos consensus algorithm.
    
    Responsibilities:
    1. Process prepare requests from proposers
    2. Process accept requests from proposers
    3. Notify learners about accepted proposals
    4. Maintain persistent state of promises and accepted values
    """
    
    def __init__(self, node_id: int, learners: List[str], persistence=None):
        self.node_id = node_id
        self.learners = learners if learners else []
        self.persistence = persistence
        self._pending_tasks = set()
        
        # State structures
        self.promises = {}  # instanceId -> highest proposal number promised
        self.accepted = {}  # instanceId -> (proposal_number, value)
        
        # Statistics
        self.prepare_requests_processed = 0
        self.accept_requests_processed = 0
        self.promises_made = 0
        self.proposals_accepted = 0
        
        # HTTP client for communication with learners
        self.http_client = HttpClient()
        
        # Circuit breakers for learners
        self.circuit_breakers = defaultdict(lambda: CircuitBreaker())
        
        # NOVA IMPLEMENTAÇÃO: Locks por instância em vez de lock global
        self.instance_locks = defaultdict(asyncio.Lock)
        
        # Lock separado para estatísticas
        self.stats_lock = asyncio.Lock()
        
        # Lock para persistência
        self.persistence_lock = asyncio.Lock()
        
        # Processing flag
        self.running = False
        
        logger.info(f"Acceptor {node_id} initialized with {len(learners)} learners")
        
        if DEBUG and DEBUG_LEVEL in ("advanced", "trace"):
            logger.debug(f"Learners: {self.learners}")
    
    async def start(self):
        """Start the acceptor asynchronously."""
        if self.running:
            return
                
        self.running = True
        
        # Load state from persistence if available
        if self.persistence:
            # Use synchronous load_state method
            state = self.persistence.load_state()
            
            # Atualizando estado: não precisamos de lock aqui
            # pois o start é chamado no setup antes de qualquer outro método
            
            # Convert keys from strings to integers (JSON serialization effect)
            self.promises = {int(k): v for k, v in state.get("promises", {}).items()}
            
            # Process accepted state properly
            accepted_dict = state.get("accepted", {})
            self.accepted = {}
            for k, v in accepted_dict.items():
                if isinstance(v, list) and len(v) == 2:
                    # Handle list format from JSON
                    self.accepted[int(k)] = (v[0], v[1])
                else:
                    # Handle tuple format (caso alternativo)
                    self.accepted[int(k)] = v
            
            # Atualiza estatísticas com lock
            async with self.stats_lock:
                self.prepare_requests_processed = state.get("prepare_requests_processed", 0)
                self.accept_requests_processed = state.get("accept_requests_processed", 0)
                self.promises_made = state.get("promises_made", 0)
                self.proposals_accepted = state.get("proposals_accepted", 0)
            
            logger.info(f"Loaded state from persistence: {len(self.promises)} promises, {len(self.accepted)} accepted values")
            
            if DEBUG and DEBUG_LEVEL == "trace":
                logger.debug(f"Loaded promises: {self.promises}")
                logger.debug(f"Loaded accepted values: {self.accepted}")
        
        logger.info(f"Acceptor {self.node_id} started")
    
    async def stop(self):
        """Stop the acceptor asynchronously."""
        if not self.running:
            return
            
        self.running = False

        # Cancela tarefas pendentes com timeout
        if self._pending_tasks:
            for task in self._pending_tasks:
                if not task.done():
                    task.cancel()
                    
            try:
                # Aguarda cancelamento com timeout
                await asyncio.wait_for(
                    asyncio.gather(*self._pending_tasks, return_exceptions=True),
                    timeout=5.0
                )
            except asyncio.TimeoutError:
                logger.warning(f"Timeout aguardando cancelamento de {len(self._pending_tasks)} tarefas")
            finally:
                self._pending_tasks.clear()
        
        # Persistência final com timeout
        if self.persistence:
            try:
                await asyncio.wait_for(self.save_state(), timeout=5.0)
            except asyncio.TimeoutError:
                logger.error(f"Timeout durante persistência final")
        
        await self.http_client.close()
        logger.info(f"Acceptor {self.node_id} stopped")
    
    async def save_state(self, timeout_seconds=5.0):
        """Save acceptor state to persistence asynchronously with timeout."""
        if not self.persistence:
            return
        
        try:
            # Use wait_for em vez de timeout contextual
            async def _save_with_lock():
                async with self.persistence_lock:
                    # Captura estatísticas atomicamente
                    async with self.stats_lock:
                        prepare_requests = self.prepare_requests_processed
                        accept_requests = self.accept_requests_processed
                        promises_made = self.promises_made
                        proposals_accepted = self.proposals_accepted
                    
                    state = {
                        "promises": self.promises, 
                        "accepted": self.accepted,
                        "prepare_requests_processed": prepare_requests,
                        "accept_requests_processed": accept_requests,
                        "promises_made": promises_made,
                        "proposals_accepted": proposals_accepted
                    }
                    
                    await self.persistence.save_state(state)
                    
                    if DEBUG and DEBUG_LEVEL == "trace":
                        logger.debug(f"State saved to persistence: {len(self.promises)} promises, {len(self.accepted)} accepted values")
                    
                    return True
                    
            await asyncio.wait_for(_save_with_lock(), timeout=timeout_seconds)
            return True
        except asyncio.TimeoutError:
            logger.error(f"Timeout durante operação save_state")
            return False
    
    async def process_prepare(self, prepare_msg: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a prepare request from a proposer asynchronously.
        """
        # Validate required fields
        required_fields = ["instanceId", "proposalNumber", "proposerId"]
        for field in required_fields:
            if field not in prepare_msg:
                logger.warning(f"Prepare request missing required field: {field}")
                return {"accepted": False, "reason": f"Missing required field: {field}"}
        
        # Get prepare message details
        instance_id = prepare_msg.get("instanceId")
        proposal_number = prepare_msg.get("proposalNumber")
        proposer_id = prepare_msg.get("proposerId")
        
        logger.info(f"Processing prepare request for instance {instance_id} with proposal number {proposal_number} from proposer {proposer_id}")
        
        if DEBUG and DEBUG_LEVEL in ("advanced", "trace"):
            logger.debug(f"Full prepare message: {prepare_msg}")
        
        # Update statistics atomically
        async with self.stats_lock:
            self.prepare_requests_processed += 1
        
        # Variáveis para armazenar estado modificado sob lock
        promise_made = False
        highest_promised = -1
        accepted_proposal = -1
        accepted_value = None
        
        # MODIFICAÇÃO: Usa lock por instância em vez de lock global
        async with self.instance_locks[instance_id]:
            # Ensure the instance exists in our state
            if instance_id not in self.promises:
                self.promises[instance_id] = -1
                    
                if DEBUG and DEBUG_LEVEL in ("advanced", "trace"):
                    logger.debug(f"New instance {instance_id} added to promises with initial value -1")
            
            highest_promised = self.promises[instance_id]
            
            # Check if we can promise (proposal number > highest promised)
            if proposal_number > highest_promised:
                # Update highest promised
                self.promises[instance_id] = proposal_number
                
                # Update statistics atomically
                async with self.stats_lock:
                    self.promises_made += 1
                
                promise_made = True
                
                if DEBUG:
                    logger.debug(f"Promise made for instance {instance_id} with proposal number {proposal_number} (previous: {highest_promised})")
                
                # Check if we have accepted a value for this instance
                if instance_id in self.accepted:
                    accepted_proposal, accepted_value = self.accepted[instance_id]
                    
                    if DEBUG and DEBUG_LEVEL in ("advanced", "trace"):
                        logger.debug(f"Previously accepted value for instance {instance_id}: proposal {accepted_proposal}, value: {accepted_value}")
        
        # MODIFICAÇÃO CHAVE: Create task for saving state OUTSIDE of the lock
        if promise_made:
            save_task = asyncio.create_task(self.save_state())
            self._pending_tasks.add(save_task)
            save_task.add_done_callback(self._pending_tasks.discard)
            
            # Return promise with any previously accepted value
            response = {
                "type": "PROMISE",
                "accepted": True,
                "highestAccepted": accepted_proposal,
                "instanceId": instance_id,
                "acceptorId": self.node_id
            }
            
            # Only include acceptedValue if there was one
            if accepted_value is not None:
                response["acceptedValue"] = accepted_value
            
            return response
        else:
            # We've promised to a higher proposal number
            logger.info(f"Promise rejected for instance {instance_id}: proposal number {proposal_number} <= {highest_promised}")
            
            return {
                "type": "NOT_PROMISE",
                "accepted": False,
                "instanceId": instance_id,
                "acceptorId": self.node_id,
                "highestPromised": highest_promised
            }
    
    async def process_accept(self, accept_msg: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process an accept request from a proposer asynchronously.
        """
        # Validate required fields
        required_fields = ["instanceId", "proposalNumber", "proposerId", "value"]
        for field in required_fields:
            if field not in accept_msg:
                logger.warning(f"Accept request missing required field: {field}")
                return {"accepted": False, "reason": f"Missing required field: {field}"}
        
        # Get accept message details
        instance_id = accept_msg.get("instanceId")
        proposal_number = accept_msg.get("proposalNumber")
        proposer_id = accept_msg.get("proposerId")
        value = accept_msg.get("value")
        
        logger.info(f"Processing accept request for instance {instance_id} with proposal number {proposal_number} from proposer {proposer_id}")
        
        if DEBUG and DEBUG_LEVEL in ("advanced", "trace"):
            logger.debug(f"Full accept message: {accept_msg}")
        
        # Update statistics atomically
        async with self.stats_lock:
            self.accept_requests_processed += 1
        
        # Variables to track state modifications
        proposal_accepted = False
        highest_promised = -1
        
        # MODIFICAÇÃO: Usa lock por instância em vez de lock global
        async with self.instance_locks[instance_id]:
            # Ensure the instance exists in our state
            if instance_id not in self.promises:
                self.promises[instance_id] = -1
                
                if DEBUG and DEBUG_LEVEL in ("advanced", "trace"):
                    logger.debug(f"New instance {instance_id} added to promises with initial value -1")
            
            highest_promised = self.promises[instance_id]
            
            # Check if we can accept (proposal number >= highest promised)
            if proposal_number >= highest_promised:
                # Update accepted value
                self.accepted[instance_id] = (proposal_number, value)
                self.promises[instance_id] = proposal_number
                
                # Update statistics atomically
                async with self.stats_lock:
                    self.proposals_accepted += 1
                
                proposal_accepted = True
                
                logger.info(f"Proposal accepted for instance {instance_id} with proposal number {proposal_number}")
                
                if DEBUG and DEBUG_LEVEL in ("advanced", "trace"):
                    logger.debug(f"Accepted value for instance {instance_id}: {value}")
        
        # MODIFICAÇÃO CHAVE: Process additional tasks OUTSIDE of lock
        if proposal_accepted:
            # Create a task for saving state
            save_task = asyncio.create_task(self.save_state())
            self._pending_tasks.add(save_task)
            save_task.add_done_callback(self._pending_tasks.discard)
            
            # Create a task for notifying learners
            notify_task = asyncio.create_task(self.notify_learners(instance_id, proposal_number, value))
            self._pending_tasks.add(notify_task)
            notify_task.add_done_callback(self._pending_tasks.discard)
            
            # Return accepted
            return {
                "type": "ACCEPTED",
                "accepted": True,
                "proposalNumber": proposal_number,
                "instanceId": instance_id,
                "acceptorId": self.node_id
            }
        else:
            # We've promised to a higher proposal number
            logger.info(f"Proposal rejected for instance {instance_id}: proposal number {proposal_number} < {highest_promised}")
            
            return {
                "type": "NOT_ACCEPTED",
                "accepted": False,
                "instanceId": instance_id,
                "acceptorId": self.node_id,
                "highestPromised": highest_promised
            }
    
    async def notify_learners(self, instance_id: int, proposal_number: int, value: Any):
        """
        Notify all learners about an accepted proposal asynchronously.
        
        Args:
            instance_id: ID of the instance
            proposal_number: Number of the accepted proposal
            value: Value that was accepted
        """
        # Create notification message
        notification = {
            "type": "LEARN",
            "instanceId": instance_id,
            "proposalNumber": proposal_number,
            "acceptorId": self.node_id,
            "accepted": True,
            "value": value,
            "timestamp": current_timestamp()
        }
        
        logger.info(f"Notifying {len(self.learners)} learners about acceptance of instance {instance_id}")
        
        # Send notification to all learners
        tasks = []
        for learner in self.learners:
            if not learner:  # Skip empty addresses
                continue
                
            cb = self.circuit_breakers[learner]
            
            # Skip if circuit breaker is open
            if not cb.allow_request():
                logger.warning(f"Circuit breaker open for learner {learner}, skipping notification")
                continue
            
            # Send notification with circuit breaker
            tasks.append(self._notify_learner(learner, notification, cb))
        
        # Wait for all notifications to complete
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Count successes
            success_count = sum(1 for r in results if r is True)
            
            logger.info(f"Learner notification complete: {success_count}/{len(tasks)} successful")
        else:
            logger.warning("No learners to notify")
    
    async def _notify_learner(self, learner: str, notification: Dict[str, Any], circuit_breaker: CircuitBreaker) -> bool:
        """
        Send notification to a specific learner with error handling asynchronously.
        
        Args:
            learner: Learner address
            notification: Notification message
            circuit_breaker: Circuit breaker for this learner
            
        Returns:
            bool: True if notification was successful, False otherwise
        """
        try:
            url = f"http://{learner}/learn"
            
            if DEBUG and DEBUG_LEVEL == "trace":
                logger.debug(f"Sending notification to {url}: {notification}")
            
            # Send notification with timeout
            await self.http_client.post(url, json=notification, timeout=1.0)
            
            # Record success in circuit breaker
            await circuit_breaker.record_success()
            
            if DEBUG:
                logger.debug(f"Notification to learner {learner} successful")
            
            return True
            
        except Exception as e:
            # Record failure in circuit breaker
            await circuit_breaker.record_failure()
            
            logger.error(f"Failed to notify learner {learner}: {e}")
            return False
    
    def get_status(self) -> Dict[str, Any]:
        """
        Get the current status of the acceptor synchronously.
        
        Returns:
            Dict[str, Any]: Status information
        """
        # Calcular instance_id_range de maneira segura
        if self.promises:
            try:
                min_id = min(self.promises.keys())
                max_id = max(self.promises.keys())
                instance_id_range = f"{min_id}-{max_id}"
            except (ValueError, TypeError):
                # Lidar com casos em que as chaves não são iteráveis ou estão vazias
                instance_id_range = "N/A-N/A"
        else:
            instance_id_range = "N/A-N/A"
        
        return {
            "node_id": self.node_id,
            "state": "running" if self.running else "stopped",
            "learners": len(self.learners),
            "active_instances": len(self.promises),
            "accepted_instances": len(self.accepted),
            "prepare_requests_processed": self.prepare_requests_processed,
            "accept_requests_processed": self.accept_requests_processed,
            "promises_made": self.promises_made,
            "proposals_accepted": self.proposals_accepted,
            "instance_id_range": instance_id_range,
            "timestamp": current_timestamp()
        }
    
    def get_instance_info(self, instance_id: int) -> Dict[str, Any]:
        """
        Get information about a specific instance synchronously.
        
        Args:
            instance_id: ID of the instance
            
        Returns:
            Dict[str, Any]: Instance information or empty dict if not found
        """
        if instance_id not in self.promises:
            return {}
            
        info = {
            "instanceId": instance_id,
            "highestPromised": self.promises[instance_id]
        }
        
        if instance_id in self.accepted:
            proposal_number, value = self.accepted[instance_id]
            info["accepted"] = True
            info["proposalNumber"] = proposal_number
            info["value"] = value
        else:
            info["accepted"] = False
            
        return info