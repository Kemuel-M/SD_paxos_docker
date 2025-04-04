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
        """
        Initialize the Acceptor.
        
        Args:
            node_id: Unique ID of this Acceptor (1-5)
            learners: List of learner addresses
            persistence: Persistence manager (if None, a new one will be created)
        """
        self.node_id = node_id
        self.learners = learners if learners else []
        self.persistence = persistence

        self._pending_tasks = []  # Inicializa a lista de tarefas pendentes
        
        # State structures (will be loaded from persistence)
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
        
        # Lock for state modification
        self.state_lock = asyncio.Lock()
        
        # Processing flag
        self.running = False
        
        logger.info(f"Acceptor {node_id} initialized with {len(learners)} learners")
        
        if DEBUG and DEBUG_LEVEL in ("advanced", "trace"):
            logger.debug(f"Learners: {self.learners}")
    
    async def start(self):
        """Start the acceptor."""
        if self.running:
            return
            
        self.running = True
        
        # Load state from persistence if available
        if self.persistence:
            state = await self.persistence.load_state()
            
            async with self.state_lock:
                self.promises = state.get("promises", {})
                self.accepted = state.get("accepted", {})
                
                # Convert keys from strings to integers (JSON serialization effect)
                self.promises = {int(k): v for k, v in self.promises.items()}
                self.accepted = {int(k): v for k, v in self.accepted.items()}
                
                # Load statistics
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
        """Stop the acceptor."""
        if not self.running:
            return
            
        self.running = False

        # Cancele todas as tarefas pendentes aqui
        for task in self._pending_tasks:
            if not task.done():
                task.cancel()
        
        # Persist state before stopping
        if self.persistence:
            await self.save_state()
            
        # Close HTTP client
        await self.http_client.close()
        
        logger.info(f"Acceptor {self.node_id} stopped")
    
    async def save_state(self):
        """Save acceptor state to persistence."""
        if not self.persistence:
            return
            
        state = {
            "promises": self.promises,
            "accepted": self.accepted,
            "prepare_requests_processed": self.prepare_requests_processed,
            "accept_requests_processed": self.accept_requests_processed,
            "promises_made": self.promises_made,
            "proposals_accepted": self.proposals_accepted
        }
        
        await self.persistence.save_state(state)
        
        if DEBUG and DEBUG_LEVEL == "trace":
            logger.debug(f"State saved to persistence: {len(self.promises)} promises, {len(self.accepted)} accepted values")
    
    async def process_prepare(self, prepare_msg: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a prepare request from a proposer.
        
        Args:
            prepare_msg: The prepare message from proposer
            
        Returns:
            Dict[str, Any]: Response to send back to proposer
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
        
        # Update statistics
        self.prepare_requests_processed += 1
        
        # Process the prepare request under lock to avoid race conditions
        async with self.state_lock:
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
                self.promises_made += 1
                
                if DEBUG:
                    logger.debug(f"Promise made for instance {instance_id} with proposal number {proposal_number} (previous: {highest_promised})")
                
                # Check if we have accepted a value for this instance
                accepted_proposal = -1
                accepted_value = None
                
                if instance_id in self.accepted:
                    accepted_proposal, accepted_value = self.accepted[instance_id]
                    
                    if DEBUG and DEBUG_LEVEL in ("advanced", "trace"):
                        logger.debug(f"Previously accepted value for instance {instance_id}: proposal {accepted_proposal}, value: {accepted_value}")
                
                # Save state after modification
                await self.save_state()
                
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
        Process an accept request from a proposer.
        
        Args:
            accept_msg: The accept message from proposer
            
        Returns:
            Dict[str, Any]: Response to send back to proposer
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
        
        # Update statistics
        self.accept_requests_processed += 1
        
        # Process the accept request under lock to avoid race conditions
        async with self.state_lock:
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
                self.proposals_accepted += 1
                
                logger.info(f"Proposal accepted for instance {instance_id} with proposal number {proposal_number}")
                
                if DEBUG and DEBUG_LEVEL in ("advanced", "trace"):
                    logger.debug(f"Accepted value for instance {instance_id}: {value}")
                
                # Save state after modification
                await self.save_state()
                
                # Notify learners about the acceptance (non-blocking)
                asyncio.create_task(self.notify_learners(instance_id, proposal_number, value))
                
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
        Notify all learners about an accepted proposal.
        
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
        Send notification to a specific learner with error handling.
        
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
        Get the current status of the acceptor.
        
        Returns:
            Dict[str, Any]: Status information
        """
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
            "timestamp": current_timestamp()
        }
    
    def get_instance_info(self, instance_id: int) -> Dict[str, Any]:
        """
        Get information about a specific instance.
        
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