"""
File: learner/learner.py
Implementation of the Learner component for the Paxos consensus algorithm.
Responsible for receiving notifications from acceptors, detecting consensus,
and taking appropriate actions based on decided values.
"""
import os
import time
import logging
import asyncio
import random
import hashlib
from typing import Dict, List, Any, Optional, Set, Tuple
from collections import defaultdict

from common.communication import HttpClient
from common.utils import current_timestamp

# Enhanced debug configuration
DEBUG = os.getenv("DEBUG", "false").lower() in ("true", "1", "yes")
DEBUG_LEVEL = os.getenv("DEBUG_LEVEL", "basic").lower()  # Levels: basic, advanced, trace

logger = logging.getLogger("learner")

class Learner:
    """
    Implementation of the Learner in the Paxos consensus algorithm.
    
    Responsibilities:
    1. Receive notifications from acceptors about accepted proposals
    2. Detect when consensus is reached (majority of acceptors agree)
    3. For Part 1: Simulate access to the resource
    4. For Part 2: Implement ROWA protocol for accessing the Cluster Store
    5. Notify clients about the result of their requests
    """
    
    def __init__(self, node_id: int, acceptors: List[str], stores: Optional[List[str]] = None,
                consensus_manager=None, rowa_manager=None, use_cluster_store: bool = False):
        """
        Initialize the Learner component.
        
        Args:
            node_id: ID of this learner
            acceptors: List of acceptor addresses
            stores: List of Cluster Store addresses (for Part 2)
            consensus_manager: ConsensusManager instance
            rowa_manager: RowaManager instance (for Part 2)
            use_cluster_store: Whether to use Cluster Store (Part 2) or simulate access (Part 1)
        """
        self.node_id = node_id
        self.acceptors = acceptors
        self.stores = stores
        self.consensus_manager = consensus_manager
        self.rowa_manager = rowa_manager
        self.use_cluster_store = use_cluster_store
        
        # HTTP client for communication
        self.http_client = HttpClient()
        
        # Track pending client requests: instance_id -> client_callback_info
        self.pending_clients = {}
        
        # Statistics
        self.notifications_processed = 0
        self.decisions_made = 0
        
        # Client notification tracking
        self.client_notifications_sent = 0
        self.client_notifications_failed = 0
        
        # Processing flag
        self.running = False
        
        # Pending tasks
        self._pending_tasks = set()
        
        logger.info(f"Learner {node_id} initialized with {len(acceptors)} acceptors")
        
        if self.use_cluster_store:
            logger.info(f"Using Cluster Store with {len(stores)} nodes")
        else:
            logger.info("Simulating resource access (Part 1)")
    
    async def start(self):
        """Start the learner asynchronously."""
        if self.running:
            return
        
        self.running = True
        
        # Register decision callback with consensus manager
        for instance_id in self.consensus_manager.get_all_instance_ids():
            self.consensus_manager.register_decision_callback(
                instance_id, self._on_decision_made)
        
        logger.info(f"Learner {self.node_id} started")
    
    async def stop(self):
        """Stop the learner asynchronously."""
        if not self.running:
            return
        
        self.running = False
        
        # Cancel pending tasks
        for task in self._pending_tasks:
            if not task.done():
                task.cancel()
        
        # Wait for tasks to finish
        if self._pending_tasks:
            try:
                await asyncio.wait(list(self._pending_tasks), timeout=5.0)
            except asyncio.TimeoutError:
                logger.warning(f"Timeout waiting for {len(self._pending_tasks)} pending tasks to finish")
        
        # Close HTTP client
        await self.http_client.close()
        
        logger.info(f"Learner {self.node_id} stopped")
    
    async def process_learn_notification(self, notification: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a learn notification from an acceptor.
        
        Args:
            notification: Notification message
            
        Returns:
            Dict[str, Any]: Response
        """
        # Validate notification
        if not self._validate_notification(notification):
            logger.warning(f"Invalid notification received: {notification}")
            return {"accepted": False, "reason": "Invalid notification"}
        
        # Update statistics
        self.notifications_processed += 1
        
        instance_id = notification.get("instanceId")
        
        if DEBUG and DEBUG_LEVEL in ("advanced", "trace"):
            logger.debug(f"Processing notification for instance {instance_id} from acceptor {notification.get('acceptorId')}")
        
        # Process notification with consensus manager
        decision_made = await self.consensus_manager.process_notification(notification)
        
        if decision_made:
            logger.info(f"Decision made for instance {instance_id}")
            
            # Register callback for the decision if it wasn't already called
            # (This happens if consensus was reached with this notification)
            if not self.consensus_manager.is_decided(instance_id):
                self.consensus_manager.register_decision_callback(
                    instance_id, self._on_decision_made)

        return {"accepted": True, "instanceId": instance_id, "learnerId": self.node_id}
    
    async def _on_decision_made(self, instance_id: int, proposal_number: int, value: Dict[str, Any]):
        """
        Callback when a decision is made for an instance.
        
        Args:
            instance_id: ID of the instance
            proposal_number: Proposal number that was decided
            value: Decided value
        """
        logger.info(f"Decision callback for instance {instance_id}, proposal {proposal_number}")
        
        # Update statistics
        self.decisions_made += 1
        
        # Process the decided value
        task = asyncio.create_task(self._process_decided_value(instance_id, proposal_number, value))
        self._pending_tasks.add(task)
        task.add_done_callback(self._pending_tasks.discard)
    
    async def _process_decided_value(self, instance_id: int, proposal_number: int, value: Dict[str, Any]):
        """
        Process a decided value.
        
        Args:
            instance_id: ID of the instance
            proposal_number: Proposal number that was decided
            value: Decided value
        """
        try:
            logger.info(f"Processing decided value for instance {instance_id}")
            
            if DEBUG:
                logger.debug(f"Decided value: {value}")
            
            # Extract information from value
            client_id = value.get("clientId")
            resource_id = value.get("resource", "R")  # Default to "R" per specification
            operation = value.get("operation", "WRITE")  # Default to "WRITE" per specification
            data = value.get("data", "")
            timestamp = value.get("timestamp", current_timestamp())
            
            # Determine if this learner should handle client notification
            should_notify = self._should_handle_client_notification(client_id)
            
            # Result to send to client
            result = {
                "status": "COMMITTED",
                "instanceId": instance_id,
                "resource": resource_id,
                "timestamp": current_timestamp()
            }
            
            # If using Cluster Store (Part 2)
            if self.use_cluster_store and self.rowa_manager:
                # Perform write using ROWA protocol
                success, write_result = await self.rowa_manager.write_resource(
                    resource_id, data, client_id, instance_id, timestamp)
                
                if not success:
                    logger.warning(f"Write failed for instance {instance_id}")
                    result["status"] = "NOT_COMMITTED"
                    # Adicionar razão da falha pode ser útil para debugging
                    if DEBUG:
                        result["reason"] = "Write to Cluster Store failed"
            else:
                # Simulate resource access (Part 1)
                delay = await self.rowa_manager.simulate_resource_access() if self.rowa_manager else await self._simulate_resource_access()
                logger.info(f"Simulated resource access for instance {instance_id} took {delay:.3f}s")
            
            # Notify client if this learner is responsible
            if should_notify:
                # Check if we have client callback information
                callback_info = self.pending_clients.get(instance_id)
                
                if callback_info:
                    callback_url = callback_info.get("callback_url")
                    await self._notify_client(callback_url, result)
                else:
                    logger.warning(f"No callback information for instance {instance_id}")
                    
                    # Try to notify based on client ID
                    # This is a fallback mechanism and might not work in all cases
                    if client_id:
                        logger.info(f"Attempting fallback notification for client {client_id}")
                        # Construct a likely callback URL based on client ID
                        # Format might vary based on your actual implementation
                        callback_url = f"http://{client_id}/notification"
                        await self._notify_client(callback_url, result)
            
            logger.info(f"Completed processing for instance {instance_id} with status {result['status']}")
            
        except Exception as e:
            logger.error(f"Error processing decided value for instance {instance_id}: {e}", exc_info=True)
    
    async def _simulate_resource_access(self) -> float:
        """
        Simulate access to a resource by waiting a random amount of time.
        Used in Part 1 when Cluster Store is not available.
        
        Returns:
            float: Time spent in seconds
        """
        # Wait between 0.2 and 1.0 seconds with millisecond precision
        delay = random.uniform(0.2, 1.0)
        await asyncio.sleep(delay)
        return delay
    
    async def _notify_client(self, callback_url: str, result: Dict[str, Any]) -> bool:
        """
        Notify a client about the result of their request.
        
        Args:
            callback_url: URL to send notification to
            result: Result data to send
            
        Returns:
            bool: True if notification was successful, False otherwise
        """
        if not callback_url:
            logger.warning("No callback URL provided for client notification")
            return False
        
        try:
            logger.info(f"Notifying client at {callback_url}")
            
            if DEBUG and DEBUG_LEVEL in ("advanced", "trace"):
                logger.debug(f"Notification data: {result}")
            
            await self.http_client.post(callback_url, json=result, timeout=5.0)
            
            # Update statistics
            self.client_notifications_sent += 1
            
            logger.info(f"Client notification sent successfully to {callback_url}")
            return True
            
        except Exception as e:
            # Update statistics
            self.client_notifications_failed += 1
            
            logger.error(f"Failed to notify client at {callback_url}: {e}")
            return False
    
    def register_client_callback(self, instance_id: int, client_id: str, callback_url: str):
        """
        Register a client callback for a specific instance.
        
        Args:
            instance_id: ID of the instance
            client_id: ID of the client
            callback_url: URL to notify when decision is made
        """
        self.pending_clients[instance_id] = {
            "client_id": client_id,
            "callback_url": callback_url,
            "timestamp": current_timestamp()
        }
        
        logger.info(f"Registered client {client_id} for instance {instance_id}")
    
    def _should_handle_client_notification(self, client_id: str) -> bool:
        """
        Determine if this learner should handle client notification.
        Based on consistent hashing of client ID.
        
        Args:
            client_id: ID of the client
            
        Returns:
            bool: True if this learner should handle notification, False otherwise
        """
        # Constantes segundo a especificação (exatamente 2 learners)
        TOTAL_LEARNERS = 2
        
        if not client_id:
            # If no client ID, use learner 1 as default
            return self.node_id == 1
        
        # Use consistent hashing to determine responsible learner
        hash_value = int(hashlib.md5(client_id.encode()).hexdigest(), 16)
        responsible_learner = (hash_value % TOTAL_LEARNERS) + 1  # Learner IDs are 1-based
        
        return self.node_id == responsible_learner
    
    def _validate_notification(self, notification: Dict[str, Any]) -> bool:
        """
        Validate a notification message.
        
        Args:
            notification: Notification message
            
        Returns:
            bool: True if notification is valid, False otherwise
        """
        # Check required fields
        required_fields = ["instanceId", "proposalNumber", "acceptorId", "accepted", "value", "timestamp"]
        for field in required_fields:
            if field not in notification:
                logger.warning(f"Notification missing required field: {field}")
                return False
        
        # Check that accepted is True (we only care about acceptances)
        if not notification.get("accepted", False):
            logger.warning(f"Notification with accepted=False: {notification}")
            return False
        
        return True
    
    def get_status(self) -> Dict[str, Any]:
        """
        Get the current status of the learner.
        
        Returns:
            Dict[str, Any]: Status information
        """
        consensus_stats = self.consensus_manager.get_stats() if self.consensus_manager else {}
        
        status = {
            "node_id": self.node_id,
            "state": "running" if self.running else "stopped",
            "active_instances": consensus_stats.get("active_instances", 0),
            "decided_instances": consensus_stats.get("decided_instances", 0),
            "notifications_processed": self.notifications_processed,
            "decisions_made": self.decisions_made,
            "client_notifications_sent": self.client_notifications_sent,
            "client_notifications_failed": self.client_notifications_failed,
            "use_cluster_store": self.use_cluster_store,
            "timestamp": current_timestamp()
        }
        
        # Add ROWA stats if available
        if self.rowa_manager:
            rowa_stats = self.rowa_manager.get_stats()
            status["rowa"] = rowa_stats
        
        return status