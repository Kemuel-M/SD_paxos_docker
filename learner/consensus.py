"""
File: learner/consensus.py
Implementation of the consensus tracking and detection logic for the Learner component.
Responsible for tracking votes from acceptors and determining when consensus is reached.
"""
import os
import time
import logging
import asyncio
from typing import Dict, Any, Set, List, Optional, Tuple
from collections import defaultdict

# Enhanced debug configuration
DEBUG = os.getenv("DEBUG", "false").lower() in ("true", "1", "yes")
DEBUG_LEVEL = os.getenv("DEBUG_LEVEL", "basic").lower()  # Levels: basic, advanced, trace

logger = logging.getLogger("learner")

class ConsensusManager:
    """
    Manages the consensus tracking and detection for the Learner component.
    
    Responsibilities:
    1. Track votes from acceptors for each Paxos instance
    2. Determine when a consensus has been reached (majority of acceptors agreed)
    3. Notify when a decision has been made for a specific instance
    """
    
    def __init__(self, node_id: int, quorum_size: int = 3):
        """
        Initialize the ConsensusManager.
        
        Args:
            node_id: ID of the learner
            quorum_size: Number of acceptors required for quorum (default: 3)
        """
        self.node_id = node_id
        self.quorum_size = quorum_size
        
        # Structure to track votes:
        # instance_id -> proposal_number -> value -> set of acceptor_ids
        self.votes = defaultdict(lambda: defaultdict(lambda: defaultdict(set)))
        
        # Track decided instances: instance_id -> (proposal_number, value)
        self.decisions = {}
        
        # Track decision callbacks: instance_id -> callback
        self.decision_callbacks = {}
        
        # Statistics
        self.notifications_processed = 0
        self.decisions_made = 0
        
        # Lock for thread safety
        self.lock = asyncio.Lock()
        
        logger.info(f"ConsensusManager initialized with quorum size {quorum_size}")
    
    async def process_notification(self, notification: Dict[str, Any]) -> bool:
        """
        Process a notification from an acceptor and check if consensus is reached.
        
        Args:
            notification: Notification message from an acceptor
            
        Returns:
            bool: True if the notification led to a new decision, False otherwise
        """
        # Extract information from notification
        instance_id = notification.get("instanceId")
        proposal_number = notification.get("proposalNumber")
        acceptor_id = notification.get("acceptorId")
        accepted = notification.get("accepted", False)
        value = notification.get("value")
        
        # Check if notification is valid
        if not accepted or None in (instance_id, proposal_number, acceptor_id, value):
            logger.warning(f"Invalid notification received: {notification}")
            return False
        
        decision_made = False
        
        async with self.lock:
            # Update statistics
            self.notifications_processed += 1
            
            # Check if this instance is already decided
            if instance_id in self.decisions:
                if DEBUG and DEBUG_LEVEL in ("advanced", "trace"):
                    logger.debug(f"Notification for already decided instance {instance_id}")
                return False
            
            # Add vote
            self.votes[instance_id][proposal_number][self._hash_value(value)].add(acceptor_id)
            
            if DEBUG and DEBUG_LEVEL in ("advanced", "trace"):
                logger.debug(f"Added vote for instance {instance_id}, proposal {proposal_number} from acceptor {acceptor_id}")
                logger.debug(f"Current votes for instance {instance_id}, proposal {proposal_number}: {self.votes[instance_id][proposal_number]}")
            
            # Check if consensus is reached
            for prop_num in sorted(self.votes[instance_id].keys(), reverse=True):
                for val_hash, acceptors in self.votes[instance_id][prop_num].items():
                    if len(acceptors) >= self.quorum_size:
                        # Consensus reached, mark as decided
                        self.decisions[instance_id] = (prop_num, value)
                        self.decisions_made += 1
                        
                        logger.info(f"Consensus reached for instance {instance_id} with proposal {prop_num}")
                        
                        if DEBUG:
                            logger.debug(f"Decided value for instance {instance_id}: {value}")
                        
                        # Call decision callback if registered
                        if instance_id in self.decision_callbacks:
                            # Schedule callback execution
                            asyncio.create_task(self._execute_callback(instance_id, prop_num, value))
                        
                        decision_made = True
                        break
                
                if decision_made:
                    break
        
        return decision_made
    
    async def _execute_callback(self, instance_id: int, proposal_number: int, value: Any):
        """
        Execute the registered callback for a decision.
        
        Args:
            instance_id: ID of the instance
            proposal_number: Proposal number that was decided
            value: Decided value
        """
        try:
            callback = self.decision_callbacks.pop(instance_id, None)
            if callback:
                await callback(instance_id, proposal_number, value)
        except Exception as e:
            logger.error(f"Error executing decision callback for instance {instance_id}: {e}", exc_info=True)
    
    def register_decision_callback(self, instance_id: int, callback):
        """
        Register a callback to be called when a decision is made for the given instance.
        
        Args:
            instance_id: ID of the instance
            callback: Async function to be called with (instance_id, proposal_number, value)
        """
        # If the instance is already decided, call the callback immediately
        if instance_id in self.decisions:
            proposal_number, value = self.decisions[instance_id]
            asyncio.create_task(callback(instance_id, proposal_number, value))
            return
        
        # Otherwise register the callback
        self.decision_callbacks[instance_id] = callback
    
    def is_decided(self, instance_id: int) -> bool:
        """
        Check if an instance has been decided.
        
        Args:
            instance_id: ID of the instance
            
        Returns:
            bool: True if the instance has been decided, False otherwise
        """
        return instance_id in self.decisions
    
    def get_decision(self, instance_id: int) -> Optional[Tuple[int, Any]]:
        """
        Get the decision for a specific instance.
        
        Args:
            instance_id: ID of the instance
            
        Returns:
            Optional[Tuple[int, Any]]: Tuple with (proposal_number, value) if decided, None otherwise
        """
        return self.decisions.get(instance_id)
    
    def get_instance_info(self, instance_id: int) -> Dict[str, Any]:
        """
        Get detailed information about a specific instance.
        
        Args:
            instance_id: ID of the instance
            
        Returns:
            Dict[str, Any]: Information about the instance or empty dict if not found
        """
        if instance_id not in self.votes and instance_id not in self.decisions:
            return {}
        
        # Prepare base info
        info = {
            "instanceId": instance_id,
            "decided": instance_id in self.decisions,
            "votes": {}
        }
        
        # Add votes info
        if instance_id in self.votes:
            for prop_num, values in self.votes[instance_id].items():
                info["votes"][prop_num] = {
                    str(val_hash): list(acceptors) 
                    for val_hash, acceptors in values.items()
                }
        
        # Add decision info if decided
        if instance_id in self.decisions:
            proposal_number, value = self.decisions[instance_id]
            info["proposalNumber"] = proposal_number
            info["value"] = value
        
        return info
    
    def get_all_instance_ids(self, decided_only: bool = False) -> List[int]:
        """
        Get all instance IDs tracked by the ConsensusManager.
        
        Args:
            decided_only: If True, return only decided instances
            
        Returns:
            List[int]: List of instance IDs
        """
        if decided_only:
            return list(self.decisions.keys())
        
        # Combine instances in votes and decisions
        return list(set(self.votes.keys()) | set(self.decisions.keys()))
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the ConsensusManager.
        
        Returns:
            Dict[str, Any]: Statistics
        """
        return {
            "active_instances": len(self.votes),
            "decided_instances": len(self.decisions),
            "notifications_processed": self.notifications_processed,
            "decisions_made": self.decisions_made
        }
    
    def get_memory_usage(self) -> Dict[str, int]:
        """
        Get memory usage statistics.
        
        Returns:
            Dict[str, int]: Memory usage statistics
        """
        return {
            "votes_count": sum(len(prop_nums) for prop_nums in self.votes.values()),
            "decisions_count": len(self.decisions)
        }
    
    def _hash_value(self, value: Any) -> str:
        """
        Create a hash representation of a value for comparison.
        
        Args:
            value: Value to hash
            
        Returns:
            str: Hash representation
        """
        # For simple values, use string representation
        if isinstance(value, (str, int, float, bool, type(None))):
            return str(value)
        
        # For dictionaries, create a sorted representation of key-value pairs
        if isinstance(value, dict):
            sorted_items = sorted(value.items())
            return str(sorted_items)
        
        # For lists, create a string of the list elements
        if isinstance(value, list):
            return str(value)
        
        # Default: use string representation
        return str(value)