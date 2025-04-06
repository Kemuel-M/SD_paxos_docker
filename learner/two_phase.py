"""
File: learner/two_phase.py
Implementation of the Two-Phase Commit (2PC) protocol for the Learner component.
Responsible for coordinating atomic writes across multiple Cluster Store nodes.
"""
import os
import time
import logging
import asyncio
from typing import Dict, Any, List, Optional, Tuple
from collections import defaultdict

from common.communication import HttpClient
from common.utils import current_timestamp, wait_with_backoff

# Enhanced debug configuration
DEBUG = os.getenv("DEBUG", "false").lower() in ("true", "1", "yes")
DEBUG_LEVEL = os.getenv("DEBUG_LEVEL", "basic").lower()  # Levels: basic, advanced, trace

logger = logging.getLogger("learner")

class TwoPhaseCommitManager:
    """
    Implements the Two-Phase Commit protocol for atomic distributed transactions.
    
    The protocol consists of two phases:
    1. Prepare Phase: Ask all participants if they can commit the transaction
    2. Commit/Abort Phase: If all agree, tell all to commit; otherwise tell all to abort
    """
    
    def __init__(self, node_id: int, stores: List[str]):
        """
        Initialize the TwoPhaseCommitManager.
        
        Args:
            node_id: ID of the learner acting as coordinator
            stores: List of Cluster Store addresses (participants)
        """
        self.node_id = node_id
        self.stores = stores
        
        # HTTP client for communication
        self.http_client = HttpClient()
        
        # Active transactions
        self.active_transactions = {}
        
        # Statistics
        self.transactions_started = 0
        self.transactions_committed = 0
        self.transactions_aborted = 0
        
        # Lock for thread safety
        self.lock = asyncio.Lock()
        
        logger.info(f"TwoPhaseCommitManager initialized with {len(stores)} participants")
        
        if DEBUG and DEBUG_LEVEL in ("advanced", "trace"):
            logger.debug(f"Participants: {self.stores}")
    
    async def execute_transaction(self, resource_id: str, value: Dict[str, Any], 
                                max_retries: int = 3) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """
        Execute a transaction using the Two-Phase Commit protocol.
        
        Args:
            resource_id: ID of the resource involved in the transaction
            value: Value to write
            max_retries: Maximum number of retry attempts
            
        Returns:
            Tuple[bool, Optional[Dict[str, Any]]]: (success, result_data)
        """
        instance_id = value.get("instanceId")
        
        async with self.lock:
            self.transactions_started += 1
            transaction_id = f"{instance_id}_{current_timestamp()}"
            self.active_transactions[transaction_id] = {
                "status": "started",
                "resource_id": resource_id,
                "value": value,
                "ready_participants": set(),
                "start_time": current_timestamp()
            }
        
        logger.info(f"Starting transaction {transaction_id} for resource {resource_id}")
        
        # Retry loop
        for attempt in range(max_retries):
            if attempt > 0:
                logger.info(f"Retry attempt {attempt+1}/{max_retries} for transaction {transaction_id}")
                await wait_with_backoff(attempt)
            
            try:
                # Phase 1: Prepare
                prepare_success, prepare_result = await self._prepare_phase(transaction_id, resource_id, value)
                
                if not prepare_success:
                    logger.warning(f"Prepare phase failed for transaction {transaction_id}")
                    continue  # Retry
                
                # Get the next version from the prepare results
                next_version = max([p.get("currentVersion", 0) for p in prepare_result.values()]) + 1
                
                # Phase 2: Commit
                commit_success, commit_result = await self._commit_phase(
                    transaction_id, resource_id, value, next_version)
                
                if not commit_success:
                    logger.warning(f"Commit phase failed for transaction {transaction_id}")
                    continue  # Retry
                
                # Transaction succeeded
                async with self.lock:
                    self.transactions_committed += 1
                    self.active_transactions[transaction_id]["status"] = "committed"
                    self.active_transactions[transaction_id]["end_time"] = current_timestamp()
                
                logger.info(f"Transaction {transaction_id} committed successfully")
                
                # Return the commit result
                return True, commit_result
                
            except Exception as e:
                logger.error(f"Error executing transaction {transaction_id}: {e}", exc_info=True)
        
        # If all retries failed
        async with self.lock:
            self.transactions_aborted += 1
            self.active_transactions[transaction_id]["status"] = "aborted"
            self.active_transactions[transaction_id]["end_time"] = current_timestamp()
        
        logger.error(f"Transaction {transaction_id} aborted after {max_retries} attempts")
        return False, None
    
    async def _prepare_phase(self, transaction_id: str, resource_id: str, 
                          value: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        """
        Execute the prepare phase of the Two-Phase Commit protocol.
        
        Args:
            transaction_id: ID of the transaction
            resource_id: ID of the resource
            value: Value to prepare
            
        Returns:
            Tuple[bool, Dict[str, Any]]: (success, prepare_results)
        """
        logger.info(f"Starting prepare phase for transaction {transaction_id}")
        
        # Prepare the prepare request
        prepare_request = {
            "type": "PREPARE",
            "transactionId": transaction_id,
            "instanceId": value.get("instanceId"),
            "resource": resource_id,
            "data": value.get("data"),
            "clientId": value.get("clientId"),
            "timestamp": value.get("timestamp"),
            "coordinatorId": self.node_id
        }
        
        if DEBUG and DEBUG_LEVEL in ("advanced", "trace"):
            logger.debug(f"Prepare request: {prepare_request}")
        
        # Send prepare request to all participants
        prepare_results = {}
        all_ready = True
        
        async with self.lock:
            # Clear previous ready participants
            self.active_transactions[transaction_id]["ready_participants"] = set()
        
        # Create tasks for each participant
        tasks = []
        for store in self.stores:
            tasks.append(self._send_prepare_to_participant(store, prepare_request, transaction_id))
        
        # Wait for all tasks to complete
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        for i, result in enumerate(results):
            store = self.stores[i]
            
            if isinstance(result, Exception):
                logger.warning(f"Error preparing store {store}: {result}")
                all_ready = False
                prepare_results[store] = {"ready": False, "error": str(result)}
            else:
                prepare_results[store] = result
                if result.get("ready", False):
                    async with self.lock:
                        self.active_transactions[transaction_id]["ready_participants"].add(store)
                else:
                    all_ready = False
        
        if all_ready and len(self.active_transactions[transaction_id]["ready_participants"]) == len(self.stores):
            logger.info(f"All participants ready for transaction {transaction_id}")
        else:
            logger.warning(f"Some participants not ready for transaction {transaction_id}")
            # Execute abort phase if any participant is not ready or not enough participants
            # For ROWA (Nw=N), we need ALL stores to be ready
            await self._abort_phase(transaction_id, resource_id, value)
        
        return all_ready, prepare_results
    
    async def _send_prepare_to_participant(self, store: str, prepare_request: Dict[str, Any], 
                                        transaction_id: str) -> Dict[str, Any]:
        """
        Send a prepare request to a specific participant.
        
        Args:
            store: Address of the store
            prepare_request: Prepare request data
            transaction_id: ID of the transaction
            
        Returns:
            Dict[str, Any]: Prepare response
        """
        try:
            url = f"http://{store}/prepare"
            result = await self.http_client.post(url, json=prepare_request, timeout=0.5)
            
            if DEBUG and DEBUG_LEVEL == "trace":
                logger.debug(f"Prepare response from {store}: {result}")
            
            return result
            
        except Exception as e:
            logger.warning(f"Failed to prepare store {store} for transaction {transaction_id}: {e}")
            raise
    
    async def _commit_phase(self, transaction_id: str, resource_id: str, 
                         value: Dict[str, Any], next_version: int) -> Tuple[bool, Dict[str, Any]]:
        """
        Execute the commit phase of the Two-Phase Commit protocol.
        
        Args:
            transaction_id: ID of the transaction
            resource_id: ID of the resource
            value: Value to commit
            next_version: Next version number for the resource
            
        Returns:
            Tuple[bool, Dict[str, Any]]: (success, commit_result)
        """
        logger.info(f"Starting commit phase for transaction {transaction_id}")
        
        # Get the list of ready participants
        ready_participants = set()
        async with self.lock:
            if transaction_id in self.active_transactions:
                ready_participants = self.active_transactions[transaction_id]["ready_participants"]
        
        if not ready_participants:
            logger.warning(f"No ready participants for transaction {transaction_id}")
            return False, {}
        
        # Prepare the commit request
        commit_request = {
            "type": "COMMIT",
            "transactionId": transaction_id,
            "instanceId": value.get("instanceId"),
            "resource": resource_id,
            "data": value.get("data"),
            "version": next_version,
            "clientId": value.get("clientId"),
            "timestamp": value.get("timestamp"),
            "coordinatorId": self.node_id
        }
        
        if DEBUG and DEBUG_LEVEL in ("advanced", "trace"):
            logger.debug(f"Commit request: {commit_request}")
        
        # Send commit request to all ready participants
        commit_results = {}
        all_committed = True
        
        # Create tasks for each ready participant
        tasks = []
        for store in ready_participants:
            tasks.append(self._send_commit_to_participant(store, commit_request, transaction_id))
        
        # Wait for all tasks to complete
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        for i, result in enumerate(results):
            store = list(ready_participants)[i]
            
            if isinstance(result, Exception):
                logger.warning(f"Error committing to store {store}: {result}")
                all_committed = False
                commit_results[store] = {"success": False, "error": str(result)}
            else:
                commit_results[store] = result
                if not result.get("success", False):
                    all_committed = False
        
        if all_committed:
            logger.info(f"All participants committed transaction {transaction_id}")
            # Return the first result (they should all be the same)
            return True, next(iter(commit_results.values())) if commit_results else {}
        else:
            logger.warning(f"Some participants failed to commit transaction {transaction_id}")
            return False, {}
    
    async def _send_commit_to_participant(self, store: str, commit_request: Dict[str, Any], 
                                       transaction_id: str) -> Dict[str, Any]:
        """
        Send a commit request to a specific participant.
        
        Args:
            store: Address of the store
            commit_request: Commit request data
            transaction_id: ID of the transaction
            
        Returns:
            Dict[str, Any]: Commit response
        """
        try:
            url = f"http://{store}/commit"
            result = await self.http_client.post(url, json=commit_request, timeout=0.5)
            
            if DEBUG and DEBUG_LEVEL == "trace":
                logger.debug(f"Commit response from {store}: {result}")
            
            return result
            
        except Exception as e:
            logger.warning(f"Failed to commit transaction {transaction_id} to store {store}: {e}")
            raise
    
    async def _abort_phase(self, transaction_id: str, resource_id: str, value: Dict[str, Any]) -> bool:
        """
        Execute the abort phase of the Two-Phase Commit protocol.
        
        Args:
            transaction_id: ID of the transaction
            resource_id: ID of the resource
            value: Value of the aborted transaction
            
        Returns:
            bool: True if abort was successful, False otherwise
        """
        logger.info(f"Starting abort phase for transaction {transaction_id}")
        
        # Get the list of ready participants
        ready_participants = set()
        async with self.lock:
            if transaction_id in self.active_transactions:
                ready_participants = self.active_transactions[transaction_id]["ready_participants"]
                self.active_transactions[transaction_id]["status"] = "aborting"
        
        if not ready_participants:
            logger.info(f"No ready participants to abort for transaction {transaction_id}")
            async with self.lock:
                self.transactions_aborted += 1
                if transaction_id in self.active_transactions:
                    self.active_transactions[transaction_id]["status"] = "aborted"
                    self.active_transactions[transaction_id]["end_time"] = current_timestamp()
            return True
        
        # Prepare the abort request
        abort_request = {
            "type": "ABORT",
            "transactionId": transaction_id,
            "instanceId": value.get("instanceId"),
            "resource": resource_id,
            "clientId": value.get("clientId"),
            "reason": "Some participants not ready",
            "coordinatorId": self.node_id
        }
        
        if DEBUG and DEBUG_LEVEL in ("advanced", "trace"):
            logger.debug(f"Abort request: {abort_request}")
        
        # Send abort request to all ready participants
        all_aborted = True
        
        # Create tasks for each ready participant
        tasks = []
        for store in ready_participants:
            tasks.append(self._send_abort_to_participant(store, abort_request, transaction_id))
        
        # Wait for all tasks to complete
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        for i, result in enumerate(results):
            store = list(ready_participants)[i]
            
            if isinstance(result, Exception):
                logger.warning(f"Error aborting transaction on store {store}: {result}")
                all_aborted = False
        
        async with self.lock:
            self.transactions_aborted += 1
            if transaction_id in self.active_transactions:
                self.active_transactions[transaction_id]["status"] = "aborted"
                self.active_transactions[transaction_id]["end_time"] = current_timestamp()
        
        if all_aborted:
            logger.info(f"Transaction {transaction_id} aborted successfully")
        else:
            logger.warning(f"Some participants failed to abort transaction {transaction_id}")
        
        return all_aborted
    
    async def _send_abort_to_participant(self, store: str, abort_request: Dict[str, Any], 
                                      transaction_id: str) -> Dict[str, Any]:
        """
        Send an abort request to a specific participant.
        
        Args:
            store: Address of the store
            abort_request: Abort request data
            transaction_id: ID of the transaction
            
        Returns:
            Dict[str, Any]: Abort response
        """
        try:
            url = f"http://{store}/abort"
            result = await self.http_client.post(url, json=abort_request, timeout=0.5)
            
            if DEBUG and DEBUG_LEVEL == "trace":
                logger.debug(f"Abort response from {store}: {result}")
            
            return result
            
        except Exception as e:
            logger.warning(f"Failed to abort transaction {transaction_id} on store {store}: {e}")
            raise
    
    def get_transaction_status(self, transaction_id: str) -> Optional[Dict[str, Any]]:
        """
        Get the status of a transaction.
        
        Args:
            transaction_id: ID of the transaction
            
        Returns:
            Optional[Dict[str, Any]]: Transaction status or None if not found
        """
        if transaction_id not in self.active_transactions:
            return None
        
        return self.active_transactions[transaction_id]
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the TwoPhaseCommitManager.
        
        Returns:
            Dict[str, Any]: Statistics
        """
        return {
            "transactions_started": self.transactions_started,
            "transactions_committed": self.transactions_committed,
            "transactions_aborted": self.transactions_aborted,
            "active_transactions": len([t for t in self.active_transactions.values() 
                                     if t["status"] in ("started", "preparing", "committing")])
        }