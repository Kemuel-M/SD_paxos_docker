"""
File: learner/rowa.py
Implementation of the Read-One-Write-All (ROWA) protocol for the Learner component.
Responsible for coordinating read and write operations to the Cluster Store nodes.
"""
import os
import time
import logging
import asyncio
import random
from typing import Dict, Any, List, Optional, Tuple
from collections import defaultdict

from common.communication import HttpClient
from common.utils import current_timestamp

# Enhanced debug configuration
DEBUG = os.getenv("DEBUG", "false").lower() in ("true", "1", "yes")
DEBUG_LEVEL = os.getenv("DEBUG_LEVEL", "basic").lower()  # Levels: basic, advanced, trace

logger = logging.getLogger("learner")

class RowaManager:
    """
    Implements the Read-One-Write-All (ROWA) protocol for distributed data management.
    
    In ROWA:
    - Reads are performed on any single node (Nr=1)
    - Writes must be performed on all nodes (Nw=N)
    
    This protocol provides strong consistency but sacrifices availability 
    for write operations when any node is unavailable.
    """
    
    def __init__(self, node_id: int, stores: List[str], two_phase_manager):
        """
        Initialize the RowaManager.
        
        Args:
            node_id: ID of the learner
            stores: List of Cluster Store addresses
            two_phase_manager: TwoPhaseCommitManager for coordinating writes
        """
        self.node_id = node_id
        self.stores = stores
        self.two_phase_manager = two_phase_manager
        
        # HTTP client for communication
        self.http_client = HttpClient()
        
        # Current round-robin index for read operations
        self.current_read_index = 0
        
        # Statistics
        self.reads_processed = 0
        self.writes_processed = 0
        self.write_successes = 0
        self.write_failures = 0
        
        # Lock for thread safety
        self.lock = asyncio.Lock()
        
        logger.info(f"RowaManager initialized with {len(stores)} store nodes")
        
        if DEBUG and DEBUG_LEVEL in ("advanced", "trace"):
            logger.debug(f"Store nodes: {self.stores}")
    
    async def read_resource(self, resource_id: str) -> Optional[Dict[str, Any]]:
        """
        Read a resource from any available store node (Nr=1).
        Uses round-robin selection for load balancing.
        
        Args:
            resource_id: ID of the resource to read
            
        Returns:
            Optional[Dict[str, Any]]: Resource data or None if not found or error
        """
        if not self.stores:
            logger.error("No store nodes available for read operation")
            return None
        
        async with self.lock:
            self.reads_processed += 1
        
        # Try all stores in round-robin order until one succeeds
        for _ in range(len(self.stores)):
            # Get next store in round-robin
            async with self.lock:
                store = self.stores[self.current_read_index]
                self.current_read_index = (self.current_read_index + 1) % len(self.stores)
            
            try:
                logger.info(f"Reading resource {resource_id} from store {store}")
                
                # Send GET request to store
                url = f"http://{store}/resource/{resource_id}"
                result = await self.http_client.get(url, timeout=0.5)
                
                if DEBUG:
                    logger.debug(f"Read result from {store}: {result}")
                
                # Return the resource if successful
                return result
                
            except Exception as e:
                logger.warning(f"Failed to read from store {store}: {e}")
                continue
        
        # If all stores failed
        logger.error(f"Failed to read resource {resource_id} from any store")
        return None
    
    async def write_resource(self, resource_id: str, data: str, client_id: str, 
                           instance_id: int, client_timestamp: int) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """
        Write a resource to all store nodes (Nw=N) using Two-Phase Commit.
        
        Args:
            resource_id: ID of the resource to write
            data: Data to write
            client_id: ID of the client that initiated the request
            instance_id: ID of the Paxos instance
            client_timestamp: Timestamp of the client request
            
        Returns:
            Tuple[bool, Optional[Dict[str, Any]]]: (success, result_data)
        """
        if not self.stores:
            logger.error("No store nodes available for write operation")
            return False, None
        
        async with self.lock:
            self.writes_processed += 1
        
        logger.info(f"Writing resource {resource_id} from instance {instance_id} (client: {client_id})")
        
        # Prepare the value to write
        value = {
            "data": data,
            "instanceId": instance_id,
            "clientId": client_id,
            "timestamp": client_timestamp
        }
        
        # Use Two-Phase Commit for the write operation
        success, result = await self.two_phase_manager.execute_transaction(
            resource_id, value, max_retries=3)
        
        # Update statistics
        async with self.lock:
            if success:
                self.write_successes += 1
            else:
                self.write_failures += 1
        
        if success:
            logger.info(f"Successfully wrote resource {resource_id} (instance {instance_id})")
            if DEBUG:
                logger.debug(f"Write result: {result}")
        else:
            logger.error(f"Failed to write resource {resource_id} (instance {instance_id})")
        
        return success, result
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the RowaManager.
        
        Returns:
            Dict[str, Any]: Statistics
        """
        return {
            "reads_processed": self.reads_processed,
            "writes_processed": self.writes_processed,
            "write_successes": self.write_successes,
            "write_failures": self.write_failures,
            "success_rate": (self.write_successes / self.writes_processed) if self.writes_processed > 0 else 0
        }
    
    async def simulate_resource_access(self) -> float:
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