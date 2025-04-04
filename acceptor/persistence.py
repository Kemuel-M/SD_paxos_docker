"""
File: acceptor/persistence.py
Implementation of the persistence layer for the Acceptor component.
Responsible for saving and loading acceptor state between restarts.
"""
import os
import json
import logging
import asyncio
from typing import Dict, Any, Optional
from pathlib import Path

logger = logging.getLogger("acceptor")

class AcceptorPersistence:
    """
    Manages the persistence of the Acceptor's state.
    
    Responsible for:
    1. Saving the acceptor state to file
    2. Loading the state on initialization
    3. Creating periodic checkpoints
    """
    
    def __init__(self, node_id: int, data_dir: str = "/data"):
        """
        Initialize the persistence manager.
        
        Args:
            node_id: ID of the Acceptor
            data_dir: Directory for storing persistent data
        """
        self.node_id = node_id
        self.data_dir = Path(data_dir)
        
        # Create data directory if it doesn't exist
        os.makedirs(self.data_dir, exist_ok=True)
        
        # Path to the state file
        self.state_file = self.data_dir / f"acceptor_{node_id}_state.json"
        
        # Path to temporary file (for atomic writes)
        self.temp_file = self.data_dir / f"acceptor_{node_id}_state.tmp.json"
        
        # Path to checkpoint directory
        self.checkpoint_dir = self.data_dir / "checkpoints"
        os.makedirs(self.checkpoint_dir, exist_ok=True)
        
        # Write lock
        self.write_lock = asyncio.Lock()
        
        # Operation counter
        self.op_counter = 0
        
        logger.info(f"Persistence initialized. State file: {self.state_file}")
    
    def load_state(self) -> Dict[str, Any]:
        """
        Load the acceptor state from file synchronously.
        
        Returns:
            Dict[str, Any]: Loaded state or default state if file doesn't exist
        """
        try:
            if self.state_file.exists():
                with open(self.state_file, 'r') as f:
                    state = json.load(f)
                    logger.info(f"State loaded successfully: {len(state.get('promises', {}))} promises, "
                            f"{len(state.get('accepted', {}))} accepted values")
                    return state
        except Exception as e:
            logger.error(f"Error loading state: {e}", exc_info=True)
            
            # Try to restore from checkpoint if reading fails
            return self._restore_from_checkpoint()
        
        # Return default state if file doesn't exist or load fails
        logger.info("State file not found, initializing with default state")
        return self._get_default_state()
    
    async def save_state(self, state: Optional[Dict[str, Any]] = None) -> bool:
        """
        Save the acceptor state to file asynchronously.
        
        Args:
            state: State to be saved, or None to use default state
        
        Returns:
            bool: True if saved successfully, False otherwise
        """
        if state is None:
            state = self._get_default_state()
        
        async with self.write_lock:
            try:
                # Convert any instance IDs that are integers to strings (for JSON compatibility)
                if "promises" in state:
                    state["promises"] = {str(k): v for k, v in state["promises"].items()}
                
                if "accepted" in state:
                    state["accepted"] = {str(k): v for k, v in state["accepted"].items()}
                
                # Write to temporary file
                with open(self.temp_file, 'w') as f:
                    json.dump(state, f, indent=2)
                
                # Rename atomically to final file
                self.temp_file.replace(self.state_file)
                
                # Increment operation counter
                self.op_counter += 1
                
                # Create checkpoint every 100 operations
                if self.op_counter % 100 == 0:
                    await self._create_checkpoint(state)
                
                return True
                
            except Exception as e:
                logger.error(f"Error saving state: {e}", exc_info=True)
                return False
    
    async def _create_checkpoint(self, state: Dict[str, Any]) -> bool:
        """
        Create a checkpoint file of the current state asynchronously.
        
        Args:
            state: State to be checkpointed
        
        Returns:
            bool: True if checkpoint created successfully, False otherwise
        """
        try:
            # Create file name with timestamp and operation counter
            checkpoint_file = self.checkpoint_dir / f"acceptor_{self.node_id}_cp_{self.op_counter}_{int(asyncio.get_event_loop().time())}.json"
            
            # Write checkpoint
            with open(checkpoint_file, 'w') as f:
                json.dump(state, f, indent=2)
            
            logger.info(f"Checkpoint created: {checkpoint_file}")
            
            # Clean up old checkpoints (keep only the 5 most recent)
            await self._cleanup_old_checkpoints()
            
            return True
            
        except Exception as e:
            logger.error(f"Error creating checkpoint: {e}", exc_info=True)
            return False
    
    async def _cleanup_old_checkpoints(self, keep: int = 5):
        """
        Remove old checkpoints, keeping only the most recent ones asynchronously.
        
        Args:
            keep: Number of checkpoints to keep
        """
        try:
            # List checkpoints sorted by modification time
            checkpoints = sorted(
                [f for f in self.checkpoint_dir.glob(f"acceptor_{self.node_id}_cp_*.json")],
                key=lambda f: f.stat().st_mtime,
                reverse=True
            )
            
            # Remove the oldest ones, keeping 'keep' files
            for old_cp in checkpoints[keep:]:
                old_cp.unlink()
                logger.info(f"Old checkpoint removed: {old_cp}")
            
        except Exception as e:
            logger.error(f"Error cleaning up old checkpoints: {e}", exc_info=True)
    
    def _restore_from_checkpoint(self) -> Dict[str, Any]:
        """
        Try to restore state from the most recent checkpoint synchronously.
        
        Returns:
            Dict[str, Any]: Restored state from checkpoint or default state
        """
        try:
            # Find the most recent checkpoint
            checkpoints = sorted(
                [f for f in self.checkpoint_dir.glob(f"acceptor_{self.node_id}_cp_*.json")],
                key=lambda f: f.stat().st_mtime,
                reverse=True
            )
            
            if not checkpoints:
                logger.warning("No checkpoints found for restoration")
                return self._get_default_state()
            
            # Load the most recent checkpoint
            with open(checkpoints[0], 'r') as f:
                state = json.load(f)
                
            logger.info(f"State restored from checkpoint: {checkpoints[0]}")
            return state
            
        except Exception as e:
            logger.error(f"Error restoring from checkpoint: {e}", exc_info=True)
            return self._get_default_state()
    
    def _get_default_state(self) -> Dict[str, Any]:
        """
        Return the default state for initialization synchronously.
        
        Returns:
            Dict[str, Any]: Default state
        """
        return {
            "promises": {},  # instanceId -> highest proposal number promised
            "accepted": {},  # instanceId -> [proposal_number, value]
            "prepare_requests_processed": 0,
            "accept_requests_processed": 0,
            "promises_made": 0,
            "proposals_accepted": 0
        }