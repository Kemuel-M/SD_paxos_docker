"""
File: acceptor/tests/unit/test_persistence.py
Unit tests for the Acceptor persistence module.
"""
import os
import json
import time
import pytest
import asyncio
import tempfile
import shutil
from pathlib import Path
from unittest.mock import patch, AsyncMock

import warnings
warnings.filterwarnings("always", category=RuntimeWarning)

# Configure environment for testing
os.environ["DEBUG"] = "true"
os.environ["NODE_ID"] = "1"

from persistence import AcceptorPersistence

@pytest.fixture
def temp_data_dir():
    """Fixture that creates a temporary directory for test data."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir)

@pytest.fixture
def persistence(temp_data_dir):
    """Fixture that creates an instance of AcceptorPersistence for tests."""
    return AcceptorPersistence(node_id=1, data_dir=temp_data_dir)

def test_initialization(persistence, temp_data_dir):
    """Test if persistence is initialized correctly."""
    # Check if directories were created
    assert os.path.exists(temp_data_dir)
    assert os.path.exists(os.path.join(temp_data_dir, "checkpoints"))
    
    # Check file paths
    assert persistence.state_file == Path(temp_data_dir) / "acceptor_1_state.json"
    assert persistence.temp_file == Path(temp_data_dir) / "acceptor_1_state.tmp.json"
    assert persistence.checkpoint_dir == Path(temp_data_dir) / "checkpoints"

def test_load_state(persistence):
    """Test loading state from existing file."""
    # Create state file
    test_state = {
        "promises": {"42": 100},
        "accepted": {"42": [100, {"value": "test"}]}, 
        "prepare_requests_processed": 42,
        "accept_requests_processed": 30,
        "promises_made": 15,
        "proposals_accepted": 10
    }
    
    with open(persistence.state_file, 'w') as f:
        json.dump(test_state, f)
    
    # Load state (synchronous)
    state = persistence.load_state()
    
    # Check if loaded correctly (keys are converted to strings in JSON)
    assert state["promises"] == {"42": 100}  
    assert state["accepted"] == {"42": [100, {"value": "test"}]}
    assert state["prepare_requests_processed"] == 42
    assert state["accept_requests_processed"] == 30
    assert state["promises_made"] == 15
    assert state["proposals_accepted"] == 10

def test_load_state_existing(persistence):
    """Test loading state from existing file."""
    # Create state file
    test_state = {
        "promises": {"42": 100},
        "accepted": {"42": [100, {"value": "test"}]},
        "prepare_requests_processed": 42,
        "accept_requests_processed": 30,
        "promises_made": 15,
        "proposals_accepted": 10
    }
    
    with open(persistence.state_file, 'w') as f:
        json.dump(test_state, f)
    
    # Load state
    state = persistence.load_state()
    
    # Check if loaded correctly
    assert state["promises"] == {"42": 100}
    assert state["accepted"] == {"42": [100, {"value": "test"}]}
    assert state["prepare_requests_processed"] == 42
    assert state["accept_requests_processed"] == 30
    assert state["promises_made"] == 15
    assert state["proposals_accepted"] == 10

def test_load_state_corrupted(persistence):
    """Test loading when file is corrupted."""
    # Create corrupted file
    with open(persistence.state_file, 'w') as f:
        f.write("This is not valid JSON")
    
    # Try to load state
    state = persistence.load_state()
    
    # Check if returned default state
    assert "promises" in state
    assert "accepted" in state
    assert state["promises"] == {}
    assert state["accepted"] == {}

@pytest.mark.asyncio
async def test_save_state(persistence):
    """Test saving state."""
    # State to save
    test_state = {
        "promises": {42: 100, 43: 101},
        "accepted": {42: [100, {"value": "test"}]},
        "prepare_requests_processed": 42,
        "accept_requests_processed": 30,
        "promises_made": 15,
        "proposals_accepted": 10
    }
    
    # Save state
    result = await persistence.save_state(test_state)
    
    # Check result
    assert result == True
    
    # Check if file was created
    assert os.path.exists(persistence.state_file)
    
    # Check content
    with open(persistence.state_file, 'r') as f:
        saved_state = json.load(f)
        # Note: Integer keys are converted to strings in JSON
        assert saved_state["promises"] == {"42": 100, "43": 101}
        assert saved_state["accepted"] == {"42": [100, {"value": "test"}]}
        assert saved_state["prepare_requests_processed"] == 42
        assert saved_state["accept_requests_processed"] == 30
        assert saved_state["promises_made"] == 15
        assert saved_state["proposals_accepted"] == 10

@pytest.mark.asyncio
async def test_save_state_atomic(persistence):
    """Test atomicity of state saving."""
    # State to save
    test_state = {
        "promises": {42: 100},
        "accepted": {42: [100, {"value": "test"}]}
    }
    
    # Simulate failure in renaming the temporary file
    with patch('pathlib.Path.replace', side_effect=Exception("Simulated failure")):
        result = await persistence.save_state(test_state)
        
        # Check that it failed
        assert result == False
        
        # Check that final file doesn't exist
        assert not os.path.exists(persistence.state_file)

@pytest.mark.asyncio
async def test_checkpoint_creation(persistence):
    """Test creation of checkpoint."""
    # Set operation counter to force checkpoint
    persistence.op_counter = 99
    
    # State to save
    test_state = {
        "promises": {42: 100},
        "accepted": {42: [100, {"value": "test"}]}
    }
    
    # Save state
    result = await persistence.save_state(test_state)
    
    # Check result
    assert result == True
    
    # Check if counter was incremented
    assert persistence.op_counter == 100
    
    # Check if checkpoint was created
    checkpoints = list(persistence.checkpoint_dir.glob(f"acceptor_{persistence.node_id}_cp_*.json"))
    assert len(checkpoints) > 0
    
    # Check checkpoint content
    with open(checkpoints[0], 'r') as f:
        cp_state = json.load(f)
        assert cp_state["promises"] == {"42": 100}
        assert cp_state["accepted"] == {"42": [100, {"value": "test"}]}

@pytest.mark.asyncio
async def test_cleanup_old_checkpoints(persistence):
    """Test cleanup of old checkpoints."""
    # Create multiple test checkpoints
    for i in range(10):
        cp_file = persistence.checkpoint_dir / f"acceptor_{persistence.node_id}_cp_{i}_{int(time.time())}.json"
        with open(cp_file, 'w') as f:
            json.dump({"test": i}, f)
        
        # Small delay to ensure different timestamps
        await asyncio.sleep(0.01)
    
    # Run cleanup (keep 5)
    await persistence._cleanup_old_checkpoints(keep=5)
    
    # Check if only 5 checkpoints remain
    checkpoints = list(persistence.checkpoint_dir.glob(f"acceptor_{persistence.node_id}_cp_*.json"))
    assert len(checkpoints) == 5
    
    # Check if they are the most recent ones (highest test values)
    for cp in checkpoints:
        with open(cp, 'r') as f:
            state = json.load(f)
            assert state["test"] >= 5

def test_restore_from_checkpoint(persistence):
    """Test restoration from checkpoint."""
    # Create test checkpoints
    for i in range(3):
        cp_file = persistence.checkpoint_dir / f"acceptor_{persistence.node_id}_cp_{i}_{int(time.time())}.json"
        with open(cp_file, 'w') as f:
            json.dump({"promises": {}, "accepted": {}, "prepare_requests_processed": i}, f)
        
        # Small delay for different timestamps
        time.sleep(0.01)
    
    # Run restoration
    state = persistence._restore_from_checkpoint()
    
    # Check if restored the most recent checkpoint
    assert state["prepare_requests_processed"] == 2

def test_restore_no_checkpoints(persistence):
    """Test restoration when no checkpoints exist."""
    # Run restoration
    state = persistence._restore_from_checkpoint()
    
    # Check if returned default state
    assert "promises" in state
    assert "accepted" in state
    assert state["promises"] == {}
    assert state["accepted"] == {}
    assert state["prepare_requests_processed"] == 0
    assert state["accept_requests_processed"] == 0