import argparse
import asyncio
import logging
import os
import sys
import time
import uvicorn
from typing import Dict, List

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from common.heartbeat import HeartbeatSystem
from common.utils import get_debug_mode, setup_logging
from proposer.api import app, initialize, add_acceptor, add_learner, add_store, add_proposer, start_leader_election_check

# Configuração do logger
logger = logging.getLogger(__name__)

async def main():
    """
    Função principal do componente Proposer.
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Proposer - Paxos Consensus System')
    parser.add_argument('--id', type=str, help='Unique ID for this proposer')
    parser.add_argument('--port', type=int, default=8000, help='Port to run the server on')
    parser.add_argument('--acceptors', type=str, help='Comma-separated list of acceptor URLs')
    parser.add_argument('--learners', type=str, help='Comma-separated list of learner URLs')
    parser.add_argument('--stores', type=str, help='Comma-separated list of store URLs')
    parser.add_argument('--proposers', type=str, help='Comma-separated list of other proposer URLs')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    
    args = parser.parse_args()
    
    # Determine debug mode
    debug = args.debug if args.debug is not None else get_debug_mode()
    
    # If ID is not provided, generate one from environment or random
    proposer_id = args.id or os.environ.get('PROPOSER_ID') or f"proposer_{int(time.time())}"
    
    # Setup logging
    setup_logging(f"proposer_{proposer_id}", debug)
    
    logger.info(f"Starting proposer {proposer_id} on port {args.port}")
    
    # Initialize the proposer
    initialize(proposer_id, debug)
    
    # Parse acceptors list
    if args.acceptors:
        acceptors = args.acceptors.split(',')
        for i, acceptor_url in enumerate(acceptors):
            acceptor_id = f"acceptor_{i+1}"
            add_acceptor(acceptor_id, acceptor_url.strip())
    else:
        # Default acceptors from environment variables
        acceptors_env = os.environ.get('ACCEPTORS', '')
        if acceptors_env:
            acceptors = acceptors_env.split(',')
            for i, acceptor_url in enumerate(acceptors):
                acceptor_id = f"acceptor_{i+1}"
                add_acceptor(acceptor_id, acceptor_url.strip())
        else:
            logger.warning("No acceptors specified. Using default localhost acceptors.")
            for i in range(5):  # Default: 5 acceptors
                port = 8100 + i
                acceptor_id = f"acceptor_{i+1}"
                add_acceptor(acceptor_id, f"http://localhost:{port}")
    
    # Parse learners list
    if args.learners:
        learners = args.learners.split(',')
        for i, learner_url in enumerate(learners):
            learner_id = f"learner_{i+1}"
            add_learner(learner_id, learner_url.strip())
    else:
        # Default learners from environment variables
        learners_env = os.environ.get('LEARNERS', '')
        if learners_env:
            learners = learners_env.split(',')
            for i, learner_url in enumerate(learners):
                learner_id = f"learner_{i+1}"
                add_learner(learner_id, learner_url.strip())
        else:
            logger.warning("No learners specified. Using default localhost learners.")
            for i in range(2):  # Default: 2 learners
                port = 8200 + i
                learner_id = f"learner_{i+1}"
                add_learner(learner_id, f"http://localhost:{port}")
    
    # Parse stores list
    if args.stores:
        stores = args.stores.split(',')
        for i, store_url in enumerate(stores):
            store_id = f"store_{i+1}"
            add_store(store_id, store_url.strip())
    else:
        # Default stores from environment variables
        stores_env = os.environ.get('STORES', '')
        if stores_env:
            stores = stores_env.split(',')
            for i, store_url in enumerate(stores):
                store_id = f"store_{i+1}"
                add_store(store_id, store_url.strip())
        else:
            logger.warning("No stores specified. Using default localhost stores.")
            for i in range(3):  # Default: 3 stores
                port = 8300 + i
                store_id = f"store_{i+1}"
                add_store(store_id, f"http://localhost:{port}")
    
    # Parse proposers list
    if args.proposers:
        proposers = args.proposers.split(',')
        for i, proposer_url in enumerate(proposers):
            prop_id = f"proposer_{i+1}"
            if prop_id != proposer_id:  # Don't add ourselves
                add_proposer(prop_id, proposer_url.strip())
    else:
        # Default proposers from environment variables
        proposers_env = os.environ.get('PROPOSERS', '')
        if proposers_env:
            proposers = proposers_env.split(',')
            for i, proposer_url in enumerate(proposers):
                prop_id = f"proposer_{i+1}"
                # Check if this URL is for ourselves based on port
                if f":{args.port}" not in proposer_url:
                    add_proposer(prop_id, proposer_url.strip())
        else:
            logger.warning("No other proposers specified. Using default localhost proposers.")
            for i in range(5):  # Default: 5 proposers
                port = 8000 + i
                if port != args.port:  # Don't add ourselves
                    prop_id = f"proposer_{i+1}"
                    add_proposer(prop_id, f"http://localhost:{port}")
    
    # Start the heartbeat system if needed
    # heartbeat = HeartbeatSystem(proposer_id, debug)
    # heartbeat.start()
    
    # Start leader election checking
    asyncio.create_task(start_leader_election_check())
    
    # Run the FastAPI server
    config = uvicorn.Config(app, host="0.0.0.0", port=args.port, log_level="info")
    server = uvicorn.Server(config)
    await server.serve()

if __name__ == "__main__":
    asyncio.run(main())