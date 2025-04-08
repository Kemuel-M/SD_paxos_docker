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
from learner.api import app, initialize, set_acceptor_count, add_store, add_client

# Configuração do logger
logger = logging.getLogger(__name__)

async def main():
    """
    Função principal do componente Learner.
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Learner - Paxos Consensus System')
    parser.add_argument('--id', type=str, help='Unique ID for this learner')
    parser.add_argument('--port', type=int, default=8200, help='Port to run the server on')
    parser.add_argument('--acceptors', type=str, help='Comma-separated list of acceptor URLs')
    parser.add_argument('--stores', type=str, help='Comma-separated list of store URLs')
    parser.add_argument('--clients', type=str, help='Comma-separated list of client URLs')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    parser.add_argument('--rowa', action='store_true', help='Enable ROWA protocol')
    
    args = parser.parse_args()
    
    # Determine debug mode
    debug = args.debug if args.debug is not None else get_debug_mode()
    
    # If ID is not provided, generate one from environment or random
    learner_id = args.id or os.environ.get('LEARNER_ID') or f"learner_{int(time.time())}"
    
    # Setup logging
    setup_logging(f"learner_{learner_id}", debug)
    
    logger.info(f"Starting learner {learner_id} on port {args.port}")
    
    # Initialize the learner
    initialize(learner_id, debug)
    
    # Parse acceptors
    acceptor_count = 5  # Default value
    if args.acceptors:
        acceptors = args.acceptors.split(',')
        acceptor_count = len(acceptors)
    else:
        # Default acceptors from environment variables
        acceptors_env = os.environ.get('ACCEPTORS', '')
        if acceptors_env:
            acceptors = acceptors_env.split(',')
            acceptor_count = len(acceptors)
    
    # Set acceptor count
    set_acceptor_count(acceptor_count)
    logger.info(f"Configured for {acceptor_count} acceptors")
    
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
    
    # Parse clients list
    if args.clients:
        clients = args.clients.split(',')
        for i, client_url in enumerate(clients):
            client_id = f"client_{i+1}"
            add_client(client_id, client_url.strip())
    else:
        # Default clients from environment variables
        clients_env = os.environ.get('CLIENTS', '')
        if clients_env:
            clients = clients_env.split(',')
            for i, client_url in enumerate(clients):
                client_id = f"client_{i+1}"
                add_client(client_id, client_url.strip())
        else:
            logger.warning("No clients specified. Using default localhost clients.")
            for i in range(5):  # Default: 5 clients
                port = 8400 + i
                client_id = f"client_{i+1}"
                add_client(client_id, f"http://localhost:{port}")
    
    # Start the heartbeat system if needed
    # heartbeat = HeartbeatSystem(learner_id, debug)
    # heartbeat.start()
    
    # Run the FastAPI server
    config = uvicorn.Config(app, host="0.0.0.0", port=args.port, log_level="info")
    server = uvicorn.Server(config)
    await server.serve()

if __name__ == "__main__":
    asyncio.run(main())