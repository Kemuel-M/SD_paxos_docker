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
from cluster_store.api import app, initialize

# Configuração do logger
logger = logging.getLogger(__name__)

async def main():
    """
    Função principal do componente Cluster Store.
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Cluster Store - Paxos Consensus System')
    parser.add_argument('--id', type=str, help='Unique ID for this store')
    parser.add_argument('--port', type=int, default=8300, help='Port to run the server on')
    parser.add_argument('--data-dir', type=str, default='data', help='Directory to store data')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    
    args = parser.parse_args()
    
    # Determine debug mode
    debug = args.debug if args.debug is not None else get_debug_mode()
    
    # If ID is not provided, generate one from environment or random
    store_id = args.id or os.environ.get('STORE_ID') or f"store_{int(time.time())}"
    
    # Setup logging
    setup_logging(f"store_{store_id}", debug)
    
    logger.info(f"Starting cluster store {store_id} on port {args.port}")
    
    # Create data directory if it doesn't exist
    os.makedirs(args.data_dir, exist_ok=True)
    
    # Initialize the store
    initialize(store_id, args.data_dir, debug)
    
    # Start the heartbeat system if needed
    # heartbeat = HeartbeatSystem(store_id, debug)
    # heartbeat.start()
    
    # Run the FastAPI server
    config = uvicorn.Config(app, host="0.0.0.0", port=args.port, log_level="info")
    server = uvicorn.Server(config)
    await server.serve()

if __name__ == "__main__":
    asyncio.run(main())