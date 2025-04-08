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
from acceptor.api import app, initialize, add_learner

# Configuração do logger
logger = logging.getLogger(__name__)

async def main():
    """
    Função principal do componente Acceptor.
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Acceptor - Paxos Consensus System')
    parser.add_argument('--id', type=str, help='Unique ID for this acceptor')
    parser.add_argument('--port', type=int, default=8100, help='Port to run the server on')
    parser.add_argument('--learners', type=str, help='Comma-separated list of learner URLs')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    
    args = parser.parse_args()
    
    # Determine debug mode
    debug = args.debug if args.debug is not None else get_debug_mode()
    
    # If ID is not provided, generate one from environment or random
    acceptor_id = args.id or os.environ.get('ACCEPTOR_ID') or f"acceptor_{int(time.time())}"
    
    # Setup logging
    setup_logging(f"acceptor_{acceptor_id}", debug)
    
    logger.info(f"Starting acceptor {acceptor_id} on port {args.port}")
    
    # Initialize the acceptor
    initialize(acceptor_id, debug)
    
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
    
    # Start the heartbeat system if needed
    # heartbeat = HeartbeatSystem(acceptor_id, debug)
    # heartbeat.start()
    
    # Run the FastAPI server
    config = uvicorn.Config(app, host="0.0.0.0", port=args.port, log_level="info")
    server = uvicorn.Server(config)
    await server.serve()

if __name__ == "__main__":
    asyncio.run(main())