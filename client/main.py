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

from common.utils import get_debug_mode, setup_logging
from client.api import app as api_app, initialize as init_api, set_proposer
from client.web_server import web_app, initialize as init_web

# Configuração do logger
logger = logging.getLogger(__name__)

async def main():
    """
    Função principal do componente Client.
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Client - Paxos Consensus System')
    parser.add_argument('--id', type=str, help='Unique ID for this client')
    parser.add_argument('--port', type=int, default=8400, help='Port to run the server on')
    parser.add_argument('--web-port', type=int, default=8500, help='Port to run the web server on')
    parser.add_argument('--proposer', type=str, help='URL of the proposer to connect to')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    parser.add_argument('--auto-start', action='store_true', help='Automatically start sending requests')
    parser.add_argument('--num-requests', type=int, help='Number of requests to send if auto-start is enabled')
    
    args = parser.parse_args()
    
    # Determine debug mode
    debug = args.debug if args.debug is not None else get_debug_mode()
    
    # If ID is not provided, generate one from environment or random
    client_id = args.id or os.environ.get('CLIENT_ID') or f"client_{int(time.time())}"
    
    # Setup logging
    setup_logging(f"client_{client_id}", debug)
    
    logger.info(f"Starting client {client_id} (API on port {args.port}, Web on port {args.web_port})")
    
    # Determine proposer URL
    proposer_url = args.proposer or os.environ.get('PROPOSER_URL')
    if not proposer_url:
        # Default to a random proposer (for the example)
        proposer_idx = hash(client_id) % 5 + 1  # Distribute clients among 5 proposers
        proposer_url = f"http://localhost:{8000 + proposer_idx - 1}"
        logger.info(f"No proposer specified. Using proposer_{proposer_idx} at {proposer_url}")
    
    # Initialize the API
    init_api(client_id, proposer_url, debug)
    
    # Initialize the web server
    from client.api import client
    init_web(client)
    
    # Run both API and web servers concurrently
    api_config = uvicorn.Config(api_app, host="0.0.0.0", port=args.port, log_level="info")
    web_config = uvicorn.Config(web_app, host="0.0.0.0", port=args.web_port, log_level="info")
    
    api_server = uvicorn.Server(api_config)
    web_server = uvicorn.Server(web_config)
    
    # If auto-start is enabled, start sending requests
    if args.auto_start:
        logger.info(f"Auto-start enabled. Will send {args.num_requests or 'random number of'} requests.")
        
        # Wait a bit for servers to start before sending requests
        await asyncio.sleep(2)
        
        # Import client from API and start requests
        from client.api import client
        if client:
            await client.start_random_requests(args.num_requests)
    
    # Run both servers
    await asyncio.gather(
        api_server.serve(),
        web_server.serve()
    )

if __name__ == "__main__":
    asyncio.run(main())