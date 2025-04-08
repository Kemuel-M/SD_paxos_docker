#!/usr/bin/env python3
"""
File: acceptor/main.py
Entry point for the Acceptor application.
Responsible for initializing the HTTP server and loading configurations.
Supports multiple debug levels.
"""
import os
import sys
import logging
import asyncio
import signal
import uvicorn
from fastapi import FastAPI

# Add current directory to PYTHONPATH
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # For common modules

from api import create_api
from acceptor import Acceptor
from persistence import AcceptorPersistence
from common.logging import setup_logging

# Load configurations
NODE_ID = int(os.getenv("NODE_ID", 1))
PORT = int(os.getenv("PORT", 8080))
DEBUG = os.getenv("DEBUG", "false").lower() in ("true", "1", "yes")
DEBUG_LEVEL = os.getenv("DEBUG_LEVEL", "basic").lower()  # Levels: basic, advanced, trace
LEARNERS = os.getenv("LEARNERS", "learner-1:8080,learner-2:8080").split(",")
LOG_DIR = os.getenv("LOG_DIR", "/data/logs")
DATA_DIR = os.getenv("DATA_DIR", "/data")

logger = logging.getLogger("acceptor")

async def shutdown(app, persistence, acceptor):
    """Function to gracefully shut down the service"""
    logger.important(f"Acceptor {NODE_ID} is being shut down...")
    
    # Save persistent state
    await persistence.save_state()
    logger.info("Persistent state saved")
    
    # Stop the acceptor
    await acceptor.stop()
    logger.info("Acceptor stopped")
    
    logger.important(f"Acceptor {NODE_ID} shut down successfully.")

def main():
    # Configure the logger with debug level support
    setup_logging(f"acceptor-{NODE_ID}", debug=DEBUG, debug_level=DEBUG_LEVEL, log_dir=LOG_DIR)
    logger.important(f"Starting Acceptor {NODE_ID}...")
    
    if DEBUG:
        logger.info(f"DEBUG mode enabled with level: {DEBUG_LEVEL}")
        logger.info(f"Configuration: PORT={PORT}, LEARNERS={len(LEARNERS)}")
    
    # Create data directories if they don't exist
    os.makedirs(LOG_DIR, exist_ok=True)
    os.makedirs(DATA_DIR, exist_ok=True)
    
    # Create persistence manager
    persistence = AcceptorPersistence(NODE_ID, DATA_DIR)
    
    # Create acceptor instance
    acceptor = Acceptor(
        node_id=NODE_ID,
        learners=LEARNERS,
        persistence=persistence
    )
    
    # Create API
    app = create_api(acceptor, persistence)
    
    # Configure callback for graceful shutdown
    @app.on_event("shutdown")
    async def on_shutdown():
        await shutdown(app, persistence, acceptor)
    
    # Startup tasks
    @app.on_event("startup")
    async def on_startup():
        # Start the acceptor
        await acceptor.start()
        logger.info("Acceptor started")
        
        # Start the persistence loop
        asyncio.create_task(persistence_loop(persistence, acceptor))
        logger.info("Persistence loop started")
    
    async def persistence_loop(persistence, acceptor):
        """Loop to periodically save state"""
        while True:
            try:
                await asyncio.sleep(10)  # Save every 10 seconds
                
                # Save current state
                current_state = {
                    "promises": acceptor.promises,
                    "accepted": acceptor.accepted,
                    "prepare_requests_processed": acceptor.prepare_requests_processed,
                    "accept_requests_processed": acceptor.accept_requests_processed,
                    "promises_made": acceptor.promises_made,
                    "proposals_accepted": acceptor.proposals_accepted
                }
                
                if DEBUG and DEBUG_LEVEL == "trace":
                    logger.debug(f"Saving state: {len(current_state['promises'])} promises, {len(current_state['accepted'])} accepted values")
                
                # Save state
                await persistence.save_state(current_state)
                
            except asyncio.CancelledError:
                logger.info("Persistence loop cancelled")
                break
            except Exception as e:
                logger.error(f"Error saving persistent state: {e}", exc_info=True)
    
    # Configure signal handlers for graceful shutdown
    loop = asyncio.get_event_loop()
    signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
    for s in signals:
        loop.add_signal_handler(
            s, lambda s=s: asyncio.create_task(shutdown(app, persistence, acceptor))
        )
    
    # Start the server
    logger.important(f"Acceptor {NODE_ID} started and listening on port {PORT}")
    uvicorn.run(app, host="0.0.0.0", port=PORT)

if __name__ == "__main__":
    main()