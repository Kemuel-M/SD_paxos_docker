#!/usr/bin/env python3
"""
File: learner/main.py
Entry point for the Learner application.
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
from learner import Learner
from consensus import ConsensusManager
from rowa import RowaManager
from two_phase import TwoPhaseCommitManager
from common.logging import setup_logging

# Load configurations
NODE_ID = int(os.getenv("NODE_ID", 1))
PORT = int(os.getenv("PORT", 8080))
DEBUG = os.getenv("DEBUG", "false").lower() in ("true", "1", "yes")
DEBUG_LEVEL = os.getenv("DEBUG_LEVEL", "basic").lower()  # Levels: basic, advanced, trace
ACCEPTORS = os.getenv("ACCEPTORS", "acceptor-1:8080,acceptor-2:8080,acceptor-3:8080,acceptor-4:8080,acceptor-5:8080").split(",")
STORES = os.getenv("STORES", "cluster-store-1:8080,cluster-store-2:8080,cluster-store-3:8080").split(",")
USE_CLUSTER_STORE = os.getenv("USE_CLUSTER_STORE", "false").lower() in ("true", "1", "yes")
LOG_DIR = os.getenv("LOG_DIR", "/data/logs")
DATA_DIR = os.getenv("DATA_DIR", "/data")

logger = logging.getLogger("learner")

async def shutdown(app, learner):
    """Function to gracefully shut down the service"""
    logger.important(f"Learner {NODE_ID} is being shut down...")
    
    # Stop the learner
    await learner.stop()
    logger.info("Learner stopped")
    
    logger.important(f"Learner {NODE_ID} shut down successfully.")

def main():
    # Configure the logger with debug level support
    setup_logging(f"learner-{NODE_ID}", debug=DEBUG, debug_level=DEBUG_LEVEL, log_dir=LOG_DIR)
    logger.important(f"Starting Learner {NODE_ID}...")
    
    if DEBUG:
        logger.info(f"DEBUG mode enabled with level: {DEBUG_LEVEL}")
        logger.info(f"Configuration: PORT={PORT}, ACCEPTORS={len(ACCEPTORS)}, STORES={len(STORES)}, USE_CLUSTER_STORE={USE_CLUSTER_STORE}")
    
    # Create data directories if they don't exist
    os.makedirs(LOG_DIR, exist_ok=True)
    os.makedirs(DATA_DIR, exist_ok=True)
    
    # Create the ConsensusManager
    consensus_manager = ConsensusManager(NODE_ID)
    
    # Create the RowaManager if USE_CLUSTER_STORE is enabled
    rowa_manager = None
    if USE_CLUSTER_STORE:
        two_phase_manager = TwoPhaseCommitManager(NODE_ID, STORES)
        rowa_manager = RowaManager(NODE_ID, STORES, two_phase_manager)
    
    # Create learner instance
    learner = Learner(
        node_id=NODE_ID,
        acceptors=ACCEPTORS,
        stores=STORES if USE_CLUSTER_STORE else None,
        consensus_manager=consensus_manager,
        rowa_manager=rowa_manager,
        use_cluster_store=USE_CLUSTER_STORE
    )
    
    # Create API
    app = create_api(learner, consensus_manager, rowa_manager)
    
    # Configure callback for graceful shutdown
    @app.on_event("shutdown")
    async def on_shutdown():
        await shutdown(app, learner)
    
    # Startup tasks
    @app.on_event("startup")
    async def on_startup():
        # Start the learner
        await learner.start()
        logger.info("Learner started")
    
    # Configure signal handlers for graceful shutdown
    loop = asyncio.get_event_loop()
    signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
    for s in signals:
        loop.add_signal_handler(
            s, lambda s=s: asyncio.create_task(shutdown(app, learner))
        )
    
    # Start the server
    logger.important(f"Learner {NODE_ID} started and listening on port {PORT}")
    uvicorn.run(app, host="0.0.0.0", port=PORT)

if __name__ == "__main__":
    main()