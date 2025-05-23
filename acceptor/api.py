"""
File: acceptor/api.py
Implementation of the REST API endpoints for the Acceptor component.
With improved logging and debug control.
"""
import os
import time
import logging
import asyncio
from typing import Dict, Any, List, Optional
from fastapi import FastAPI, HTTPException, Body, BackgroundTasks, Query, Depends
from pydantic import BaseModel, Field

# Enhanced debug configuration
DEBUG = os.getenv("DEBUG", "false").lower() in ("true", "1", "yes")
DEBUG_LEVEL = os.getenv("DEBUG_LEVEL", "basic").lower()  # Levels: basic, advanced, trace

logger = logging.getLogger("acceptor")

# Data models for the API
class PrepareRequest(BaseModel):
    type: str = Field("PREPARE", description="Message type")
    proposalNumber: int = Field(..., description="Proposal number")
    instanceId: int = Field(..., description="Instance ID")
    proposerId: int = Field(..., description="ID of the proposer")
    clientRequest: Dict[str, Any] = Field({}, description="Original client request (optional)")

class AcceptRequest(BaseModel):
    type: str = Field("ACCEPT", description="Message type")
    proposalNumber: int = Field(..., description="Proposal number")
    instanceId: int = Field(..., description="Instance ID")
    proposerId: int = Field(..., description="ID of the proposer")
    value: Dict[str, Any] = Field(..., description="Value to be accepted")

class StatusResponse(BaseModel):
    node_id: int = Field(..., description="ID of this acceptor")
    state: str = Field(..., description="Current state of the acceptor (running/stopped)")
    active_instances: int = Field(..., description="Number of active Paxos instances")
    accepted_instances: int = Field(..., description="Number of instances with accepted values")
    prepare_requests_processed: int = Field(..., description="Total prepare requests processed")
    accept_requests_processed: int = Field(..., description="Total accept requests processed")
    promises_made: int = Field(..., description="Total promises made")
    proposals_accepted: int = Field(..., description="Total proposals accepted")
    uptime: float = Field(..., description="Time running in seconds")

class HealthResponse(BaseModel):
    status: str = Field(..., description="Health status of the service")
    timestamp: int = Field(..., description="Current timestamp")
    debug_enabled: bool = Field(..., description="Debug mode status")
    debug_level: str = Field(..., description="Current debug level")

class DebugConfigRequest(BaseModel):
    enabled: bool = Field(..., description="Enable or disable debug")
    level: str = Field("basic", description="Debug level (basic, advanced, trace)")

def get_current_debug_state():
    """
    Return the current debug state for use in endpoints.
    """
    from common.logging import DEBUG, DEBUG_LEVEL
    return {
        "enabled": DEBUG,
        "level": DEBUG_LEVEL
    }

def create_api(acceptor, persistence):
    """
    Create the FastAPI application for the Acceptor.
    
    Args:
        acceptor: Instance of the Acceptor
        persistence: Instance of AcceptorPersistence
    
    Returns:
        FastAPI: Configured FastAPI application
    """
    app = FastAPI(title="Acceptor API", description="API for the Acceptor component of the Paxos consensus system")
    start_time = time.time()
    
    # Endpoint for prepare requests
    @app.post("/prepare")
    async def prepare(request: PrepareRequest):
        if DEBUG and DEBUG_LEVEL in ("advanced", "trace"):
            logger.debug(f"Received prepare request: {request.dict()}")
            
        logger.info(f"Received prepare request for instance {request.instanceId} from proposer {request.proposerId}")
        
        # Process the prepare request asynchronously
        result = await acceptor.process_prepare(request.dict())
        
        return result
    
    # Endpoint for accept requests
    @app.post("/accept")
    async def accept(request: AcceptRequest):
        if DEBUG and DEBUG_LEVEL in ("advanced", "trace"):
            logger.debug(f"Received accept request: {request.dict()}")
            
        logger.info(f"Received accept request for instance {request.instanceId} from proposer {request.proposerId}")
        
        # Process the accept request asynchronously
        result = await acceptor.process_accept(request.dict())
        
        return result
    
    # Endpoint for acceptor status
    @app.get("/status", response_model=StatusResponse)
    async def get_status():
        # Get status synchronously
        status = acceptor.get_status()
        # Calculate uptime synchronously
        status["uptime"] = time.time() - start_time
        return status
    
    # Endpoint for health check (heartbeat)
    @app.get("/health", response_model=HealthResponse)
    async def health_check(debug_state: Dict = Depends(get_current_debug_state)):
        return {
            "status": "healthy",
            "timestamp": int(time.time() * 1000),
            "debug_enabled": debug_state["enabled"],
            "debug_level": debug_state["level"]
        }
    
    # Endpoint for getting instance information
    @app.get("/instance/{instance_id}")
    async def get_instance(instance_id: int):
        # Get instance info synchronously
        info = acceptor.get_instance_info(instance_id)
        
        if not info:
            raise HTTPException(status_code=404, detail=f"Instance {instance_id} not found")
            
        return info
    
    # Endpoint for listing active instances
    @app.get("/instances")
    async def list_instances(limit: int = Query(10, ge=1, le=100), 
                           offset: int = Query(0, ge=0),
                           accepted_only: bool = Query(False)):
        """List active Paxos instances."""
        instances = []
        
        # Get all instance IDs
        instance_ids = set(acceptor.promises.keys())
        
        # Filter to only include accepted instances if requested
        if accepted_only:
            instance_ids = set(acceptor.accepted.keys())
        
        # Sort instance IDs
        sorted_ids = sorted(instance_ids, reverse=True)
        
        # Apply pagination
        paginated_ids = sorted_ids[offset:offset+limit]
        
        # Get info for each instance
        for instance_id in paginated_ids:
            instances.append(acceptor.get_instance_info(instance_id))
        
        return {
            "instances": instances,
            "total": len(sorted_ids),
            "offset": offset,
            "limit": limit
        }
    
    # Endpoint to configure debug settings
    @app.post("/debug/config")
    async def configure_debug(config: DebugConfigRequest):
        """Configure debug mode at runtime."""
        if DEBUG and DEBUG_LEVEL in ("advanced", "trace"):
            logger.debug(f"Changing debug configuration: {config.dict()}")
            
        # Import and use the logging module to configure debug
        from common.logging import set_debug_level
        set_debug_level(config.enabled, config.level)
        
        logger.important(f"Debug configuration changed: enabled={config.enabled}, level={config.level}")
        return {"status": "success", "debug": {"enabled": config.enabled, "level": config.level}}
    
    # Endpoints for logs
    @app.get("/logs")
    async def get_logs(limit: int = Query(100, ge=1, le=1000)):
        """Return acceptor logs. Available only when DEBUG=true."""
        from common.logging import get_log_entries
        
        if not DEBUG:
            raise HTTPException(status_code=403, detail="DEBUG mode not enabled")
        
        return {"logs": get_log_entries("acceptor", limit=limit)}
    
    @app.get("/logs/important")
    async def get_important_logs(limit: int = Query(100, ge=1, le=1000)):
        """Return important acceptor logs."""
        from common.logging import get_important_log_entries
        return {"logs": get_important_log_entries("acceptor", limit=limit)}
    
    # Endpoint for getting stats
    @app.get("/stats")
    async def get_stats():
        """Return acceptor statistics."""
        # Use synchronous get_status to get all required data at once
        status = acceptor.get_status()
        
        stats = {
            # Keep using start time for uptime
            "uptime": time.time() - start_time,
            
            # Use only data from the status method
            "node_id": status["node_id"],
            "prepare_requests_processed": status["prepare_requests_processed"],
            "accept_requests_processed": status["accept_requests_processed"],
            "promises_made": status["promises_made"],
            "proposals_accepted": status["proposals_accepted"],
            "active_instances": status["active_instances"],
            "accepted_instances": status["accepted_instances"],
            "instance_id_range": status.get("instance_id_range", "N/A-N/A")
        }
        
        # Optional: add system load information
        if DEBUG and DEBUG_LEVEL in ("advanced", "trace"):
            stats["memory_usage"] = {
                "promises_size": len(status.get("active_instances", 0)),
                "accepted_size": len(status.get("accepted_instances", 0))
            }
        
        return {"stats": stats}
    
    return app