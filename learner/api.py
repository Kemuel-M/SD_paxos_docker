"""
File: learner/api.py
Implementation of the REST API endpoints for the Learner component.
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

logger = logging.getLogger("learner")

# Data models for the API
class LearnRequest(BaseModel):
    type: str = Field("LEARN", description="Message type")
    proposalNumber: int = Field(..., description="Proposal number")
    instanceId: int = Field(..., description="Instance ID")
    acceptorId: int = Field(..., description="ID of the acceptor")
    accepted: bool = Field(..., description="Whether the proposal was accepted")
    value: Dict[str, Any] = Field(..., description="Proposed value")
    timestamp: int = Field(..., description="Timestamp of the acceptance")

class StatusResponse(BaseModel):
    node_id: int = Field(..., description="ID of this learner")
    state: str = Field(..., description="Current state of the learner (running/stopped)")
    active_instances: int = Field(..., description="Number of active Paxos instances")
    decided_instances: int = Field(..., description="Number of instances with decided values")
    notifications_processed: int = Field(..., description="Total notifications processed")
    decisions_made: int = Field(..., description="Total decisions made")
    uptime: float = Field(..., description="Time running in seconds")
    use_cluster_store: bool = Field(..., description="Whether Cluster Store is being used")

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

def create_api(learner, consensus_manager, rowa_manager=None):
    """
    Create the FastAPI application for the Learner.
    
    Args:
        learner: Instance of the Learner
        consensus_manager: Instance of ConsensusManager
        rowa_manager: Instance of RowaManager (optional)
    
    Returns:
        FastAPI: Configured FastAPI application
    """
    app = FastAPI(title="Learner API", description="API for the Learner component of the Paxos consensus system")
    start_time = time.time()
    
    # Endpoint for learn notifications
    @app.post("/learn")
    async def learn(request: LearnRequest):
        if DEBUG and DEBUG_LEVEL in ("advanced", "trace"):
            logger.debug(f"Received learn notification: {request.dict()}")
            
        logger.info(f"Received learn notification for instance {request.instanceId} from acceptor {request.acceptorId}")
        
        # Process the learn notification asynchronously
        result = await learner.process_learn_notification(request.dict())
        
        return result
    
    # Endpoint for learner status
    @app.get("/status", response_model=StatusResponse)
    async def get_status():
        # Get status synchronously
        status = learner.get_status()
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
        info = consensus_manager.get_instance_info(instance_id)
        
        if not info:
            raise HTTPException(status_code=404, detail=f"Instance {instance_id} not found")
            
        return info
    
    # Endpoint for listing active instances
    @app.get("/instances")
    async def list_instances(limit: int = Query(10, ge=1, le=100), 
                           offset: int = Query(0, ge=0),
                           decided_only: bool = Query(False)):
        """List active Paxos instances."""
        instances = []
        
        # Get all instance IDs
        instance_ids = consensus_manager.get_all_instance_ids(decided_only)
        
        # Sort instance IDs
        sorted_ids = sorted(instance_ids, reverse=True)
        
        # Apply pagination
        paginated_ids = sorted_ids[offset:offset+limit]
        
        # Get info for each instance
        for instance_id in paginated_ids:
            instances.append(consensus_manager.get_instance_info(instance_id))
        
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
        """Return learner logs. Available only when DEBUG=true."""
        from common.logging import get_log_entries
        
        if not DEBUG:
            raise HTTPException(status_code=403, detail="DEBUG mode not enabled")
        
        return {"logs": get_log_entries("learner", limit=limit)}
    
    @app.get("/logs/important")
    async def get_important_logs(limit: int = Query(100, ge=1, le=1000)):
        """Return important learner logs."""
        from common.logging import get_important_log_entries
        return {"logs": get_important_log_entries("learner", limit=limit)}
    
    # Endpoint for getting stats
    @app.get("/stats")
    async def get_stats():
        """Return learner statistics."""
        # Use synchronous get_status to get all required data at once
        status = learner.get_status()
        
        stats = {
            # Keep using start time for uptime
            "uptime": time.time() - start_time,
            
            # Use only data from the status method
            "node_id": status["node_id"],
            "notifications_processed": status["notifications_processed"],
            "decisions_made": status["decisions_made"],
            "active_instances": status["active_instances"],
            "decided_instances": status["decided_instances"],
            "use_cluster_store": status["use_cluster_store"]
        }
        
        # Add ROWA statistics if available
        if rowa_manager:
            rowa_stats = rowa_manager.get_stats()
            stats["rowa"] = {
                "reads_processed": rowa_stats["reads_processed"],
                "writes_processed": rowa_stats["writes_processed"],
                "write_successes": rowa_stats["write_successes"],
                "write_failures": rowa_stats["write_failures"]
            }
        
        # Optional: add system load information
        if DEBUG and DEBUG_LEVEL in ("advanced", "trace"):
            stats["memory_usage"] = {
                "instances_size": consensus_manager.get_memory_usage()
            }
        
        return {"stats": stats}
    
    return app