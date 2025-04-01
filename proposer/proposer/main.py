import os
import yaml
import json
import time
import asyncio
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import structlog
from prometheus_client import Counter, Histogram, Gauge, generate_latest
import aiohttp
from typing import Dict, List, Set, Optional, Any
from tinydb import TinyDB, Query
import aiofiles
import random

# Configure structured logging
log = structlog.get_logger()

# Load configuration
def load_config():
    with open("config/config.yaml", "r") as f:
        config = yaml.safe_load(f)
    
    # Replace environment variables
    config["node"]["id"] = int(os.environ.get("NODE_ID", "1"))
    config["paxos"]["quorumSize"] = int(os.environ.get("QUORUM_SIZE", "3"))
    
    return config

config = load_config()

# Initialize FastAPI application
app = FastAPI(title="Paxos Proposer")

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Prometheus metrics
proposals_total = Counter("paxos_proposer_proposals_total", "Total number of proposals initiated")
proposals_success = Counter("paxos_proposer_proposals_success", "Number of successful proposals")
prepare_duration = Histogram("paxos_proposer_prepare_duration_seconds", "Time spent in prepare phase")
accept_duration = Histogram("paxos_proposer_accept_duration_seconds", "Time spent in accept phase")
leader_status = Gauge("paxos_proposer_leader_status", "Whether this proposer is the current leader (1) or not (0)")
retries_count = Counter("paxos_proposer_retries_total", "Number of retries due to conflicts or failures")
active_proposals = Gauge("paxos_proposer_active_proposals", "Number of active proposals")
batch_size = Histogram("paxos_proposer_batch_size", "Size of processed batches")

# Pydantic models
class ProposeRequest(BaseModel):
    """Model for client proposals"""
    operation: str  # "CREATE", "MODIFY", "DELETE"
    path: str
    content: Optional[str] = None

class PrepareResponse(BaseModel):
    """Model for prepare phase responses from acceptors"""
    accepted: bool
    highest_accepted: Optional[int] = None
    accepted_value: Optional[Dict[str, Any]] = None
    highestPromised: Optional[int] = None

class AcceptResponse(BaseModel):
    """Model for accept phase responses from acceptors"""
    accepted: bool
    highestPromised: Optional[int] = None

class LeaderHeartbeat(BaseModel):
    """Model for leader heartbeat messages"""
    leader_id: int
    timestamp: float
    proposal_number: int

# WebSocket connection manager for real-time updates
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception as e:
                log.error("Error broadcasting message", error=str(e))

manager = ConnectionManager()

# Proposer state
class ProposerState:
    def __init__(self, node_id):
        self.node_id = node_id
        self.proposal_counter = 0
        self.active_proposals = {}  # instance_id -> {proposal_number, value, state, timestamp}
        self.is_leader = False
        self.leader_id = None
        self.instance_counter = 0
        self.prepared_instances = set()  # For Multi-Paxos optimization
        self.batch_queue = []  # Queue for proposal batching
        self.last_leader_activity = time.time()
        
        # Ensure data directory exists
        os.makedirs("/app/data", exist_ok=True)
        
        # Initialize TinyDB for persistent storage
        self.db = TinyDB("/app/data/proposer.json")
        self.load_state()
        
        # Update metrics
        active_proposals.set(len(self.active_proposals))
        leader_status.set(1 if self.is_leader else 0)
    
    def load_state(self):
        """Load persisted state from disk"""
        try:
            log.info("Loading state from disk")
            state_table = self.db.table("state")
            if state_record := state_table.get(doc_id=1):
                self.proposal_counter = state_record.get("proposal_counter", 0)
                self.instance_counter = state_record.get("instance_counter", 0)
                self.leader_id = state_record.get("leader_id")
                self.is_leader = state_record.get("is_leader", False)
                
                # Load active proposals
                proposals_table = self.db.table("proposals")
                for proposal in proposals_table.all():
                    self.active_proposals[proposal["instance_id"]] = {
                        "proposal_number": proposal["proposal_number"],
                        "value": proposal["value"],
                        "state": proposal["state"],
                        "timestamp": proposal["timestamp"]
                    }
                
                log.info("State loaded successfully", 
                        proposal_counter=self.proposal_counter,
                        instance_counter=self.instance_counter,
                        active_proposals=len(self.active_proposals))
        except Exception as e:
            log.error("Error loading state", error=str(e))
    
    async def save_state(self):
        """Persist state to disk"""
        try:
            state_table = self.db.table("state")
            state_table.upsert(
                {
                    "proposal_counter": self.proposal_counter,
                    "instance_counter": self.instance_counter,
                    "leader_id": self.leader_id,
                    "is_leader": self.is_leader
                },
                doc_id=1
            )
            
            # Save active proposals
            proposals_table = self.db.table("proposals")
            proposals_table.truncate()  # Clear current proposals
            for instance_id, proposal in self.active_proposals.items():
                proposals_table.insert({
                    "instance_id": instance_id,
                    "proposal_number": proposal["proposal_number"],
                    "value": proposal["value"],
                    "state": proposal["state"],
                    "timestamp": proposal["timestamp"]
                })
        except Exception as e:
            log.error("Error saving state", error=str(e))
    
    def generate_proposal_number(self):
        """Generate a unique proposal number using counter and node ID"""
        self.proposal_counter += 1
        # Format: (counter << 8) | node_id to ensure uniqueness
        return (self.proposal_counter << 8) | self.node_id
    
    def get_next_instance_id(self):
        """Get the next available instance ID"""
        self.instance_counter += 1
        return self.instance_counter
    
    async def check_and_update_leader(self):
        """Check if leader is active, initiate election if needed"""
        current_time = time.time()
        leader_timeout = config["paxos"]["proposalTimeout"] / 1000 * 2  # Double the proposal timeout
        
        if (not self.is_leader and 
            (self.leader_id is None or current_time - self.last_leader_activity > leader_timeout)):
            log.info("Leader timeout, initiating election")
            await self.initiate_leader_election()
    
    async def initiate_leader_election(self):
        """Initiate leader election"""
        log.info("Initiating leader election")
        
        # Generate a high proposal number to win election
        proposal_number = self.generate_proposal_number()
        
        # Create special election value
        election_value = {
            "operation": "LEADER_ELECTION",
            "proposer_id": self.node_id,
            "timestamp": time.time()
        }
        
        # Use special instance ID for election (-1)
        election_instance = -1
        
        # Run Paxos for leader election
        success = await self.run_paxos(election_instance, proposal_number, election_value)
        
        if success:
            log.info("Successfully elected as leader")
            self.is_leader = True
            self.leader_id = self.node_id
            leader_status.set(1)
            
            # Prepare instances for Multi-Paxos optimization
            await self.prepare_instances_for_multi_paxos()
        else:
            log.info("Leader election unsuccessful")
            self.is_leader = False
            leader_status.set(0)
    
    async def prepare_instances_for_multi_paxos(self):
        """Prepare multiple instances at once for Multi-Paxos optimization"""
        log.info("Preparing instances for Multi-Paxos")
        
        # Use a high proposal number
        proposal_number = self.generate_proposal_number()
        
        # Prepare for current instance and future ones (100 is arbitrary)
        start_instance = self.instance_counter + 1
        end_instance = start_instance + 100
        
        success = True
        prepared_count = 0
        
        # Send prepare for each instance in range
        for instance_id in range(start_instance, end_instance + 1):
            prepare_success, _, _ = await self.prepare_phase(instance_id, proposal_number)
            
            if prepare_success:
                self.prepared_instances.add(instance_id)
                prepared_count += 1
            else:
                success = False
                break
        
        log.info("Multi-Paxos preparation completed", 
                success=success,
                prepared_count=prepared_count)
        
        return success
    
    async def run_paxos(self, instance_id, proposal_number, value):
        """Run the complete Paxos protocol for a value"""
        # For Multi-Paxos optimization: skip prepare phase if we're leader and instance is prepared
        if self.is_leader and instance_id in self.prepared_instances:
            log.info("Skipping prepare phase (Multi-Paxos optimization)", 
                    instance_id=instance_id)
            return await self.accept_phase(instance_id, proposal_number, value)
        
        # Run full Paxos protocol
        prepare_success, highest_accepted, accepted_value = await self.prepare_phase(instance_id, proposal_number)
        
        if not prepare_success:
            return False
        
        # If any acceptor has already accepted a value, we must propose that value
        if highest_accepted is not None and accepted_value is not None:
            log.info("Using previously accepted value", 
                    instance_id=instance_id, 
                    proposal_number=proposal_number)
            value = accepted_value
        
        # Run accept phase
        return await self.accept_phase(instance_id, proposal_number, value)
    
    async def prepare_phase(self, instance_id, proposal_number):
        """Run the prepare phase of Paxos"""
        start_time = time.time()
        
        log.info("Starting prepare phase", 
                instance_id=instance_id, 
                proposal_number=proposal_number)
        
        # Create prepare request
        prepare_request = {
            "proposal_number": proposal_number,
            "instance_id": instance_id,
            "proposer_id": self.node_id
        }
        
        # Send prepare request to all acceptors
        responses = await self.send_prepare_to_acceptors(prepare_request)
        
        # Calculate quorum size (majority of acceptors)
        quorum_size = config["paxos"]["quorumSize"]
        
        # Calculate duration for metrics
        prepare_duration.observe(time.time() - start_time)
        
        # Check if we got enough responses
        accepts = [r for r in responses if r.get("accepted") == True]
        
        if len(accepts) >= quorum_size:
            log.info("Prepare phase successful", 
                    instance_id=instance_id, 
                    accepts=len(accepts))
            
            # Find highest accepted proposal among responses
            highest_accepted = None
            accepted_value = None
            
            for response in accepts:
                if response.get("highest_accepted") is not None:
                    if highest_accepted is None or response["highest_accepted"] > highest_accepted:
                        highest_accepted = response["highest_accepted"]
                        accepted_value = response.get("accepted_value")
            
            return True, highest_accepted, accepted_value
        else:
            log.warning("Prepare phase failed", 
                       instance_id=instance_id, 
                       accepts=len(accepts),
                       responses=len(responses))
            return False, None, None
    
    async def send_prepare_to_acceptors(self, prepare_request):
        """Send prepare request to all acceptors and collect responses"""
        responses = []
        
        # Set timeout from configuration
        timeout = config["paxos"]["proposalTimeout"] / 1000
        
        try:
            async with aiohttp.ClientSession() as session:
                tasks = []
                
                # Create task for each acceptor
                for acceptor_url in config["networking"]["acceptors"]:
                    task = asyncio.create_task(
                        self.send_prepare_to_acceptor(session, acceptor_url, prepare_request, timeout)
                    )
                    tasks.append(task)
                
                # Wait for all responses or timeout
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Process results
                for result in results:
                    if isinstance(result, Exception):
                        log.warning("Exception in prepare request", error=str(result))
                    elif result is not None:
                        responses.append(result)
        except Exception as e:
            log.error("Error sending prepare requests", error=str(e))
        
        return responses
    
    async def send_prepare_to_acceptor(self, session, acceptor_url, prepare_request, timeout):
        """Send prepare request to a single acceptor"""
        try:
            async with session.post(
                f"http://{acceptor_url}/prepare",
                json=prepare_request,
                timeout=timeout
            ) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    log.warning("Non-200 response from acceptor", 
                               acceptor=acceptor_url, 
                               status=response.status)
                    return None
        except asyncio.TimeoutError:
            log.warning("Timeout contacting acceptor", acceptor=acceptor_url)
            return None
        except Exception as e:
            log.error("Error contacting acceptor", 
                     acceptor=acceptor_url, 
                     error=str(e))
            return None
    
    async def accept_phase(self, instance_id, proposal_number, value):
        """Run the accept phase of Paxos"""
        start_time = time.time()
        
        log.info("Starting accept phase", 
                instance_id=instance_id, 
                proposal_number=proposal_number)
        
        # Create accept request
        accept_request = {
            "proposal_number": proposal_number,
            "instance_id": instance_id,
            "proposer_id": self.node_id,
            "value": value
        }
        
        # Send accept request to all acceptors
        responses = await self.send_accept_to_acceptors(accept_request)
        
        # Calculate quorum size
        quorum_size = config["paxos"]["quorumSize"]
        
        # Calculate duration for metrics
        accept_duration.observe(time.time() - start_time)
        
        # Check if we got enough acceptances
        accepts = [r for r in responses if r.get("accepted") == True]
        
        if len(accepts) >= quorum_size:
            log.info("Accept phase successful", 
                    instance_id=instance_id, 
                    accepts=len(accepts))
            return True
        else:
            log.warning("Accept phase failed", 
                       instance_id=instance_id, 
                       accepts=len(accepts))
            
            # If we got any rejections with higher proposal numbers, update our counter
            for response in responses:
                if not response.get("accepted", False) and "highestPromised" in response:
                    if response["highestPromised"] >> 8 > self.proposal_counter:
                        self.proposal_counter = response["highestPromised"] >> 8
                        log.info("Updated proposal counter based on rejection", 
                                new_counter=self.proposal_counter)
            
            return False
    
    async def send_accept_to_acceptors(self, accept_request):
        """Send accept request to all acceptors and collect responses"""
        responses = []
        
        # Set timeout from configuration
        timeout = config["paxos"]["proposalTimeout"] / 1000
        
        try:
            async with aiohttp.ClientSession() as session:
                tasks = []
                
                # Create task for each acceptor
                for acceptor_url in config["networking"]["acceptors"]:
                    task = asyncio.create_task(
                        self.send_accept_to_acceptor(session, acceptor_url, accept_request, timeout)
                    )
                    tasks.append(task)
                
                # Wait for all responses or timeout
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Process results
                for result in results:
                    if isinstance(result, Exception):
                        log.warning("Exception in accept request", error=str(result))
                    elif result is not None:
                        responses.append(result)
        except Exception as e:
            log.error("Error sending accept requests", error=str(e))
        
        return responses
    
    async def send_accept_to_acceptor(self, session, acceptor_url, accept_request, timeout):
        """Send accept request to a single acceptor"""
        try:
            async with session.post(
                f"http://{acceptor_url}/accept",
                json=accept_request,
                timeout=timeout
            ) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    log.warning("Non-200 response from acceptor", 
                               acceptor=acceptor_url, 
                               status=response.status)
                    return None
        except asyncio.TimeoutError:
            log.warning("Timeout contacting acceptor", acceptor=acceptor_url)
            return None
        except Exception as e:
            log.error("Error contacting acceptor", 
                     acceptor=acceptor_url, 
                     error=str(e))
            return None
    
    async def process_batch(self):
        """Process a batch of proposals"""
        if not self.batch_queue:
            return
        
        # Forward to leader if we're not the leader
        if not self.is_leader and self.leader_id is not None and self.leader_id != self.node_id:
            await self.forward_batch_to_leader()
            return
        
        # Take up to batchSize items
        batch_size = min(len(self.batch_queue), config["paxos"]["batchSize"])
        current_batch = self.batch_queue[:batch_size]
        self.batch_queue = self.batch_queue[batch_size:]
        
        log.info("Processing batch", size=batch_size)
        batch_size.observe(batch_size)
        
        # Process each proposal in the batch
        for value in current_batch:
            instance_id = self.get_next_instance_id()
            proposal_number = self.generate_proposal_number()
            
            # Record in active proposals
            self.active_proposals[instance_id] = {
                "proposal_number": proposal_number,
                "value": value,
                "state": "proposing",
                "timestamp": time.time()
            }
            active_proposals.set(len(self.active_proposals))
            await self.save_state()
            
            # Run Paxos
            success = await self.run_paxos(instance_id, proposal_number, value)
            
            if success:
                # Update proposal state and metrics
                self.active_proposals[instance_id]["state"] = "accepted"
                await self.save_state()
                proposals_success.inc()
                
                # Notify clients via WebSocket
                await manager.broadcast({
                    "type": "proposal_accepted",
                    "instance_id": instance_id,
                    "value": value
                })
                
                log.info("Proposal accepted", 
                        instance_id=instance_id, 
                        operation=value.get("operation"),
                        path=value.get("path"))
            else:
                # Update proposal state
                self.active_proposals[instance_id]["state"] = "rejected"
                await self.save_state()
                
                # Retry with higher proposal number
                retry_proposal_number = self.generate_proposal_number()
                log.info("Retrying proposal with higher number", 
                        instance_id=instance_id,
                        original=proposal_number,
                        retry=retry_proposal_number)
                
                retries_count.inc()
                retry_success = await self.run_paxos(instance_id, retry_proposal_number, value)
                
                if retry_success:
                    self.active_proposals[instance_id]["state"] = "accepted_on_retry"
                    await self.save_state()
                    proposals_success.inc()
                    
                    # Notify clients
                    await manager.broadcast({
                        "type": "proposal_accepted_retry",
                        "instance_id": instance_id,
                        "value": value
                    })
                    
                    log.info("Proposal accepted on retry", 
                            instance_id=instance_id)
                else:
                    log.warning("Proposal rejected even after retry", 
                               instance_id=instance_id)
            
            # Clean up completed proposal
            if instance_id in self.active_proposals:
                del self.active_proposals[instance_id]
                active_proposals.set(len(self.active_proposals))
                await self.save_state()
    
    async def forward_batch_to_leader(self):
        """Forward batch of proposals to current leader"""
        if not self.leader_id or self.leader_id == self.node_id:
            return
        
        leader_url = f"http://proposer-{self.leader_id}:8080"
        log.info("Forwarding batch to leader", 
                leader_id=self.leader_id,
                batch_size=len(self.batch_queue))
        
        forwarded_count = 0
        failed_count = 0
        
        try:
            async with aiohttp.ClientSession() as session:
                for value in self.batch_queue:
                    try:
                        async with session.post(
                            f"{leader_url}/propose",
                            json=value,
                            timeout=1.0
                        ) as response:
                            if response.status == 200:
                                forwarded_count += 1
                            else:
                                failed_count += 1
                                log.warning("Failed to forward proposal", 
                                           status=response.status)
                    except Exception as e:
                        failed_count += 1
                        log.error("Error forwarding proposal", 
                                 error=str(e))
            
            # Clear successfully forwarded proposals
            if failed_count == 0:
                self.batch_queue = []
            elif forwarded_count > 0:
                # Keep only failed proposals
                self.batch_queue = self.batch_queue[-failed_count:]
            
            log.info("Batch forwarding completed", 
                    forwarded=forwarded_count,
                    failed=failed_count)
        except Exception as e:
            log.error("Error in batch forwarding", error=str(e))

# Initialize proposer state
state = ProposerState(config["node"]["id"])

# API routes
@app.post("/propose")
async def propose(request: ProposeRequest):
    """Endpoint for clients to propose values"""
    log.info("Received proposal", 
            operation=request.operation, 
            path=request.path)
    
    # Increment metrics
    proposals_total.inc()
    
    # Convert to dictionary for storage
    value_dict = {
        "operation": request.operation,
        "path": request.path,
        "content": request.content,
        "timestamp": time.time()
    }
    
    # Add to batch queue
    state.batch_queue.append(value_dict)
    
    # Schedule batch processing if not already scheduled
    if not hasattr(app.state, "batch_task") or app.state.batch_task.done():
        app.state.batch_task = asyncio.create_task(
            process_batch_after_delay(config["paxos"]["batchDelayMs"] / 1000)
        )
    
    return {
        "success": True, 
        "queued": True,
        "batch_size": len(state.batch_queue),
        "is_leader": state.is_leader,
        "leader_id": state.leader_id
    }

async def process_batch_after_delay(delay):
    """Process batch after specified delay"""
    await asyncio.sleep(delay)
    await state.process_batch()

@app.get("/status")
async def get_status():
    """Return current status of the proposer"""
    return {
        "node_id": state.node_id,
        "role": "proposer",
        "is_leader": state.is_leader,
        "leader_id": state.leader_id,
        "proposal_counter": state.proposal_counter,
        "instance_counter": state.instance_counter,
        "active_proposals": len(state.active_proposals),
        "batch_queue_size": len(state.batch_queue),
        "prepared_instances": len(state.prepared_instances),
        "uptime_seconds": time.time() - app.state.start_time
    }

@app.post("/heartbeat")
async def receive_heartbeat(heartbeat: LeaderHeartbeat):
    """Receive heartbeat from current leader"""
    # Update leader information
    state.leader_id = heartbeat.leader_id
    state.is_leader = (heartbeat.leader_id == state.node_id)
    state.last_leader_activity = heartbeat.timestamp
    
    # Update proposal counter if leader's is higher
    leader_counter = heartbeat.proposal_number >> 8
    if leader_counter > state.proposal_counter:
        state.proposal_counter = leader_counter
    
    # Update metrics
    leader_status.set(1 if state.is_leader else 0)
    
    return {"acknowledged": True}

@app.get("/metrics")
async def metrics():
    """Expose Prometheus metrics"""
    return generate_latest()

# WebSocket for real-time updates
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            # Could process client commands here
    except WebSocketDisconnect:
        manager.disconnect(websocket)

# Lifecycle events
@app.on_event("startup")
async def startup_event():
    """Initialize on startup"""
    app.state.start_time = time.time()
    log.info("Proposer starting up", 
            node_id=state.node_id,
            config=config)
    
    # Start leader check task
    app.state.leader_check_task = asyncio.create_task(periodic_leader_check())
    
    # Start leader heartbeat task
    app.state.heartbeat_task = asyncio.create_task(send_leader_heartbeats())
    
    # Wait a random time before checking for leader
    # This prevents all proposers from starting election simultaneously
    await asyncio.sleep(random.uniform(1.0, 5.0))
    await state.check_and_update_leader()

@app.on_event("shutdown")
async def shutdown_event():
    """Clean up on shutdown"""
    log.info("Proposer shutting down", node_id=state.node_id)
    
    # Cancel background tasks
    if hasattr(app.state, "leader_check_task"):
        app.state.leader_check_task.cancel()
    
    if hasattr(app.state, "heartbeat_task"):
        app.state.heartbeat_task.cancel()
    
    if hasattr(app.state, "batch_task") and not app.state.batch_task.done():
        app.state.batch_task.cancel()

async def periodic_leader_check():
    """Periodically check if leader is active"""
    while True:
        try:
            await state.check_and_update_leader()
        except Exception as e:
            log.error("Error in leader check", error=str(e))
        
        # Wait before next check
        await asyncio.sleep(2.0)

async def send_leader_heartbeats():
    """Send heartbeats if this proposer is the leader"""
    while True:
        try:
            if state.is_leader:
                heartbeat = {
                    "leader_id": state.node_id,
                    "timestamp": time.time(),
                    "proposal_number": state.proposal_counter
                }
                
                # Send to all other proposers
                async with aiohttp.ClientSession() as session:
                    for i in range(1, 6):  # 5 proposers
                        if i == state.node_id:
                            continue  # Skip self
                        
                        try:
                            await session.post(
                                f"http://proposer-{i}:8080/heartbeat",
                                json=heartbeat,
                                timeout=0.5  # Short timeout for heartbeats
                            )
                        except Exception as e:
                            log.debug("Error sending heartbeat", 
                                     target=i, 
                                     error=str(e))
        except Exception as e:
            log.error("Error in heartbeat task", error=str(e))
        
        # Send heartbeats every second
        await asyncio.sleep(1.0)