"""
Proposer module for Paxos distributed consensus algorithm.

The proposer initiates the consensus process and coordinates the protocol phases.
Key responsibilities:
- Receive client requests and propose values to the system
- Coordinate the prepare and accept phases of the Paxos protocol
- Manage proposal numbers to ensure uniqueness
- Handle leader election and implement Multi-Paxos optimizations
- Implement batching for better performance
- Handle rejections and retries
"""

__version__ = "1.0.0"