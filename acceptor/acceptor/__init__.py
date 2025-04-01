"""
Acceptor module for Paxos distributed consensus algorithm.

The acceptor plays a critical role in the Paxos protocol, acting as the "memory" 
of the consensus system. Acceptors vote on proposals from proposers and help
ensure that only a single value is chosen for each instance.

Key responsibilities:
- Process "prepare" requests from proposers
- Process "accept" requests from proposers
- Maintain promises and accepted values persistently
- Notify learners about accepted proposals
"""

__version__ = "1.0.0"