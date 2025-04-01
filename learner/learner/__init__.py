"""
Learner module for Paxos distributed consensus algorithm.

The learner is responsible for observing the consensus process and determining
when a value has been decided by the distributed system. Learners apply the
decided values to maintain a consistent state across the system.

Key responsibilities:
- Collect acceptance notifications from acceptors
- Determine when consensus is reached on a value
- Apply decisions to the shared file system
- Maintain consistent state with other learners
- Serve client requests for reading the current state
"""

__version__ = "1.0.0"