# Paxos Distributed Consensus System

This project implements a distributed consensus system using the Paxos algorithm, with replication and fault tolerance provided by a voting protocol.

## System Architecture

The system consists of the following components:

1. **Proposers (Cluster Sync)**: 5 nodes that receive client requests and coordinate the Paxos protocol.
2. **Acceptors**: 5 nodes that participate in the Paxos protocol by promising and accepting proposals.
3. **Learners**: 2 nodes that learn the consensus decisions and coordinate with the Store cluster.
4. **Store (Cluster Store)**: 3 nodes that implement the actual storage with replication.
5. **Web UI**: Interface for interacting with the system.
6. **Monitoring**: Prometheus and Grafana for system observability.

## Protocol Implementation

The system implements two protocols:

1. **Paxos Protocol**: Used for distributed consensus on the order of operations.
2. **ROWA (Read One, Write All) Protocol**: Used for replication in the Store cluster with Nr=1 and Nw=3.

## Fault Tolerance

The system handles several failure scenarios:

1. **Proposer Failures**: Multi-Paxos with leader election for proposer fault tolerance.
2. **Acceptor Failures**: Quorum-based consensus (tolerates up to 2 acceptor failures out of 5).
3. **Learner Failures**: Both learners maintain the same state through synchronization.
4. **Store Failures**:
   - Store without pending request: Detected via heartbeats, system continues with remaining nodes.
   - Store with pending request: Timeout detection, transaction will be aborted.
   - Store with write permission: Two-Phase Commit protocol handles partial failures.

## Getting Started

### Prerequisites

- Docker and Docker Compose
- bash (for testing scripts)

### Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/paxos-system.git
cd paxos-system

# Build and start the containers
docker-compose build
docker-compose up -d

# Check if all services are running
docker-compose ps
```

### Usage

The system provides a web interface available at http://localhost:80 for interacting with the distributed file system.

### Testing Failure Scenarios

The project includes a script to test the required failure scenarios:

```bash
# Make the script executable
chmod +x scripts/test_failures.sh

# Run the failure tests
./scripts/test_failures.sh
```

## Implementation Details

### Two-Phase Commit Protocol

The system uses a Two-Phase Commit (2PC) protocol for coordinating writes across the Store nodes:

1. **Prepare Phase**: Learners send prepare requests to all Store nodes.
2. **Commit Phase**: If all nodes are ready, commit is sent; otherwise, the transaction is aborted.

### ROWA Implementation

For replication, we use:

- **Read Operations**: Nr = 1 (read from any one Store node)
- **Write Operations**: Nw = 3 (write to all Store nodes)

This satisfies the requirement that Nr + Nw > N where N=3.

### Failure Detection

- **Heartbeats**: Sent between components every 2 seconds
- **Timeouts**: Adaptive timeout mechanism for detecting node failures
- **Synchronization**: Periodic synchronization between Learners and Store nodes

## Monitoring

Access the monitoring interfaces at:
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000

## Project Structure

```
.
├── acceptor/            # Acceptor implementation
├── proposer/            # Proposer implementation
├── learner/             # Learner implementation
├── store/               # Cluster Store implementation
├── web-ui/              # Web interface
├── prometheus/          # Prometheus configuration
├── grafana/             # Grafana dashboards
├── scripts/             # Testing scripts
└── docker-compose.yml   # Docker Compose configuration
```

## Limitations and Future Work

- The current implementation focuses on functionality rather than performance optimization.
- Security features (authentication, authorization, encryption) are not implemented.
- Snapshots and compaction for long-term operation are not fully implemented.

## License

This project is licensed under the MIT License - see the LICENSE file for details.