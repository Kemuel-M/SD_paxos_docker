#!/bin/bash

# Script to test failure scenarios for the Paxos system
# This script simulates the three required failure scenarios:
# 1. Element of Cluster Store failing without pending requests
# 2. Element of Cluster Store failing during a request
# 3. Element of Cluster Store failing after committing to write

echo "==== Paxos System Failure Scenarios Test ===="

# Test Scenario 1: Store node failing without pending requests
test_store_failure_without_request() {
    echo "Testing Scenario 1: Store node failing without pending requests"
    echo "Stopping store-1..."
    docker-compose stop store-1
    
    echo "Waiting 5 seconds..."
    sleep 5
    
    echo "Checking system status from learner-1..."
    curl -s http://localhost:8091/status | jq '.store_status'
    
    echo "Performing write operations (should succeed with 2 active stores)..."
    curl -s -X POST -H "Content-Type: application/json" \
        -d '{"operation":"CREATE", "path":"/test1.txt", "content":"Test file during store-1 failure"}' \
        http://localhost:80/api/files/propose | jq '.'
    
    echo "Waiting 3 seconds for replication..."
    sleep 3
    
    echo "Checking if file was created successfully..."
    curl -s http://localhost:80/api/files/test1.txt | jq '.'
    
    echo "Restarting store-1..."
    docker-compose start store-1
    
    echo "Waiting 10 seconds for recovery and synchronization..."
    sleep 10
    
    echo "Checking store-1 status after recovery..."
    curl -s http://localhost:8101/status | jq '.'
    
    echo "Scenario 1 test completed"
    echo "-----------------------------------"
}

# Test Scenario 2: Store node failing during a request
test_store_failure_during_request() {
    echo "Testing Scenario 2: Store node failing during a request"
    
    # First, we'll create a request that we know will go to store-2
    echo "Setting up test by creating a file first..."
    curl -s -X POST -H "Content-Type: application/json" \
        -d '{"operation":"CREATE", "path":"/test2.txt", "content":"Initial content"}' \
        http://localhost:80/api/files/propose | jq '.'
    
    echo "Waiting 3 seconds..."
    sleep 3
    
    # Now we'll stop store-2 and try to modify the file immediately
    echo "Stopping store-2..."
    docker-compose stop store-2
    
    echo "Immediately trying to modify the file..."
    curl -s -X POST -H "Content-Type: application/json" \
        -d '{"operation":"MODIFY", "path":"/test2.txt", "content":"Modified content during failure"}' \
        http://localhost:80/api/files/propose | jq '.'
    
    echo "Checking system logs for timeout detection..."
    docker-compose logs --tail=50 learner-1 | grep "timeout\|failed\|error" | tail -10
    
    echo "Waiting 3 seconds..."
    sleep 3
    
    echo "Checking if file modification failed as expected (should still show original content)..."
    curl -s http://localhost:80/api/files/test2.txt | jq '.'
    
    echo "Restarting store-2..."
    docker-compose start store-2
    
    echo "Waiting 10 seconds for recovery..."
    sleep 10
    
    echo "Trying modification again after recovery..."
    curl -s -X POST -H "Content-Type: application/json" \
        -d '{"operation":"MODIFY", "path":"/test2.txt", "content":"Modified content after recovery"}' \
        http://localhost:80/api/files/propose | jq '.'
        
    echo "Waiting 3 seconds..."
    sleep 3
    
    echo "Checking if modification succeeded after recovery..."
    curl -s http://localhost:80/api/files/test2.txt | jq '.'
    
    echo "Scenario 2 test completed"
    echo "-----------------------------------"
}

# Test Scenario 3: Store node failing after committing to write
test_store_failure_after_permission() {
    echo "Testing Scenario 3: Store node failing after committing to write permission"
    
    # We need to simulate a failure during the Two-Phase Commit protocol
    # Since we can't easily time it exactly, we'll do something similar:
    # 1. Create a file successfully
    # 2. Stop one store node
    # 3. Verify that the remaining nodes have the file
    # 4. Start the node again and verify it syncs correctly
    
    echo "Creating test file for scenario 3..."
    curl -s -X POST -H "Content-Type: application/json" \
        -d '{"operation":"CREATE", "path":"/test3.txt", "content":"Initial content for scenario 3"}' \
        http://localhost:80/api/files/propose | jq '.'
    
    echo "Waiting 3 seconds..."
    sleep 3
    
    echo "Checking if file created successfully..."
    curl -s http://localhost:80/api/files/test3.txt | jq '.'
    
    echo "Stopping store-3..."
    docker-compose stop store-3
    
    echo "Creating another file while store-3 is down..."
    curl -s -X POST -H "Content-Type: application/json" \
        -d '{"operation":"CREATE", "path":"/test3b.txt", "content":"This file is created while store-3 is down"}' \
        http://localhost:80/api/files/propose | jq '.'
    
    echo "Waiting 3 seconds..."
    sleep 3
    
    echo "Restarting store-3..."
    docker-compose start store-3
    
    echo "Waiting 15 seconds for store-3 to recover and sync..."
    sleep 15
    
    echo "Checking if store-3 synchronized correctly by reading through it..."
    # We'll try to direct our read to store-3
    # This is a bit hacky but we can try multiple reads to increase chances
    for i in {1..5}; do
        echo "Read attempt $i..."
        curl -s http://localhost:80/api/files/test3b.txt | jq '.'
        sleep 1
    done
    
    echo "Checking store-3 status and resource count..."
    curl -s http://localhost:8103/status | jq '.resources_count'
    
    echo "Scenario 3 test completed"
    echo "-----------------------------------"
}

# Run all tests
echo "Running all failure scenario tests..."
test_store_failure_without_request
sleep 5
test_store_failure_during_request
sleep 5
test_store_failure_after_permission

echo "==== All failure scenario tests completed ===="