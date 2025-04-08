// WebSocket connection
let socket = null;

// DOM Elements
const startRequestsBtn = document.getElementById('start-requests');
const stopRequestsBtn = document.getElementById('stop-requests');
const sendRequestBtn = document.getElementById('send-request');
const updateSettingsBtn = document.getElementById('update-settings');
const numRequestsInput = document.getElementById('num-requests');
const minWaitInput = document.getElementById('min-wait');
const maxWaitInput = document.getElementById('max-wait');
const resourceDataTextarea = document.getElementById('resource-data');
const requestResultPanel = document.getElementById('request-result');
const resultContentPre = document.getElementById('result-content');
const activityLogDiv = document.getElementById('activity-log');
const clientStatusSpan = document.getElementById('client-status');
const totalRequestsSpan = document.getElementById('total-requests');
const successfulRequestsSpan = document.getElementById('successful-requests');
const failedRequestsSpan = document.getElementById('failed-requests');

// Initialize WebSocket connection
function initWebSocket() {
    // Create WebSocket connection
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = `${protocol}//${window.location.host}/ws`;
    
    socket = new WebSocket(wsUrl);
    
    // Connection opened
    socket.addEventListener('open', (event) => {
        console.log('WebSocket connection established');
        addActivityItem('Connected to server', 'system');
        
        // Request initial status
        sendMessage({ type: 'get_status' });
    });
    
    // Listen for messages
    socket.addEventListener('message', (event) => {
        try {
            const message = JSON.parse(event.data);
            handleWebSocketMessage(message);
        } catch (error) {
            console.error('Error parsing WebSocket message:', error);
        }
    });
    
    // Connection closed
    socket.addEventListener('close', (event) => {
        console.log('WebSocket connection closed');
        addActivityItem('Disconnected from server', 'error');
        
        // Attempt to reconnect after a delay
        setTimeout(() => {
            initWebSocket();
        }, 3000);
    });
    
    // Connection error
    socket.addEventListener('error', (event) => {
        console.error('WebSocket error:', event);
        addActivityItem('WebSocket connection error', 'error');
    });
}

// Send message through WebSocket
function sendMessage(message) {
    if (socket && socket.readyState === WebSocket.OPEN) {
        socket.send(JSON.stringify(message));
    } else {
        console.error('WebSocket is not connected');
        addActivityItem('Cannot send message: WebSocket is not connected', 'error');
    }
}

// Handle WebSocket messages
function handleWebSocketMessage(message) {
    console.log('Received WebSocket message:', message);
    
    switch (message.type) {
        case 'status':
            updateClientStatus(message.data);
            break;
        
        case 'request_result':
            showRequestResult(message.data);
            break;
        
        case 'started':
            startRequestsBtn.disabled = true;
            stopRequestsBtn.disabled = false;
            clientStatusSpan.textContent = 'Running';
            clientStatusSpan.className = 'status-value status-running';
            addActivityItem(`Started sending ${message.data.num_requests} requests`, 'system');
            break;
        
        case 'stopped':
            startRequestsBtn.disabled = false;
            stopRequestsBtn.disabled = true;
            clientStatusSpan.textContent = 'Idle';
            clientStatusSpan.className = 'status-value status-idle';
            addActivityItem('Stopped sending requests', 'system');
            break;
        
        case 'request_start':
            addActivityItem(`Sending request with timestamp ${message.data.timestamp}`, 'info');
            break;
        
        case 'request_accepted':
            addActivityItem(`Request accepted: proposal_id=${message.data.proposal_id}`, 'success');
            break;
        
        case 'request_rejected':
            addActivityItem(`Request rejected: ${message.data.error}`, 'error');
            break;
        
        case 'request_error':
            addActivityItem(`Request error: ${message.data.error}`, 'error');
            break;
        
        case 'request_wait':
            addActivityItem(`Waiting ${message.data.wait_time.toFixed(2)}s before next request (${message.data.current_request}/${message.data.total_requests})`, 'info');
            break;
        
        case 'proposal_committed':
            addActivityItem(`Proposal committed: ${message.data.proposal_id} (${message.data.protocol || 'standard'})`, 'success');
            break;
        
        case 'proposal_not_committed':
            addActivityItem(`Proposal not committed: ${message.data.proposal_id}, status=${message.data.status}`, 'warning');
            break;
        
        case 'requests_completed':
            addActivityItem(`Completed all requests: ${message.data.successful_requests} successful, ${message.data.failed_requests} failed`, 'system');
            break;
        
        case 'requests_error':
            addActivityItem(`Error in request loop: ${message.data.error}`, 'error');
            break;
    }
}

// Update client status display
function updateClientStatus(status) {
    totalRequestsSpan.textContent = status.total_requests;
    successfulRequestsSpan.textContent = status.successful_requests;
    failedRequestsSpan.textContent = status.failed_requests;
    
    if (status.running) {
        clientStatusSpan.textContent = 'Running';
        clientStatusSpan.className = 'status-value status-running';
        startRequestsBtn.disabled = true;
        stopRequestsBtn.disabled = false;
    } else {
        clientStatusSpan.textContent = 'Idle';
        clientStatusSpan.className = 'status-value status-idle';
        startRequestsBtn.disabled = false;
        stopRequestsBtn.disabled = true;
    }
    
    // Update settings inputs
    minWaitInput.value = status.min_wait;
    maxWaitInput.value = status.max_wait;
    if (!numRequestsInput.value) {
        numRequestsInput.value = status.min_requests;
    }
}

// Show request result
function showRequestResult(result) {
    requestResultPanel.classList.remove('hidden');
    resultContentPre.textContent = JSON.stringify(result, null, 2);
    
    if (result.success) {
        resultContentPre.className = 'success';
        addActivityItem(`Request successful: proposal_id=${result.proposal_id}`, 'success');
    } else {
        resultContentPre.className = 'error';
        addActivityItem(`Request failed: ${result.error}`, 'error');
    }
}

// Add activity item to log
function addActivityItem(message, type = 'info') {
    // Remove 'activity-empty' message if present
    const emptyMsg = activityLogDiv.querySelector('.activity-empty');
    if (emptyMsg) {
        activityLogDiv.removeChild(emptyMsg);
    }
    
    // Create activity item
    const activityItem = document.createElement('div');
    activityItem.className = `activity-item ${type}`;
    
    // Create timestamp
    const timestamp = document.createElement('div');
    timestamp.className = 'activity-timestamp';
    const now = new Date();
    timestamp.textContent = now.toLocaleTimeString();
    
    // Create message
    const messageDiv = document.createElement('div');
    messageDiv.className = 'activity-message';
    messageDiv.textContent = message;
    
    // Append elements to activity item
    activityItem.appendChild(timestamp);
    activityItem.appendChild(messageDiv);
    
    // Add to log
    activityLogDiv.prepend(activityItem);
    
    // Limit the number of items
    const items = activityLogDiv.querySelectorAll('.activity-item');
    if (items.length > 100) {
        activityLogDiv.removeChild(items[items.length - 1]);
    }
}

// Event Listeners
startRequestsBtn.addEventListener('click', () => {
    const numRequests = numRequestsInput.value ? parseInt(numRequestsInput.value) : null;
    sendMessage({
        type: 'start',
        num_requests: numRequests
    });
});

stopRequestsBtn.addEventListener('click', () => {
    sendMessage({ type: 'stop' });
});

sendRequestBtn.addEventListener('click', () => {
    try {
        const resourceData = JSON.parse(resourceDataTextarea.value);
        sendMessage({
            type: 'request',
            resource_data: resourceData
        });
    } catch (error) {
        alert('Invalid JSON data: ' + error.message);
        addActivityItem('Invalid JSON data: ' + error.message, 'error');
    }
});

updateSettingsBtn.addEventListener('click', () => {
    const minWait = parseFloat(minWaitInput.value);
    const maxWait = parseFloat(maxWaitInput.value);
    
    if (minWait >= maxWait) {
        alert('Min wait time must be less than max wait time');
        return;
    }
    
    fetch('/config', {
        method: 'PUT',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({
            min_wait: minWait,
            max_wait: maxWait
        })
    })
    .then(response => response.json())
    .then(data => {
        addActivityItem('Settings updated', 'system');
    })
    .catch(error => {
        console.error('Error updating settings:', error);
        addActivityItem('Error updating settings: ' + error.message, 'error');
    });
});

// Initialize WebSocket on page load
document.addEventListener('DOMContentLoaded', () => {
    initWebSocket();
    addActivityItem('Client interface initialized', 'system');
});