// WebSocket connection
let socket = null;

// DOM Elements
const clientStatusSpan = document.getElementById('client-status');
const totalRequestsSpan = document.getElementById('total-requests');
const successfulRequestsSpan = document.getElementById('successful-requests');
const failedRequestsSpan = document.getElementById('failed-requests');
const successRateSpan = document.getElementById('success-rate');
const totalProposalsSpan = document.getElementById('total-proposals');
const committedCountSpan = document.getElementById('committed-count');
const avgResponseTimeSpan = document.getElementById('avg-response-time');
const historyTableBody = document.getElementById('history-table-body');
const refreshHistoryBtn = document.getElementById('refresh-history');
const historyLimitSelect = document.getElementById('history-limit');

// Initialize WebSocket connection
function initWebSocket() {
    // Create WebSocket connection
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = `${protocol}//${window.location.host}/ws`;
    
    socket = new WebSocket(wsUrl);
    
    // Connection opened
    socket.addEventListener('open', (event) => {
        console.log('WebSocket connection established');
        
        // Request initial status and history
        sendMessage({ type: 'get_status' });
        sendMessage({ 
            type: 'get_history',
            limit: parseInt(historyLimitSelect.value)
        });
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
        
        // Attempt to reconnect after a delay
        setTimeout(() => {
            initWebSocket();
        }, 3000);
    });
    
    // Connection error
    socket.addEventListener('error', (event) => {
        console.error('WebSocket error:', event);
    });
}

// Send message through WebSocket
function sendMessage(message) {
    if (socket && socket.readyState === WebSocket.OPEN) {
        socket.send(JSON.stringify(message));
    } else {
        console.error('WebSocket is not connected');
    }
}

// Handle WebSocket messages
function handleWebSocketMessage(message) {
    console.log('Received WebSocket message:', message);
    
    switch (message.type) {
        case 'status':
            updateClientStatus(message.data);
            break;
        
        case 'history':
            updateHistoryTable(message.data.history);
            break;
        
        case 'proposal_committed':
        case 'proposal_not_committed':
        case 'request_accepted':
        case 'request_rejected':
        case 'requests_completed':
            // When we receive these events, refresh the data
            sendMessage({ type: 'get_status' });
            sendMessage({ 
                type: 'get_history',
                limit: parseInt(historyLimitSelect.value)
            });
            break;
    }
}

// Update client status display
function updateClientStatus(status) {
    totalRequestsSpan.textContent = status.total_requests;
    successfulRequestsSpan.textContent = status.successful_requests;
    failedRequestsSpan.textContent = status.failed_requests;
    totalProposalsSpan.textContent = status.total_requests;
    
    // Calculate success rate
    if (status.total_requests > 0) {
        const successRate = (status.successful_requests / status.total_requests * 100).toFixed(2);
        successRateSpan.textContent = `${successRate}%`;
    } else {
        successRateSpan.textContent = '0%';
    }
    
    if (status.running) {
        clientStatusSpan.textContent = 'Running';
        clientStatusSpan.className = 'status-value status-running';
    } else {
        clientStatusSpan.textContent = 'Idle';
        clientStatusSpan.className = 'status-value status-idle';
    }
}

// Update history table
function updateHistoryTable(history) {
    if (!history || history.length === 0) {
        historyTableBody.innerHTML = `
            <tr>
                <td colspan="6" class="no-data">No request history available</td>
            </tr>
        `;
        return;
    }
    
    // Clear table body
    historyTableBody.innerHTML = '';
    
    // Calculate committed count
    let committedCount = 0;
    let totalResponseTime = 0;
    let responseTimeCount = 0;
    
    // Add rows for each history item
    history.forEach((request, index) => {
        if (request.status === 'committed') {
            committedCount++;
        }
        
        if (request.end_time && request.start_time) {
            const responseTime = (request.end_time - request.start_time) * 1000;
            totalResponseTime += responseTime;
            responseTimeCount++;
        }
        
        // Create main row
        const row = document.createElement('tr');
        row.className = `history-row status-${request.status}`;
        
        row.innerHTML = `
            <td>${formatTimestamp(request.start_time)}</td>
            <td>${request.proposal_id || 'N/A'}</td>
            <td class="status-cell">
                <span class="status-badge status-${request.status}">${request.status}</span>
            </td>
            <td>${request.response && request.response.tid ? request.response.tid : 'N/A'}</td>
            <td>${request.end_time && request.start_time ? 
                ((request.end_time - request.start_time) * 1000).toFixed(2) + ' ms' : 
                'N/A'}</td>
            <td>
                <button class="btn btn-mini toggle-details" data-index="${index}">Show</button>
            </td>
        `;
        
        historyTableBody.appendChild(row);
        
        // Create details row
        const detailsRow = document.createElement('tr');
        detailsRow.className = 'details-row hidden';
        detailsRow.id = `details-${index}`;
        
        detailsRow.innerHTML = `
            <td colspan="6">
                <div class="details-content">
                    <h4>Resource Data</h4>
                    <pre>${formatJSON(request.resource_data)}</pre>
                    
                    ${request.response ? `
                        <h4>Response</h4>
                        <pre>${formatJSON(request.response)}</pre>
                    ` : ''}
                    
                    ${request.notification ? `
                        <h4>Notification</h4>
                        <pre>${formatJSON(request.notification)}</pre>
                    ` : ''}
                </div>
            </td>
        `;
        
        historyTableBody.appendChild(detailsRow);
    });
    
    // Update statistics
    committedCountSpan.textContent = committedCount;
    
    if (responseTimeCount > 0) {
        const avgResponseTime = totalResponseTime / responseTimeCount;
        avgResponseTimeSpan.textContent = `${avgResponseTime.toFixed(2)} ms`;
    } else {
        avgResponseTimeSpan.textContent = '0 ms';
    }
    
    // Add event listeners to toggle details buttons
    document.querySelectorAll('.toggle-details').forEach(button => {
        button.addEventListener('click', () => {
            const index = button.getAttribute('data-index');
            const detailsRow = document.getElementById(`details-${index}`);
            
            if (detailsRow.classList.contains('hidden')) {
                detailsRow.classList.remove('hidden');
                button.textContent = 'Hide';
            } else {
                detailsRow.classList.add('hidden');
                button.textContent = 'Show';
            }
        });
    });
}

// Format timestamp
function formatTimestamp(timestamp) {
    if (!timestamp) return 'N/A';
    
    const date = new Date(timestamp * 1000);
    return date.toLocaleTimeString();
}

// Format JSON
function formatJSON(obj) {
    if (!obj) return 'N/A';
    
    try {
        return JSON.stringify(obj, null, 2);
    } catch (error) {
        return String(obj);
    }
}

// Event Listeners
refreshHistoryBtn.addEventListener('click', () => {
    sendMessage({ 
        type: 'get_history',
        limit: parseInt(historyLimitSelect.value)
    });
});

historyLimitSelect.addEventListener('change', () => {
    sendMessage({ 
        type: 'get_history',
        limit: parseInt(historyLimitSelect.value)
    });
});

// Initialize WebSocket on page load
document.addEventListener('DOMContentLoaded', () => {
    initWebSocket();
});