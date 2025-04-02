import React, { useState, useEffect } from 'react';
import { toast } from 'react-toastify';
import './SystemStatus.css';

const SystemStatus = () => {
  const [systemStatus, setSystemStatus] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [autoRefresh, setAutoRefresh] = useState(true);
  const [refreshInterval, setRefreshInterval] = useState(5000); // 5 seconds
  
  // Fetch system status
  const fetchSystemStatus = async () => {
    try {
      const response = await fetch('/api/system/status');
      
      if (!response.ok) {
        throw new Error(`Server responded with ${response.status}: ${response.statusText}`);
      }
      
      const data = await response.json();
      setSystemStatus(data);
      setError(null);
    } catch (err) {
      setError(err.message || 'Failed to load system status');
      toast.error(err.message || 'Failed to load system status');
    } finally {
      setLoading(false);
    }
  };
  
  // Initial fetch
  useEffect(() => {
    fetchSystemStatus();
  }, []);
  
  // Auto-refresh setup
  useEffect(() => {
    let intervalId;
    
    if (autoRefresh) {
      intervalId = setInterval(() => {
        fetchSystemStatus();
      }, refreshInterval);
    }
    
    return () => {
      if (intervalId) {
        clearInterval(intervalId);
      }
    };
  }, [autoRefresh, refreshInterval]);
  
  // Format uptime in human-readable format
  const formatUptime = (seconds) => {
    if (!seconds && seconds !== 0) return 'Unknown';
    
    const days = Math.floor(seconds / 86400);
    const hours = Math.floor((seconds % 86400) / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    const remainingSeconds = Math.floor(seconds % 60);
    
    const parts = [];
    if (days > 0) parts.push(`${days}d`);
    if (hours > 0) parts.push(`${hours}h`);
    if (minutes > 0) parts.push(`${minutes}m`);
    if (remainingSeconds > 0 || parts.length === 0) parts.push(`${remainingSeconds}s`);
    
    return parts.join(' ');
  };
  
  // Get status indicator class
  const getStatusClass = (component) => {
    if (!component) return 'status-unknown';
    if (component.status === 'error') return 'status-down';
    return 'status-up';
  };
  
  // Get status text
  const getStatusText = (component) => {
    if (!component) return 'Unknown';
    if (component.status === 'error') return 'Down';
    return 'Up';
  };
  
  return (
    <div className="system-status">
      <div className="card">
        <div className="card-header">
          <h2 className="card-title">System Status</h2>
          <div className="button-group">
            <button
              className="btn btn-primary"
              onClick={() => fetchSystemStatus()}
            >
              <i className="fas fa-sync"></i> Refresh
            </button>
            <button
              className={`btn ${autoRefresh ? 'btn-success' : 'btn-secondary'}`}
              onClick={() => setAutoRefresh(!autoRefresh)}
            >
              <i className={`fas ${autoRefresh ? 'fa-toggle-on' : 'fa-toggle-off'}`}></i>
              {' '}Auto Refresh
            </button>
          </div>
        </div>
        
        {/* Error Message */}
        {error && <div className="alert alert-danger">{error}</div>}
        
        {/* Loading Spinner */}
        {loading && !systemStatus ? (
          <div className="loading-container">
            <div className="spinner"></div>
            <p>Loading system status...</p>
          </div>
        ) : (
          <>
            {/* Overview */}
            <div className="status-overview">
              <div className="overview-item">
                <h3>Proposers</h3>
                <div className="status-summary">
                  {systemStatus && (
                    <div className="status-counts">
                      <span className="status-count">
                        <span className="status-indicator status-up"></span>
                        {Object.values(systemStatus.proposers).filter(p => p.status !== 'error').length}
                      </span>
                      <span className="status-count">
                        <span className="status-indicator status-down"></span>
                        {Object.values(systemStatus.proposers).filter(p => p.status === 'error').length}
                      </span>
                    </div>
                  )}
                </div>
              </div>
              
              <div className="overview-item">
                <h3>Acceptors</h3>
                <div className="status-summary">
                  {systemStatus && (
                    <div className="status-counts">
                      <span className="status-count">
                        <span className="status-indicator status-up"></span>
                        {Object.values(systemStatus.acceptors).filter(a => a.status !== 'error').length}
                      </span>
                      <span className="status-count">
                        <span className="status-indicator status-down"></span>
                        {Object.values(systemStatus.acceptors).filter(a => a.status === 'error').length}
                      </span>
                    </div>
                  )}
                </div>
              </div>
              
              <div className="overview-item">
                <h3>Learners</h3>
                <div className="status-summary">
                  {systemStatus && (
                    <div className="status-counts">
                      <span className="status-count">
                        <span className="status-indicator status-up"></span>
                        {Object.values(systemStatus.learners).filter(l => l.status !== 'error').length}
                      </span>
                      <span className="status-count">
                        <span className="status-indicator status-down"></span>
                        {Object.values(systemStatus.learners).filter(l => l.status === 'error').length}
                      </span>
                    </div>
                  )}
                </div>
              </div>
            </div>
            
            {/* Detailed Status */}
            <div className="status-details">
              {/* Proposers */}
              <div className="status-section">
                <h3>Proposers</h3>
                <div className="status-grid">
                  {systemStatus && Object.entries(systemStatus.proposers).map(([id, proposer]) => (
                    <div key={`proposer-${id}`} className="status-card">
                      <div className="status-card-header">
                        <h4>Proposer {id}</h4>
                        <span className={`status-badge ${getStatusClass(proposer)}`}>
                          {getStatusText(proposer)}
                        </span>
                      </div>
                      {proposer && proposer.status !== 'error' ? (
                        <div className="status-card-body">
                          <div className="status-property">
                            <span className="property-label">Role:</span>
                            <span className="property-value">{proposer.role}</span>
                          </div>
                          <div className="status-property">
                            <span className="property-label">Leader:</span>
                            <span className="property-value">
                              {proposer.is_leader ? (
                                <span className="leader-badge">Yes</span>
                              ) : (
                                <span>No</span>
                              )}
                            </span>
                          </div>
                          <div className="status-property">
                            <span className="property-label">Proposals:</span>
                            <span className="property-value">{proposer.active_proposals || 0}</span>
                          </div>
                          <div className="status-property">
                            <span className="property-label">Uptime:</span>
                            <span className="property-value">{formatUptime(proposer.uptime_seconds)}</span>
                          </div>
                        </div>
                      ) : (
                        <div className="status-card-body">
                          <div className="status-error">
                            {proposer && proposer.message ? (
                              <p>{proposer.message}</p>
                            ) : (
                              <p>Node is unreachable</p>
                            )}
                          </div>
                        </div>
                      )}
                    </div>
                  ))}
                </div>
              </div>
              
              {/* Acceptors */}
              <div className="status-section">
                <h3>Acceptors</h3>
                <div className="status-grid">
                  {systemStatus && Object.entries(systemStatus.acceptors).map(([id, acceptor]) => (
                    <div key={`acceptor-${id}`} className="status-card">
                      <div className="status-card-header">
                        <h4>Acceptor {id}</h4>
                        <span className={`status-badge ${getStatusClass(acceptor)}`}>
                          {getStatusText(acceptor)}
                        </span>
                      </div>
                      {acceptor && acceptor.status !== 'error' ? (
                        <div className="status-card-body">
                          <div className="status-property">
                            <span className="property-label">Role:</span>
                            <span className="property-value">{acceptor.role}</span>
                          </div>
                          <div className="status-property">
                            <span className="property-label">Promises:</span>
                            <span className="property-value">{acceptor.promises_count || 0}</span>
                          </div>
                          <div className="status-property">
                            <span className="property-label">Accepts:</span>
                            <span className="property-value">{acceptor.accepted_count || 0}</span>
                          </div>
                          <div className="status-property">
                            <span className="property-label">Uptime:</span>
                            <span className="property-value">{formatUptime(acceptor.uptime_seconds)}</span>
                          </div>
                        </div>
                      ) : (
                        <div className="status-card-body">
                          <div className="status-error">
                            {acceptor && acceptor.message ? (
                              <p>{acceptor.message}</p>
                            ) : (
                              <p>Node is unreachable</p>
                            )}
                          </div>
                        </div>
                      )}
                    </div>
                  ))}
                </div>
              </div>
              
              {/* Learners */}
              <div className="status-section">
                <h3>Learners</h3>
                <div className="status-grid">
                  {systemStatus && Object.entries(systemStatus.learners).map(([id, learner]) => (
                    <div key={`learner-${id}`} className="status-card">
                      <div className="status-card-header">
                        <h4>Learner {id}</h4>
                        <span className={`status-badge ${getStatusClass(learner)}`}>
                          {getStatusText(learner)}
                        </span>
                      </div>
                      {learner && learner.status !== 'error' ? (
                        <div className="status-card-body">
                          <div className="status-property">
                            <span className="property-label">Role:</span>
                            <span className="property-value">{learner.role}</span>
                          </div>
                          <div className="status-property">
                            <span className="property-label">Decisions:</span>
                            <span className="property-value">{learner.decisions_count || 0}</span>
                          </div>
                          <div className="status-property">
                            <span className="property-label">Last Applied:</span>
                            <span className="property-value">{learner.last_applied_instance || 0}</span>
                          </div>
                          <div className="status-property">
                            <span className="property-label">Files:</span>
                            <span className="property-value">{learner.file_count || 0}</span>
                          </div>
                          <div className="status-property">
                            <span className="property-label">Directories:</span>
                            <span className="property-value">{learner.directory_count || 0}</span>
                          </div>
                          <div className="status-property">
                            <span className="property-label">Uptime:</span>
                            <span className="property-value">{formatUptime(learner.uptime_seconds)}</span>
                          </div>
                        </div>
                      ) : (
                        <div className="status-card-body">
                          <div className="status-error">
                            {learner && learner.message ? (
                              <p>{learner.message}</p>
                            ) : (
                              <p>Node is unreachable</p>
                            )}
                          </div>
                        </div>
                      )}
                    </div>
                  ))}
                </div>
              </div>
            </div>
          </>
        )}
      </div>
      
      {/* Status Footer */}
      <div className="status-footer">
        <div className="refresh-info">
          {autoRefresh ? (
            <span>Auto-refreshing every {refreshInterval / 1000} seconds</span>
          ) : (
            <span>Auto-refresh disabled</span>
          )}
        </div>
        <div className="refresh-controls">
          <label htmlFor="refreshInterval">Refresh interval: </label>
          <select
            id="refreshInterval"
            value={refreshInterval}
            onChange={(e) => setRefreshInterval(Number(e.target.value))}
            disabled={!autoRefresh}
          >
            <option value={2000}>2 seconds</option>
            <option value={5000}>5 seconds</option>
            <option value={10000}>10 seconds</option>
            <option value={30000}>30 seconds</option>
            <option value={60000}>1 minute</option>
          </select>
        </div>
      </div>
    </div>
  );
};

export default SystemStatus;