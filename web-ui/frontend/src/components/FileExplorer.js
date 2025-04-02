import React, { useState, useEffect } from 'react';
import { Link, useNavigate, useLocation } from 'react-router-dom';
import { toast } from 'react-toastify';
import './FileExplorer.css';

const FileExplorer = () => {
  const [files, setFiles] = useState([]);
  const [currentPath, setCurrentPath] = useState('/');
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [showCreateForm, setShowCreateForm] = useState(false);
  const [newFileName, setNewFileName] = useState('');
  const [newFileContent, setNewFileContent] = useState('');
  const [createType, setCreateType] = useState('file'); // 'file' or 'directory'
  
  const navigate = useNavigate();
  const location = useLocation();
  
  // Parse path from URL if available
  useEffect(() => {
    const pathFromUrl = location.pathname.replace('/explorer', '') || '/';
    setCurrentPath(pathFromUrl);
  }, [location.pathname]);
  
  // Fetch files for the current directory
  useEffect(() => {
    fetchFiles(currentPath);
  }, [currentPath]);
  
  const fetchFiles = async (path) => {
    setLoading(true);
    setError(null);
    
    try {
      const response = await fetch(`/api/files?directory=${encodeURIComponent(path)}`);
      
      if (!response.ok) {
        throw new Error(`Server responded with ${response.status}: ${response.statusText}`);
      }
      
      const data = await response.json();
      
      if (data.success) {
        setFiles(data.items || []);
      } else {
        setError(data.error || 'Failed to load files');
        toast.error(data.error || 'Failed to load files');
      }
    } catch (err) {
      setError(err.message || 'Failed to load files');
      toast.error(err.message || 'Failed to load files');
    } finally {
      setLoading(false);
    }
  };
  
  const handleCreateFile = async (e) => {
    e.preventDefault();
    
    if (!newFileName) {
      toast.error('Please enter a name');
      return;
    }
    
    // Prepare the file path
    const filePath = currentPath === '/' 
      ? `/${newFileName}` 
      : `${currentPath}/${newFileName}`;
    
    try {
      const operation = {
        operation: 'CREATE',
        path: filePath,
        content: createType === 'file' ? newFileContent : ''
      };
      
      const response = await fetch('/api/files/propose', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(operation)
      });
      
      if (!response.ok) {
        throw new Error(`Server responded with ${response.status}: ${response.statusText}`);
      }
      
      const data = await response.json();
      
      if (data.success) {
        toast.success('File created successfully');
        setShowCreateForm(false);
        setNewFileName('');
        setNewFileContent('');
        
        // Refresh file list after a short delay to allow the system to process the change
        setTimeout(() => fetchFiles(currentPath), 1000);
        
        // If it's a file and has content, navigate to the editor
        if (createType === 'file' && newFileContent) {
          navigate(`/edit${filePath}`);
        }
      } else {
        throw new Error(data.error || 'Failed to create file');
      }
    } catch (err) {
      toast.error(err.message || 'Failed to create file');
    }
  };
  
  const handleDeleteFile = async (path, isDirectory) => {
    if (window.confirm(`Are you sure you want to delete this ${isDirectory ? 'directory' : 'file'}?`)) {
      try {
        const operation = {
          operation: 'DELETE',
          path: path
        };
        
        const response = await fetch('/api/files/propose', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json'
          },
          body: JSON.stringify(operation)
        });
        
        if (!response.ok) {
          throw new Error(`Server responded with ${response.status}: ${response.statusText}`);
        }
        
        const data = await response.json();
        
        if (data.success) {
          toast.success('Deleted successfully');
          
          // Refresh file list after a short delay
          setTimeout(() => fetchFiles(currentPath), 1000);
        } else {
          throw new Error(data.error || 'Failed to delete');
        }
      } catch (err) {
        toast.error(err.message || 'Failed to delete');
      }
    }
  };
  
  const navigateToParent = () => {
    if (currentPath === '/') return;
    
    const pathParts = currentPath.split('/').filter(part => part !== '');
    pathParts.pop();
    const parentPath = pathParts.length === 0 ? '/' : `/${pathParts.join('/')}`;
    
    navigate(`/explorer${parentPath}`);
  };
  
  // Build breadcrumb navigation
  const buildBreadcrumbs = () => {
    if (currentPath === '/') {
      return [{ name: 'Root', path: '/' }];
    }
    
    const pathParts = currentPath.split('/').filter(part => part !== '');
    const breadcrumbs = [{ name: 'Root', path: '/' }];
    
    let cumulativePath = '';
    for (const part of pathParts) {
      cumulativePath += `/${part}`;
      breadcrumbs.push({
        name: part,
        path: cumulativePath
      });
    }
    
    return breadcrumbs;
  };
  
  const breadcrumbs = buildBreadcrumbs();
  
  // Format timestamp to readable date
  const formatDate = (timestamp) => {
    if (!timestamp) return 'Unknown';
    return new Date(timestamp * 1000).toLocaleString();
  };
  
  // Format file size
  const formatSize = (bytes) => {
    if (bytes === undefined || bytes === null) return 'Unknown';
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(2)} KB`;
    return `${(bytes / (1024 * 1024)).toFixed(2)} MB`;
  };
  
  return (
    <div className="file-explorer">
      <div className="card">
        <div className="card-header">
          <h2 className="card-title">File Explorer</h2>
          <div className="button-group">
            <button 
              className="btn btn-success" 
              onClick={() => {
                setCreateType('file');
                setShowCreateForm(true);
              }}
            >
              <i className="fas fa-file-plus"></i> New File
            </button>
            <button 
              className="btn btn-primary" 
              onClick={() => {
                setCreateType('directory');
                setShowCreateForm(true);
              }}
            >
              <i className="fas fa-folder-plus"></i> New Directory
            </button>
          </div>
        </div>
        
        {/* Create File/Directory Form */}
        {showCreateForm && (
          <div className="create-form-container">
            <form className="create-form" onSubmit={handleCreateFile}>
              <div className="form-header">
                <h3>{createType === 'file' ? 'Create New File' : 'Create New Directory'}</h3>
                <button 
                  type="button" 
                  className="btn-close" 
                  onClick={() => setShowCreateForm(false)}
                >
                  &times;
                </button>
              </div>
              
              <div className="form-group">
                <label htmlFor="fileName">Name:</label>
                <input
                  type="text"
                  id="fileName"
                  className="form-control"
                  value={newFileName}
                  onChange={(e) => setNewFileName(e.target.value)}
                  placeholder={createType === 'file' ? 'example.txt' : 'new-folder'}
                  required
                />
              </div>
              
              {createType === 'file' && (
                <div className="form-group">
                  <label htmlFor="fileContent">Content:</label>
                  <textarea
                    id="fileContent"
                    className="form-control"
                    value={newFileContent}
                    onChange={(e) => setNewFileContent(e.target.value)}
                    rows={10}
                    placeholder="Enter file content here..."
                  ></textarea>
                </div>
              )}
              
              <div className="form-actions">
                <button type="submit" className="btn btn-success">
                  Create {createType === 'file' ? 'File' : 'Directory'}
                </button>
                <button
                  type="button"
                  className="btn btn-danger"
                  onClick={() => setShowCreateForm(false)}
                >
                  Cancel
                </button>
              </div>
            </form>
          </div>
        )}
        
        {/* Breadcrumb Navigation */}
        <div className="breadcrumb-container">
          <ul className="breadcrumb">
            {breadcrumbs.map((crumb, index) => (
              <li key={index} className="breadcrumb-item">
                {index < breadcrumbs.length - 1 ? (
                  <Link to={`/explorer${crumb.path}`}>{crumb.name}</Link>
                ) : (
                  <span className="active">{crumb.name}</span>
                )}
              </li>
            ))}
          </ul>
        </div>
        
        {/* Error Message */}
        {error && <div className="alert alert-danger">{error}</div>}
        
        {/* Loading Spinner */}
        {loading ? (
          <div className="loading-container">
            <div className="spinner"></div>
            <p>Loading files...</p>
          </div>
        ) : (
          <div className="file-list-container">
            {/* Parent Directory Navigation */}
            {currentPath !== '/' && (
              <div className="file-item directory" onClick={navigateToParent}>
                <div className="file-icon">
                  <i className="fas fa-arrow-up"></i>
                </div>
                <div className="file-details">
                  <div className="file-name">..</div>
                  <div className="file-info">Parent Directory</div>
                </div>
              </div>
            )}
            
            {/* Files and Directories */}
            {files.length === 0 ? (
              <div className="empty-directory">
                <i className="fas fa-folder-open"></i>
                <p>This directory is empty</p>
              </div>
            ) : (
              files.map((file, index) => (
                <div key={index} className={`file-item ${file.type}`}>
                  <div className="file-icon">
                    <i className={file.type === 'directory' ? 'fas fa-folder' : 'fas fa-file-alt'}></i>
                  </div>
                  
                  <div 
                    className="file-details"
                    onClick={() => {
                      if (file.type === 'directory') {
                        navigate(`/explorer${file.path}`);
                      } else {
                        navigate(`/edit${file.path}`);
                      }
                    }}
                  >
                    <div className="file-name">{file.name}</div>
                    <div className="file-info">
                      {file.type === 'file' ? (
                        <>
                          <span>{formatSize(file.size)}</span>
                          <span>·</span>
                          <span>{formatDate(file.modified)}</span>
                        </>
                      ) : (
                        <span>Directory</span>
                      )}
                    </div>
                  </div>
                  
                  <div className="file-actions">
                    {file.type === 'file' && (
                      <button
                        className="btn btn-sm btn-primary"
                        onClick={() => navigate(`/edit${file.path}`)}
                      >
                        <i className="fas fa-edit"></i> Edit
                      </button>
                    )}
                    <button
                      className="btn btn-sm btn-danger"
                      onClick={() => handleDeleteFile(file.path, file.type === 'directory')}
                    >
                      <i className="fas fa-trash"></i> Delete
                    </button>
                  </div>
                </div>
              ))
            )}
          </div>
        )}
      </div>
    </div>
  );
};

export default FileExplorer;