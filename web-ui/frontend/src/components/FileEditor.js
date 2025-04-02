import React, { useState, useEffect } from 'react';
import { useParams, useNavigate, useLocation } from 'react-router-dom';
import { toast } from 'react-toastify';
import './FileEditor.css';

const FileEditor = () => {
  const [fileContent, setFileContent] = useState('');
  const [originalContent, setOriginalContent] = useState('');
  const [fileName, setFileName] = useState('');
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState(null);
  const [unsavedChanges, setUnsavedChanges] = useState(false);
  
  const navigate = useNavigate();
  const location = useLocation();
  
  // Get file path from URL
  const filePath = location.pathname.replace('/edit', '') || '/';
  
  // Extract filename from path
  useEffect(() => {
    const pathParts = filePath.split('/');
    setFileName(pathParts[pathParts.length - 1]);
  }, [filePath]);
  
  // Load file content
  useEffect(() => {
    fetchFileContent();
  }, [filePath]);
  
  // Detect unsaved changes
  useEffect(() => {
    setUnsavedChanges(fileContent !== originalContent);
  }, [fileContent, originalContent]);
  
  // Confirm before leaving with unsaved changes
  useEffect(() => {
    const handleBeforeUnload = (e) => {
      if (unsavedChanges) {
        e.preventDefault();
        e.returnValue = '';
        return '';
      }
    };
    
    window.addEventListener('beforeunload', handleBeforeUnload);
    
    return () => {
      window.removeEventListener('beforeunload', handleBeforeUnload);
    };
  }, [unsavedChanges]);
  
  const fetchFileContent = async () => {
    setLoading(true);
    setError(null);
    
    try {
      const response = await fetch(`/api/files${filePath}`);
      
      if (!response.ok) {
        throw new Error(`Server responded with ${response.status}: ${response.statusText}`);
      }
      
      const data = await response.json();
      
      if (data.success) {
        setFileContent(data.content || '');
        setOriginalContent(data.content || '');
      } else {
        setError(data.error || 'Failed to load file');
        toast.error(data.error || 'Failed to load file');
      }
    } catch (err) {
      setError(err.message || 'Failed to load file');
      toast.error(err.message || 'Failed to load file');
    } finally {
      setLoading(false);
    }
  };
  
  const handleSave = async () => {
    if (!unsavedChanges) return;
    
    setSaving(true);
    setError(null);
    
    try {
      const operation = {
        operation: 'MODIFY',
        path: filePath,
        content: fileContent
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
        toast.success('File saved successfully');
        setOriginalContent(fileContent);
        setUnsavedChanges(false);
      } else {
        throw new Error(data.error || 'Failed to save file');
      }
    } catch (err) {
      setError(err.message || 'Failed to save file');
      toast.error(err.message || 'Failed to save file');
    } finally {
      setSaving(false);
    }
  };
  
  const handleContentChange = (e) => {
    setFileContent(e.target.value);
  };
  
  const navigateToFileExplorer = () => {
    if (unsavedChanges) {
      if (window.confirm('You have unsaved changes. Do you want to leave without saving?')) {
        const parentPath = filePath.substring(0, filePath.lastIndexOf('/')) || '/';
        navigate(`/explorer${parentPath}`);
      }
    } else {
      const parentPath = filePath.substring(0, filePath.lastIndexOf('/')) || '/';
      navigate(`/explorer${parentPath}`);
    }
  };
  
  return (
    <div className="file-editor">
      <div className="card">
        <div className="card-header">
          <h2 className="card-title">
            {loading ? 'Loading...' : fileName}
            {unsavedChanges && <span className="unsaved-indicator">*</span>}
          </h2>
          <div className="button-group">
            <button
              className="btn btn-secondary"
              onClick={navigateToFileExplorer}
            >
              <i className="fas fa-arrow-left"></i> Back
            </button>
            <button
              className="btn btn-primary"
              onClick={handleSave}
              disabled={saving || !unsavedChanges}
            >
              {saving ? (
                <>
                  <i className="fas fa-spinner fa-spin"></i> Saving...
                </>
              ) : (
                <>
                  <i className="fas fa-save"></i> Save
                </>
              )}
            </button>
          </div>
        </div>
        
        {/* Error Message */}
        {error && <div className="alert alert-danger">{error}</div>}
        
        {/* Loading Spinner */}
        {loading ? (
          <div className="loading-container">
            <div className="spinner"></div>
            <p>Loading file content...</p>
          </div>
        ) : (
          <div className="editor-container">
            <textarea
              className="file-content-editor"
              value={fileContent}
              onChange={handleContentChange}
              spellCheck="false"
              autoFocus
            ></textarea>
          </div>
        )}
      </div>
      
      {/* Status Bar */}
      <div className="editor-status-bar">
        <div className="status-item">
          <i className="fas fa-info-circle"></i>
          {unsavedChanges ? 'Unsaved changes' : 'No changes'}
        </div>
        <div className="status-item">
          <i className="fas fa-text-height"></i>
          {fileContent.split('\n').length} lines
        </div>
        <div className="status-item">
          <i className="fas fa-text-width"></i>
          {fileContent.length} characters
        </div>
      </div>
    </div>
  );
};

export default FileEditor;