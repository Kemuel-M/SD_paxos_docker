import React from 'react';
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom';
import { ToastContainer } from 'react-toastify';
import 'react-toastify/dist/ReactToastify.css';

import Navbar from './components/Navbar';
import FileExplorer from './components/FileExplorer';
import FileEditor from './components/FileEditor';
import SystemStatus from './components/SystemStatus';
import './App.css';

function App() {
  return (
    <Router>
      <div className="App">
        <Navbar />
        <div className="container">
          <Routes>
            <Route path="/" element={<FileExplorer />} />
            <Route path="/explorer/*" element={<FileExplorer />} />
            <Route path="/edit/*" element={<FileEditor />} />
            <Route path="/status" element={<SystemStatus />} />
          </Routes>
        </div>
        <ToastContainer position="bottom-right" />
      </div>
    </Router>
  );
}

export default App;