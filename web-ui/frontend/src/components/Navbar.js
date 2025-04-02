import React from 'react';
import { Link, useLocation } from 'react-router-dom';
import './Navbar.css';

const Navbar = () => {
  const location = useLocation();
  
  return (
    <nav className="navbar">
      <div className="navbar-container">
        <Link to="/" className="navbar-logo">
          Paxos <span className="logo-highlight">Consensus</span>
        </Link>
        
        <ul className="nav-menu">
          <li className="nav-item">
            <Link 
              to="/" 
              className={`nav-link ${location.pathname === '/' || location.pathname.startsWith('/explorer') ? 'active' : ''}`}
            >
              <i className="fas fa-folder-open"></i> File Explorer
            </Link>
          </li>
          <li className="nav-item">
            <Link 
              to="/status" 
              className={`nav-link ${location.pathname === '/status' ? 'active' : ''}`}
            >
              <i className="fas fa-chart-line"></i> System Status
            </Link>
          </li>
        </ul>
      </div>
    </nav>
  );
};

export default Navbar;