import React, { useState } from 'react';
import { Database, TerminalSquare, Activity, Settings2 } from 'lucide-react';
import SqlConsole from './components/SqlConsole';
import TableBrowser from './components/TableBrowser';
import MetricsDashboard from './components/MetricsDashboard';
import axios from 'axios';

// Setup globally
axios.defaults.baseURL = 'http://localhost:3000';

function App() {
  const [activeView, setActiveView] = useState('sql-console');

  const renderView = () => {
    switch (activeView) {
      case 'sql-console': return <SqlConsole />;
      case 'table-browser': return <TableBrowser />;
      case 'metrics': return <MetricsDashboard />;
      default: return <SqlConsole />;
    }
  };

  return (
    <div className="app-layout">
      {/* Sidebar */}
      <aside className="sidebar">
        <div className="sidebar-header">
          <Database size={24} color="var(--accent-base)" />
          ArborDB
        </div>
        
        <nav className="nav-menu">
          <button 
            className={`nav-item ${activeView === 'sql-console' ? 'active' : ''}`}
            onClick={() => setActiveView('sql-console')}
          >
            <TerminalSquare /> SQL Console
          </button>
          <button 
            className={`nav-item ${activeView === 'table-browser' ? 'active' : ''}`}
            onClick={() => setActiveView('table-browser')}
          >
            <Settings2 /> Table Builder
          </button>
          <button 
            className={`nav-item ${activeView === 'metrics' ? 'active' : ''}`}
            onClick={() => setActiveView('metrics')}
          >
            <Activity /> Metrics
          </button>
        </nav>
      </aside>

      {/* Main Content Area */}
      <main className="main-content">
        {/* Topbar */}
        <header className="topbar">
          <h2>{
            activeView === 'sql-console' ? 'SQL Query Console' : 
            activeView === 'table-browser' ? 'Visual Table Manager' : 
            'Database Metrics'
          }</h2>
          <div className="text-muted" style={{fontSize: '0.9rem'}}>
            Connected: <span style={{color: 'var(--accent-base)'}}>localhost:3000</span>
          </div>
        </header>

        {renderView()}
      </main>
    </div>
  );
}

export default App;
