import React, { useState } from 'react';
import { Database, TerminalSquare, Activity, Settings2 } from 'lucide-react';
import SqlConsole from './components/SqlConsole';
import TableBrowser from './components/TableBrowser';
import MetricsDashboard from './components/MetricsDashboard';
import axios from 'axios';
import { Button } from "@/components/ui/button";

// Setup globally
axios.defaults.baseURL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:3000';
axios.defaults.timeout = 15000;

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
          <Database size={24} className="text-primary" />
          ArborDB
        </div>
        
        <nav className="flex-1 p-4 flex flex-col gap-2">
          <Button 
            variant={activeView === 'sql-console' ? 'secondary' : 'ghost'} 
            className="w-full justify-start gap-3 text-muted-foreground hover:text-foreground"
            onClick={() => setActiveView('sql-console')}
          >
            <TerminalSquare size={18} /> SQL Console
          </Button>
          <Button 
            variant={activeView === 'table-browser' ? 'secondary' : 'ghost'} 
            className="w-full justify-start gap-3 text-muted-foreground hover:text-foreground"
            onClick={() => setActiveView('table-browser')}
          >
            <Settings2 size={18} /> Table Manager
          </Button>
          <Button 
            variant={activeView === 'metrics' ? 'secondary' : 'ghost'} 
            className="w-full justify-start gap-3 text-muted-foreground hover:text-foreground"
            onClick={() => setActiveView('metrics')}
          >
            <Activity size={18} /> Metrics
          </Button>
        </nav>
      </aside>

      {/* Main Content Area */}
      <main className="main-content">
        {/* Topbar */}
        <header className="topbar">
          <h2 className="text-lg font-semibold">{
            activeView === 'sql-console' ? 'SQL Query Console' : 
            activeView === 'table-browser' ? 'Visual Table Manager' : 
            'Database Metrics'
          }</h2>
          <div className="text-sm text-muted-foreground">
            Connected: <span className="text-primary">localhost:3000</span>
          </div>
        </header>

        {renderView()}
      </main>
    </div>
  );
}

export default App;
