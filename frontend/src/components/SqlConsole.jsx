import React, { useState } from 'react';
import axios from 'axios';
import { Play, Activity } from 'lucide-react';

export default function SqlConsole() {
  const [sql, setSql] = useState('SELECT * FROM users;');
  const [result, setResult] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const handleRun = async () => {
    if (!sql.trim()) return;
    setLoading(true);
    setError(null);
    setResult(null);

    try {
      const res = await axios.post('/query', { sql });
      setResult(res.data);
    } catch (err) {
      if (err.response && err.response.data && err.response.data.error) {
        setError(err.response.data.error.message);
      } else {
        setError(err.message);
      }
    } finally {
      setLoading(false);
    }
  };

  const handleKeyDown = (e) => {
    if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
      handleRun();
    }
  };

  const renderTable = (rows) => {
    if (!rows || rows.length === 0) return <div style={{padding: '24px', color: 'var(--text-muted)'}}>0 rows returned (Empty Set)</div>;
    const columns = Object.keys(rows[0]);
    
    return (
      <table className="data-table">
        <thead>
          <tr>
            {columns.map(col => <th key={col}>{col}</th>)}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, i) => (
            <tr key={i}>
              {columns.map(col => (
                <td key={col} style={{ color: row[col] === null ? 'var(--text-muted)' : 'inherit' }}>
                  {row[col] === null ? 'NULL' : String(row[col])}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    );
  };

  return (
    <div className="console-layout">
      {/* Editor Pane */}
      <div className="editor-card">
        <div className="editor-header">
          <div style={{ color: 'var(--text-secondary)', fontSize: '0.9rem', fontWeight: 500 }}>
            Query Editor
          </div>
          <button className="btn btn-primary" onClick={handleRun} disabled={loading}>
            <Play size={16} /> Run Query (Ctrl+Enter)
          </button>
        </div>
        <textarea 
          className="sql-textarea"
          value={sql}
          onChange={(e) => setSql(e.target.value)}
          onKeyDown={handleKeyDown}
          spellCheck={false}
          placeholder="Enter SQL Query Here..."
        />
      </div>

      {/* Results Pane */}
      <div className="editor-card" style={{ flex: 1 }}>
        <div className="editor-header">
          <div style={{ color: 'var(--text-secondary)', fontSize: '0.9rem', fontWeight: 500 }}>
            Results
          </div>
          {result && result.metrics && (
            <div style={{ fontSize: '0.85rem', color: 'var(--text-muted)', display: 'flex', gap: '16px' }}>
              <span><Activity size={14} style={{display: 'inline', verticalAlign: '-2px'}}/> {result.metrics.totalTimeMs}ms</span>
              <span>Rows: {result.result?.rowCount || 0}</span>
            </div>
          )}
        </div>
        
        <div className="table-container" style={{ borderRadius: 0, border: 'none', borderTop: 'none', height: '100%', overflow: 'auto' }}>
          {loading && <div style={{padding: '24px', color: 'var(--accent-base)'}}>Executing query...</div>}
          
          {error && (
            <div style={{padding: '24px', color: 'var(--danger-base)', fontFamily: 'var(--font-mono)'}}>
              [ERROR] {error}
            </div>
          )}

          {!loading && !error && result && renderTable(result.result?.rows)}
          
          {!loading && !error && !result && (
            <div style={{padding: '24px', color: 'var(--text-muted)'}}>
              Run a query to see results here
            </div>
          )}
        </div>
        
        {!loading && !error && result && result.optimization && (
          <div style={{ padding: '8px 20px', backgroundColor: 'var(--bg-surface-elevated)', borderTop: '1px solid var(--border-subtle)', fontSize: '0.8rem', color: 'var(--text-muted)', display: 'flex', gap: '12px' }}>
             <strong style={{color: 'var(--accent-base)'}}>Optimizer:</strong> 
             <span>{result.optimization.strategy}</span>
             <span>({result.optimization.reason})</span>
          </div>
        )}
      </div>
    </div>
  );
}
