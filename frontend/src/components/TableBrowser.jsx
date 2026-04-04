import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { Database, RefreshCw, Plus, Upload } from 'lucide-react';

export default function TableBrowser() {
  const [tables, setTables] = useState([]);
  const [loading, setLoading] = useState(true);

  const fetchTables = async () => {
    setLoading(true);
    try {
      const res = await axios.get('/tables');
      setTables(res.data.tables || []);
    } catch (err) {
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchTables();
  }, []);

  return (
    <div style={{ padding: '32px' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '32px' }}>
        <h2 style={{ fontSize: '1.5rem', fontWeight: 600 }}>Table Schema Manager</h2>
        <div style={{ display: 'flex', gap: '12px' }}>
          <button className="btn btn-secondary" onClick={fetchTables}>
            <RefreshCw size={16} /> Refresh
          </button>
          <button className="btn btn-primary">
            <Plus size={16} /> New Table
          </button>
          <button className="btn btn-primary" style={{ backgroundColor: 'var(--bg-surface-elevated)', color: 'var(--accent-base)', border: '1px solid var(--accent-base)' }}>
            <Upload size={16} /> Upload CSV
          </button>
        </div>
      </div>

      {loading ? (
        <div style={{ color: 'var(--text-muted)' }}>Loading tables...</div>
      ) : tables.length === 0 ? (
        <div style={{ padding: '48px', textAlign: 'center', backgroundColor: 'var(--bg-surface)', borderRadius: 'var(--radius-lg)' }}>
          <Database size={48} color="var(--border-subtle)" style={{ marginBottom: '16px' }} />
          <h3 style={{ marginBottom: '8px' }}>No Tables Found</h3>
          <p className="text-muted">Create a table using SQL or use the New Table button.</p>
        </div>
      ) : (
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(300px, 1fr))', gap: '24px' }}>
          {tables.map(table => (
            <div key={table.name} className="metric-card" style={{ padding: '20px' }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: '12px', marginBottom: '16px' }}>
                <div style={{ backgroundColor: 'var(--accent-glow)', padding: '8px', borderRadius: '8px' }}>
                  <Database size={20} color="var(--accent-base)" />
                </div>
                <div>
                  <div style={{ fontWeight: 600, fontSize: '1.1rem' }}>{table.name}</div>
                  <div style={{ fontSize: '0.85rem', color: 'var(--text-muted)' }}>{table.rowCount} rows</div>
                </div>
              </div>
              
              <div style={{ backgroundColor: 'var(--bg-base)', padding: '12px', borderRadius: 'var(--radius-md)' }}>
                <div style={{ fontSize: '0.8rem', color: 'var(--text-muted)', marginBottom: '8px', textTransform: 'uppercase', letterSpacing: '0.05em' }}>Schema</div>
                {Object.entries(table.schema).map(([col, type]) => (
                  <div key={col} style={{ display: 'flex', justifyContent: 'space-between', fontSize: '0.9rem', marginBottom: '4px' }}>
                    <span style={{ fontFamily: 'var(--font-mono)' }}>{col}</span>
                    <span style={{ color: 'var(--accent-base)', fontSize: '0.8rem' }}>{type}</span>
                  </div>
                ))}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
