import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { Activity, Clock, Database, AlertCircle } from 'lucide-react';

export default function MetricsDashboard() {
  const [metrics, setMetrics] = useState(null);
  const [loading, setLoading] = useState(true);

  const fetchMetrics = async () => {
    try {
      const res = await axios.get('/metrics');
      setMetrics(res.data);
    } catch (err) {
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchMetrics();
    const interval = setInterval(fetchMetrics, 5000);
    return () => clearInterval(interval);
  }, []);

  if (loading) return <div style={{ padding: '32px', color: 'var(--text-muted)' }}>Loading metrics...</div>;
  if (!metrics) return <div style={{ padding: '32px', color: 'var(--danger-base)' }}>Failed to load metrics.</div>;

  const { counters, performance, recentQueries } = metrics.metrics;

  return (
    <div style={{ padding: '32px' }}>
      
      <div className="metrics-grid" style={{ padding: 0, marginBottom: '32px' }}>
        <div className="metric-card">
          <div className="metric-title"><Database size={16} style={{display: 'inline', marginRight: '8px'}} /> Total Queries</div>
          <div className="metric-value">{counters.totalQueries}</div>
        </div>
        <div className="metric-card">
          <div className="metric-title"><Activity size={16} style={{display: 'inline', marginRight: '8px'}} /> Avg Latency</div>
          <div className="metric-value highlight">{performance.avgExecutionTimeMs} <span style={{fontSize:'1rem', color:'var(--text-muted)'}}>ms</span></div>
        </div>
        <div className="metric-card">
          <div className="metric-title"><Clock size={16} style={{display: 'inline', marginRight: '8px'}} /> P95 Latency</div>
          <div className="metric-value">{performance.p95ExecutionTimeMs} <span style={{fontSize:'1rem', color:'var(--text-muted)'}}>ms</span></div>
        </div>
        <div className="metric-card">
          <div className="metric-title"><AlertCircle size={16} style={{display: 'inline', marginRight: '8px'}} /> Errors</div>
          <div className="metric-value" style={{ color: counters.totalErrors > 0 ? 'var(--danger-base)' : 'inherit' }}>
            {counters.totalErrors}
          </div>
        </div>
      </div>

      <div style={{ backgroundColor: 'var(--bg-surface)', border: '1px solid var(--border-subtle)', borderRadius: 'var(--radius-lg)', overflow: 'hidden' }}>
        <div style={{ padding: '20px', borderBottom: '1px solid var(--border-subtle)', fontWeight: 600 }}>
          Recent Query Log
        </div>
        <div className="table-container" style={{ border: 'none', borderRadius: 0 }}>
          <table className="data-table">
            <thead>
              <tr>
                <th>Time</th>
                <th>Type</th>
                <th>SQL</th>
                <th>Latency</th>
                <th>Status</th>
              </tr>
            </thead>
            <tbody>
              {recentQueries && recentQueries.map((q, i) => (
                <tr key={i}>
                  <td style={{ color: 'var(--text-muted)' }}>{new Date(q.timestamp).toLocaleTimeString()}</td>
                  <td><span style={{ backgroundColor: 'var(--bg-surface-elevated)', padding: '4px 8px', borderRadius: '4px', fontSize: '0.8rem' }}>{q.type}</span></td>
                  <td>{q.sql && q.sql.length > 50 ? q.sql.substring(0, 50) + '...' : q.sql}</td>
                  <td style={{ color: 'var(--accent-base)' }}>{q.executionTimeMs}ms</td>
                  <td>{q.status === 'success' ? '✅' : '❌'}</td>
                </tr>
              ))}
              {(!recentQueries || recentQueries.length === 0) && (
                <tr>
                  <td colSpan={5} style={{ textAlign: 'center', padding: '24px', color: 'var(--text-muted)' }}>No recent queries.</td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
