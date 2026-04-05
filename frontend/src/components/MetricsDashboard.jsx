import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { Activity, Clock, Database, AlertCircle } from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Badge } from "@/components/ui/badge";

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

  if (loading) return <div className="p-8 text-muted-foreground flex items-center gap-2"><div className="w-2 h-2 rounded-full bg-primary animate-ping"></div> Loading metrics...</div>;
  if (!metrics) return <div className="p-8 text-destructive font-medium">Failed to load metrics.</div>;

  const { counters, performance, recentQueries } = metrics.metrics;

  return (
    <div className="p-8">
      
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-8">
        <Card>
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground">Total Queries</CardTitle>
            <Database size={16} className="text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-3xl font-bold">{counters.totalQueries}</div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground">Avg Latency</CardTitle>
            <Activity size={16} className="text-primary" />
          </CardHeader>
          <CardContent>
            <div className="text-3xl font-bold text-primary">{performance.avgExecutionTimeMs} <span className="text-base font-medium text-muted-foreground">ms</span></div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground">P95 Latency</CardTitle>
            <Clock size={16} className="text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-3xl font-bold">{performance.p95ExecutionTimeMs} <span className="text-base font-medium text-muted-foreground">ms</span></div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground">Errors</CardTitle>
            <AlertCircle size={16} className={counters.totalErrors > 0 ? "text-destructive" : "text-muted-foreground"} />
          </CardHeader>
          <CardContent>
            <div className={`text-3xl font-bold ${counters.totalErrors > 0 ? 'text-destructive' : ''}`}>
              {counters.totalErrors}
            </div>
            {counters.totalErrors === 0 && (
              <p className="text-xs text-muted-foreground mt-1">System is healthy</p>
            )}
          </CardContent>
        </Card>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Recent Query Log</CardTitle>
        </CardHeader>
        <CardContent className="p-0">
          <Table>
            <TableHeader className="bg-muted/30">
              <TableRow>
                <TableHead className="w-[150px]">Time</TableHead>
                <TableHead className="w-[120px]">Type</TableHead>
                <TableHead>SQL</TableHead>
                <TableHead className="w-[100px] text-right">Latency</TableHead>
                <TableHead className="w-[100px] text-center">Status</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {recentQueries && recentQueries.map((q, i) => (
                <TableRow key={i}>
                  <TableCell className="text-muted-foreground text-xs">{new Date(q.timestamp).toLocaleTimeString()}</TableCell>
                  <TableCell>
                    <Badge variant="outline" className="font-mono text-[10px] bg-muted/20">
                      {q.type}
                    </Badge>
                  </TableCell>
                  <TableCell className="font-mono text-xs opacity-90 max-w-sm truncate" title={q.sql || q.filename || ''}>
                    {q.sql || q.filename || '-'}
                  </TableCell>
                  <TableCell className="text-right text-primary font-mono text-xs font-semibold">{q.executionTimeMs ?? '-'}{q.executionTimeMs !== undefined ? 'ms' : ''}</TableCell>
                  <TableCell className="text-center">
                    {q.status === 'success' ? (
                      <Badge variant="outline" className="bg-emerald-500/10 text-emerald-500 border-none px-2 shadow-none">OK</Badge>
                    ) : (
                      <Badge variant="outline" className="bg-destructive/10 text-destructive border-none px-2 shadow-none">FAIL</Badge>
                    )}
                  </TableCell>
                </TableRow>
              ))}
              {(!recentQueries || recentQueries.length === 0) && (
                <TableRow>
                  <TableCell colSpan={5} className="h-24 text-center text-muted-foreground">
                    No recent queries logged.
                  </TableCell>
                </TableRow>
              )}
            </TableBody>
          </Table>
        </CardContent>
      </Card>
    </div>
  );
}
