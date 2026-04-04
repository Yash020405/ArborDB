import React, { useState } from 'react';
import axios from 'axios';
import { Play, Activity } from 'lucide-react';
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Textarea } from "@/components/ui/textarea";

export default function SqlConsole() {
  const [sql, setSql] = useState('SELECT * FROM users;\n');
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
    if (!rows || rows.length === 0) return <div className="p-6 text-muted-foreground text-sm">0 rows returned (Empty Set)</div>;
    const columns = Object.keys(rows[0]);
    
    return (
      <div className="rounded-md border m-4 overflow-hidden">
        <Table>
          <TableHeader className="bg-muted/50">
            <TableRow>
              {columns.map(col => <TableHead key={col} className="font-semibold">{col}</TableHead>)}
            </TableRow>
          </TableHeader>
          <TableBody>
            {rows.map((row, i) => (
              <TableRow key={i}>
                {columns.map(col => (
                  <TableCell key={col} className={`font-mono text-xs ${row[col] === null ? 'text-muted-foreground' : ''}`}>
                    {row[col] === null ? 'NULL' : String(row[col])}
                  </TableCell>
                ))}
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </div>
    );
  };

  return (
    <div className="console-layout">
      {/* Editor Pane */}
      <Card className="flex flex-col min-h-[300px]">
        <CardHeader className="flex flex-row items-center justify-between p-4 bg-muted/20 border-b space-y-0">
          <CardTitle className="text-sm font-medium text-muted-foreground">Query Editor</CardTitle>
          <Button size="sm" onClick={handleRun} disabled={loading} className="gap-2">
            <Play size={14} /> Run Query <span className="opacity-70 text-xs ml-1">(Ctrl+Enter)</span>
          </Button>
        </CardHeader>
        <CardContent className="flex-1 p-0">
          <Textarea 
            className="w-full h-full border-none shadow-none rounded-none focus-visible:ring-0 p-6 font-mono text-sm resize-none bg-transparent"
            value={sql}
            onChange={(e) => setSql(e.target.value)}
            onKeyDown={handleKeyDown}
            spellCheck={false}
            placeholder="Enter SQL Query Here..."
          />
        </CardContent>
      </Card>

      {/* Results Pane */}
      <Card className="flex-1 flex flex-col overflow-hidden">
        <CardHeader className="flex flex-row items-center justify-between p-4 bg-muted/20 border-b space-y-0">
          <CardTitle className="text-sm font-medium text-muted-foreground">Results</CardTitle>
          {result && result.metrics && (
            <div className="text-xs text-muted-foreground flex gap-4">
              <span className="flex items-center gap-1"><Activity size={12}/> {result.metrics.totalTimeMs}ms</span>
              <span>Rows: {result.result?.rowCount || 0}</span>
            </div>
          )}
        </CardHeader>
        
        <CardContent className="flex-1 p-0 overflow-auto">
          {loading && <div className="p-6 text-primary flex items-center gap-2"><div className="w-2 h-2 rounded-full bg-primary animate-ping"></div> Executing query...</div>}
          
          {error && (
            <div className="p-6 text-destructive font-mono text-sm">
              [ERROR] {error}
            </div>
          )}

          {!loading && !error && result && renderTable(result.result?.rows)}
          
          {!loading && !error && !result && (
            <div className="p-6 text-muted-foreground text-sm flex h-full items-center justify-center">
              Run a query to see results here
            </div>
          )}
        </CardContent>
        
        {!loading && !error && result && result.optimization && (
          <div className="px-5 py-3 bg-muted/30 border-t text-xs text-muted-foreground flex items-center gap-3">
             <strong className="text-primary font-medium">Optimizer:</strong> 
             <span>{result.optimization.strategy}</span>
             <span className="opacity-70">({result.optimization.reason})</span>
          </div>
        )}
      </Card>
    </div>
  );
}
