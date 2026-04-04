import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { Database, RefreshCw, Plus, Upload } from 'lucide-react';
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";

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
    <div className="p-8">
      <div className="flex justify-between items-center mb-8">
        <h2 className="text-2xl font-bold tracking-tight">Table Schema Manager</h2>
        <div className="flex gap-3">
          <Button variant="outline" onClick={fetchTables} className="gap-2">
            <RefreshCw size={14} /> Refresh
          </Button>
          <Button className="gap-2">
            <Plus size={14} /> New Table
          </Button>
          <Button variant="outline" className="border-primary text-primary hover:bg-primary/10 gap-2">
            <Upload size={14} /> Upload CSV
          </Button>
        </div>
      </div>

      {loading ? (
        <div className="text-muted-foreground flex items-center gap-2">
           <div className="w-2 h-2 rounded-full bg-primary animate-ping"></div> Loading tables...
        </div>
      ) : tables.length === 0 ? (
        <Card className="p-16 flex flex-col items-center justify-center text-center bg-muted/20 border-dashed">
          <Database size={48} className="text-muted-foreground/50 mb-4" />
          <h3 className="text-lg font-semibold mb-2">No Tables Found</h3>
          <p className="text-sm text-muted-foreground mb-6">Create a table using SQL or use the New Table button.</p>
          <Button>Create your first table</Button>
        </Card>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-6">
          {tables.map(table => (
            <Card key={table.name} className="transition-all hover:border-primary/50 hover:shadow-md">
              <CardHeader className="flex flex-row items-start gap-4 pb-4">
                <div className="p-2.5 rounded-lg bg-primary/10 text-primary">
                  <Database size={22} />
                </div>
                <div>
                  <CardTitle className="text-lg">{table.name}</CardTitle>
                  <div className="text-sm text-muted-foreground mt-1 font-medium">{table.rowCount} rows</div>
                </div>
              </CardHeader>
              <CardContent>
                <div className="bg-muted/30 p-4 rounded-md border text-sm">
                  <div className="text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-3">Schema</div>
                  <div className="space-y-2.5">
                    {Object.entries(table.schema).map(([col, type]) => (
                      <div key={col} className="flex justify-between items-center bg-background/50 p-2 rounded border border-transparent hover:border-border">
                        <span className="font-mono text-sm">{col}</span>
                        <Badge variant="outline" className="text-primary border-primary/20 bg-primary/5 font-mono text-[10px] uppercase font-semibold">
                          {type}
                        </Badge>
                      </div>
                    ))}
                  </div>
                </div>
              </CardContent>
            </Card>
          ))}
        </div>
      )}
    </div>
  );
}
