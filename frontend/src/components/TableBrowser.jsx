import React, { useState, useEffect, useCallback } from 'react';
import axios from 'axios';
import { Database, RefreshCw, Plus, Upload, X, Search, FileText, Table as TableIcon } from 'lucide-react';
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";

const Modal = ({ isOpen, onClose, title, children, maxWidth = "max-w-2xl" }) => {
  if (!isOpen) return null;
  return (
    <div className="fixed inset-0 bg-black/60 z-50 flex items-center justify-center p-4 backdrop-blur-sm" onClick={onClose}>
      <div className={`bg-background border shadow-2xl rounded-xl w-full ${maxWidth} max-h-[90vh] flex flex-col`} onClick={e => e.stopPropagation()}>
        <div className="px-6 py-4 border-b flex justify-between items-center bg-muted/20 rounded-t-xl">
          <h3 className="font-semibold text-lg flex items-center gap-2">{title}</h3>
          <Button variant="ghost" size="icon" className="h-8 w-8 text-muted-foreground hover:bg-muted" onClick={onClose}>
            <X size={16} />
          </Button>
        </div>
        <div className="p-6 overflow-y-auto flex-1">
          {children}
        </div>
      </div>
    </div>
  );
};

export default function TableBrowser() {
  const [tables, setTables] = useState([]);
  const [loading, setLoading] = useState(true);
  
  // Modal states
  const [activeModal, setActiveModal] = useState(null);
  const [errorObj, setErrorObj] = useState(null);
  const [successMsg, setSuccessMsg] = useState(null);

  // New Table State
  const [tableName, setTableName] = useState('');
  const [columns, setColumns] = useState([{ name: 'id', type: 'INT', isPrimary: true }]);

  // Upload State
  const [uploadFile, setUploadFile] = useState(null);
  const [uploadTable, setUploadTable] = useState('');

  // Data Viewer State
  const [selectedTable, setSelectedTable] = useState(null);
  const [tableData, setTableData] = useState([]);
  const [dataLoading, setDataLoading] = useState(false);
  const [filterCol, setFilterCol] = useState('');
  const [filterOp, setFilterOp] = useState('=');
  const [filterVal, setFilterVal] = useState('');
  const [filterStart, setFilterStart] = useState('');
  const [filterEnd, setFilterEnd] = useState('');

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

  const closeModal = () => {
    setActiveModal(null);
    setErrorObj(null);
    setSuccessMsg(null);
  };

  const handleAlert = (err) => {
    if (err?.response?.data?.error) {
      setErrorObj(err.response.data.error.message);
    } else {
      setErrorObj(err.message || String(err));
    }
  };

  const formatSqlValue = (rawValue) => {
    const value = String(rawValue ?? '').trim();
    if (value === '') return null;

    const num = Number(value);
    if (!Number.isNaN(num)) {
      return value;
    }

    // Escape single quotes for SQL literals
    return `'${value.replace(/'/g, "\\'")}'`;
  };

  // --- Create Table Logic ---
  const handleAddColumn = () => {
    setColumns([...columns, { name: '', type: 'STRING', isPrimary: false }]);
  };

  const handleRemoveColumn = (index) => {
    setColumns(columns.filter((_, i) => i !== index));
  };

  const handleColumnChange = (index, field, val) => {
    const newCols = [...columns];
    newCols[index][field] = val;
    if (field === 'isPrimary' && val === true) {
      // uncheck other defaults
      newCols.forEach((c, i) => { if (i !== index) c.isPrimary = false; });
    }
    setColumns(newCols);
  };

  const submitCreateTable = async () => {
    setErrorObj(null);
    if (!tableName.trim()) return setErrorObj("Table name is required.");
    if (columns.length === 0) return setErrorObj("At least one column is required.");
    
    // Build AST query string manually
    const colDefs = columns.map(c => {
      let def = `${c.name} ${c.type}`;
      if (c.isPrimary) def += ' PRIMARY KEY';
      return def;
    }).join(', ');

    const sql = `CREATE TABLE ${tableName.trim()} (${colDefs});`;
    
    try {
      await axios.post('/query', { sql });
      fetchTables();
      closeModal();
    } catch (err) {
      handleAlert(err);
    }
  };

  // --- Upload CSV Logic ---
  const submitUpload = async () => {
    setErrorObj(null);
    setSuccessMsg(null);
    if (!uploadTable) return setErrorObj("Please select a target table.");
    if (!uploadFile) return setErrorObj("Please select a file.");

    const formData = new FormData();
    formData.append('file', uploadFile);
    formData.append('table', uploadTable);

    try {
      const res = await axios.post('/upload', formData, {
        headers: { 'Content-Type': 'multipart/form-data' }
      });
      setSuccessMsg(`Successfully inserted ${res.data.insertion.insertedCount} rows.`);
      setTimeout(() => { fetchTables(); closeModal(); }, 2000);
    } catch (err) {
      handleAlert(err);
    }
  };

  // --- View Data Logic ---
  const fetchTableData = useCallback(async () => {
    if (!selectedTable) return;
    setDataLoading(true);
    setErrorObj(null);
    
    let sql = `SELECT * FROM ${selectedTable.name}`;
    if (filterCol) {
      if (filterOp === 'BETWEEN') {
        if (!filterStart.trim() || !filterEnd.trim()) {
          setErrorObj('BETWEEN filter requires both start and end values.');
          setDataLoading(false);
          return;
        }

        const startValue = formatSqlValue(filterStart);
        const endValue = formatSqlValue(filterEnd);
        sql += ` WHERE ${filterCol} BETWEEN ${startValue} AND ${endValue}`;
      } else if (filterVal.trim()) {
        const formattedVal = formatSqlValue(filterVal);
        sql += ` WHERE ${filterCol} ${filterOp} ${formattedVal}`;
      }
    }
    sql += ';';

    try {
      const res = await axios.post('/query', { sql });
      setTableData(res.data.result.rows || []);
    } catch (err) {
      handleAlert(err);
      setTableData([]);
    } finally {
      setDataLoading(false);
    }
  }, [selectedTable, filterCol, filterOp, filterVal, filterStart, filterEnd]);

  useEffect(() => {
    if (activeModal === 'data' && selectedTable) {
      fetchTableData();
    }
  }, [activeModal, selectedTable, fetchTableData]);

  useEffect(() => {
    if (filterOp !== 'BETWEEN') {
      setFilterStart('');
      setFilterEnd('');
    }
  }, [filterOp]);

  return (
    <div className="p-8">
      <div className="flex justify-between items-center mb-8">
        <h2 className="text-2xl font-bold tracking-tight">Table Schema Manager</h2>
        <div className="flex gap-3">
          <Button variant="outline" onClick={fetchTables} className="gap-2">
            <RefreshCw size={14} className={loading ? "animate-spin" : ""} /> Refresh
          </Button>
          <Button className="gap-2" onClick={() => { setTableName(''); setColumns([{ name: 'id', type: 'INT', isPrimary: true }]); setActiveModal('create'); }}>
            <Plus size={14} /> New Table
          </Button>
          <Button 
            variant="outline" 
            className="border-emerald-500 text-emerald-500 hover:bg-emerald-500/10 gap-2"
            onClick={() => { setUploadTable(tables[0]?.name || ''); setUploadFile(null); setActiveModal('upload'); }}
            disabled={tables.length === 0}
          >
            <Upload size={14} /> Upload CSV
          </Button>
        </div>
      </div>

      {loading && tables.length === 0 ? (
        <div className="text-muted-foreground flex items-center gap-2">
           <div className="w-2 h-2 rounded-full bg-primary animate-ping"></div> Loading tables...
        </div>
      ) : tables.length === 0 ? (
        <Card className="p-16 flex flex-col items-center justify-center text-center bg-muted/20 border-dashed">
          <Database size={48} className="text-muted-foreground/50 mb-4" />
          <h3 className="text-lg font-semibold mb-2">No Tables Found</h3>
          <p className="text-sm text-muted-foreground mb-6">Create a table using SQL or use the New Table button.</p>
          <Button onClick={() => setActiveModal('create')}>Create your first table</Button>
        </Card>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-6">
          {tables.map(table => (
            <Card key={table.name} className="transition-all hover:border-primary/50 hover:shadow-md cursor-pointer group flex flex-col" onClick={() => { setSelectedTable(table); setFilterCol(''); setFilterOp('='); setFilterVal(''); setFilterStart(''); setFilterEnd(''); setActiveModal('data'); }}>
              <div className="p-4 border-b flex items-start gap-4 bg-muted/10 group-hover:bg-primary/5 transition-colors">
                <div className="p-2.5 rounded-lg bg-primary/10 text-primary">
                  <TableIcon size={22} />
                </div>
                <div className="flex-1">
                  <CardTitle className="text-lg group-hover:text-primary transition-colors">{table.name}</CardTitle>
                  <div className="text-sm text-muted-foreground mt-1 font-medium">{table.rowCount} rows</div>
                </div>
              </div>
              <div className="p-4 bg-background flex-1 text-sm">
                <div className="space-y-2.5">
                  {Object.entries(table.schema).map(([col, type]) => (
                    <div key={col} className="flex justify-between items-center rounded border border-transparent">
                      <span className="font-mono text-xs opacity-80">{col}</span>
                      <span className="text-[10px] uppercase font-bold text-muted-foreground">{type}</span>
                    </div>
                  ))}
                </div>
              </div>
              <div className="bg-primary/5 p-2 text-center text-xs font-semibold text-primary opacity-0 group-hover:opacity-100 transition-opacity">
                Click to View Data &rarr;
              </div>
            </Card>
          ))}
        </div>
      )}

      {/* CREATE TABLE MODAL */}
      <Modal isOpen={activeModal === 'create'} onClose={closeModal} title={<><Plus size={18}/> Create New Table</>}>
        <div className="space-y-6">
          {errorObj && <div className="p-3 bg-destructive/10 text-destructive text-sm rounded-md font-medium border border-destructive/20">{errorObj}</div>}
          
          <div className="space-y-2">
            <label className="text-sm font-semibold">Table Name</label>
            <Input placeholder="e.g., users, orders" value={tableName} onChange={e => setTableName(e.target.value)} />
          </div>

          <div className="space-y-3">
            <div className="flex justify-between items-center">
              <label className="text-sm font-semibold">Columns Schema</label>
              <Button size="sm" variant="outline" onClick={handleAddColumn} className="h-7 text-xs">Add Column</Button>
            </div>
            
            <div className="space-y-2">
              {columns.map((col, idx) => (
                <div key={idx} className="flex items-center gap-2 bg-muted/30 p-2 rounded-md border">
                  <Input className="h-8 flex-1" placeholder="col_name" value={col.name} onChange={e => handleColumnChange(idx, 'name', e.target.value)} />
                  <select className="h-8 rounded-md border bg-background px-3 text-sm flex-1" value={col.type} onChange={e => handleColumnChange(idx, 'type', e.target.value)}>
                    <option value="INT">INT</option>
                    <option value="STRING">STRING</option>
                    <option value="FLOAT">FLOAT</option>
                    <option value="BOOLEAN">BOOLEAN</option>
                  </select>
                  <label className="flex items-center gap-2 text-xs flex-1 cursor-pointer">
                    <input type="checkbox" checked={col.isPrimary} onChange={e => handleColumnChange(idx, 'isPrimary', e.target.checked)} className="rounded text-primary focus:ring-primary" />
                    Primary Key
                  </label>
                  <Button variant="ghost" size="icon" className="h-8 w-8 text-destructive hover:text-destructive hover:bg-destructive/10 shrink-0" onClick={() => handleRemoveColumn(idx)}>
                    <X size={14} />
                  </Button>
                </div>
              ))}
            </div>
          </div>

          <div className="pt-4 border-t flex justify-end gap-2">
            <Button variant="outline" onClick={closeModal}>Cancel</Button>
            <Button onClick={submitCreateTable}>Create Table</Button>
          </div>
        </div>
      </Modal>

      {/* UPLOAD CSV MODAL */}
      <Modal isOpen={activeModal === 'upload'} onClose={closeModal} title={<><Upload size={18}/> Upload Data File</>}>
         <div className="space-y-6">
          {errorObj && <div className="p-3 bg-destructive/10 text-destructive text-sm rounded-md font-medium border border-destructive/20">{errorObj}</div>}
          {successMsg && <div className="p-3 bg-emerald-500/10 text-emerald-500 text-sm rounded-md font-medium border border-emerald-500/20">{successMsg}</div>}
          
          <div className="space-y-2">
            <label className="text-sm font-semibold">Target Table</label>
            <select className="w-full h-10 rounded-md border bg-background px-3 text-sm" value={uploadTable} onChange={e => setUploadTable(e.target.value)}>
               {tables.map(t => <option key={t.name} value={t.name}>{t.name}</option>)}
            </select>
          </div>

          <div className="space-y-2">
            <label className="text-sm font-semibold">File (.csv, .xlsx)</label>
            <div className="border-2 border-dashed rounded-lg p-8 flex flex-col items-center justify-center text-center hover:bg-muted/30 transition-colors cursor-pointer relative">
               <FileText size={32} className="text-muted-foreground mb-3 opacity-50" />
               <div className="text-sm font-medium">Browse Files</div>
               <div className="text-xs text-muted-foreground mt-1">
                 {uploadFile ? <strong className="text-primary">{uploadFile.name}</strong> : "Click to select"}
               </div>
               <input type="file" className="absolute inset-0 opacity-0 cursor-pointer" accept=".csv,.xlsx,.xls" onChange={e => setUploadFile(e.target.files[0])} />
            </div>
          </div>

          <div className="pt-4 border-t flex justify-end gap-2">
            <Button variant="outline" onClick={closeModal} disabled={!!successMsg}>Cancel</Button>
            <Button onClick={submitUpload} disabled={!!successMsg}>Upload & Ingest</Button>
          </div>
        </div>
      </Modal>

      {/* VIEW DATA / MAP MODAL */}
      <Modal isOpen={activeModal === 'data'} onClose={closeModal} title={<><Database size={18}/> {selectedTable?.name}</>} maxWidth="max-w-5xl">
         <div className="flex flex-col h-[60vh]">
           {/* Filters Bar */}
           <div className="flex items-end gap-3 p-4 bg-muted/20 border rounded-t-lg mb-0">
             <div className="flex-1 space-y-1">
               <label className="text-xs font-semibold uppercase text-muted-foreground">Filter Column</label>
               <select className="w-full h-9 rounded-md border bg-background px-3 text-sm" value={filterCol} onChange={e => setFilterCol(e.target.value)}>
                 <option value="">-- Select Column --</option>
                 {selectedTable && Object.keys(selectedTable.schema).map(c => <option key={c} value={c}>{c}</option>)}
               </select>
             </div>
             
             <div className="w-[120px] space-y-1">
               <label className="text-xs font-semibold uppercase text-muted-foreground">Operator</label>
               <select className="w-full h-9 rounded-md border bg-background px-3 text-sm font-mono" value={filterOp} onChange={e => setFilterOp(e.target.value)}>
                 <option value="=">=</option>
                 <option value="BETWEEN">BETWEEN</option>
               </select>
             </div>

             {filterOp === 'BETWEEN' ? (
               <>
                 <div className="flex-1 space-y-1">
                   <label className="text-xs font-semibold uppercase text-muted-foreground">Start</label>
                   <Input className="h-9 font-mono" placeholder="Start value" value={filterStart} onChange={e => setFilterStart(e.target.value)} onKeyDown={e => e.key === 'Enter' && fetchTableData()} />
                 </div>
                 <div className="flex-1 space-y-1">
                   <label className="text-xs font-semibold uppercase text-muted-foreground">End</label>
                   <Input className="h-9 font-mono" placeholder="End value" value={filterEnd} onChange={e => setFilterEnd(e.target.value)} onKeyDown={e => e.key === 'Enter' && fetchTableData()} />
                 </div>
               </>
             ) : (
               <div className="flex-[2] space-y-1">
                 <label className="text-xs font-semibold uppercase text-muted-foreground">Value</label>
                 <Input className="h-9 font-mono" placeholder="Search value..." value={filterVal} onChange={e => setFilterVal(e.target.value)} onKeyDown={e => e.key === 'Enter' && fetchTableData()} />
               </div>
             )}

             <Button onClick={fetchTableData} disabled={dataLoading} className="gap-2 h-9 px-6">
                <Search size={14} /> Run Filter
             </Button>
           </div>
           
           {errorObj && <div className="p-3 bg-destructive/10 text-destructive text-sm font-medium border-x border-b border-destructive/20">{errorObj}</div>}

           {/* Data Grid */}
           <div className="flex-1 border rounded-b-lg overflow-auto relative">
             {dataLoading && (
                <div className="absolute inset-0 bg-background/50 flex items-center justify-center backdrop-blur-[1px] z-10">
                  <div className="px-4 py-2 bg-background border shadow-lg rounded-full text-sm font-mono flex items-center gap-2">
                     <RefreshCw size={14} className="animate-spin text-primary" /> Fetching blocks...
                  </div>
                </div>
             )}
             
             <Table>
                <TableHeader className="bg-muted/50 sticky top-0 z-0">
                  <TableRow>
                     {selectedTable && Object.keys(selectedTable.schema).map(col => (
                        <TableHead key={col} className="font-semibold text-xs whitespace-nowrap">
                           {col} <span className="opacity-50 font-normal ml-1">({selectedTable.schema[col]})</span>
                        </TableHead>
                     ))}
                  </TableRow>
                </TableHeader>
                <TableBody>
                   {tableData.length === 0 && !dataLoading ? (
                     <TableRow>
                        <TableCell colSpan={selectedTable ? Object.keys(selectedTable.schema).length : 1} className="h-32 text-center text-muted-foreground">
                           No data found matching criteria.
                        </TableCell>
                     </TableRow>
                   ) : (
                     tableData.map((row, i) => (
                        <TableRow key={i}>
                           {selectedTable && Object.keys(selectedTable.schema).map(col => (
                              <TableCell key={col} className={`py-2 text-sm max-w-[250px] truncate ${row[col] === null ? 'text-muted-foreground italic' : ''}`}>
                                 {row[col] === null ? 'NULL' : String(row[col])}
                              </TableCell>
                           ))}
                        </TableRow>
                     ))
                   )}
                </TableBody>
             </Table>
           </div>
           
           <div className="pt-2 text-xs text-muted-foreground text-right font-mono">
              Showing {tableData.length} object(s)
           </div>
         </div>
      </Modal>
    </div>
  );
}
