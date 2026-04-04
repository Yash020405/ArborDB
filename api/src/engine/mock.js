'use strict';

// Mock engine — in-memory implementation that simulates B+ Tree behavior
// using Maps and sorted arrays. Used for development without the C++ engine.

const { EngineError } = require('../errors');

const tables = new Map();

async function callEngine(engineJson) {
  const startTime = process.hrtime.bigint();

  try {
    let result;

    switch (engineJson.operation) {
      case 'create_table': result = handleCreateTable(engineJson); break;
      case 'insert':       result = handleInsert(engineJson); break;
      case 'search':       result = handleSearch(engineJson); break;
      case 'range':        result = handleRange(engineJson); break;
      case 'full_scan':    result = handleFullScan(engineJson); break;
      default: throw new EngineError(`Unknown operation: ${engineJson.operation}`);
    }

    const endTime = process.hrtime.bigint();
    const timeMs = Number(endTime - startTime) / 1_000_000;

    return {
      status: 'ok',
      rows: result.rows || [],
      error: null,
      metrics: {
        time_ms: Math.round(timeMs * 100) / 100,
        disk_reads: result.diskReads || 0,
        nodes_traversed: result.nodesTraversed || 0,
      },
    };
  } catch (err) {
    if (err instanceof EngineError) throw err;
    throw new EngineError(`Mock engine error: ${err.message}`);
  }
}

function handleCreateTable(cmd) {
  const { table, schema, primary_key } = cmd;

  if (tables.has(table)) {
    throw new EngineError(`Table '${table}' already exists`);
  }

  const pk = primary_key || Object.keys(schema)[0];

  tables.set(table, {
    schema,
    primaryKey: pk,
    rows: new Map(),
    secondaryIndexes: new Map(),
    createdAt: new Date().toISOString(),
  });

  return { rows: [], diskReads: 0, nodesTraversed: 0 };
}

function handleInsert(cmd) {
  const { table, key, data } = cmd;
  const tableData = getTable(table);

  if (key === null || key === undefined) {
    throw new EngineError('Insert requires a non-null key');
  }

  if (tableData.rows.has(key)) {
    throw new EngineError(`Duplicate key '${key}' in table '${table}'`);
  }

  tableData.rows.set(key, { ...data });

  // Update secondary indexes if any exist
  for (const [col, indexMap] of tableData.secondaryIndexes) {
    const val = data[col];
    if (val !== undefined) {
      if (!indexMap.has(val)) {
        indexMap.set(val, new Set());
      }
      indexMap.get(val).add(key);
    }
  }

  // Simulate B+ Tree traversal depth
  const depth = Math.max(1, Math.ceil(Math.log2(tableData.rows.size + 1)));
  return { rows: [], diskReads: depth, nodesTraversed: depth };
}

function handleSearch(cmd) {
  const { table, key } = cmd;
  const tableData = getTable(table);
  const row = tableData.rows.get(key);
  const depth = Math.max(1, Math.ceil(Math.log2(tableData.rows.size + 1)));

  if (row) {
    return { rows: [{ ...row }], diskReads: depth, nodesTraversed: depth };
  }
  return { rows: [], diskReads: depth, nodesTraversed: depth };
}

function handleRange(cmd) {
  const { table, start, end } = cmd;
  const tableData = getTable(table);

  const allKeys = Array.from(tableData.rows.keys()).sort((a, b) => {
    if (typeof a === 'number' && typeof b === 'number') return a - b;
    return String(a).localeCompare(String(b));
  });

  const rows = [];
  for (const key of allKeys) {
    if (key >= start && key <= end) {
      rows.push({ ...tableData.rows.get(key) });
    }
  }

  const depth = Math.max(1, Math.ceil(Math.log2(tableData.rows.size + 1)));
  return {
    rows,
    diskReads: depth + Math.ceil(rows.length / 10),
    nodesTraversed: depth + rows.length,
  };
}

function handleFullScan(cmd) {
  const { table, filter } = cmd;
  const tableData = getTable(table);

  let rows = Array.from(tableData.rows.values()).map(r => ({ ...r }));

  if (filter) {
    rows = rows.filter(row => {
      const val = row[filter.column];
      switch (filter.operator) {
        case '=': return val === filter.value;
        default: return true;
      }
    });
  }

  return {
    rows,
    diskReads: Math.ceil(tableData.rows.size / 10),
    nodesTraversed: tableData.rows.size,
  };
}

function getTable(name) {
  const tableData = tables.get(name);
  if (!tableData) {
    throw new EngineError(`Table '${name}' does not exist`);
  }
  return tableData;
}

function listTables() {
  const result = [];
  for (const [name, data] of tables) {
    result.push({
      name,
      schema: data.schema,
      primaryKey: data.primaryKey,
      rowCount: data.rows.size,
      createdAt: data.createdAt,
    });
  }
  return result;
}

function getTableInfo(name) {
  const data = tables.get(name);
  if (!data) return null;
  return {
    name,
    schema: data.schema,
    primaryKey: data.primaryKey,
    rowCount: data.rows.size,
    secondaryIndexes: Array.from(data.secondaryIndexes.keys()),
    createdAt: data.createdAt,
  };
}

function getSchemaMap() {
  const map = {};
  for (const [name, data] of tables) {
    map[name] = {
      columns: Object.entries(data.schema).map(([colName, colType]) => ({
        name: colName,
        type: colType,
      })),
      primaryKey: data.primaryKey,
      secondaryIndexes: Array.from(data.secondaryIndexes.keys()),
    };
  }
  return map;
}

function reset() {
  tables.clear();
}

module.exports = { callEngine, listTables, getTableInfo, getSchemaMap, reset };
