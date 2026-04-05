'use strict';

// Native engine facade — metadata helpers + C++ engine command execution.

const fs = require('fs');
const path = require('path');
const {
  callEngine: callNativeEngine,
  resetPersistentEngine,
  shutdownPersistentEngine,
  getEngineExecutionMode,
} = require('./caller');

async function callEngine(engineJson) {
  return callNativeEngine(engineJson);
}

function getTablesDir() {
  const apiDir = path.resolve(__dirname, '../../');
  const baseDir = process.env.DATA_DIR 
    ? (path.isAbsolute(process.env.DATA_DIR) ? process.env.DATA_DIR : path.resolve(apiDir, process.env.DATA_DIR))
    : path.resolve(apiDir, '../data');
    
  const d = path.join(baseDir, 'tables');
  if (!fs.existsSync(d)) {
    fs.mkdirSync(d, { recursive: true });
  }
  return d;
}

function normalizeSecondaryIndexDefs(tableName, schemaJson) {
  if (!Array.isArray(schemaJson.secondary_indexes)) {
    return [];
  }

  const defs = [];
  for (const idx of schemaJson.secondary_indexes) {
    if (typeof idx === 'string') {
      defs.push({
        name: `idx_${tableName}_${idx}`,
        column: idx,
        unique: false,
      });
      continue;
    }

    if (idx && typeof idx === 'object' && typeof idx.name === 'string' && typeof idx.column === 'string') {
      defs.push({
        name: idx.name,
        column: idx.column,
        unique: Boolean(idx.unique),
      });
    }
  }

  return defs;
}

function listTables() {
  const dir = getTablesDir();
  const files = fs.readdirSync(dir);
  const tableNames = files
    .filter((f) => f.endsWith('.schema.json'))
    .map((f) => f.replace('.schema.json', ''));

  return tableNames
    .map((name) => getTableInfo(name))
    .filter(Boolean);
}

function getTableInfo(name) {
  const schemaPath = path.join(getTablesDir(), `${name}.schema.json`);
  if (!fs.existsSync(schemaPath)) {
    return null;
  }

  const dbPath = path.join(getTablesDir(), `${name}.db`);

  const j = JSON.parse(fs.readFileSync(schemaPath, 'utf8'));
  const schemaObj = {};
  j.columns.forEach((c) => { schemaObj[c.name] = c.type; });
  const secondaryIndexDefs = normalizeSecondaryIndexDefs(name, j);
  const secondaryIndexes = secondaryIndexDefs.map((idx) => idx.column);

  let rowCount = 0;
  // Keep a numeric rowCount in native mode so /metrics aggregation and frontend cards stay consistent.
  if (fs.existsSync(dbPath)) {
    const stats = fs.statSync(dbPath);
    rowCount = Math.max(0, Math.floor(stats.size / 4096));
  }

  return {
    name,
    schema: schemaObj,
    columns: j.columns,
    primaryKey: j.primary_key,
    secondaryIndexes,
    secondaryIndexDefs,
    rowCount,
    createdAt: fs.statSync(schemaPath).mtime,
  };
}

function getSchemaMap() {
  const map = {};
  const tables = listTables();

  for (const table of tables) {
    if (!table) continue;
    map[table.name] = {
      columns: table.columns || Object.keys(table.schema).map((colName) => ({
        name: colName,
        type: table.schema[colName],
      })),
      primaryKey: table.primaryKey,
      secondaryIndexes: table.secondaryIndexes || [],
      secondaryIndexDefs: table.secondaryIndexDefs || [],
    };
  }

  return map;
}

function reset() {
  // Reset should only clear data automatically while running tests.
  if (process.env.NODE_ENV !== 'test') {
    return;
  }

  // Ensure worker memory/cache is reset between tests.
  resetPersistentEngine('test-reset');

  const dir = getTablesDir();
  const files = fs.readdirSync(dir);

  for (const file of files) {
    if (file.endsWith('.schema.json') || file.endsWith('.db') || file === 'wal.log') {
      fs.rmSync(path.join(dir, file), { force: true });
    }
  }
}

function shutdown() {
  shutdownPersistentEngine();
}

function getExecutionMode() {
  return getEngineExecutionMode();
}

module.exports = {
  callEngine,
  listTables,
  getTableInfo,
  getSchemaMap,
  reset,
  shutdown,
  getExecutionMode,
};
