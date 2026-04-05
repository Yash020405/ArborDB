'use strict';

// Engine factory — routes to mock or real engine based on USE_MOCK_ENGINE env var.

const fs = require('fs');
const path = require('path');
const mock = require('./mock');
const { callEngine: callRealEngine } = require('./caller');

function useMockEngine() {
  const useMock = process.env.USE_MOCK_ENGINE;
  return useMock === undefined || useMock === 'true' || useMock === '1';
}

async function callEngine(engineJson) {
  if (useMockEngine()) {
    return mock.callEngine(engineJson);
  }
  return callRealEngine(engineJson);
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

function listTables() {
  if (useMockEngine()) {
    return mock.listTables();
  }

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
  if (useMockEngine()) {
    return mock.getTableInfo(name);
  }

  const schemaPath = path.join(getTablesDir(), `${name}.schema.json`);
  if (!fs.existsSync(schemaPath)) {
    return null;
  }

  const dbPath = path.join(getTablesDir(), `${name}.db`);

  const j = JSON.parse(fs.readFileSync(schemaPath, 'utf8'));
  const schemaObj = {};
  j.columns.forEach((c) => { schemaObj[c.name] = c.type; });

  const secondaryIndexes = j.columns
    .map((c) => c.name)
    .filter((colName) => colName !== j.primary_key);

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
    rowCount,
    createdAt: fs.statSync(schemaPath).mtime,
  };
}

function getSchemaMap() {
  if (useMockEngine()) {
    return mock.getSchemaMap();
  }

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
    };
  }

  return map;
}

function reset() {
  if (useMockEngine()) {
    mock.reset();
  }
}

module.exports = { callEngine, listTables, getTableInfo, getSchemaMap, reset, useMockEngine };
