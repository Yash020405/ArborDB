'use strict';

// Engine factory — routes to mock or real engine based on USE_MOCK_ENGINE env var.

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

const fs = require('fs');
const path = require('path');

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
  if (useMockEngine()) return mock.listTables();
  const dir = getTablesDir();
  const files = fs.readdirSync(dir);
  const tables = files
    .filter(f => f.endsWith('.schema.json'))
    .map(f => f.replace('.schema.json', ''));
  return tables;
}

function getTableInfo(name) {
  if (useMockEngine()) return mock.getTableInfo(name);
  const schemaPath = path.join(getTablesDir(), `${name}.schema.json`);
  if (!fs.existsSync(schemaPath)) return null;
  
  const j = JSON.parse(fs.readFileSync(schemaPath, 'utf8'));
  const schemaObj = {};
  j.columns.forEach(c => { schemaObj[c.name] = c.type; });
  
  // Note: Finding actual rowCount requires a full scan or reading stat. We return ? for now.
  let rowCount = 0;
  const dbPath = path.join(getTablesDir(), `${name}.db`);
  if (fs.existsSync(dbPath)) {
    const stats = fs.statSync(dbPath);
    // Rough estimate: page size 4096. This isn't perfect but gives a sense.
    rowCount = Math.max(0, Math.floor(stats.size / 4096)); // Just a loose mock metric for UI
  }

  return {
    name,
    schema: schemaObj,
    primaryKey: j.primary_key,
    rowCount: `(Native Engine)`,
    createdAt: fs.statSync(schemaPath).mtime,
  };
}

function getSchemaMap() {
  if (useMockEngine()) return mock.getSchemaMap();
  const map = {};
  const tables = listTables();
  for (const t of tables) {
    const info = getTableInfo(t);
    if (info) map[t] = info.schema;
  }
  return map;
}

function reset() {
  if (useMockEngine()) mock.reset();
}

module.exports = { callEngine, listTables, getTableInfo, getSchemaMap, reset, useMockEngine };
