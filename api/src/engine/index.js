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

function listTables() {
  if (useMockEngine()) return mock.listTables();
  return [];
}

function getTableInfo(name) {
  if (useMockEngine()) return mock.getTableInfo(name);
  return null;
}

function getSchemaMap() {
  if (useMockEngine()) return mock.getSchemaMap();
  return {};
}

function reset() {
  if (useMockEngine()) mock.reset();
}

module.exports = { callEngine, listTables, getTableInfo, getSchemaMap, reset, useMockEngine };
