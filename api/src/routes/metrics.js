'use strict';

// GET /metrics — aggregated query performance metrics and recent query log.

const express = require('express');
const metricsService = require('../services/metrics');
const engine = require('../engine');

const router = express.Router();

router.get('/', (req, res) => {
  const metrics = metricsService.getMetrics();
  const tables = engine.listTables();

  res.json({
    status: 'ok',
    metrics,
    engine: {
      type: 'native',
      mode: engine.getExecutionMode(),
      tablesCount: tables.length,
      totalRows: tables.reduce((sum, t) => sum + (t.rowCount || 0), 0),
    },
    uptime: process.uptime(),
    memoryUsage: process.memoryUsage(),
  });
});

router.get('/recent', (req, res) => {
  const parsedLimit = parseInt(req.query.limit || '50', 10);
  const safeLimit = Number.isFinite(parsedLimit) && parsedLimit > 0 ? parsedLimit : 50;
  const limit = Math.min(safeLimit, 100);
  const recentQueries = metricsService.getRecentQueries(limit);
  res.json({ status: 'ok', queries: recentQueries, count: recentQueries.length });
});

module.exports = router;
