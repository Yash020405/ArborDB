'use strict';

// POST /query — accepts SQL, parses it, executes via engine, returns results.

const express = require('express');
const Joi = require('joi');
const { processQuery } = require('../../../engine/src/query');
const engine = require('../engine');
const metricsService = require('../services/metrics');
const { ParseError } = require('../errors');

const router = express.Router();

const querySchema = Joi.object({
  sql: Joi.string().trim().min(1).max(10000).required()
    .messages({
      'string.empty': 'SQL query cannot be empty',
      'string.min': 'SQL query cannot be empty',
      'string.max': 'SQL query exceeds maximum length of 10,000 characters',
      'any.required': 'SQL query is required',
    }),
});

router.post('/', async (req, res, next) => {
  const totalStart = Date.now();

  try {
    const { error, value } = querySchema.validate(req.body);
    if (error) {
      throw new ParseError('Invalid request', { details: error.details.map(d => d.message) });
    }

    const { sql } = value;

    // Parse, optimize, and build engine command
    const parseStart = Date.now();
    let queryResult;
    try {
      const schemaMap = engine.getSchemaMap();
      queryResult = processQuery(sql, schemaMap);
    } catch (err) {
      throw new ParseError(err.message, { sql });
    }
    const parseTimeMs = Date.now() - parseStart;

    const { ast, optimized, command } = queryResult;

    // Execute via engine
    const engineStart = Date.now();
    let engineResponse;
    try {
      engineResponse = await engine.callEngine(command);
    } catch (err) {
      metricsService.recordQuery({
        sql, type: ast.type, table: ast.table,
        executionTimeMs: Date.now() - totalStart, rowsReturned: 0,
        status: 'error', error: err.message,
      });
      throw err;
    }
    const engineTimeMs = Date.now() - engineStart;
    const totalTimeMs = Date.now() - totalStart;

    metricsService.recordQuery({
      sql, type: ast.type, table: ast.table,
      executionTimeMs: totalTimeMs,
      rowsReturned: engineResponse.rows ? engineResponse.rows.length : 0,
      engineMetrics: engineResponse.metrics,
      optimizationHint: optimized.optimizationHint,
      status: 'success',
    });

    const response = {
      status: 'ok',
      query: { sql, type: ast.type, table: ast.table },
      result: {
        rows: engineResponse.rows || [],
        rowCount: engineResponse.rows ? engineResponse.rows.length : 0,
      },
      metrics: { parseTimeMs, engineTimeMs, totalTimeMs, ...(engineResponse.metrics || {}) },
    };

    if (optimized.optimizationHint) {
      response.optimization = optimized.optimizationHint;
    }

    res.json(response);
  } catch (err) {
    next(err);
  }
});

module.exports = router;
