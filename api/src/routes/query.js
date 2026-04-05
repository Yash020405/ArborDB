'use strict';

// POST /query — accepts SQL, parses it, executes via engine, returns results.

const express = require('express');
const Joi = require('joi');
const { tokenize, parseTokens, optimize, buildEngineCommand } = require('../../../engine/src/query');
const { executeAdvancedSelect } = require('../../../engine/src/query/advanced');
const engine = require('../engine');
const metricsService = require('../services/metrics');
const {
  ArborDBError,
  QueryValidationError,
  QueryTokenizeError,
  QueryParseError,
  QueryPlanError,
  QueryExecutionError,
  EngineError,
} = require('../errors');

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

const SQL_PREVIEW_LIMIT = 240;

function buildSqlDetails(sql, extra = {}) {
  if (typeof sql !== 'string') {
    return extra;
  }

  return {
    sqlPreview: sql.length > SQL_PREVIEW_LIMIT ? `${sql.slice(0, SQL_PREVIEW_LIMIT)}...` : sql,
    ...extra,
  };
}

router.post('/', async (req, res, next) => {
  const totalStart = Date.now();
  let sqlForMetrics = null;
  let astForMetrics = null;

  try {
    const { error, value } = querySchema.validate(req.body);
    if (error) {
      throw new QueryValidationError('Invalid query payload', {
        details: error.details.map((d) => d.message),
      });
    }

    const { sql } = value;
    sqlForMetrics = sql;
    const schemaMap = engine.getSchemaMap();

    // Parse and plan in explicit stages to keep error taxonomy stable.
    const parseStart = Date.now();
    let tokens;

    try {
      tokens = tokenize(sql);
    } catch (err) {
      throw new QueryTokenizeError(err.message, buildSqlDetails(sql, { stage: 'tokenize' }));
    }

    let ast;
    try {
      ast = parseTokens(tokens);
      astForMetrics = ast;
    } catch (err) {
      throw new QueryParseError(err.message, buildSqlDetails(sql, { stage: 'parse' }));
    }

    let optimized;
    let command;
    try {
      const tableMetadata = schemaMap[ast.table] ? {
        primaryKey: schemaMap[ast.table].primaryKey,
        secondaryIndexes: schemaMap[ast.table].secondaryIndexes || schemaMap[ast.table].secondaryIndexDefs || [],
      } : null;

      optimized = optimize(ast, tableMetadata);
      command = buildEngineCommand(ast, schemaMap);
    } catch (err) {
      throw new QueryPlanError(err.message, buildSqlDetails(sql, {
        stage: 'plan',
        queryType: ast.type,
      }));
    }
    const parseTimeMs = Date.now() - parseStart;

    // Execute via engine
    const engineStart = Date.now();
    let engineResponse;
    try {
      if (command.operation === 'select_advanced') {
        engineResponse = await executeAdvancedSelect(ast, (cmd) => engine.callEngine(cmd), schemaMap);
      } else {
        engineResponse = await engine.callEngine(command);
      }
    } catch (err) {
      if (err instanceof EngineError) {
        throw err;
      }

      throw new QueryExecutionError(err.message, buildSqlDetails(sql, {
        stage: 'execute',
        queryType: ast.type,
      }));
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
        affectedRows: engineResponse.affected_rows || 0,
      },
      metrics: { parseTimeMs, engineTimeMs, totalTimeMs, ...(engineResponse.metrics || {}) },
    };

    if (optimized.optimizationHint) {
      response.optimization = optimized.optimizationHint;
    }

    res.json(response);
  } catch (err) {
    if (typeof sqlForMetrics === 'string') {
      metricsService.recordQuery({
        sql: sqlForMetrics,
        type: astForMetrics ? astForMetrics.type : 'UNKNOWN',
        table: astForMetrics ? astForMetrics.table : null,
        executionTimeMs: Date.now() - totalStart,
        rowsReturned: 0,
        status: 'error',
        error: err.errorCode || err.message,
      });
    }

    if (err instanceof ArborDBError) {
      return next(err);
    }

    return next(new QueryExecutionError(err.message));
  }
});

module.exports = router;
