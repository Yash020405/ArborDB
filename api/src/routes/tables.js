'use strict';

// GET /tables — list all tables or get info about a specific table.

const express = require('express');
const engine = require('../engine');
const { NotFoundError } = require('../errors');

const router = express.Router();

router.get('/', async (req, res, next) => {
  try {
    const tables = engine.listTables();
    for (const t of tables) {
      try {
        const result = await engine.callEngine({ operation: 'full_scan', table: t.name });
        t.rowCount = result.rows ? result.rows.length : 0;
      } catch {
        t.rowCount = 0;
      }
    }
    res.json({ status: 'ok', tables, count: tables.length });
  } catch (err) {
    next(err);
  }
});

router.get('/:name', async (req, res, next) => {
  try {
    const { name } = req.params;
    const tableInfo = engine.getTableInfo(name);

    if (!tableInfo) {
      throw new NotFoundError(`Table '${name}' not found`);
    }

    try {
      const result = await engine.callEngine({ operation: 'full_scan', table: name });
      tableInfo.rowCount = result.rows ? result.rows.length : 0;
    } catch {
      tableInfo.rowCount = 0;
    }

    res.json({ status: 'ok', table: tableInfo });
  } catch (err) {
    next(err);
  }
});

module.exports = router;
