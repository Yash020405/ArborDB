'use strict';

// GET /tables — list all tables or get info about a specific table.

const express = require('express');
const engine = require('../engine');
const { NotFoundError } = require('../errors');

const router = express.Router();

router.get('/', (req, res) => {
  const tables = engine.listTables();
  res.json({ status: 'ok', tables, count: tables.length });
});

router.get('/:name', (req, res, next) => {
  try {
    const { name } = req.params;
    const tableInfo = engine.getTableInfo(name);

    if (!tableInfo) {
      throw new NotFoundError(`Table '${name}' not found`);
    }

    res.json({ status: 'ok', table: tableInfo });
  } catch (err) {
    next(err);
  }
});

module.exports = router;
