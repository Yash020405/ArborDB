'use strict';

const request = require('supertest');
const { createApp } = require('../src/app');
const engine = require('../src/engine');
const metricsService = require('../src/services/metrics');

let app;

beforeAll(() => {
  process.env.USE_MOCK_ENGINE = 'true';
  app = createApp();
});

beforeEach(() => {
  engine.reset();
  metricsService.reset();
});

describe('API Endpoints', () => {
  // ─── Health Check ────────────────────────────────────────
  describe('GET /health', () => {
    test('returns health status', async () => {
      const res = await request(app).get('/health');
      expect(res.status).toBe(200);
      expect(res.body.status).toBe('ok');
      expect(res.body.service).toBe('ArborDB API');
    });
  });

  // ─── POST /query ────────────────────────────────────────
  describe('POST /query', () => {
    test('executes CREATE TABLE', async () => {
      const res = await request(app)
        .post('/query')
        .send({ sql: 'CREATE TABLE users (id INT, name STRING)' });

      expect(res.status).toBe(200);
      expect(res.body.status).toBe('ok');
      expect(res.body.query.type).toBe('CREATE_TABLE');
      expect(res.body.query.table).toBe('users');
    });

    test('executes INSERT', async () => {
      // Create table first
      await request(app)
        .post('/query')
        .send({ sql: 'CREATE TABLE users (id INT, name STRING)' });

      const res = await request(app)
        .post('/query')
        .send({ sql: "INSERT INTO users VALUES (1, 'Yash')" });

      expect(res.status).toBe(200);
      expect(res.body.status).toBe('ok');
      expect(res.body.query.type).toBe('INSERT');
    });

    test('executes SELECT with results', async () => {
      // Setup: create + insert
      await request(app)
        .post('/query')
        .send({ sql: 'CREATE TABLE users (id INT, name STRING)' });
      await request(app)
        .post('/query')
        .send({ sql: "INSERT INTO users VALUES (1, 'Yash')" });

      const res = await request(app)
        .post('/query')
        .send({ sql: 'SELECT * FROM users WHERE id = 1' });

      expect(res.status).toBe(200);
      expect(res.body.result.rows).toHaveLength(1);
      expect(res.body.result.rows[0].name).toBe('Yash');
      expect(res.body.result.rowCount).toBe(1);
    });

    test('executes SELECT with BETWEEN', async () => {
      // Setup
      await request(app)
        .post('/query')
        .send({ sql: 'CREATE TABLE scores (id INT, value INT)' });
      for (let i = 1; i <= 5; i++) {
        await request(app)
          .post('/query')
          .send({ sql: `INSERT INTO scores VALUES (${i}, ${i * 10})` });
      }

      const res = await request(app)
        .post('/query')
        .send({ sql: 'SELECT * FROM scores WHERE id BETWEEN 2 AND 4' });

      expect(res.status).toBe(200);
      expect(res.body.result.rows).toHaveLength(3);
    });

    test('executes full scan SELECT', async () => {
      await request(app)
        .post('/query')
        .send({ sql: 'CREATE TABLE items (id INT, name STRING)' });
      await request(app)
        .post('/query')
        .send({ sql: "INSERT INTO items VALUES (1, 'Apple')" });
      await request(app)
        .post('/query')
        .send({ sql: "INSERT INTO items VALUES (2, 'Banana')" });

      const res = await request(app)
        .post('/query')
        .send({ sql: 'SELECT * FROM items' });

      expect(res.status).toBe(200);
      expect(res.body.result.rows).toHaveLength(2);
    });

    test('includes metrics in response', async () => {
      await request(app)
        .post('/query')
        .send({ sql: 'CREATE TABLE test (id INT)' });

      const res = await request(app)
        .post('/query')
        .send({ sql: 'SELECT * FROM test' });

      expect(res.body.metrics).toBeDefined();
      expect(res.body.metrics.parseTimeMs).toBeGreaterThanOrEqual(0);
      expect(res.body.metrics.engineTimeMs).toBeGreaterThanOrEqual(0);
      expect(res.body.metrics.totalTimeMs).toBeGreaterThanOrEqual(0);
    });

    test('includes optimization hints for SELECT', async () => {
      await request(app)
        .post('/query')
        .send({ sql: 'CREATE TABLE test (id INT)' });

      const res = await request(app)
        .post('/query')
        .send({ sql: 'SELECT * FROM test WHERE id = 1' });

      expect(res.body.optimization).toBeDefined();
      expect(res.body.optimization.strategy).toBeDefined();
    });

    test('executes UPDATE and reflects changed values', async () => {
      await request(app)
        .post('/query')
        .send({ sql: 'CREATE TABLE users (id INT, name STRING)' });
      await request(app)
        .post('/query')
        .send({ sql: "INSERT INTO users VALUES (1, 'Old')" });

      const updateRes = await request(app)
        .post('/query')
        .send({ sql: "UPDATE users SET name = 'New' WHERE id = 1" });

      expect(updateRes.status).toBe(200);

      const selectRes = await request(app)
        .post('/query')
        .send({ sql: 'SELECT * FROM users WHERE id = 1' });

      expect(selectRes.body.result.rows).toHaveLength(1);
      expect(selectRes.body.result.rows[0].name).toBe('New');
    });

    test('executes DELETE and removes matching rows', async () => {
      await request(app)
        .post('/query')
        .send({ sql: 'CREATE TABLE logs (id INT, msg STRING)' });
      await request(app)
        .post('/query')
        .send({ sql: "INSERT INTO logs VALUES (1, 'a')" });
      await request(app)
        .post('/query')
        .send({ sql: "INSERT INTO logs VALUES (2, 'b')" });

      const deleteRes = await request(app)
        .post('/query')
        .send({ sql: 'DELETE FROM logs WHERE id = 1' });

      expect(deleteRes.status).toBe(200);

      const selectRes = await request(app)
        .post('/query')
        .send({ sql: 'SELECT * FROM logs' });

      expect(selectRes.body.result.rowCount).toBe(1);
      expect(selectRes.body.result.rows[0].id).toBe(2);
    });

    test('executes DROP TABLE and prevents further reads', async () => {
      await request(app)
        .post('/query')
        .send({ sql: 'CREATE TABLE tmp (id INT)' });

      const dropRes = await request(app)
        .post('/query')
        .send({ sql: 'DROP TABLE tmp' });

      expect(dropRes.status).toBe(200);

      const afterDrop = await request(app)
        .post('/query')
        .send({ sql: 'SELECT * FROM tmp' });

      expect(afterDrop.status).toBe(502);
      expect(afterDrop.body.error.code).toBe('ENGINE_ERROR');
    });
  });

  // ─── GET /tables ─────────────────────────────────────────
  describe('GET /tables', () => {
    test('returns empty list initially', async () => {
      const res = await request(app).get('/tables');
      expect(res.status).toBe(200);
      expect(res.body.tables).toEqual([]);
      expect(res.body.count).toBe(0);
    });

    test('returns tables after creation', async () => {
      await request(app)
        .post('/query')
        .send({ sql: 'CREATE TABLE users (id INT, name STRING)' });

      const res = await request(app).get('/tables');
      expect(res.body.tables).toHaveLength(1);
      expect(res.body.tables[0].name).toBe('users');
      expect(res.body.tables[0].rowCount).toBe(0);
    });

    test('returns specific table info', async () => {
      await request(app)
        .post('/query')
        .send({ sql: 'CREATE TABLE users (id INT, name STRING)' });

      const res = await request(app).get('/tables/users');
      expect(res.status).toBe(200);
      expect(res.body.table.name).toBe('users');
      expect(res.body.table.schema).toEqual({ id: 'INT', name: 'STRING' });
    });

    test('returns 404 for non-existent table', async () => {
      const res = await request(app).get('/tables/nonexistent');
      expect(res.status).toBe(404);
      expect(res.body.error.code).toBe('NOT_FOUND');
    });
  });

  // ─── GET /metrics ────────────────────────────────────────
  describe('GET /metrics', () => {
    test('returns initial empty metrics', async () => {
      const res = await request(app).get('/metrics');
      expect(res.status).toBe(200);
      expect(res.body.metrics.counters.totalQueries).toBe(0);
    });

    test('tracks query counts', async () => {
      await request(app)
        .post('/query')
        .send({ sql: 'CREATE TABLE test (id INT)' });
      await request(app)
        .post('/query')
        .send({ sql: 'INSERT INTO test VALUES (1)' });
      await request(app)
        .post('/query')
        .send({ sql: 'SELECT * FROM test' });

      const res = await request(app).get('/metrics');
      expect(res.body.metrics.counters.totalQueries).toBe(3);
      expect(res.body.metrics.breakdown.createTables).toBe(1);
      expect(res.body.metrics.breakdown.inserts).toBe(1);
      expect(res.body.metrics.breakdown.selects).toBe(1);
    });

    test('includes engine info', async () => {
      const res = await request(app).get('/metrics');
      expect(res.body.engine.type).toBe('mock');
    });
  });

  // ─── 404 Handler ─────────────────────────────────────────
  describe('404 Handler', () => {
    test('returns 404 for unknown endpoints', async () => {
      const res = await request(app).get('/unknown');
      expect(res.status).toBe(404);
      expect(res.body.error.code).toBe('NOT_FOUND');
      expect(res.body.error.availableEndpoints).toBeDefined();
    });
  });
});
