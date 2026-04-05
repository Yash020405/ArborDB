'use strict';

const fs = require('fs');
const path = require('path');
const request = require('supertest');
const { createApp } = require('../src/app');
const engine = require('../src/engine');
const metricsService = require('../src/services/metrics');

let app;
let tempDataDir;

const previousDataDir = process.env.DATA_DIR;
const previousRateLimitMaxRequests = process.env.RATE_LIMIT_MAX_REQUESTS;

beforeAll(() => {
  tempDataDir = path.resolve(__dirname, `../.tmp/api-${Date.now()}`);

  process.env.DATA_DIR = tempDataDir;
  process.env.RATE_LIMIT_MAX_REQUESTS = '1000';

  app = createApp();
});

beforeEach(() => {
  fs.rmSync(tempDataDir, { recursive: true, force: true });
  fs.mkdirSync(path.join(tempDataDir, 'tables'), { recursive: true });

  engine.reset();
  metricsService.reset();
});

afterAll(() => {
  fs.rmSync(tempDataDir, { recursive: true, force: true });

  if (previousDataDir === undefined) {
    delete process.env.DATA_DIR;
  } else {
    process.env.DATA_DIR = previousDataDir;
  }

  if (previousRateLimitMaxRequests === undefined) {
    delete process.env.RATE_LIMIT_MAX_REQUESTS;
  } else {
    process.env.RATE_LIMIT_MAX_REQUESTS = previousRateLimitMaxRequests;
  }
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
      expect(afterDrop.body.error.code).toBe('ENGINE_OPERATION_FAILED');
    });

    test('executes CREATE INDEX and uses indexed lookup for non-primary key equality', async () => {
      await request(app)
        .post('/query')
        .send({ sql: 'CREATE TABLE users (id INT PRIMARY KEY, name STRING)' });
      await request(app)
        .post('/query')
        .send({ sql: "INSERT INTO users VALUES (1, 'Yash')" });
      await request(app)
        .post('/query')
        .send({ sql: "INSERT INTO users VALUES (2, 'Raj')" });

      const createIndexRes = await request(app)
        .post('/query')
        .send({ sql: 'CREATE INDEX idx_users_name ON users (name)' });

      expect(createIndexRes.status).toBe(200);
      expect(createIndexRes.body.query.type).toBe('CREATE_INDEX');

      const indexedSelectRes = await request(app)
        .post('/query')
        .send({ sql: "SELECT * FROM users WHERE name = 'Yash'" });

      expect(indexedSelectRes.status).toBe(200);
      expect(indexedSelectRes.body.result.rowCount).toBe(1);
      expect(indexedSelectRes.body.result.rows[0].id).toBe(1);
      expect(indexedSelectRes.body.optimization.strategy).toBe('secondary_index_lookup');
    });

    test('executes DROP INDEX and falls back to full scan filter strategy', async () => {
      await request(app)
        .post('/query')
        .send({ sql: 'CREATE TABLE users (id INT PRIMARY KEY, name STRING)' });
      await request(app)
        .post('/query')
        .send({ sql: "INSERT INTO users VALUES (1, 'Yash')" });
      await request(app)
        .post('/query')
        .send({ sql: "INSERT INTO users VALUES (2, 'Raj')" });
      await request(app)
        .post('/query')
        .send({ sql: 'CREATE INDEX idx_users_name ON users (name)' });

      const dropIndexRes = await request(app)
        .post('/query')
        .send({ sql: 'DROP INDEX idx_users_name ON users' });

      expect(dropIndexRes.status).toBe(200);
      expect(dropIndexRes.body.query.type).toBe('DROP_INDEX');

      const postDropSelectRes = await request(app)
        .post('/query')
        .send({ sql: "SELECT * FROM users WHERE name = 'Yash'" });

      expect(postDropSelectRes.status).toBe(200);
      expect(postDropSelectRes.body.result.rowCount).toBe(1);
      expect(postDropSelectRes.body.optimization.strategy).toBe('full_scan_filter');
    });

    test('executes INNER JOIN query', async () => {
      await request(app)
        .post('/query')
        .send({ sql: 'CREATE TABLE users (id INT PRIMARY KEY, name STRING)' });
      await request(app)
        .post('/query')
        .send({ sql: 'CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount FLOAT)' });

      await request(app)
        .post('/query')
        .send({ sql: "INSERT INTO users VALUES (1, 'Alice')" });
      await request(app)
        .post('/query')
        .send({ sql: "INSERT INTO users VALUES (2, 'Bob')" });

      await request(app)
        .post('/query')
        .send({ sql: 'INSERT INTO orders VALUES (100, 1, 20.5)' });
      await request(app)
        .post('/query')
        .send({ sql: 'INSERT INTO orders VALUES (101, 1, 30.0)' });
      await request(app)
        .post('/query')
        .send({ sql: 'INSERT INTO orders VALUES (102, 2, 10.0)' });

      const res = await request(app)
        .post('/query')
        .send({ sql: 'SELECT users.id, users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id WHERE users.id = 1' });

      expect(res.status).toBe(200);
      expect(res.body.optimization.strategy).toBe('advanced_select');
      expect(res.body.result.rowCount).toBe(2);
      expect(res.body.result.rows[0].id).toBe(1);
      expect(res.body.result.rows[0].name).toBe('Alice');
    });

    test('executes GROUP BY aggregate query with HAVING', async () => {
      await request(app)
        .post('/query')
        .send({ sql: 'CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount FLOAT)' });

      await request(app)
        .post('/query')
        .send({ sql: 'INSERT INTO orders VALUES (100, 1, 20.5)' });
      await request(app)
        .post('/query')
        .send({ sql: 'INSERT INTO orders VALUES (101, 1, 30.0)' });
      await request(app)
        .post('/query')
        .send({ sql: 'INSERT INTO orders VALUES (102, 2, 10.0)' });

      const res = await request(app)
        .post('/query')
        .send({
          sql: 'SELECT user_id, COUNT(*) AS orders_count, SUM(amount) AS total_amount FROM orders GROUP BY user_id HAVING orders_count = 2',
        });

      expect(res.status).toBe(200);
      expect(res.body.optimization.strategy).toBe('advanced_select');
      expect(res.body.result.rowCount).toBe(1);
      expect(res.body.result.rows[0].user_id).toBe(1);
      expect(res.body.result.rows[0].orders_count).toBe(2);
      expect(res.body.result.rows[0].total_amount).toBeCloseTo(50.5, 5);
    });

    test('executes ORDER BY with LIMIT/OFFSET', async () => {
      await request(app)
        .post('/query')
        .send({ sql: 'CREATE TABLE sort_test (id INT PRIMARY KEY, name STRING)' });

      await request(app).post('/query').send({ sql: "INSERT INTO sort_test VALUES (1, 'a')" });
      await request(app).post('/query').send({ sql: "INSERT INTO sort_test VALUES (2, 'b')" });
      await request(app).post('/query').send({ sql: "INSERT INTO sort_test VALUES (3, 'c')" });
      await request(app).post('/query').send({ sql: "INSERT INTO sort_test VALUES (4, 'd')" });

      const res = await request(app)
        .post('/query')
        .send({ sql: 'SELECT id, name FROM sort_test ORDER BY id DESC LIMIT 2 OFFSET 1' });

      expect(res.status).toBe(200);
      expect(res.body.optimization.strategy).toBe('advanced_select');
      expect(res.body.result.rowCount).toBe(2);
      expect(res.body.result.rows.map((r) => r.id)).toEqual([3, 2]);
    });

    test('executes aggregate ORDER BY alias', async () => {
      await request(app)
        .post('/query')
        .send({ sql: 'CREATE TABLE orders_rank (id INT PRIMARY KEY, user_id INT, amount FLOAT)' });

      await request(app).post('/query').send({ sql: 'INSERT INTO orders_rank VALUES (1, 1, 10)' });
      await request(app).post('/query').send({ sql: 'INSERT INTO orders_rank VALUES (2, 1, 15)' });
      await request(app).post('/query').send({ sql: 'INSERT INTO orders_rank VALUES (3, 2, 5)' });

      const res = await request(app)
        .post('/query')
        .send({
          sql: 'SELECT user_id, SUM(amount) AS total_amount FROM orders_rank GROUP BY user_id ORDER BY total_amount DESC',
        });

      expect(res.status).toBe(200);
      expect(res.body.result.rowCount).toBe(2);
      expect(res.body.result.rows[0].user_id).toBe(1);
      expect(res.body.result.rows[0].total_amount).toBeCloseTo(25, 5);
    });

    test('keeps low traversal for ORDER BY on primary-key filtered query', async () => {
      await request(app)
        .post('/query')
        .send({ sql: 'CREATE TABLE latency_test (id INT PRIMARY KEY, name STRING)' });

      for (let i = 1; i <= 10; i++) {
        await request(app)
          .post('/query')
          .send({ sql: `INSERT INTO latency_test VALUES (${i}, 'user_${i}')` });
      }

      const plainRes = await request(app)
        .post('/query')
        .send({ sql: 'SELECT * FROM latency_test WHERE id = 5' })
        .expect(200);

      const orderedRes = await request(app)
        .post('/query')
        .send({ sql: 'SELECT * FROM latency_test WHERE id = 5 ORDER BY id DESC LIMIT 1' })
        .expect(200);

      expect(orderedRes.body.result.rowCount).toBe(1);
      expect(orderedRes.body.result.rows[0].id).toBe(5);

      // ORDER BY path should preserve indexed lookup behavior via pushdown.
      expect(orderedRes.body.metrics.nodes_traversed).toBeLessThanOrEqual(
        plainRes.body.metrics.nodes_traversed + 1,
      );
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
      expect(res.body.engine.type).toBe('native');
      expect(res.body.engine.mode).toBeDefined();
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
