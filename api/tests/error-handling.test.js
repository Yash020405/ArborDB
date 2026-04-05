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
  tempDataDir = path.resolve(__dirname, `../.tmp/error-${Date.now()}`);

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

describe('Error Handling', () => {
  describe('Invalid SQL', () => {
    test('returns parse error for empty SQL', async () => {
      const res = await request(app)
        .post('/query')
        .send({ sql: '' });

      expect(res.status).toBe(400);
      expect(res.body.error.code).toBe('QUERY_VALIDATION_ERROR');
    });

    test('returns parse error for invalid SQL syntax', async () => {
      const res = await request(app)
        .post('/query')
        .send({ sql: 'INVALID SQL QUERY' });

      expect(res.status).toBe(400);
      expect(res.body.error.code).toBe('QUERY_PARSE_ERROR');
    });

    test('returns parse error for incomplete CREATE', async () => {
      const res = await request(app)
        .post('/query')
        .send({ sql: 'CREATE TABLE' });

      expect(res.status).toBe(400);
      expect(res.body.error.code).toBe('QUERY_PARSE_ERROR');
    });

    test('returns parse error for missing body', async () => {
      const res = await request(app)
        .post('/query')
        .send({});

      expect(res.status).toBe(400);
      expect(res.body.error.code).toBe('QUERY_VALIDATION_ERROR');
    });

    test('returns parse error for incomplete INSERT', async () => {
      const res = await request(app)
        .post('/query')
        .send({ sql: 'INSERT INTO users' });

      expect(res.status).toBe(400);
      expect(res.body.error.code).toBe('QUERY_PARSE_ERROR');
    });

    test('returns tokenizer error for illegal character', async () => {
      const res = await request(app)
        .post('/query')
        .send({ sql: 'SELECT * FROM users WHERE id = @1' });

      expect(res.status).toBe(400);
      expect(res.body.error.code).toBe('QUERY_TOKENIZE_ERROR');
    });

    test('returns parse error for invalid content type', async () => {
      const res = await request(app)
        .post('/query')
        .send('not json');

      expect(res.status).toBe(400);
    });
  });

  describe('Engine Errors', () => {
    test('returns error for duplicate table creation', async () => {
      await request(app)
        .post('/query')
        .send({ sql: 'CREATE TABLE users (id INT, name STRING)' });

      const res = await request(app)
        .post('/query')
        .send({ sql: 'CREATE TABLE users (id INT)' });

      expect(res.status).toBe(502);
      expect(res.body.error.code).toBe('ENGINE_OPERATION_FAILED');
    });

    test('returns error for query on non-existent table', async () => {
      const res = await request(app)
        .post('/query')
        .send({ sql: 'SELECT * FROM nonexistent' });

      expect(res.status).toBe(502);
      expect(res.body.error.code).toBe('ENGINE_OPERATION_FAILED');
    });

    test('returns error for insert with duplicate key', async () => {
      await request(app)
        .post('/query')
        .send({ sql: 'CREATE TABLE test (id INT, val STRING)' });
      await request(app)
        .post('/query')
        .send({ sql: "INSERT INTO test VALUES (1, 'first')" });

      const res = await request(app)
        .post('/query')
        .send({ sql: "INSERT INTO test VALUES (1, 'duplicate')" });

      expect(res.status).toBe(502);
      expect(res.body.error.code).toBe('ENGINE_OPERATION_FAILED');
    });

    test('returns error for duplicate index creation', async () => {
      await request(app)
        .post('/query')
        .send({ sql: 'CREATE TABLE users (id INT PRIMARY KEY, name STRING)' });
      await request(app)
        .post('/query')
        .send({ sql: 'CREATE INDEX idx_users_name ON users (name)' });

      const res = await request(app)
        .post('/query')
        .send({ sql: 'CREATE INDEX idx_users_name ON users (name)' });

      expect(res.status).toBe(502);
      expect(res.body.error.code).toBe('ENGINE_OPERATION_FAILED');
    });

    test('returns error for dropping missing index', async () => {
      await request(app)
        .post('/query')
        .send({ sql: 'CREATE TABLE users (id INT PRIMARY KEY, name STRING)' });

      const res = await request(app)
        .post('/query')
        .send({ sql: 'DROP INDEX idx_missing ON users' });

      expect(res.status).toBe(502);
      expect(res.body.error.code).toBe('ENGINE_OPERATION_FAILED');
    });

    test('returns engine error for JOIN with missing table', async () => {
      await request(app)
        .post('/query')
        .send({ sql: 'CREATE TABLE users (id INT PRIMARY KEY, name STRING)' });

      const res = await request(app)
        .post('/query')
        .send({ sql: 'SELECT users.id FROM users JOIN orders ON users.id = orders.user_id' });

      expect(res.status).toBe(502);
      expect(res.body.error.code).toBe('ENGINE_OPERATION_FAILED');
    });
  });

  describe('Upload Errors', () => {
    test('returns error for unsupported file type', async () => {
      await request(app)
        .post('/query')
        .send({ sql: 'CREATE TABLE test (id INT)' });

      const res = await request(app)
        .post('/upload')
        .field('table', 'test')
        .attach('file', Buffer.from('test'), 'test.txt');

      expect(res.status).toBe(400);
    });

    test('returns error for missing table parameter', async () => {
      const csv = 'id,name\n1,Test';
      const res = await request(app)
        .post('/upload')
        .attach('file', Buffer.from(csv), 'test.csv');

      // Should fail validation since table is required
      expect(res.status).toBeGreaterThanOrEqual(400);
    });
  });

  describe('Error Response Structure', () => {
    test('all errors have consistent structure', async () => {
      const res = await request(app)
        .post('/query')
        .send({ sql: 'INVALID' });

      expect(res.body).toHaveProperty('error');
      expect(res.body.error).toHaveProperty('code');
      expect(res.body.error).toHaveProperty('message');
      expect(res.body.error).toHaveProperty('timestamp');
    });

    test('errors include ISO 8601 timestamps', async () => {
      const res = await request(app)
        .post('/query')
        .send({ sql: 'INVALID' });

      const timestamp = res.body.error.timestamp;
      expect(() => new Date(timestamp)).not.toThrow();
      expect(new Date(timestamp).toISOString()).toBe(timestamp);
    });
  });

  describe('Metrics Track Errors', () => {
    test('error queries are tracked in metrics', async () => {
      // Make a query that will fail
      await request(app)
        .post('/query')
        .send({ sql: 'SELECT * FROM nonexistent' });

      const metricsRes = await request(app).get('/metrics');
      expect(metricsRes.body.metrics.counters.totalErrors).toBeGreaterThanOrEqual(1);
    });
  });

  describe('Edge Cases', () => {
    test('handles very long SQL gracefully', async () => {
      const longSql = 'SELECT ' + 'a'.repeat(15000) + ' FROM users';
      const res = await request(app)
        .post('/query')
        .send({ sql: longSql });

      expect(res.status).toBe(400);
    });

    test('handles multiple rapid requests', async () => {
      await request(app)
        .post('/query')
        .send({ sql: 'CREATE TABLE rapid (id INT)' });

      const promises = [];
      for (let i = 0; i < 10; i++) {
        promises.push(
          request(app)
            .post('/query')
            .send({ sql: `INSERT INTO rapid VALUES (${i})` })
        );
      }

      const results = await Promise.all(promises);
      const allOk = results.every(r => r.status === 200);
      expect(allOk).toBe(true);
    });

    test('handles search for non-existent key', async () => {
      await request(app)
        .post('/query')
        .send({ sql: 'CREATE TABLE users (id INT, name STRING)' });

      const res = await request(app)
        .post('/query')
        .send({ sql: 'SELECT * FROM users WHERE id = 999' });

      expect(res.status).toBe(200);
      expect(res.body.result.rows).toEqual([]);
      expect(res.body.result.rowCount).toBe(0);
    });

    test('handles empty range query', async () => {
      await request(app)
        .post('/query')
        .send({ sql: 'CREATE TABLE nums (id INT)' });

      const res = await request(app)
        .post('/query')
        .send({ sql: 'SELECT * FROM nums WHERE id BETWEEN 1 AND 100' });

      expect(res.status).toBe(200);
      expect(res.body.result.rows).toEqual([]);
    });
  });
});
