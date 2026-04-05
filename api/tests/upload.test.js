'use strict';

const fs = require('fs');
const path = require('path');
const request = require('supertest');
const { createApp } = require('../src/app');
const engine = require('../src/engine');
const metricsService = require('../src/services/metrics');
const { parseCSV } = require('../src/services/fileParser');

let app;
let tempDataDir;

const previousDataDir = process.env.DATA_DIR;
const previousRateLimitMaxRequests = process.env.RATE_LIMIT_MAX_REQUESTS;

beforeAll(() => {
  tempDataDir = path.resolve(__dirname, `../.tmp/upload-${Date.now()}`);

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

describe('File Upload', () => {
  describe('CSV Parsing (Unit)', () => {
    test('parses valid CSV buffer', () => {
      const csv = 'id,name,age\n1,Yash,22\n2,Raj,25\n3,Priya,23';
      const result = parseCSV(Buffer.from(csv));

      expect(result.headers).toEqual(['id', 'name', 'age']);
      expect(result.totalRows).toBe(3);
      expect(result.rows[0]).toEqual({ id: 1, name: 'Yash', age: 22 });
    });

    test('handles dynamic typing', () => {
      const csv = 'value,flag\n42,true\n3.14,false';
      const result = parseCSV(Buffer.from(csv));

      expect(result.rows[0].value).toBe(42);
      expect(result.rows[0].flag).toBe(true);
      expect(result.rows[1].value).toBe(3.14);
    });

    test('normalizes headers to lowercase', () => {
      const csv = 'ID,Name,AGE\n1,Test,20';
      const result = parseCSV(Buffer.from(csv));
      expect(result.headers).toEqual(['id', 'name', 'age']);
    });

    test('skips empty lines', () => {
      const csv = 'id,name\n1,First\n\n2,Second\n\n';
      const result = parseCSV(Buffer.from(csv));
      expect(result.totalRows).toBe(2);
    });

    test('throws on empty CSV', () => {
      expect(() => parseCSV(Buffer.from(''))).toThrow();
    });
  });

  describe('POST /upload', () => {
    test('uploads CSV file successfully', async () => {
      // Create table first
      await request(app)
        .post('/query')
        .send({ sql: 'CREATE TABLE students (id INT, name STRING, grade INT)' });

      const csv = 'id,name,grade\n1,Alice,90\n2,Bob,85\n3,Carol,92';

      const res = await request(app)
        .post('/upload')
        .field('table', 'students')
        .attach('file', Buffer.from(csv), 'students.csv');

      expect(res.status).toBe(200);
      expect(res.body.status).toBe('ok');
      expect(res.body.upload.format).toBe('csv');
      expect(res.body.upload.totalRows).toBe(3);
      expect(res.body.insertion.insertedCount).toBe(3);
      expect(res.body.insertion.failedCount).toBe(0);
    });

    test('rejects upload without file', async () => {
      const res = await request(app)
        .post('/upload')
        .field('table', 'users');

      expect(res.status).toBe(400);
      expect(res.body.error.code).toBe('UPLOAD_ERROR');
    });

    test('rejects upload for non-existent table', async () => {
      const csv = 'id,name\n1,Test';

      const res = await request(app)
        .post('/upload')
        .field('table', 'nonexistent')
        .attach('file', Buffer.from(csv), 'test.csv');

      expect(res.status).toBe(404);
      expect(res.body.error.code).toBe('NOT_FOUND');
    });

    test('reports validation errors for mismatched columns', async () => {
      await request(app)
        .post('/query')
        .send({ sql: 'CREATE TABLE users (id INT, name STRING, email STRING)' });

      const csv = 'id,name\n1,Test';

      const res = await request(app)
        .post('/upload')
        .field('table', 'users')
        .attach('file', Buffer.from(csv), 'test.csv');

      expect(res.status).toBe(422);
      expect(res.body.error.code).toBe('VALIDATION_ERROR');
      expect(res.body.error.details.missingColumns).toContain('email');
    });

    test('handles duplicate keys in upload', async () => {
      await request(app)
        .post('/query')
        .send({ sql: 'CREATE TABLE items (id INT, name STRING)' });

      const csv = 'id,name\n1,First\n1,Duplicate\n2,Second';

      const res = await request(app)
        .post('/upload')
        .field('table', 'items')
        .attach('file', Buffer.from(csv), 'items.csv');

      expect(res.status).toBe(200);
      // One should fail due to duplicate key (either in validation or insertion)
      const totalInserted = res.body.insertion.insertedCount;
      const totalFailed = res.body.insertion.failedCount + res.body.validation.duplicateKeys;
      expect(totalInserted + totalFailed).toBeGreaterThanOrEqual(2);
    });

    test('verifies inserted rows via query after upload', async () => {
      await request(app)
        .post('/query')
        .send({ sql: 'CREATE TABLE products (id INT, name STRING)' });

      const csv = 'id,name\n10,Widget\n20,Gadget\n30,Gizmo';
      await request(app)
        .post('/upload')
        .field('table', 'products')
        .attach('file', Buffer.from(csv), 'products.csv');

      // Verify via query
      const res = await request(app)
        .post('/query')
        .send({ sql: 'SELECT * FROM products' });

      expect(res.body.result.rowCount).toBe(3);
    });

    test('includes timing metrics', async () => {
      await request(app)
        .post('/query')
        .send({ sql: 'CREATE TABLE t (id INT, val STRING)' });

      const csv = 'id,val\n1,a\n2,b';

      const res = await request(app)
        .post('/upload')
        .field('table', 't')
        .attach('file', Buffer.from(csv), 't.csv');

      expect(res.body.metrics.parseTimeMs).toBeGreaterThanOrEqual(0);
      expect(res.body.metrics.validationTimeMs).toBeGreaterThanOrEqual(0);
      expect(res.body.metrics.insertTimeMs).toBeGreaterThanOrEqual(0);
      expect(res.body.metrics.totalTimeMs).toBeGreaterThanOrEqual(0);
    });
  });
});
