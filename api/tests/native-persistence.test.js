'use strict';

const fs = require('fs');
const path = require('path');
const request = require('supertest');

const ENGINE_BINARY = path.resolve(__dirname, '../../engine/build/engine');
const runNativeSuite = fs.existsSync(ENGINE_BINARY) ? describe : describe.skip;

runNativeSuite('Native Persistence', () => {
  let app;
  let metricsService;
  let tempDataDir;

  const previousDataDir = process.env.DATA_DIR;

  beforeAll(() => {
    tempDataDir = path.resolve(__dirname, `../.tmp/native-${Date.now()}`);

    process.env.DATA_DIR = tempDataDir;

    jest.resetModules();
    const { createApp } = require('../src/app');
    app = createApp();
    metricsService = require('../src/services/metrics');
  });

  beforeEach(() => {
    fs.rmSync(tempDataDir, { recursive: true, force: true });
    fs.mkdirSync(path.join(tempDataDir, 'tables'), { recursive: true });
    metricsService.reset();
  });

  afterAll(() => {
    fs.rmSync(tempDataDir, { recursive: true, force: true });

    if (previousDataDir === undefined) {
      delete process.env.DATA_DIR;
    } else {
      process.env.DATA_DIR = previousDataDir;
    }
  });

  test('persists schema and table data to disk', async () => {
    await request(app)
      .post('/query')
      .send({ sql: 'CREATE TABLE native_users (id INT PRIMARY KEY, name STRING)' })
      .expect(200);

    await request(app)
      .post('/query')
      .send({ sql: "INSERT INTO native_users VALUES (1, 'alice')" })
      .expect(200);

    const selectRes = await request(app)
      .post('/query')
      .send({ sql: 'SELECT * FROM native_users WHERE id = 1' })
      .expect(200);

    expect(selectRes.body.result.rowCount).toBe(1);
    expect(selectRes.body.result.rows[0].name).toBe('alice');

    const schemaPath = path.join(tempDataDir, 'tables', 'native_users.schema.json');
    const dbPath = path.join(tempDataDir, 'tables', 'native_users.db');

    expect(fs.existsSync(schemaPath)).toBe(true);
    expect(fs.existsSync(dbPath)).toBe(true);

    const tablesRes = await request(app).get('/tables').expect(200);
    expect(tablesRes.body.tables.some((t) => t.name === 'native_users')).toBe(true);
  });

  test('removes persisted artifacts after DROP TABLE', async () => {
    await request(app)
      .post('/query')
      .send({ sql: 'CREATE TABLE native_cleanup (id INT PRIMARY KEY, name STRING)' })
      .expect(200);

    await request(app)
      .post('/query')
      .send({ sql: "INSERT INTO native_cleanup VALUES (1, 'gone')" })
      .expect(200);

    await request(app)
      .post('/query')
      .send({ sql: 'DROP TABLE native_cleanup' })
      .expect(200);

    const schemaPath = path.join(tempDataDir, 'tables', 'native_cleanup.schema.json');
    const dbPath = path.join(tempDataDir, 'tables', 'native_cleanup.db');

    expect(fs.existsSync(schemaPath)).toBe(false);
    expect(fs.existsSync(dbPath)).toBe(false);

    const tablesRes = await request(app).get('/tables').expect(200);
    expect(tablesRes.body.tables.some((t) => t.name === 'native_cleanup')).toBe(false);
  });

  test('persists secondary index definitions and updates them after drop', async () => {
    await request(app)
      .post('/query')
      .send({ sql: 'CREATE TABLE native_idx (id INT PRIMARY KEY, email STRING, age INT)' })
      .expect(200);

    await request(app)
      .post('/query')
      .send({ sql: 'CREATE UNIQUE INDEX idx_native_idx_email ON native_idx (email)' })
      .expect(200);

    const schemaPath = path.join(tempDataDir, 'tables', 'native_idx.schema.json');
    const schemaAfterCreate = JSON.parse(fs.readFileSync(schemaPath, 'utf8'));

    expect(Array.isArray(schemaAfterCreate.secondary_indexes)).toBe(true);
    expect(schemaAfterCreate.secondary_indexes).toHaveLength(1);
    expect(schemaAfterCreate.secondary_indexes[0]).toMatchObject({
      name: 'idx_native_idx_email',
      column: 'email',
      unique: true,
    });

    await request(app)
      .post('/query')
      .send({ sql: 'DROP INDEX idx_native_idx_email ON native_idx' })
      .expect(200);

    const schemaAfterDrop = JSON.parse(fs.readFileSync(schemaPath, 'utf8'));
    expect(Array.isArray(schemaAfterDrop.secondary_indexes)).toBe(true);
    expect(schemaAfterDrop.secondary_indexes).toHaveLength(0);
  });
});
