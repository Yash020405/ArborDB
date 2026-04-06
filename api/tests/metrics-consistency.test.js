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

describe('Metrics and Consistency', () => {
  beforeAll(() => {
    tempDataDir = path.resolve(__dirname, `../.tmp/metrics-${Date.now()}`);

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

  test('reports consistent strategy and traversal differences for indexed lookup vs scan filter', async () => {
    await request(app)
      .post('/query')
      .send({ sql: 'CREATE TABLE users (id INT PRIMARY KEY, name STRING, age INT)' })
      .expect(200);

    for (let i = 1; i <= 50; i++) {
      await request(app)
        .post('/query')
        .send({ sql: `INSERT INTO users VALUES (${i}, 'user_${i}', ${20 + i})` })
        .expect(200);
    }

    const indexedLookup = await request(app)
      .post('/query')
      .send({ sql: 'SELECT * FROM users WHERE id = 25' })
      .expect(200);

    const filteredScan = await request(app)
      .post('/query')
      .send({ sql: 'SELECT * FROM users WHERE age BETWEEN 30 AND 45' })
      .expect(200);

    expect(indexedLookup.body.optimization.strategy).toBe('primary_key_lookup');
    expect(filteredScan.body.optimization.strategy).toBe('full_scan_filter');

    expect(indexedLookup.body.metrics.nodes_traversed).toBeLessThanOrEqual(
      filteredScan.body.metrics.nodes_traversed,
    );

    expect(indexedLookup.body.result.rowCount).toBe(1);
    expect(filteredScan.body.result.rowCount).toBeGreaterThan(1);

    const metricsResponse = await request(app).get('/metrics').expect(200);

    expect(metricsResponse.body.status).toBe('ok');
    expect(metricsResponse.body.metrics.counters.totalQueries).toBeGreaterThanOrEqual(53);
    expect(metricsResponse.body.metrics.breakdown.selects).toBeGreaterThanOrEqual(2);
    expect(metricsResponse.body.engine.tablesCount).toBe(1);
  });

  test('clamps /metrics/recent limit to 100', async () => {
    const getRecentQueriesSpy = jest
      .spyOn(metricsService, 'getRecentQueries')
      .mockReturnValue([]);

    try {
      const response = await request(app)
        .get('/metrics/recent?limit=999')
        .expect(200);

      expect(response.body.status).toBe('ok');
      expect(getRecentQueriesSpy).toHaveBeenCalledWith(100);
    } finally {
      getRecentQueriesSpy.mockRestore();
    }
  });

  test('uses default /metrics/recent limit when input is invalid', async () => {
    const getRecentQueriesSpy = jest
      .spyOn(metricsService, 'getRecentQueries')
      .mockReturnValue([]);

    try {
      const response = await request(app)
        .get('/metrics/recent?limit=not-a-number')
        .expect(200);

      expect(response.body.status).toBe('ok');
      expect(getRecentQueriesSpy).toHaveBeenCalledWith(50);
    } finally {
      getRecentQueriesSpy.mockRestore();
    }
  });
});
