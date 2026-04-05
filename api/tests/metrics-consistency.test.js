'use strict';

const request = require('supertest');
const { createApp } = require('../src/app');
const engine = require('../src/engine');
const metricsService = require('../src/services/metrics');

const app = createApp();

describe('Metrics and Consistency', () => {
  beforeEach(() => {
    process.env.USE_MOCK_ENGINE = 'true';
    engine.reset();
    metricsService.reset();
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
});
