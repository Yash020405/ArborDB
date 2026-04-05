'use strict';

const fs = require('fs');
const path = require('path');
const request = require('supertest');
const { createApp } = require('../src/app');
const engine = require('../src/engine');
const metricsService = require('../src/services/metrics');

function average(values) {
  if (!values.length) return 0;
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function percentile(values, p) {
  if (!values.length) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const idx = Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length));
  return sorted[idx];
}

async function run() {
  const previousDataDir = process.env.DATA_DIR;
  const previousRateLimit = process.env.RATE_LIMIT_MAX_REQUESTS;
  const previousNodeEnv = process.env.NODE_ENV;

  const tempDataDir = path.resolve(__dirname, `../.tmp/bench-${Date.now()}`);
  const originalConsole = {
    log: console.log,
    warn: console.warn,
    error: console.error,
  };
  const quietLogs = process.env.BENCHMARK_VERBOSE !== '1';

  try {
    process.env.DATA_DIR = tempDataDir;
    process.env.RATE_LIMIT_MAX_REQUESTS = '100000';
    process.env.NODE_ENV = 'test';

    fs.mkdirSync(path.join(tempDataDir, 'tables'), { recursive: true });

    if (quietLogs) {
      console.log = () => {};
      console.warn = () => {};
      console.error = () => {};
    }

    const app = createApp();
    engine.reset();
    metricsService.reset();

    const query = async (sql) => {
      const res = await request(app).post('/query').send({ sql });
      if (res.status !== 200) {
        throw new Error(`Query failed (${res.status}): ${sql} :: ${JSON.stringify(res.body)}`);
      }
      return res.body;
    };

    const totalRows = 750;
    await query('CREATE TABLE perf_users (id INT PRIMARY KEY, email STRING, age INT)');

    for (let i = 1; i <= totalRows; i++) {
      await query(`INSERT INTO perf_users VALUES (${i}, 'user_${i}@arbordb.local', ${18 + (i % 50)})`);
    }

    const targetSql = "SELECT * FROM perf_users WHERE email = 'user_615@arbordb.local'";

    // Warm-up non-index path.
    await query(targetSql);

    const runBatch = async (sql, iterations) => {
      const nodesTraversed = [];
      const totalTimeMs = [];
      const engineTimeMs = [];
      let strategy = null;

      for (let i = 0; i < iterations; i++) {
        const res = await query(sql);
        strategy = res.optimization ? res.optimization.strategy : strategy;
        nodesTraversed.push(Number(res.metrics.nodes_traversed || 0));
        totalTimeMs.push(Number(res.metrics.totalTimeMs || 0));
        engineTimeMs.push(Number(res.metrics.engineTimeMs || 0));
      }

      return {
        iterations,
        strategy,
        avgNodesTraversed: Number(average(nodesTraversed).toFixed(2)),
        p95NodesTraversed: Number(percentile(nodesTraversed, 95).toFixed(2)),
        avgTotalTimeMs: Number(average(totalTimeMs).toFixed(2)),
        p95TotalTimeMs: Number(percentile(totalTimeMs, 95).toFixed(2)),
        avgEngineTimeMs: Number(average(engineTimeMs).toFixed(2)),
        p95EngineTimeMs: Number(percentile(engineTimeMs, 95).toFixed(2)),
      };
    };

    const nonIndexed = await runBatch(targetSql, 20);

    await query('CREATE INDEX idx_perf_users_email ON perf_users (email)');

    // Warm-up indexed path.
    await query(targetSql);

    const indexed = await runBatch(targetSql, 20);

    const summary = {
      dataset: {
        table: 'perf_users',
        rows: totalRows,
        predicate: "email = 'user_615@arbordb.local'",
      },
      nonIndexed,
      indexed,
      improvement: {
        traversalReductionPct: Number(
          (((nonIndexed.avgNodesTraversed - indexed.avgNodesTraversed) / Math.max(nonIndexed.avgNodesTraversed, 1)) * 100).toFixed(2)
        ),
        totalTimeReductionPct: Number(
          (((nonIndexed.avgTotalTimeMs - indexed.avgTotalTimeMs) / Math.max(nonIndexed.avgTotalTimeMs, 0.0001)) * 100).toFixed(2)
        ),
      },
      generatedAt: new Date().toISOString(),
    };

    originalConsole.log(JSON.stringify(summary, null, 2));
  } finally {
    console.log = originalConsole.log;
    console.warn = originalConsole.warn;
    console.error = originalConsole.error;

    fs.rmSync(tempDataDir, { recursive: true, force: true });

    if (previousDataDir === undefined) {
      delete process.env.DATA_DIR;
    } else {
      process.env.DATA_DIR = previousDataDir;
    }

    if (previousRateLimit === undefined) {
      delete process.env.RATE_LIMIT_MAX_REQUESTS;
    } else {
      process.env.RATE_LIMIT_MAX_REQUESTS = previousRateLimit;
    }

    if (previousNodeEnv === undefined) {
      delete process.env.NODE_ENV;
    } else {
      process.env.NODE_ENV = previousNodeEnv;
    }
  }
}

run().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
