'use strict';

const fs = require('fs');
const path = require('path');
const request = require('supertest');
const { createApp } = require('../src/app');
const { resetPersistentEngineAsync, shutdownPersistentEngine } = require('../src/engine/caller');

function percentile(values, p) {
  if (!values.length) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const idx = Math.max(0, Math.ceil((p / 100) * sorted.length) - 1);
  return sorted[idx];
}

function average(values) {
  if (!values.length) return 0;
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function summarize(samples) {
  return {
    count: samples.length,
    avg: Number(average(samples).toFixed(2)),
    p50: Number(percentile(samples, 50).toFixed(2)),
    p95: Number(percentile(samples, 95).toFixed(2)),
    p99: Number(percentile(samples, 99).toFixed(2)),
    max: Number(percentile(samples, 100).toFixed(2)),
  };
}

function summarizeWorkload(runRows) {
  const clientRoundTripMs = runRows.map((r) => r.clientRoundTripMs);
  const statementMs = runRows.map((r) => r.statementMs);
  const engineBoundaryMs = runRows.map((r) => r.engineBoundaryMs);
  const engineCoreMs = runRows.map((r) => r.engineCoreMs);
  const nodesTraversed = runRows.map((r) => r.nodesTraversed);
  const diskReads = runRows.map((r) => r.diskReads);

  const rowsReturned = runRows.length ? runRows[runRows.length - 1].rowsReturned : 0;
  const strategy = runRows.length ? runRows[runRows.length - 1].strategy : null;

  return {
    strategy,
    rowsReturned,
    query_time_ms: summarize(statementMs),
    client_round_trip_ms: summarize(clientRoundTripMs),
    engine_boundary_ms: summarize(engineBoundaryMs),
    engine_core_ms: summarize(engineCoreMs),
    nodes_traversed: summarize(nodesTraversed),
    disk_reads: summarize(diskReads),
  };
}

function improvementPct(reference, improved) {
  if (!Number.isFinite(reference) || reference <= 0) return 0;
  return Number((((reference - improved) / reference) * 100).toFixed(2));
}

function pickMetrics(result) {
  const metrics = result.body.metrics || {};
  return {
    client_round_trip_ms: Number(result.clientRoundTripMs || 0),
    query_time_ms: Number(metrics.totalTimeMs || 0),
    engine_boundary_ms: Number(metrics.engineTimeMs || 0),
    engine_core_ms: Number(metrics.time_ms || 0),
    nodes_traversed: Number(metrics.nodes_traversed || 0),
    disk_reads: Number(metrics.disk_reads || 0),
  };
}

function buildCacheEffect(coldStart, steadyState) {
  return {
    client_round_trip_p50_improvement_pct: improvementPct(
      coldStart.client_round_trip_ms,
      steadyState.client_round_trip_ms.p50,
    ),
    query_time_p50_improvement_pct: improvementPct(
      coldStart.query_time_ms,
      steadyState.query_time_ms.p50,
    ),
    engine_boundary_p50_improvement_pct: improvementPct(
      coldStart.engine_boundary_ms,
      steadyState.engine_boundary_ms.p50,
    ),
  };
}

async function runEvaluation() {
  const previousDataDir = process.env.DATA_DIR;
  const previousRateLimitMaxRequests = process.env.RATE_LIMIT_MAX_REQUESTS;
  const previousNodeEnv = process.env.NODE_ENV;

  const tempDataDir = path.resolve(__dirname, `../.tmp/evaluation-${Date.now()}`);
  const rows = 1500;
  const probeId = Math.max(2, Math.floor(rows * 0.6));
  const scoreStart = 1000 + Math.max(1, Math.floor(rows * 0.3));
  const scoreEnd = scoreStart + Math.max(50, Math.floor(rows * 0.2));

  try {
    process.env.DATA_DIR = tempDataDir;
    process.env.RATE_LIMIT_MAX_REQUESTS = '200000';
    process.env.NODE_ENV = 'development';

    fs.rmSync(tempDataDir, { recursive: true, force: true });
    fs.mkdirSync(path.join(tempDataDir, 'tables'), { recursive: true });

    await resetPersistentEngineAsync('evaluation-start');
    const app = createApp();

    const query = async (sql) => {
      const startedAt = process.hrtime.bigint();
      const res = await request(app).post('/query').send({ sql });
      const endedAt = process.hrtime.bigint();

      if (res.status !== 200) {
        throw new Error(`Query failed (${res.status}): ${sql} :: ${JSON.stringify(res.body)}`);
      }

      const clientRoundTripMs = Number((Number(endedAt - startedAt) / 1_000_000).toFixed(3));
      return { body: res.body, clientRoundTripMs };
    };

    await query('CREATE TABLE bench_users (id INT PRIMARY KEY, email STRING, age INT, score INT)');

    for (let i = 1; i <= rows; i++) {
      await query(`INSERT INTO bench_users VALUES (${i}, 'user_${i}@arbordb.local', ${18 + (i % 50)}, ${1000 + (i % 9000)})`);
    }

    await query('CREATE INDEX idx_bench_users_email ON bench_users (email)');

    const workloads = [
      {
        name: 'pk_point_lookup',
        sql: `SELECT * FROM bench_users WHERE id = ${probeId}`,
        warmup: 5,
        iterations: 30,
      },
      {
        name: 'secondary_index_lookup',
        sql: `SELECT * FROM bench_users WHERE email = 'user_${probeId}@arbordb.local'`,
        warmup: 5,
        iterations: 30,
      },
      {
        name: 'non_index_scan_filter',
        sql: `SELECT * FROM bench_users WHERE score BETWEEN ${scoreStart} AND ${scoreEnd}`,
        warmup: 5,
        iterations: 25,
      },
      {
        name: 'order_by_limit',
        sql: `SELECT id, score FROM bench_users WHERE id BETWEEN 1 AND ${rows} ORDER BY score DESC LIMIT 20`,
        warmup: 3,
        iterations: 20,
      },
    ];

    const workloadResults = {};

    for (const workload of workloads) {
      // Capture cold-start behavior by cycling worker before first sample.
      await resetPersistentEngineAsync(`cold-start-${workload.name}`);
      const coldResult = await query(workload.sql);
      const coldStart = pickMetrics(coldResult);

      for (let i = 0; i < workload.warmup; i++) {
        await query(workload.sql);
      }

      const rowsOut = [];
      for (let i = 0; i < workload.iterations; i++) {
        const result = await query(workload.sql);
        const metrics = result.body.metrics || {};

        rowsOut.push({
          clientRoundTripMs: result.clientRoundTripMs,
          statementMs: Number(metrics.totalTimeMs || 0),
          engineBoundaryMs: Number(metrics.engineTimeMs || 0),
          engineCoreMs: Number(metrics.time_ms || 0),
          nodesTraversed: Number(metrics.nodes_traversed || 0),
          diskReads: Number(metrics.disk_reads || 0),
          rowsReturned: Number(result.body.result ? result.body.result.rowCount : 0),
          strategy: result.body.optimization ? result.body.optimization.strategy : null,
        });
      }

      const steadyState = summarizeWorkload(rowsOut);
      workloadResults[workload.name] = {
        cold_start: coldStart,
        steady_state: steadyState,
        cache_effect: buildCacheEffect(coldStart, steadyState),
      };
    }

    const metricsRes = await request(app).get('/metrics');
    const reportedMode = metricsRes.body && metricsRes.body.engine ? metricsRes.body.engine.mode : 'unknown';

    return {
      generatedAt: new Date().toISOString(),
      engineMode: reportedMode,
      profile: 'full',
      dataset: {
        rows,
        table: 'bench_users',
      },
      notes: {
        query_time_ms: 'Server-side statement duration measured by API query handler.',
        client_round_trip_ms: 'End-to-end application observed latency.',
        engine_core_ms: 'Native execution core time from engine metrics.',
      },
      workloads: workloadResults,
    };
  } finally {
    await resetPersistentEngineAsync('evaluation-end');
    await shutdownPersistentEngine(1500);
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

    if (previousNodeEnv === undefined) {
      delete process.env.NODE_ENV;
    } else {
      process.env.NODE_ENV = previousNodeEnv;
    }
  }
}

async function main() {
  const originalConsole = {
    log: console.log,
    warn: console.warn,
    error: console.error,
  };

  if (process.env.BENCHMARK_VERBOSE !== '1') {
    console.log = () => {};
    console.warn = () => {};
    console.error = () => {};
  }

  try {
    const results = await runEvaluation();
    originalConsole.log(JSON.stringify(results, null, 2));
  } finally {
    console.log = originalConsole.log;
    console.warn = originalConsole.warn;
    console.error = originalConsole.error;
  }
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
