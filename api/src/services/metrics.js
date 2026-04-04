'use strict';

// Metrics collector — tracks per-query stats, aggregates, and recent query log.

const MAX_RECENT_QUERIES = 100;
const recentQueries = [];
const executionTimes = [];

const counters = {
  totalQueries: 0,
  totalInserts: 0,
  totalSelects: 0,
  totalCreateTables: 0,
  totalUploads: 0,
  totalErrors: 0,
  totalRowsReturned: 0,
  totalExecutionTimeMs: 0,
};

function recordQuery(entry) {
  const record = {
    ...entry,
    timestamp: new Date().toISOString(),
    status: entry.status || 'success',
  };

  counters.totalQueries++;
  counters.totalExecutionTimeMs += entry.executionTimeMs || 0;
  counters.totalRowsReturned += entry.rowsReturned || 0;

  if (entry.status === 'error') counters.totalErrors++;

  switch (entry.type) {
    case 'CREATE_TABLE': counters.totalCreateTables++; break;
    case 'INSERT': counters.totalInserts++; break;
    case 'SELECT': counters.totalSelects++; break;
  }

  if (entry.executionTimeMs !== undefined) {
    executionTimes.push(entry.executionTimeMs);
  }

  recentQueries.push(record);
  if (recentQueries.length > MAX_RECENT_QUERIES) {
    recentQueries.shift();
  }
}

function recordUpload(entry) {
  counters.totalUploads++;
  counters.totalInserts += entry.rowsInserted || 0;

  recentQueries.push({
    type: 'UPLOAD',
    ...entry,
    timestamp: new Date().toISOString(),
  });
  if (recentQueries.length > MAX_RECENT_QUERIES) {
    recentQueries.shift();
  }
}

function getMetrics() {
  const sortedTimes = [...executionTimes].sort((a, b) => a - b);

  return {
    counters: { ...counters },
    performance: {
      avgExecutionTimeMs: counters.totalQueries > 0
        ? Math.round((counters.totalExecutionTimeMs / counters.totalQueries) * 100) / 100
        : 0,
      p50ExecutionTimeMs: percentile(sortedTimes, 50),
      p95ExecutionTimeMs: percentile(sortedTimes, 95),
      p99ExecutionTimeMs: percentile(sortedTimes, 99),
      maxExecutionTimeMs: sortedTimes.length > 0 ? sortedTimes[sortedTimes.length - 1] : 0,
      minExecutionTimeMs: sortedTimes.length > 0 ? sortedTimes[0] : 0,
    },
    breakdown: {
      createTables: counters.totalCreateTables,
      inserts: counters.totalInserts,
      selects: counters.totalSelects,
      uploads: counters.totalUploads,
      errors: counters.totalErrors,
    },
    recentQueries: recentQueries.slice(-20),
  };
}

function getRecentQueries(limit = 50) {
  return recentQueries.slice(-limit);
}

function reset() {
  recentQueries.length = 0;
  executionTimes.length = 0;
  Object.keys(counters).forEach(key => { counters[key] = 0; });
}

function percentile(sortedArr, p) {
  if (sortedArr.length === 0) return 0;
  const index = Math.ceil((p / 100) * sortedArr.length) - 1;
  return Math.round(sortedArr[Math.max(0, index)] * 100) / 100;
}

module.exports = { recordQuery, recordUpload, getMetrics, getRecentQueries, reset };
