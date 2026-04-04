'use strict';

// Batch insert — inserts rows into a table in configurable batches,
// continuing on individual failures and collecting errors.

const engine = require('../engine');

const DEFAULT_BATCH_SIZE = 100;

async function batchInsert(tableName, rows, schema, options = {}) {
  const {
    primaryKey = Object.keys(schema)[0],
    batchSize = DEFAULT_BATCH_SIZE,
    stopOnError = false,
  } = options;

  const results = {
    insertedCount: 0,
    failedCount: 0,
    errors: [],
    totalRows: rows.length,
    batchesProcessed: 0,
  };

  const startTime = Date.now();

  for (let i = 0; i < rows.length; i += batchSize) {
    const batch = rows.slice(i, i + batchSize);
    results.batchesProcessed++;

    for (let j = 0; j < batch.length; j++) {
      const row = batch[j];
      const rowIndex = i + j;

      try {
        const key = row[primaryKey];
        if (key === null || key === undefined) {
          throw new Error(`Missing primary key '${primaryKey}'`);
        }

        await engine.callEngine({
          operation: 'insert',
          table: tableName,
          key,
          data: row,
        });
        results.insertedCount++;
      } catch (err) {
        results.failedCount++;
        results.errors.push({ rowIndex, row, error: err.message });

        if (stopOnError) {
          results.stoppedAt = rowIndex;
          break;
        }
      }
    }

    if (stopOnError && results.failedCount > 0) break;
  }

  results.executionTimeMs = Date.now() - startTime;
  return results;
}

module.exports = { batchInsert };
