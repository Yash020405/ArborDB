'use strict';

// Engine caller — invokes the C++ storage engine binary via child_process.
// The engine receives a JSON command string as argv and returns JSON on stdout.

const { execFile } = require('child_process');
const path = require('path');
const { EngineError } = require('../errors');

const DEFAULT_TIMEOUT = 30000;
const MAX_RETRIES = 2;

async function callEngine(engineJson, options = {}) {
  const {
    enginePath = process.env.ENGINE_PATH || path.resolve(__dirname, '../../../engine/build/engine'),
    timeout = DEFAULT_TIMEOUT,
    retries = MAX_RETRIES,
  } = options;

  const jsonStr = JSON.stringify(engineJson);
  let lastError = null;

  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      return await executeEngine(enginePath, jsonStr, timeout);
    } catch (err) {
      lastError = err;

      // Don't retry on permanent failures
      if (err.errorCode === 'ENGINE_NOT_FOUND' || err.errorCode === 'ENGINE_PARSE_ERROR') {
        throw err;
      }

      if (attempt < retries) {
        await new Promise(resolve => setTimeout(resolve, 100 * Math.pow(2, attempt)));
      }
    }
  }

  throw lastError;
}

function executeEngine(enginePath, jsonStr, timeout) {
  return new Promise((resolve, reject) => {
    
    // Resolve DATA_DIR relative to the api directory
    const apiDir = path.resolve(__dirname, '../../');
    const baseDir = process.env.DATA_DIR 
      ? (path.isAbsolute(process.env.DATA_DIR) ? process.env.DATA_DIR : path.resolve(apiDir, process.env.DATA_DIR))
      : path.resolve(apiDir, '../data');
      
    const tablesDir = path.join(baseDir, 'tables');
    
    execFile(enginePath, [jsonStr, tablesDir], { timeout, maxBuffer: 10 * 1024 * 1024 }, (error, stdout, stderr) => {
      if (error) {
        if (error.code === 'ENOENT') {
          return reject(new EngineError(
            `Engine binary not found at: ${enginePath}`,
            { enginePath, errorCode: 'ENGINE_NOT_FOUND' }
          ));
        }
        if (error.killed) {
          return reject(new EngineError(
            `Engine execution timed out after ${timeout}ms`,
            { timeout, command: jsonStr }
          ));
        }
        return reject(new EngineError(
          `Engine execution failed: ${error.message}`,
          { stderr: stderr || '', exitCode: error.code }
        ));
      }

      try {
        const response = JSON.parse(stdout.trim());
        if (response.status === 'error' || response.error) {
          return reject(new EngineError(
            response.error || 'Engine returned error status',
            { engineResponse: response }
          ));
        }
        return resolve(response);
      } catch (parseErr) {
        return reject(new EngineError(
          'Failed to parse engine response as JSON',
          { stdout, stderr, errorCode: 'ENGINE_PARSE_ERROR' }
        ));
      }
    });
  });
}

module.exports = { callEngine };
