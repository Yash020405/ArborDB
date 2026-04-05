'use strict';

// Engine caller — maintains a persistent C++ worker and sends JSON commands
// over stdin/stdout for low-latency execution.

const { spawn } = require('child_process');
const path = require('path');
const { EngineError } = require('../errors');

const DEFAULT_TIMEOUT = 30000;
const MAX_RETRIES = 2;
const MAX_STDERR_BYTES = 4096;

let persistentWorker = null;

function getEnginePath(options = {}) {
  return options.enginePath || process.env.ENGINE_PATH || path.resolve(__dirname, '../../../engine/build/engine');
}

function getTablesDir() {
  const apiDir = path.resolve(__dirname, '../../');
  const baseDir = process.env.DATA_DIR
    ? (path.isAbsolute(process.env.DATA_DIR) ? process.env.DATA_DIR : path.resolve(apiDir, process.env.DATA_DIR))
    : path.resolve(apiDir, '../data');

  return path.join(baseDir, 'tables');
}

function isPermanentFailure(err) {
  return err.errorCode === 'ENGINE_NOT_FOUND' || err.errorCode === 'ENGINE_PARSE_ERROR';
}

function trimStderr(buf) {
  if (!buf) return '';
  if (buf.length <= MAX_STDERR_BYTES) return buf;
  return buf.slice(buf.length - MAX_STDERR_BYTES);
}

function failWorkerQueue(worker, err) {
  worker.exited = true;

  for (const pending of worker.queue.splice(0)) {
    clearTimeout(pending.timer);
    pending.reject(err);
  }

  if (persistentWorker && persistentWorker.key === worker.key) {
    persistentWorker = null;
  }
}

async function stopWorker(worker, reason = 'manual-stop', timeoutMs = 2000) {
  if (!worker) return;

  worker.intentionalShutdown = true;

  const resetError = new EngineError(
    'Persistent engine worker reset',
    { reason },
    'ENGINE_WORKER_RESET',
    503
  );
  failWorkerQueue(worker, resetError);

  if (worker.process && !worker.process.killed) {
    worker.process.kill('SIGTERM');
  }

  if (!worker.exitPromise) {
    return;
  }

  let timer = null;
  const timeoutPromise = new Promise((resolve) => {
    timer = setTimeout(() => {
      if (worker.process && !worker.process.killed) {
        worker.process.kill('SIGKILL');
      }
      resolve();
    }, timeoutMs);
  });

  await Promise.race([worker.exitPromise, timeoutPromise]);
  if (timer) {
    clearTimeout(timer);
  }
}

function parseEngineResponse(stdout, stderr) {
  try {
    const response = JSON.parse(stdout.trim());
    if (response.status === 'error' || response.error) {
      throw new EngineError(
        response.error || 'Engine returned error status',
        { engineResponse: response },
        'ENGINE_OPERATION_FAILED',
        502
      );
    }
    return response;
  } catch (err) {
    if (err instanceof EngineError) {
      throw err;
    }

    throw new EngineError(
      'Failed to parse engine response as JSON',
      { stdout, stderr },
      'ENGINE_PARSE_ERROR',
      502
    );
  }
}

function ensurePersistentWorker(enginePath) {
  const tablesDir = getTablesDir();
  const key = `${enginePath}::${tablesDir}`;

  if (persistentWorker && !persistentWorker.exited && persistentWorker.key === key) {
    return persistentWorker;
  }

  if (persistentWorker && !persistentWorker.exited && persistentWorker.key !== key) {
    resetPersistentEngine('reconfigured');
  }

  const child = spawn(enginePath, ['--server', tablesDir], {
    stdio: ['pipe', 'pipe', 'pipe'],
  });

  const worker = {
    key,
    process: child,
    queue: [],
    stdoutBuffer: '',
    stderrBuffer: '',
    exited: false,
    intentionalShutdown: false,
    exitResolve: null,
    exitPromise: null,
  };

  worker.exitPromise = new Promise((resolve) => {
    worker.exitResolve = resolve;
  });

  child.stdout.setEncoding('utf8');
  child.stderr.setEncoding('utf8');

  child.stdout.on('data', (chunk) => {
    worker.stdoutBuffer += chunk;

    while (true) {
      const newlineIndex = worker.stdoutBuffer.indexOf('\n');
      if (newlineIndex === -1) break;

      const line = worker.stdoutBuffer.slice(0, newlineIndex).trim();
      worker.stdoutBuffer = worker.stdoutBuffer.slice(newlineIndex + 1);

      if (!line) continue;

      const pending = worker.queue.shift();
      if (!pending) continue;

      clearTimeout(pending.timer);

      try {
        const parsed = parseEngineResponse(line, worker.stderrBuffer);
        pending.resolve(parsed);
      } catch (err) {
        pending.reject(err);
      }
    }
  });

  child.stderr.on('data', (chunk) => {
    worker.stderrBuffer = trimStderr(worker.stderrBuffer + chunk);
  });

  child.on('error', (error) => {
    const mapped = new EngineError(
      `Persistent engine failed to start: ${error.message}`,
      { enginePath, errorCode: error.code },
      error.code === 'ENOENT' ? 'ENGINE_NOT_FOUND' : 'ENGINE_EXECUTION_FAILED',
      error.code === 'ENOENT' ? 500 : 502
    );
    failWorkerQueue(worker, mapped);
  });

  child.on('exit', (code, signal) => {
    if (typeof worker.exitResolve === 'function') {
      worker.exitResolve({ code, signal });
      worker.exitResolve = null;
    }

    if (worker.intentionalShutdown) {
      worker.exited = true;
      if (persistentWorker && persistentWorker.key === worker.key) {
        persistentWorker = null;
      }
      return;
    }

    const mapped = new EngineError(
      'Persistent engine worker exited unexpectedly',
      { code, signal, stderr: worker.stderrBuffer },
      'ENGINE_WORKER_EXITED',
      502
    );
    failWorkerQueue(worker, mapped);
  });

  persistentWorker = worker;
  return worker;
}

function executeEnginePersistent(enginePath, jsonStr, timeout) {
  return new Promise((resolve, reject) => {
    const worker = ensurePersistentWorker(enginePath);

    if (worker.exited || !worker.process || worker.process.killed) {
      return reject(new EngineError(
        'Persistent engine worker is unavailable',
        null,
        'ENGINE_WORKER_UNAVAILABLE',
        502
      ));
    }

    const pending = {
      resolve,
      reject,
      timer: null,
    };

    pending.timer = setTimeout(() => {
      const index = worker.queue.indexOf(pending);
      if (index >= 0) {
        worker.queue.splice(index, 1);
      }

      reject(new EngineError(
        `Engine execution timed out after ${timeout}ms`,
        { timeout, command: jsonStr },
        'ENGINE_TIMEOUT',
        504
      ));

      resetPersistentEngine('timeout');
    }, timeout);

    worker.queue.push(pending);

    worker.process.stdin.write(`${jsonStr}\n`, 'utf8', (error) => {
      if (!error) return;

      clearTimeout(pending.timer);
      const index = worker.queue.indexOf(pending);
      if (index >= 0) {
        worker.queue.splice(index, 1);
      }

      reject(new EngineError(
        `Engine execution failed: ${error.message}`,
        { stderr: worker.stderrBuffer, exitCode: error.code },
        'ENGINE_EXECUTION_FAILED',
        502
      ));

      resetPersistentEngine('stdin-error');
    });
  });
}

async function callEngine(engineJson, options = {}) {
  const enginePath = getEnginePath(options);
  const timeout = options.timeout || DEFAULT_TIMEOUT;
  const retries = options.retries !== undefined ? options.retries : MAX_RETRIES;

  const jsonStr = JSON.stringify(engineJson);
  let lastError = null;

  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      return await executeEnginePersistent(enginePath, jsonStr, timeout);
    } catch (err) {
      lastError = err;

      // Don't retry on permanent failures
      if (isPermanentFailure(err)) {
        throw err;
      }

      if (attempt < retries) {
        await new Promise(resolve => setTimeout(resolve, 100 * Math.pow(2, attempt)));
      }
    }
  }

  throw lastError;
}

function resetPersistentEngine(reason = 'manual-reset') {
  resetPersistentEngineAsync(reason).catch(() => {});
}

async function resetPersistentEngineAsync(reason = 'manual-reset', timeoutMs = 2000) {
  if (!persistentWorker) return;

  const worker = persistentWorker;
  persistentWorker = null;

  await stopWorker(worker, reason, timeoutMs);
}

async function shutdownPersistentEngine(timeoutMs = 2000) {
  await resetPersistentEngineAsync('shutdown', timeoutMs);
}

function getEngineExecutionMode() {
  return 'persistent-worker';
}

module.exports = {
  callEngine,
  resetPersistentEngine,
  resetPersistentEngineAsync,
  shutdownPersistentEngine,
  getEngineExecutionMode,
};
