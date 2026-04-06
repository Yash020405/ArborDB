'use strict';

// Request logger middleware — logs method, path, status, and response time.

function requestLogger() {
  return (req, res, next) => {
    const startTime = Date.now();

    const originalEnd = res.end;
    res.end = function (...args) {
      const duration = Date.now() - startTime;
      const { method, path } = req;
      const { statusCode } = res;

      if (process.env.NODE_ENV === 'test' && process.env.ARBORDB_TEST_LOGS !== '1') {
        originalEnd.apply(res, args);
        return;
      }

      const logFn = statusCode >= 500 ? console.error : statusCode >= 400 ? console.warn : console.log;
      logFn(`[${new Date().toISOString()}] ${method} ${path} → ${statusCode} (${duration}ms)`);

      originalEnd.apply(res, args);
    };

    next();
  };
}

module.exports = { requestLogger };
