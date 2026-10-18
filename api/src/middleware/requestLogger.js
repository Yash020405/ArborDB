'use strict';

// Logs each request as one JSON line with a request ID

const crypto = require('crypto');

function requestLogger() {
  return (req, res, next) => {
    const startTime = Date.now();
    const requestId = req.get('x-request-id') || crypto.randomUUID();
    req.id = requestId;
    res.set('X-Request-ID', requestId);

    res.on('finish', () => {
      if (process.env.NODE_ENV === 'test' && process.env.ARBORDB_TEST_LOGS !== '1') return;

      const { statusCode } = res;
      console.log(JSON.stringify({
        time: new Date().toISOString(),
        level: statusCode >= 500 ? 'error' : statusCode >= 400 ? 'warn' : 'info',
        requestId,
        method: req.method,
        path: req.originalUrl,
        status: statusCode,
        durationMs: Date.now() - startTime,
        version: process.env.APP_VERSION || 'dev',
      }));
    });

    next();
  };
}

module.exports = { requestLogger };
