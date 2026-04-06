'use strict';

// Global error handler middleware — catches all errors and returns
// structured JSON responses with appropriate status codes.

const { ArborDBError } = require('../errors');

function shouldLogInCurrentEnv() {
  return process.env.NODE_ENV !== 'test' || process.env.ARBORDB_TEST_LOGS === '1';
}

function errorHandler(err, req, res, _next) {
  if (err instanceof ArborDBError) {
    if (shouldLogInCurrentEnv()) {
      console.error(`[ArborDB Error] ${err.errorCode}: ${err.message}`);
    }
    return res.status(err.statusCode).json(err.toJSON());
  }

  // Multer file size limit
  if (err.code === 'LIMIT_FILE_SIZE') {
    return res.status(413).json({
      error: { code: 'FILE_TOO_LARGE', message: 'File size exceeds the maximum allowed limit', timestamp: new Date().toISOString() },
    });
  }

  if (err.code === 'LIMIT_UNEXPECTED_FILE') {
    return res.status(400).json({
      error: { code: 'UNEXPECTED_FILE_FIELD', message: 'Unexpected file field in upload', timestamp: new Date().toISOString() },
    });
  }

  // Joi validation
  if (err.isJoi) {
    return res.status(400).json({
      error: {
        code: 'VALIDATION_ERROR',
        message: 'Request validation failed',
        details: err.details.map(d => ({ field: d.path.join('.'), message: d.message })),
        timestamp: new Date().toISOString(),
      },
    });
  }

  // Malformed JSON body
  if (err.type === 'entity.parse.failed') {
    return res.status(400).json({
      error: { code: 'INVALID_JSON', message: 'Request body contains invalid JSON', timestamp: new Date().toISOString() },
    });
  }

  // Fallback
  if (shouldLogInCurrentEnv()) {
    console.error('[Unhandled Error]', err.message);
  }
  const statusCode = err.statusCode || err.status || 500;
  return res.status(statusCode).json({
    error: {
      code: 'INTERNAL_ERROR',
      message: process.env.NODE_ENV === 'production' ? 'An internal error occurred' : err.message,
      timestamp: new Date().toISOString(),
    },
  });
}

module.exports = { errorHandler };
