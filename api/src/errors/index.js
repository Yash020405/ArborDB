'use strict';

// Custom error classes for structured error handling across the API.

class ArborDBError extends Error {
  constructor(message, statusCode = 500, errorCode = 'INTERNAL_ERROR', details = null) {
    super(message);
    this.name = this.constructor.name;
    this.statusCode = statusCode;
    this.errorCode = errorCode;
    this.details = details;
    this.timestamp = new Date().toISOString();

    if (Error.captureStackTrace) {
      Error.captureStackTrace(this, this.constructor);
    }
  }

  toJSON() {
    const json = {
      error: {
        code: this.errorCode,
        message: this.message,
        timestamp: this.timestamp,
      },
    };
    if (this.details) {
      json.error.details = this.details;
    }
    return json;
  }
}

class ParseError extends ArborDBError {
  constructor(message, details = null) {
    super(message, 400, 'PARSE_ERROR', details);
  }
}

class QueryValidationError extends ArborDBError {
  constructor(message, details = null) {
    super(message, 400, 'QUERY_VALIDATION_ERROR', details);
  }
}

class QueryTokenizeError extends ArborDBError {
  constructor(message, details = null) {
    super(message, 400, 'QUERY_TOKENIZE_ERROR', details);
  }
}

class QueryParseError extends ArborDBError {
  constructor(message, details = null) {
    super(message, 400, 'QUERY_PARSE_ERROR', details);
  }
}

class QueryPlanError extends ArborDBError {
  constructor(message, details = null) {
    super(message, 422, 'QUERY_PLAN_ERROR', details);
  }
}

class QueryExecutionError extends ArborDBError {
  constructor(message, details = null) {
    super(message, 500, 'QUERY_EXECUTION_ERROR', details);
  }
}

class ValidationError extends ArborDBError {
  constructor(message, details = null) {
    super(message, 422, 'VALIDATION_ERROR', details);
  }
}

class EngineError extends ArborDBError {
  constructor(message, details = null, errorCode = 'ENGINE_ERROR', statusCode = 502) {
    super(message, statusCode, errorCode, details);
  }
}

class UploadError extends ArborDBError {
  constructor(message, details = null) {
    super(message, 400, 'UPLOAD_ERROR', details);
  }
}

class NotFoundError extends ArborDBError {
  constructor(message, details = null) {
    super(message, 404, 'NOT_FOUND', details);
  }
}

class RateLimitError extends ArborDBError {
  constructor(message = 'Too many requests, please try again later') {
    super(message, 429, 'RATE_LIMIT_EXCEEDED');
  }
}

module.exports = {
  ArborDBError,
  ParseError,
  QueryValidationError,
  QueryTokenizeError,
  QueryParseError,
  QueryPlanError,
  QueryExecutionError,
  ValidationError,
  EngineError,
  UploadError,
  NotFoundError,
  RateLimitError,
};
