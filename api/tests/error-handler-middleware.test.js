'use strict';

const express = require('express');
const request = require('supertest');
const { errorHandler } = require('../src/middleware/errorHandler');
const { NotFoundError } = require('../src/errors');

function createTestApp() {
  const app = express();

  app.get('/arbordb-error', (_req, _res, next) => {
    next(new NotFoundError('Resource was not found'));
  });

  app.get('/limit-file-size', (_req, _res, next) => {
    next({ code: 'LIMIT_FILE_SIZE' });
  });

  app.get('/limit-unexpected-file', (_req, _res, next) => {
    next({ code: 'LIMIT_UNEXPECTED_FILE' });
  });

  app.get('/joi-error', (_req, _res, next) => {
    next({
      isJoi: true,
      details: [
        { path: ['body', 'sql'], message: '"sql" is required' },
      ],
    });
  });

  app.get('/invalid-json', (_req, _res, next) => {
    next({ type: 'entity.parse.failed' });
  });

  app.get('/generic-error', (_req, _res, next) => {
    next(new Error('generic boom'));
  });

  app.use(errorHandler);
  return app;
}

describe('Error Handler Middleware', () => {
  const previousNodeEnv = process.env.NODE_ENV;
  let app;

  beforeAll(() => {
    app = createTestApp();
  });

  afterAll(() => {
    if (previousNodeEnv === undefined) {
      delete process.env.NODE_ENV;
    } else {
      process.env.NODE_ENV = previousNodeEnv;
    }
  });

  test('returns ArborDBError payload and status', async () => {
    const res = await request(app).get('/arbordb-error');

    expect(res.status).toBe(404);
    expect(res.body.error.code).toBe('NOT_FOUND');
    expect(res.body.error.message).toContain('not found');
  });

  test('handles multer file size limit errors', async () => {
    const res = await request(app).get('/limit-file-size');

    expect(res.status).toBe(413);
    expect(res.body.error.code).toBe('FILE_TOO_LARGE');
  });

  test('handles multer unexpected file field errors', async () => {
    const res = await request(app).get('/limit-unexpected-file');

    expect(res.status).toBe(400);
    expect(res.body.error.code).toBe('UNEXPECTED_FILE_FIELD');
  });

  test('handles Joi validation errors', async () => {
    const res = await request(app).get('/joi-error');

    expect(res.status).toBe(400);
    expect(res.body.error.code).toBe('VALIDATION_ERROR');
    expect(res.body.error.details).toHaveLength(1);
  });

  test('handles malformed JSON parse errors', async () => {
    const res = await request(app).get('/invalid-json');

    expect(res.status).toBe(400);
    expect(res.body.error.code).toBe('INVALID_JSON');
  });

  test('returns original message in non-production generic errors', async () => {
    process.env.NODE_ENV = 'development';

    const res = await request(app).get('/generic-error');

    expect(res.status).toBe(500);
    expect(res.body.error.code).toBe('INTERNAL_ERROR');
    expect(res.body.error.message).toBe('generic boom');
  });

  test('masks generic error message in production', async () => {
    process.env.NODE_ENV = 'production';

    const res = await request(app).get('/generic-error');

    expect(res.status).toBe(500);
    expect(res.body.error.code).toBe('INTERNAL_ERROR');
    expect(res.body.error.message).toBe('An internal error occurred');
  });
});
