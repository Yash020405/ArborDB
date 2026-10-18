'use strict';

// ArborDB Express application setup

const fs = require('fs');
const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const rateLimit = require('express-rate-limit');
const { errorHandler } = require('./middleware/errorHandler');
const { requestLogger } = require('./middleware/requestLogger');
const { metricsMiddleware, metricsHandler } = require('./middleware/prometheus');
const engine = require('./engine');

const queryRoutes = require('./routes/query');
const uploadRoutes = require('./routes/upload');
const tablesRoutes = require('./routes/tables');
const metricsRoutes = require('./routes/metrics');

function createApp() {
  const app = express();

  // Running behind the ingress, so take the client IP from X-Forwarded-For
  app.set('trust proxy', 1);

  app.use(requestLogger());
  app.use(metricsMiddleware());

  // Health and metrics routes go before the rate limiter, otherwise
  // k8s probes and Prometheus scrapes would start getting 429s
  app.get('/health', (_req, res) => {
    res.json({
      status: 'ok',
      service: 'ArborDB API',
      version: process.env.APP_VERSION || 'dev',
      timestamp: new Date().toISOString(),
      uptime: process.uptime(),
    });
  });

  // Ready only if the data directory is writable
  app.get('/ready', (_req, res) => {
    try {
      fs.accessSync(engine.getTablesDir(), fs.constants.W_OK);
      res.json({ status: 'ready' });
    } catch (err) {
      res.status(503).json({ status: 'not_ready', reason: err.code || err.message });
    }
  });

  app.get('/metrics/prometheus', metricsHandler);

  // Security
  app.use(helmet({ contentSecurityPolicy: false }));

  app.use(cors({
    origin: process.env.CORS_ORIGIN || '*',
    methods: ['GET', 'POST', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization'],
    maxAge: 86400,
  }));

  // Rate limiting
  const limiter = rateLimit({
    windowMs: parseInt(process.env.RATE_LIMIT_WINDOW_MS || '900000', 10),
    max: parseInt(process.env.RATE_LIMIT_MAX_REQUESTS || '100', 10),
    standardHeaders: true,
    legacyHeaders: false,
    message: {
      error: {
        code: 'RATE_LIMIT_EXCEEDED',
        message: 'Too many requests, please try again later',
      },
    },
  });
  app.use(limiter);

  // Body parsing
  app.use(express.json({ limit: '1mb' }));
  app.use(express.urlencoded({ extended: true, limit: '1mb' }));

  // Routes
  app.use('/query', queryRoutes);
  app.use('/upload', uploadRoutes);
  app.use('/tables', tablesRoutes);
  app.use('/metrics', metricsRoutes);

  // 404 catch-all
  app.use((_req, res) => {
    res.status(404).json({
      error: {
        code: 'NOT_FOUND',
        message: 'The requested endpoint does not exist',
        availableEndpoints: [
          'POST /query',
          'POST /upload',
          'GET /tables',
          'GET /tables/:name',
          'GET /metrics',
          'GET /metrics/recent',
          'GET /metrics/prometheus',
          'GET /health',
          'GET /ready',
        ],
      },
    });
  });

  // Error handler must be registered last
  app.use(errorHandler);

  return app;
}

module.exports = { createApp };
