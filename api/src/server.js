'use strict';

// ArborDB API Server entry point

require('dotenv').config();

const { createApp } = require('./app');
const engine = require('./engine');

const PORT = parseInt(process.env.PORT || '3000', 10);
const NODE_ENV = process.env.NODE_ENV || 'development';

const app = createApp();

const server = app.listen(PORT, () => {
  console.log('');
  console.log('  ArborDB API Server');
  console.log('  ─────────────────────────────────────');
  console.log(`  Environment : ${NODE_ENV}`);
  console.log(`  Port        : ${PORT}`);
  console.log(`  Engine      : ${engine.useMockEngine() ? 'Mock (in-memory)' : 'Native (C++)'}`);
  console.log(`  URL         : http://localhost:${PORT}`);
  console.log('');
  console.log('  Endpoints:');
  console.log(`    POST  http://localhost:${PORT}/query`);
  console.log(`    POST  http://localhost:${PORT}/upload`);
  console.log(`    GET   http://localhost:${PORT}/tables`);
  console.log(`    GET   http://localhost:${PORT}/metrics`);
  console.log(`    GET   http://localhost:${PORT}/health`);
  console.log('');
});

function gracefulShutdown(signal) {
  console.log(`\n  Received ${signal}. Shutting down...`);
  server.close(() => {
    console.log('  Server closed.');
    process.exit(0);
  });

  // Force exit after 10s if connections are still hanging
  setTimeout(() => {
    console.error('  Forced shutdown after timeout');
    process.exit(1);
  }, 10000);
}

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

process.on('unhandledRejection', (reason) => {
  console.error('Unhandled Rejection:', reason);
});

process.on('uncaughtException', (error) => {
  console.error('Uncaught Exception:', error);
  process.exit(1);
});

module.exports = { app, server };
