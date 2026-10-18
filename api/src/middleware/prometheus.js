'use strict';

const client = require('prom-client');

const register = new client.Registry();
client.collectDefaultMetrics({ register });

const httpRequestDuration = new client.Histogram({
  name: 'http_request_duration_seconds',
  help: 'HTTP request latency in seconds',
  // use the route pattern, not the raw URL, so label values stay limited
  labelNames: ['method', 'route', 'status_code'],
  registers: [register],
});

const buildInfo = new client.Gauge({
  name: 'arbordb_build_info',
  help: 'Build version (git SHA) of the running API',
  labelNames: ['version'],
  registers: [register],
});
buildInfo.set({ version: process.env.APP_VERSION || 'dev' }, 1);

function routeLabel(req) {
  if (req.route) return `${req.baseUrl}${req.route.path}`;
  return 'unmatched';
}

function metricsMiddleware() {
  return (req, res, next) => {
    const end = httpRequestDuration.startTimer();
    res.on('finish', () => {
      end({ method: req.method, route: routeLabel(req), status_code: res.statusCode });
    });
    next();
  };
}

async function metricsHandler(_req, res) {
  res.set('Content-Type', register.contentType);
  res.end(await register.metrics());
}

module.exports = { metricsMiddleware, metricsHandler, register };
