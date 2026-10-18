# Build from repo root: docker build -f api/Dockerfile .

FROM node:22.23.3-trixie-slim AS engine
RUN apt-get update \
 && apt-get install -y --no-install-recommends g++ make \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /src/engine
COPY engine/ ./
RUN make build/engine

FROM node:22.23.3-trixie-slim AS deps
WORKDIR /app/api
COPY api/package.json api/package-lock.json ./
RUN npm ci --omit=dev --no-audit --no-fund

FROM node:22.23.3-trixie-slim
ARG APP_VERSION=dev
ENV NODE_ENV=production \
    PORT=3000 \
    ENGINE_PATH=/app/engine/engine \
    DATA_DIR=/data \
    APP_VERSION=${APP_VERSION}
LABEL org.opencontainers.image.source="https://github.com/Yash020405/ArborDB" \
      org.opencontainers.image.revision="${APP_VERSION}"

WORKDIR /app/api
COPY --from=deps /app/api/node_modules ./node_modules
COPY api/package.json ./
COPY api/src ./src
# the API requires the SQL parser from engine/src/query
COPY engine/src/query /app/engine/src/query
COPY --from=engine /src/engine/build/engine /app/engine/engine
RUN mkdir -p /data/tables && chown -R 1000:1000 /data

# node user
USER 1000
EXPOSE 3000
CMD ["node", "src/server.js"]
