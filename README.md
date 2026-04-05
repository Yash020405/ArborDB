# ArborDB

ArborDB is a local database system built for learning and systems engineering practice. It combines:

- A native C++ storage engine
- A Node.js API layer for SQL parsing and execution
- A React frontend for querying and monitoring
- A Node.js CLI for interactive usage

The project focuses on correctness, observability, and clear separation between query planning and storage execution.

## Architecture

```text
Client (Frontend or CLI)
  -> API (/query, /upload, /tables, /metrics)
  -> SQL tokenizer/parser/executor/optimizer
  -> Engine bridge (mock or native)
  -> Native storage engine (B+ tree, schema, WAL log, persistence)
  -> Disk files under data/tables
```

## Current Scope

Implemented:

- Table lifecycle: `CREATE TABLE`, `DROP TABLE`
- Data operations: `INSERT`, `SELECT`, `UPDATE`, `DELETE`
- Predicate support: `=` and `BETWEEN`
- Primary-key and secondary-index aware execution paths
- CSV and XLSX upload pipeline through API
- Query metrics and recent-query tracking
- Persistent native storage on disk

Not implemented yet:

- Transaction semantics (BEGIN/COMMIT/ROLLBACK)
- Multi-user auth/session management
- Distributed replication

## Repository Layout

```text
api/        Express API, query execution, upload services, tests
cli/        Interactive CLI client
engine/     C++ engine and JS query layer
frontend/   React UI (SQL console, table browser, metrics dashboard)
data/       Persistent table/schema/WAL files
```

## Query and Execution Behavior

Supported SQL forms:

```sql
CREATE TABLE users (id INT PRIMARY KEY, name STRING, age INT);
INSERT INTO users VALUES (1, 'Alice', 30);
SELECT * FROM users WHERE id = 1;
SELECT * FROM users WHERE age BETWEEN 20 AND 40;
UPDATE users SET name = 'Alicia' WHERE id = 1;
DELETE FROM users WHERE id = 2;
DROP TABLE users;
```

Execution strategies currently used:

- `WHERE pk = value` -> primary key lookup (`search`)
- `WHERE secondary_col = value` -> secondary index lookup (`search_by_column`)
- `WHERE pk BETWEEN a AND b` -> primary key range scan (`range`)
- `WHERE non_pk BETWEEN a AND b` -> full scan with filter (`full_scan` + filter)

Indexing notes:

- There is no user-facing `CREATE INDEX` command in the current SQL grammar.
- Index usage is automatic: the planner uses primary/secondary index paths when available and falls back to full scan + filter when not available.

Notes on consistency:

- Optimizer hints are aligned with current executable engine capabilities.
- Secondary-index range scans are not executed yet; non-primary `BETWEEN` falls back to filtered full scan.

## Persistence Model

In native mode, tables are persisted under the configured data directory:

```text
data/tables/
  <table>.schema.json
  <table>.db
  wal.log
```

Behavior verified by tests:

- Create/insert/select are persisted to disk
- Drop removes schema and data artifacts
- API metadata endpoints (`/tables`, `/metrics`) reflect persisted state

## Metrics and Observability

API query responses include merged metrics:

- Parse/total timing from API
- Engine timing
- Disk reads
- Node traversals
- Rows returned / affected rows

The API also provides:

- `GET /metrics` for aggregates and engine summary
- `GET /metrics/recent` for recent query log

`/metrics` tracks and exposes differences between indexed lookup paths and scan/filter paths via traversal and timing counters.

## Prerequisites

- Node.js 18+
- npm 9+
- g++ with C++17 support

## Quick Start (Fresh Clone)

One-command setup and run:

```bash
./scripts/bootstrap.sh up
```

Other useful bootstrap commands:

```bash
./scripts/bootstrap.sh setup
./scripts/bootstrap.sh check
./scripts/bootstrap.sh api
./scripts/bootstrap.sh frontend
./scripts/bootstrap.sh cli
```

If you prefer manual setup, use the steps below.

```bash
git clone https://github.com/Yash020405/ArborDB.git
cd ArborDB

# Build native engine
cd engine
mkdir -p build
g++ -std=c++17 -O2 \
  -I./vendor -I./src \
  main.cpp \
  src/engine.cpp \
  src/wal.cpp \
  src/disk/pager.cpp \
  src/schema/schema.cpp \
  src/storage/btree.cpp \
  src/storage/secondary_index.cpp \
  src/storage/serializer.cpp \
  src/storage/table_store.cpp \
  -o build/engine

# Install dependencies
cd ../api && npm install
cd ../frontend && npm install
cd ../cli && npm install

# Start API (terminal 1)
cd ../api && npm start

# Start frontend (terminal 2)
cd ../frontend && npm run dev

# Optional: run CLI (terminal 3)
cd ../cli && node arbordb.js
```

After startup:

- API: `http://localhost:3000`
- Frontend: Vite default URL shown in terminal (usually `http://localhost:5173`)

## Setup

### 1. Build the native engine

There is currently no checked-in CMake file in `engine/`, so build with g++ directly:

```bash
cd engine
mkdir -p build

g++ -std=c++17 -O2 \
  -I./vendor -I./src \
  main.cpp \
  src/engine.cpp \
  src/wal.cpp \
  src/disk/pager.cpp \
  src/schema/schema.cpp \
  src/storage/btree.cpp \
  src/storage/secondary_index.cpp \
  src/storage/serializer.cpp \
  src/storage/table_store.cpp \
  -o build/engine
```

### 2. Install API dependencies

```bash
cd api
npm install
```

Optional API environment setup:

```bash
cp .env.example .env
```

Important environment variables:

- `USE_MOCK_ENGINE=true|false`
- `ENGINE_PATH=../engine/build/engine`
- `DATA_DIR=../data`

### 3. Install frontend dependencies

```bash
cd frontend
npm install
```

Optional frontend environment variable:

- `VITE_API_BASE_URL=http://localhost:3000`

### 4. Install CLI dependencies

```bash
cd cli
npm install
```

## Running the System

Start API:

```bash
cd api
npm start
```

Start frontend:

```bash
cd frontend
npm run dev
```

Run CLI:

```bash
cd cli
node arbordb.js
```

## Quality Gates

### Linting

API:

```bash
cd api
npm run lint
```

CLI:

```bash
cd cli
npm run lint
```

Frontend:

```bash
cd frontend
npm run lint
```

### Tests

API suite:

```bash
cd api
npm test -- --runInBand
```

Current tested areas include:

- SQL tokenizer/parser/executor
- API endpoints and error handling
- Upload and validation services
- Metrics consistency behavior
- Native persistence lifecycle (runs when native engine binary exists)

Targeted verification for error handling and indexing behavior:

```bash
cd api
npm test -- --runInBand tests/error-handling.test.js tests/executor.test.js tests/metrics-consistency.test.js
```

## API Endpoints

- `POST /query` execute SQL
- `POST /upload` upload CSV/XLSX into existing table
- `GET /tables` list tables and metadata
- `GET /tables/:name` table details
- `GET /metrics` aggregate metrics
- `GET /metrics/recent` recent query events
- `GET /health` service health

## Development Notes

- Keep parser/executor output and engine operation contract in sync.
- Prefer schema-aware command generation for stable behavior across mock/native modes.
- For native mode, ensure `engine/build/engine` is rebuilt after C++ changes.

## License

MIT
