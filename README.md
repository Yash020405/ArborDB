# 🌳 ArborDB

### A Mini MySQL-like Database Engine Built from Scratch

---

## 🚀 Overview

**ArborDB** is a lightweight, extensible database engine inspired by real-world systems like MySQL and PostgreSQL.
It is designed to demonstrate how databases work internally — from storage engines and indexing to query execution and data visualization.

> 🌳 *“Arbor” (Latin for tree) reflects the core of the system — a B+ Tree–based storage engine.*

---

## 🎯 Key Features

### 🧱 Core Database Engine

* B+ Tree–based primary index
* Key-value storage model (Primary Key → Row)
* Efficient **O(log n)** search and insert
* Range queries via linked leaf nodes

---

### 🔍 Query Engine

* SQL-like query support:

  * `CREATE TABLE`
  * `INSERT`
  * `SELECT`
  * `WHERE (=, BETWEEN)`
* Custom-built parser + executor

---

### 📊 Interfaces

* 🖥️ CLI for developers
* 🌐 GUI (React) for interactive usage
* 📈 Dashboard for metrics & monitoring

---

### 📂 Data Import

* CSV upload
* Excel (.xlsx) support
* JSON ingestion (optional)
* Column mapping + preview before insert

---

### 📈 Metrics & Observability

* Query execution time
* Disk reads/writes
* Rows scanned
* Index usage

---

### 💾 Persistence

* Disk-backed storage
* Page-based system (like real DBs)
* Serialized B+ Tree nodes

---

### ⚡ Advanced (Planned)

* Secondary indexes
* Transactions (WAL)
* Concurrency control
* Buffer pool caching

---

## 🧠 Architecture

```
Frontend (React GUI)
        ↓
API Layer (Node.js)
        ↓
Query Engine (Parser + Executor)
        ↓
Storage Engine (B+ Tree)
        ↓
Disk (Page-based storage)
```

---

## 🧱 System Components

---

### 1. Storage Engine (C++)

* B+ Tree implementation
* Node splitting & balancing
* Leaf node linking for range queries

---

### 2. Disk Layer

* Page-based storage (e.g., 4KB pages)
* Serialization / deserialization
* File-backed persistence

---

### 3. Query Engine

* Tokenizer
* SQL parser (AST)
* Execution engine

---

### 4. API Layer (Node.js)

* Query execution endpoint
* File upload handling
* Communication bridge with engine

---

### 5. Frontend (React)

* Query console
* Table viewer
* File upload UI
* Metrics dashboard

---

## 📂 Repository Structure

```
arbor-db/
├── engine/        # C++ core engine
├── api/           # Node.js backend
├── frontend/      # React frontend
├── data/          # Stored database files
├── docs/          # Design docs / PRD
└── scripts/       # Setup scripts
```

---

## ⚙️ Tech Stack

| Layer          | Technology         |
| -------------- | ------------------ |
| Storage Engine | C++ (C++17/20)     |
| API            | Node.js + Express  |
| Frontend       | React + TypeScript |
| Build Tools    | CMake, Ninja       |
| File Parsing   | papaparse, xlsx    |

---

## 🔄 Data Flow

### Insert Flow

```
GUI → API → Query Engine → Storage Engine → Disk
```

### Query Flow

```
User Query → Parser → Executor → B+ Tree → Result
```

### CSV Upload Flow

```
Upload → Parse → Validate → Batch Insert → Index Update
```

---

## 📊 Supported Queries

```sql
CREATE TABLE users (id INT, name STRING);

INSERT INTO users VALUES (1, "Yash");

SELECT * FROM users WHERE id = 1;

SELECT * FROM users WHERE id BETWEEN 10 AND 20;
```

---

## 📦 Data Model

```
Primary Key → Serialized Row
```

Example:

```
1 → { id: 1, name: "Yash" }
```

---

## 📁 Storage Layout

```
data/
├── users/
│   ├── primary_index.db
│   ├── secondary_index_name.db
│
├── schema.json
├── wal.log
```

---

## 🧪 Testing Strategy

### Engine Tests

* B+ Tree correctness
* Insert/search/range

### API Tests

* Query execution
* Upload endpoints

### Frontend Tests

* UI interactions
* API integration

---

## ⚠️ Edge Cases Handled

* Duplicate primary keys
* Invalid schema
* File upload errors
* Disk write failures
* Partial writes / crashes

---

## 🔐 Constraints

* ❌ No external database usage
* ✅ File system–based storage
* ✅ Custom indexing implementation

---

## 📅 Development Roadmap

### Phase 1 (Core Engine)

* B+ Tree
* Insert/Search/Range

### Phase 2 (Persistence + CLI)

* Disk storage
* CLI interface

### Phase 3 (Query Engine)

* SQL parser
* Execution engine

### Phase 4 (GUI + Upload)

* React UI
* CSV/Excel ingestion

### Phase 5 (Enhancements)

* Secondary indexes
* Metrics dashboard

### Phase 6 (Advanced)

* Transactions (WAL)
* Concurrency control

---

## 👨‍💻 Team Responsibilities

| Role         | Responsibility |
| ------------ | -------------- |
| Engine Dev   | B+ Tree + Disk |
| Backend Dev  | API + Upload   |
| Frontend Dev | UI + Dashboard |
| Systems Dev  | Query Engine   |

---

## 🛠️ Setup Instructions

### 1. Clone Repo

```bash
git clone https://github.com/your-username/arbor-db.git
cd arbor-db
```

---

### 2. Build Engine

```bash
cd engine
mkdir build && cd build
cmake .. && make
```

---

### 3. Run API

```bash
cd ../../api
npm install
npm start
```

---

### 4. Run Frontend

```bash
cd ../frontend
npm install
npm run dev
```

---

## 📈 Example Output

```
Query OK
Rows Returned: 10
Execution Time: 0.003 sec
Disk Reads: 2
```

---

## 🚀 Future Scope

* Query optimizer
* Index selection strategies
* Distributed storage
* Replication
* Cloud deployment

---

## 💡 Inspiration

* MySQL (InnoDB)
* PostgreSQL
* LevelDB / RocksDB

---

## 🧾 Resume Description

> Built a disk-backed database engine with B+ Tree indexing, supporting SQL-like queries, range scans, and real-time performance metrics, inspired by MySQL architecture.

---

## ⭐ Final Note

ArborDB is not just a project — it’s a **deep dive into how databases actually work under the hood**.

---

**🌳 Grow your own database. Understand it. Control it.**
