#include "table_store.h"
#include <stdexcept>

namespace arbor {

TableStore::TableStore(const std::string& dataDir)
    : dataDir_(dataDir), schemaManager_(dataDir) {}

BTree* TableStore::getOrLoadTree(const std::string& tableName) {
    auto it = trees_.find(tableName);
    if (it != trees_.end()) {
        return it->second.get();
    }
    if (!schemaManager_.tableExists(tableName)) {
        throw std::runtime_error("Table does not exist: " + tableName);
    }
    schemaManager_.loadTable(tableName);
    trees_[tableName] = std::make_unique<BTree>();
    return trees_[tableName].get();
}

SecondaryIndex& TableStore::getOrCreateIndex(const std::string& tableName, const TableSchema& schema) {
    auto it = secondaryIndexes_.find(tableName);
    if (it != secondaryIndexes_.end()) {
        return it->second;
    }
    SecondaryIndex idx;
    for (const auto& col : schema.columns) {
        if (col.name != schema.primaryKey) {
            idx.addColumn(col.name);
        }
    }
    secondaryIndexes_[tableName] = std::move(idx);
    return secondaryIndexes_[tableName];
}

std::string TableStore::columnValueToString(const nlohmann::json& value) const {
    if (value.is_string()) return value.get<std::string>();
    return value.dump();
}

void TableStore::validateRow(const TableSchema& schema, const nlohmann::json& row) const {
    for (const auto& col : schema.columns) {
        if (!row.contains(col.name)) {
            throw std::runtime_error("Missing column in row: " + col.name);
        }
        switch (col.type) {
            case ColumnType::INT:
                if (!row[col.name].is_number_integer())
                    throw std::runtime_error("Column '" + col.name + "' expects INT");
                break;
            case ColumnType::STRING:
                if (!row[col.name].is_string())
                    throw std::runtime_error("Column '" + col.name + "' expects STRING");
                break;
            case ColumnType::FLOAT:
                if (!row[col.name].is_number())
                    throw std::runtime_error("Column '" + col.name + "' expects FLOAT");
                break;
            case ColumnType::BOOL:
                if (!row[col.name].is_boolean())
                    throw std::runtime_error("Column '" + col.name + "' expects BOOL");
                break;
        }
    }
}

Metrics TableStore::createTable(const TableSchema& schema) {
    Timer t;
    schemaManager_.createTable(schema);
    trees_[schema.tableName] = std::make_unique<BTree>();
    getOrCreateIndex(schema.tableName, schema);
    return {t.elapsedMs(), 1, 0, 0};
}

Metrics TableStore::insert(const std::string& tableName, int64_t key, const nlohmann::json& row) {
    Timer t;
    BTree* tree = getOrLoadTree(tableName);
    TableSchema schema = schemaManager_.loadTable(tableName);
    validateRow(schema, row);
    tree->resetMetrics();
    tree->insert(key, row);

    SecondaryIndex& idx = getOrCreateIndex(tableName, schema);
    for (const auto& col : schema.columns) {
        if (col.name != schema.primaryKey && row.contains(col.name)) {
            idx.insertForColumn(col.name, columnValueToString(row[col.name]), key);
        }
    }

    return {t.elapsedMs(), tree->nodeTraversals(), tree->nodeTraversals(), 1};
}

std::pair<std::optional<nlohmann::json>, Metrics> TableStore::search(const std::string& tableName, int64_t key) {
    Timer t;
    BTree* tree = getOrLoadTree(tableName);
    tree->resetMetrics();
    auto result = tree->search(key);
    uint64_t traversals = tree->nodeTraversals();
    uint64_t rows = result.has_value() ? 1 : 0;
    return {result, {t.elapsedMs(), traversals, traversals, rows}};
}

std::pair<std::vector<nlohmann::json>, Metrics> TableStore::rangeQuery(const std::string& tableName, int64_t start, int64_t end) {
    Timer t;
    BTree* tree = getOrLoadTree(tableName);
    tree->resetMetrics();
    auto rows = tree->rangeQuery(start, end);
    uint64_t traversals = tree->nodeTraversals();
    return {rows, {t.elapsedMs(), traversals, traversals, static_cast<uint64_t>(rows.size())}};
}

std::pair<std::vector<nlohmann::json>, Metrics> TableStore::fullScan(const std::string& tableName) {
    Timer t;
    BTree* tree = getOrLoadTree(tableName);
    tree->resetMetrics();
    auto rows = tree->fullScan();
    uint64_t traversals = tree->nodeTraversals();
    return {rows, {t.elapsedMs(), traversals, traversals, static_cast<uint64_t>(rows.size())}};
}

std::pair<std::vector<nlohmann::json>, Metrics> TableStore::searchByColumn(
    const std::string& tableName, const std::string& column, const std::string& value)
{
    Timer t;
    BTree* tree = getOrLoadTree(tableName);
    TableSchema schema = schemaManager_.loadTable(tableName);

    auto it = secondaryIndexes_.find(tableName);
    if (it == secondaryIndexes_.end() || !it->second.hasColumn(column)) {
        getOrCreateIndex(tableName, schema);
    }

    SecondaryIndex& idx = secondaryIndexes_[tableName];
    std::vector<int64_t> primaryKeys = idx.lookupColumn(column, value);

    tree->resetMetrics();
    std::vector<nlohmann::json> rows;
    for (int64_t pk : primaryKeys) {
        auto row = tree->search(pk);
        if (row.has_value()) {
            rows.push_back(row.value());
        }
    }
    uint64_t traversals = tree->nodeTraversals();
    return {rows, {t.elapsedMs(), traversals, traversals, static_cast<uint64_t>(rows.size())}};
}

std::vector<std::string> TableStore::listTables() const {
    return schemaManager_.listTables();
}

TableSchema TableStore::getSchema(const std::string& tableName) {
    return schemaManager_.loadTable(tableName);
}

} // namespace arbor
