#include "table_store.h"
#include <stdexcept>

namespace arbor {

TableStore::TableStore(const std::string& dataDir)
    : dataDir_(dataDir), schemaManager_(dataDir), lastNodeTraversals_(0) {}

void TableStore::createTable(const TableSchema& schema) {
    schemaManager_.createTable(schema);
    trees_[schema.tableName] = std::make_unique<BTree>();
}

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

void TableStore::validateRow(const TableSchema& schema, const nlohmann::json& row) const {
    for (const auto& col : schema.columns) {
        if (!row.contains(col.name)) {
            throw std::runtime_error("Missing column in row: " + col.name);
        }
        switch (col.type) {
            case ColumnType::INT:
                if (!row[col.name].is_number_integer()) {
                    throw std::runtime_error("Column '" + col.name + "' expects INT");
                }
                break;
            case ColumnType::STRING:
                if (!row[col.name].is_string()) {
                    throw std::runtime_error("Column '" + col.name + "' expects STRING");
                }
                break;
            case ColumnType::FLOAT:
                if (!row[col.name].is_number()) {
                    throw std::runtime_error("Column '" + col.name + "' expects FLOAT");
                }
                break;
            case ColumnType::BOOL:
                if (!row[col.name].is_boolean()) {
                    throw std::runtime_error("Column '" + col.name + "' expects BOOL");
                }
                break;
        }
    }
}

void TableStore::insert(const std::string& tableName, int64_t key, const nlohmann::json& row) {
    BTree* tree = getOrLoadTree(tableName);
    TableSchema schema = schemaManager_.loadTable(tableName);
    validateRow(schema, row);
    tree->resetMetrics();
    tree->insert(key, row);
    lastNodeTraversals_ = tree->nodeTraversals();
}

std::optional<nlohmann::json> TableStore::search(const std::string& tableName, int64_t key) {
    BTree* tree = getOrLoadTree(tableName);
    tree->resetMetrics();
    auto result = tree->search(key);
    lastNodeTraversals_ = tree->nodeTraversals();
    return result;
}

std::vector<nlohmann::json> TableStore::rangeQuery(const std::string& tableName, int64_t start, int64_t end) {
    BTree* tree = getOrLoadTree(tableName);
    tree->resetMetrics();
    auto result = tree->rangeQuery(start, end);
    lastNodeTraversals_ = tree->nodeTraversals();
    return result;
}

std::vector<nlohmann::json> TableStore::fullScan(const std::string& tableName) {
    BTree* tree = getOrLoadTree(tableName);
    tree->resetMetrics();
    auto result = tree->fullScan();
    lastNodeTraversals_ = tree->nodeTraversals();
    return result;
}

std::vector<std::string> TableStore::listTables() const {
    return schemaManager_.listTables();
}

TableSchema TableStore::getSchema(const std::string& tableName) {
    return schemaManager_.loadTable(tableName);
}

uint64_t TableStore::lastNodeTraversals() const {
    return lastNodeTraversals_;
}

} // namespace arbor
