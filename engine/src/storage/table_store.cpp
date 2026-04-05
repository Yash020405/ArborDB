#include "table_store.h"
#include <filesystem>
#include <stdexcept>

namespace arbor {

namespace {

bool tryParseNumber(const nlohmann::json& value, double& out) {
    if (value.is_number()) {
        out = value.get<double>();
        return true;
    }

    if (value.is_string()) {
        try {
            std::size_t idx = 0;
            const std::string s = value.get<std::string>();
            out = std::stod(s, &idx);
            return idx == s.size();
        } catch (...) {
            return false;
        }
    }

    return false;
}

std::string comparableString(const nlohmann::json& value) {
    if (value.is_string()) {
        return value.get<std::string>();
    }
    return value.dump();
}

bool equalsValue(const nlohmann::json& left, const nlohmann::json& right) {
    if (left == right) {
        return true;
    }

    double dl = 0;
    double dr = 0;
    if (tryParseNumber(left, dl) && tryParseNumber(right, dr)) {
        return dl == dr;
    }

    return comparableString(left) == comparableString(right);
}

bool betweenValue(const nlohmann::json& value, const nlohmann::json& start, const nlohmann::json& end) {
    double v = 0;
    double s = 0;
    double e = 0;

    if (tryParseNumber(value, v) && tryParseNumber(start, s) && tryParseNumber(end, e)) {
        return v >= s && v <= e;
    }

    const std::string sv = comparableString(value);
    const std::string ss = comparableString(start);
    const std::string se = comparableString(end);
    return sv >= ss && sv <= se;
}

bool rowMatchesFilter(const nlohmann::json& row, const nlohmann::json& filter) {
    if (filter.is_null() || !filter.is_object()) {
        return true;
    }

    if (!filter.contains("column") || !filter.contains("operator")) {
        return false;
    }

    const std::string column = filter["column"].get<std::string>();
    const std::string op = filter["operator"].get<std::string>();
    if (!row.contains(column)) {
        return false;
    }

    if (op == "=") {
        if (!filter.contains("value")) {
            return false;
        }
        return equalsValue(row[column], filter["value"]);
    }

    if (op == "BETWEEN") {
        if (!filter.contains("start") || !filter.contains("end")) {
            return false;
        }
        return betweenValue(row[column], filter["start"], filter["end"]);
    }

    return false;
}

} // namespace

TableStore::TableStore(const std::string& dataDir)
    : dataDir_(dataDir), schemaManager_(dataDir) {}

std::string TableStore::pagerPath(const std::string& tableName) const {
    return dataDir_ + "/" + tableName + ".db";
}

Pager* TableStore::getOrOpenPager(const std::string& tableName) {
    auto it = pagers_.find(tableName);
    if (it != pagers_.end()) return it->second.get();
    pagers_[tableName] = std::make_unique<Pager>(pagerPath(tableName));
    return pagers_[tableName].get();
}

BTree* TableStore::getOrLoadTree(const std::string& tableName) {
    auto it = trees_.find(tableName);
    if (it != trees_.end()) return it->second.get();

    if (!schemaManager_.tableExists(tableName)) {
        throw std::runtime_error("Table does not exist: " + tableName);
    }

    schemaManager_.loadTable(tableName);
    trees_[tableName] = std::make_unique<BTree>();

    Pager* pager = getOrOpenPager(tableName);
    if (pager->totalPages() > 0) {
        Serializer::load(*trees_[tableName], *pager);
    }

    return trees_[tableName].get();
}

SecondaryIndex& TableStore::getOrCreateIndex(const std::string& tableName, const TableSchema& schema) {
    auto it = secondaryIndexes_.find(tableName);
    if (it != secondaryIndexes_.end()) return it->second;

    SecondaryIndex idx;
    for (const auto& col : schema.columns) {
        if (col.name != schema.primaryKey) idx.addColumn(col.name);
    }
    secondaryIndexes_[tableName] = std::move(idx);
    return secondaryIndexes_[tableName];
}

void TableStore::persistTree(const std::string& tableName) {
    pagers_.erase(tableName);
    std::filesystem::remove(pagerPath(tableName));
    pagers_[tableName] = std::make_unique<Pager>(pagerPath(tableName));
    Serializer::save(*trees_[tableName], *pagers_[tableName]);
}

void TableStore::rebuildTable(const std::string& tableName, const TableSchema& schema, const std::vector<nlohmann::json>& rows) {
    trees_[tableName] = std::make_unique<BTree>();
    secondaryIndexes_.erase(tableName);
    SecondaryIndex& idx = getOrCreateIndex(tableName, schema);

    for (const auto& row : rows) {
        validateRow(schema, row);

        if (!row.contains(schema.primaryKey)) {
            throw std::runtime_error("Missing primary key in row during rebuild: " + schema.primaryKey);
        }

        int64_t key = row[schema.primaryKey].get<int64_t>();
        trees_[tableName]->insert(key, row);

        for (const auto& col : schema.columns) {
            if (col.name != schema.primaryKey && row.contains(col.name)) {
                idx.insertForColumn(col.name, columnValueToString(row[col.name]), key);
            }
        }
    }

    persistTree(tableName);
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

std::string TableStore::columnValueToString(const nlohmann::json& value) const {
    if (value.is_string()) return value.get<std::string>();
    return value.dump();
}

Metrics TableStore::createTable(const TableSchema& schema) {
    Timer t;
    schemaManager_.createTable(schema);
    trees_[schema.tableName]  = std::make_unique<BTree>();
    getOrOpenPager(schema.tableName);
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

    persistTree(tableName);

    uint64_t traversals = tree->nodeTraversals();
    return {t.elapsedMs(), traversals, traversals, 1};
}

std::pair<std::optional<nlohmann::json>, Metrics> TableStore::search(const std::string& tableName, int64_t key) {
    Timer t;
    BTree* tree = getOrLoadTree(tableName);
    tree->resetMetrics();
    auto result = tree->search(key);
    uint64_t traversals = tree->nodeTraversals();
    return {result, {t.elapsedMs(), traversals, traversals, result.has_value() ? 1u : 0u}};
}

std::pair<std::vector<nlohmann::json>, Metrics> TableStore::rangeQuery(const std::string& tableName, int64_t start, int64_t end) {
    Timer t;
    BTree* tree = getOrLoadTree(tableName);
    tree->resetMetrics();
    auto rows = tree->rangeQuery(start, end);
    uint64_t traversals = tree->nodeTraversals();
    return {rows, {t.elapsedMs(), traversals, traversals, static_cast<uint64_t>(rows.size())}};
}

std::pair<std::vector<nlohmann::json>, Metrics> TableStore::fullScan(const std::string& tableName, const nlohmann::json& filter) {
    Timer t;
    BTree* tree = getOrLoadTree(tableName);
    tree->resetMetrics();
    auto rows = tree->fullScan();

    if (!filter.is_null() && filter.is_object()) {
        std::vector<nlohmann::json> filtered;
        filtered.reserve(rows.size());
        for (const auto& row : rows) {
            if (rowMatchesFilter(row, filter)) {
                filtered.push_back(row);
            }
        }
        rows = std::move(filtered);
    }

    uint64_t traversals = tree->nodeTraversals();
    return {rows, {t.elapsedMs(), traversals, traversals, static_cast<uint64_t>(rows.size())}};
}

std::pair<std::vector<nlohmann::json>, Metrics> TableStore::searchByColumn(
    const std::string& tableName, const std::string& column, const std::string& value)
{
    Timer t;
    BTree* tree = getOrLoadTree(tableName);
    TableSchema schema = schemaManager_.loadTable(tableName);

    if (!secondaryIndexes_.count(tableName)) {
        SecondaryIndex& idx = getOrCreateIndex(tableName, schema);
        auto existingRows = tree->fullScan();
        for (const auto& row : existingRows) {
            if (!row.contains(schema.primaryKey)) continue;
            int64_t pk = row[schema.primaryKey].get<int64_t>();
            for (const auto& col : schema.columns) {
                if (col.name == schema.primaryKey || !row.contains(col.name)) continue;
                idx.insertForColumn(col.name, columnValueToString(row[col.name]), pk);
            }
        }
    }

    SecondaryIndex& idx = secondaryIndexes_[tableName];
    std::vector<int64_t> primaryKeys;

    if (idx.hasColumn(column)) {
        primaryKeys = idx.lookupColumn(column, value);
    }

    tree->resetMetrics();
    std::vector<nlohmann::json> rows;
    if (idx.hasColumn(column)) {
        for (int64_t pk : primaryKeys) {
            auto row = tree->search(pk);
            if (row.has_value()) rows.push_back(row.value());
        }
    } else {
        auto allRows = tree->fullScan();
        for (const auto& row : allRows) {
            if (!row.contains(column)) continue;
            if (columnValueToString(row[column]) == value) {
                rows.push_back(row);
            }
        }
    }

    uint64_t traversals = tree->nodeTraversals();
    return {rows, {t.elapsedMs(), traversals, traversals, static_cast<uint64_t>(rows.size())}};
}

std::pair<uint64_t, Metrics> TableStore::updateRows(
    const std::string& tableName,
    const std::string& column,
    const nlohmann::json& value,
    const nlohmann::json& filter)
{
    Timer t;
    BTree* tree = getOrLoadTree(tableName);
    TableSchema schema = schemaManager_.loadTable(tableName);

    bool columnExists = false;
    for (const auto& col : schema.columns) {
        if (col.name == column) {
            columnExists = true;
            break;
        }
    }
    if (!columnExists) {
        throw std::runtime_error("Unknown column in update: " + column);
    }

    tree->resetMetrics();
    auto rows = tree->fullScan();
    uint64_t traversals = tree->nodeTraversals();

    uint64_t affected = 0;
    for (auto& row : rows) {
        if (!rowMatchesFilter(row, filter)) {
            continue;
        }

        row[column] = value;
        validateRow(schema, row);
        affected++;
    }

    if (affected > 0) {
        rebuildTable(tableName, schema, rows);
    }

    return {affected, {t.elapsedMs(), traversals, traversals, affected}};
}

std::pair<uint64_t, Metrics> TableStore::deleteRows(const std::string& tableName, const nlohmann::json& filter) {
    Timer t;
    BTree* tree = getOrLoadTree(tableName);
    TableSchema schema = schemaManager_.loadTable(tableName);

    tree->resetMetrics();
    auto rows = tree->fullScan();
    uint64_t traversals = tree->nodeTraversals();

    std::vector<nlohmann::json> keptRows;
    keptRows.reserve(rows.size());

    uint64_t affected = 0;
    for (const auto& row : rows) {
        if (rowMatchesFilter(row, filter)) {
            affected++;
            continue;
        }
        keptRows.push_back(row);
    }

    if (affected > 0) {
        rebuildTable(tableName, schema, keptRows);
    }

    return {affected, {t.elapsedMs(), traversals, traversals, affected}};
}

Metrics TableStore::dropTable(const std::string& tableName) {
    Timer t;

    if (!schemaManager_.tableExists(tableName)) {
        throw std::runtime_error("Table does not exist: " + tableName);
    }

    trees_.erase(tableName);
    pagers_.erase(tableName);
    secondaryIndexes_.erase(tableName);

    schemaManager_.dropTable(tableName);
    std::filesystem::remove(pagerPath(tableName));

    return {t.elapsedMs(), 1, 0, 0};
}

std::vector<std::string> TableStore::listTables() const {
    return schemaManager_.listTables();
}

TableSchema TableStore::getSchema(const std::string& tableName) {
    return schemaManager_.loadTable(tableName);
}

} // namespace arbor
