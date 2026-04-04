#pragma once

#include <string>
#include <unordered_map>
#include <memory>
#include "btree.h"
#include "../schema/schema.h"
#include "../metrics.h"
#include "../../vendor/json.hpp"

namespace arbor {

class TableStore {
public:
    explicit TableStore(const std::string& dataDir);

    Metrics createTable(const TableSchema& schema);
    Metrics insert(const std::string& tableName, int64_t key, const nlohmann::json& row);
    std::pair<std::optional<nlohmann::json>, Metrics> search(const std::string& tableName, int64_t key);
    std::pair<std::vector<nlohmann::json>, Metrics> rangeQuery(const std::string& tableName, int64_t start, int64_t end);
    std::pair<std::vector<nlohmann::json>, Metrics> fullScan(const std::string& tableName);
    std::vector<std::string> listTables() const;
    TableSchema getSchema(const std::string& tableName);

private:
    std::string dataDir_;
    SchemaManager schemaManager_;
    std::unordered_map<std::string, std::unique_ptr<BTree>> trees_;

    BTree* getOrLoadTree(const std::string& tableName);
    void validateRow(const TableSchema& schema, const nlohmann::json& row) const;
};

} // namespace arbor
