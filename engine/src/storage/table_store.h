#pragma once

#include <string>
#include <unordered_map>
#include <memory>
#include "btree.h"
#include "../schema/schema.h"
#include "../../vendor/json.hpp"

namespace arbor {

class TableStore {
public:
    explicit TableStore(const std::string& dataDir);

    void createTable(const TableSchema& schema);
    void insert(const std::string& tableName, int64_t key, const nlohmann::json& row);
    std::optional<nlohmann::json> search(const std::string& tableName, int64_t key);
    std::vector<nlohmann::json> rangeQuery(const std::string& tableName, int64_t start, int64_t end);
    std::vector<nlohmann::json> fullScan(const std::string& tableName);
    std::vector<std::string> listTables() const;
    TableSchema getSchema(const std::string& tableName);

    uint64_t lastNodeTraversals() const;

private:
    std::string dataDir_;
    SchemaManager schemaManager_;
    std::unordered_map<std::string, std::unique_ptr<BTree>> trees_;
    mutable uint64_t lastNodeTraversals_;

    BTree* getOrLoadTree(const std::string& tableName);
    void validateRow(const TableSchema& schema, const nlohmann::json& row) const;
};

} // namespace arbor
