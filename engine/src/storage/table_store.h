#pragma once

#include <string>
#include <unordered_map>
#include <memory>
#include "btree.h"
#include "secondary_index.h"
#include "serializer.h"
#include "../disk/pager.h"
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
    std::pair<std::vector<nlohmann::json>, Metrics> fullScan(const std::string& tableName, const nlohmann::json& filter = nlohmann::json());
    std::pair<std::vector<nlohmann::json>, Metrics> searchByColumn(const std::string& tableName, const std::string& column, const std::string& value);
    Metrics createSecondaryIndex(const std::string& tableName, const std::string& indexName, const std::string& column, bool unique = false);
    Metrics dropSecondaryIndex(const std::string& tableName, const std::string& indexName);
    std::pair<uint64_t, Metrics> updateRows(const std::string& tableName, const std::string& column, const nlohmann::json& value, const nlohmann::json& filter);
    std::pair<uint64_t, Metrics> deleteRows(const std::string& tableName, const nlohmann::json& filter);
    Metrics dropTable(const std::string& tableName);
    std::vector<std::string> listTables() const;
    TableSchema getSchema(const std::string& tableName);

private:
    std::string dataDir_;
    SchemaManager schemaManager_;
    std::unordered_map<std::string, std::unique_ptr<BTree>>   trees_;
    std::unordered_map<std::string, std::unique_ptr<Pager>>   pagers_;
    std::unordered_map<std::string, SecondaryIndex>            secondaryIndexes_;

    BTree*  getOrLoadTree(const std::string& tableName);
    Pager*  getOrOpenPager(const std::string& tableName);
    SecondaryIndex& getOrCreateIndex(const std::string& tableName, const TableSchema& schema);
    void    rebuildSecondaryIndexCache(const std::string& tableName, const TableSchema& schema, const std::vector<nlohmann::json>& rows);
    void    persistTree(const std::string& tableName);
    void    rebuildTable(const std::string& tableName, const TableSchema& schema, const std::vector<nlohmann::json>& rows);
    void    validateRow(const TableSchema& schema, const nlohmann::json& row) const;
    std::string columnValueToString(const nlohmann::json& value) const;
    std::string pagerPath(const std::string& tableName) const;
};

} // namespace arbor
