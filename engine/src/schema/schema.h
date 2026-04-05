#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include "../../vendor/json.hpp"

namespace arbor {

enum class ColumnType { INT, STRING, FLOAT, BOOL };

struct Column {
    std::string name;
    ColumnType type;
};

struct TableSchema {
    std::string tableName;
    std::vector<Column> columns;
    std::string primaryKey;
};

ColumnType parseColumnType(const std::string& s);
std::string columnTypeToString(ColumnType t);

class SchemaManager {
public:
    explicit SchemaManager(const std::string& dataDir);

    void createTable(const TableSchema& schema);
    TableSchema loadTable(const std::string& tableName);
    bool tableExists(const std::string& tableName) const;
    std::vector<std::string> listTables() const;

private:
    std::string dataDir_;
    mutable std::unordered_map<std::string, TableSchema> cache_;

    std::string schemaPath(const std::string& tableName) const;
    void persistSchema(const TableSchema& schema) const;
    TableSchema parseSchemaFile(const std::string& path) const;
};

} // namespace arbor
