#include "schema.h"
#include <fstream>
#include <stdexcept>
#include <filesystem>
#include <string>

namespace arbor {

namespace fs = std::filesystem;

ColumnType parseColumnType(const std::string& s) {
    if (s == "INT")    return ColumnType::INT;
    if (s == "STRING") return ColumnType::STRING;
    if (s == "FLOAT")  return ColumnType::FLOAT;
    if (s == "BOOL")   return ColumnType::BOOL;
    throw std::invalid_argument("Unknown column type: " + s);
}

std::string columnTypeToString(ColumnType t) {
    switch (t) {
        case ColumnType::INT:    return "INT";
        case ColumnType::STRING: return "STRING";
        case ColumnType::FLOAT:  return "FLOAT";
        case ColumnType::BOOL:   return "BOOL";
    }
    return "UNKNOWN";
}

SchemaManager::SchemaManager(const std::string& dataDir) : dataDir_(dataDir) {
    fs::create_directories(dataDir_);
}

std::string SchemaManager::schemaPath(const std::string& tableName) const {
    return dataDir_ + "/" + tableName + ".schema.json";
}

void SchemaManager::persistSchema(const TableSchema& schema) const {
    nlohmann::json j;
    j["table"] = schema.tableName;
    j["primary_key"] = schema.primaryKey;
    j["columns"] = nlohmann::json::array();
    for (const auto& col : schema.columns) {
        j["columns"].push_back({{"name", col.name}, {"type", columnTypeToString(col.type)}});
    }
    std::ofstream out(schemaPath(schema.tableName));
    if (!out.is_open()) {
        throw std::runtime_error("Cannot write schema for table: " + schema.tableName);
    }
    out << j.dump(2);
}

TableSchema SchemaManager::parseSchemaFile(const std::string& path) const {
    std::ifstream in(path);
    if (!in.is_open()) {
        throw std::runtime_error("Cannot read schema file: " + path);
    }
    nlohmann::json j = nlohmann::json::parse(in);

    TableSchema schema;
    schema.tableName = j["table"].get<std::string>();
    schema.primaryKey = j["primary_key"].get<std::string>();
    for (const auto& col : j["columns"]) {
        Column c;
        c.name = col["name"].get<std::string>();
        c.type = parseColumnType(col["type"].get<std::string>());
        schema.columns.push_back(c);
    }
    return schema;
}

void SchemaManager::createTable(const TableSchema& schema) {
    if (tableExists(schema.tableName)) {
        throw std::runtime_error("Table already exists: " + schema.tableName);
    }
    persistSchema(schema);
    cache_[schema.tableName] = schema;
}

TableSchema SchemaManager::loadTable(const std::string& tableName) {
    auto it = cache_.find(tableName);
    if (it != cache_.end()) {
        return it->second;
    }
    TableSchema schema = parseSchemaFile(schemaPath(tableName));
    cache_[tableName] = schema;
    return schema;
}

void SchemaManager::dropTable(const std::string& tableName) {
    if (!tableExists(tableName)) {
        throw std::runtime_error("Table does not exist: " + tableName);
    }

    const std::string path = schemaPath(tableName);
    if (fs::exists(path)) {
        fs::remove(path);
    }
    cache_.erase(tableName);
}

bool SchemaManager::tableExists(const std::string& tableName) const {
    if (cache_.count(tableName)) return true;
    return fs::exists(schemaPath(tableName));
}

std::vector<std::string> SchemaManager::listTables() const {
    std::vector<std::string> tables;
    for (const auto& entry : fs::directory_iterator(dataDir_)) {
        const std::string filename = entry.path().filename().string();
        const std::string suffix = ".schema.json";
        if (filename.size() > suffix.size() &&
            filename.substr(filename.size() - suffix.size()) == suffix) {
            tables.push_back(filename.substr(0, filename.size() - suffix.size()));
        }
    }
    return tables;
}

} // namespace arbor
