#pragma once

#include <string>
#include <unordered_map>
#include <vector>
#include <optional>
#include <cstdint>

namespace arbor {

class SecondaryIndex {
public:
    void insert(const std::string& columnValue, int64_t primaryKey);
    std::vector<int64_t> lookup(const std::string& columnValue) const;
    bool hasColumn(const std::string& columnName) const;
    void addColumn(const std::string& columnName);
    std::vector<int64_t> lookupColumn(const std::string& columnName, const std::string& value) const;
    void insertForColumn(const std::string& columnName, const std::string& value, int64_t primaryKey);

private:
    std::unordered_map<std::string, std::unordered_map<std::string, std::vector<int64_t>>> index_;
};

} // namespace arbor
