#include "secondary_index.h"

namespace arbor {

void SecondaryIndex::addColumn(const std::string& columnName) {
    index_.emplace(columnName, std::unordered_map<std::string, std::vector<int64_t>>{});
}

void SecondaryIndex::removeColumn(const std::string& columnName) {
    index_.erase(columnName);
}

bool SecondaryIndex::hasColumn(const std::string& columnName) const {
    return index_.count(columnName) > 0;
}

void SecondaryIndex::insertForColumn(const std::string& columnName, const std::string& value, int64_t primaryKey) {
    auto colIt = index_.find(columnName);
    if (colIt == index_.end()) {
        return;
    }
    colIt->second[value].push_back(primaryKey);
}

std::vector<int64_t> SecondaryIndex::lookupColumn(const std::string& columnName, const std::string& value) const {
    auto colIt = index_.find(columnName);
    if (colIt == index_.end()) return {};
    auto valIt = colIt->second.find(value);
    if (valIt == colIt->second.end()) return {};
    return valIt->second;
}

void SecondaryIndex::insert(const std::string& columnValue, int64_t primaryKey) {
    index_["_default"][columnValue].push_back(primaryKey);
}

std::vector<int64_t> SecondaryIndex::lookup(const std::string& columnValue) const {
    return lookupColumn("_default", columnValue);
}

} // namespace arbor
