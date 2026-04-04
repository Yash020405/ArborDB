#pragma once

#include <string>
#include <fstream>
#include <mutex>
#include "../vendor/json.hpp"

namespace arbor {

enum class WALOpType { CREATE_TABLE, INSERT };

struct WALEntry {
    WALOpType op;
    std::string table;
    int64_t     key;
    nlohmann::json data;
};

class WAL {
public:
    explicit WAL(const std::string& logPath);
    ~WAL();

    void logCreateTable(const std::string& table, const nlohmann::json& schema);
    void logInsert(const std::string& table, int64_t key, const nlohmann::json& row);
    std::vector<WALEntry> recover();
    void truncate();

private:
    std::string   logPath_;
    std::ofstream out_;
    std::mutex    mu_;

    void append(const nlohmann::json& entry);
};

} // namespace arbor
