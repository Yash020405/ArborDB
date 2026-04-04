#include "wal.h"
#include <fstream>
#include <sstream>
#include <stdexcept>

namespace arbor {

WAL::WAL(const std::string& logPath) : logPath_(logPath) {
    out_.open(logPath_, std::ios::app | std::ios::out);
    if (!out_.is_open()) {
        throw std::runtime_error("Cannot open WAL file: " + logPath_);
    }
}

WAL::~WAL() {
    if (out_.is_open()) {
        out_.flush();
        out_.close();
    }
}

void WAL::append(const nlohmann::json& entry) {
    std::lock_guard<std::mutex> lock(mu_);
    out_ << entry.dump() << "\n";
    out_.flush();
}

void WAL::logCreateTable(const std::string& table, const nlohmann::json& schema) {
    append({{"op", "create_table"}, {"table", table}, {"schema", schema}});
}

void WAL::logInsert(const std::string& table, int64_t key, const nlohmann::json& row) {
    append({{"op", "insert"}, {"table", table}, {"key", key}, {"data", row}});
}

std::vector<WALEntry> WAL::recover() {
    std::ifstream in(logPath_);
    std::vector<WALEntry> entries;
    if (!in.is_open()) return entries;

    std::string line;
    while (std::getline(in, line)) {
        if (line.empty()) continue;
        try {
            nlohmann::json j = nlohmann::json::parse(line);
            WALEntry e;
            std::string op = j["op"].get<std::string>();
            e.table = j["table"].get<std::string>();
            if (op == "create_table") {
                e.op   = WALOpType::CREATE_TABLE;
                e.data = j["schema"];
                e.key  = 0;
            } else if (op == "insert") {
                e.op   = WALOpType::INSERT;
                e.key  = j["key"].get<int64_t>();
                e.data = j["data"];
            } else {
                continue;
            }
            entries.push_back(std::move(e));
        } catch (...) {
            continue;
        }
    }
    return entries;
}

void WAL::truncate() {
    std::lock_guard<std::mutex> lock(mu_);
    if (out_.is_open()) {
        out_.close();
    }
    out_.open(logPath_, std::ios::trunc | std::ios::out);
}

} // namespace arbor
