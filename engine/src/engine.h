#pragma once

#include <string>
#include "storage/table_store.h"
#include "metrics.h"
#include "../vendor/json.hpp"

namespace arbor {

class Engine {
public:
    explicit Engine(const std::string& dataDir);

    nlohmann::json execute(const nlohmann::json& command);

private:
    TableStore store_;

    nlohmann::json handleCreateTable(const nlohmann::json& cmd);
    nlohmann::json handleInsert(const nlohmann::json& cmd);
    nlohmann::json handleSearch(const nlohmann::json& cmd);
    nlohmann::json handleRange(const nlohmann::json& cmd);
    nlohmann::json handleFullScan(const nlohmann::json& cmd);

    nlohmann::json okResponse(std::vector<nlohmann::json> rows, const Metrics& m) const;
    nlohmann::json errorResponse(const std::string& message) const;
};

} // namespace arbor
