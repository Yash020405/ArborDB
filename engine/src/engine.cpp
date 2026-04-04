#include "engine.h"
#include <chrono>
#include <stdexcept>

namespace arbor {

Engine::Engine(const std::string& dataDir) : store_(dataDir) {}

nlohmann::json Engine::execute(const nlohmann::json& command) {
    try {
        if (!command.contains("operation") || !command["operation"].is_string()) {
            return errorResponse("Missing or invalid 'operation' field");
        }

        std::string op = command["operation"].get<std::string>();

        if (op == "create_table") return handleCreateTable(command);
        if (op == "insert")       return handleInsert(command);
        if (op == "search")       return handleSearch(command);
        if (op == "range")        return handleRange(command);
        if (op == "full_scan")    return handleFullScan(command);

        return errorResponse("Unknown operation: " + op);
    } catch (const std::exception& e) {
        return errorResponse(e.what());
    }
}

nlohmann::json Engine::handleCreateTable(const nlohmann::json& cmd) {
    if (!cmd.contains("table") || !cmd.contains("schema")) {
        return errorResponse("create_table requires 'table' and 'schema'");
    }

    auto t0 = std::chrono::steady_clock::now();

    TableSchema schema;
    schema.tableName = cmd["table"].get<std::string>();

    if (cmd.contains("primary_key")) {
        schema.primaryKey = cmd["primary_key"].get<std::string>();
    } else {
        schema.primaryKey = "id";
    }

    for (auto& [colName, colType] : cmd["schema"].items()) {
        Column col;
        col.name = colName;
        col.type = parseColumnType(colType.get<std::string>());
        schema.columns.push_back(col);
    }

    store_.createTable(schema);

    auto t1 = std::chrono::steady_clock::now();
    uint64_t ms = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();

    return okResponse({}, ms, 0);
}

nlohmann::json Engine::handleInsert(const nlohmann::json& cmd) {
    if (!cmd.contains("table") || !cmd.contains("key") || !cmd.contains("data")) {
        return errorResponse("insert requires 'table', 'key', and 'data'");
    }

    auto t0 = std::chrono::steady_clock::now();

    std::string tableName = cmd["table"].get<std::string>();
    int64_t key = cmd["key"].get<int64_t>();
    nlohmann::json row = cmd["data"];

    store_.insert(tableName, key, row);

    auto t1 = std::chrono::steady_clock::now();
    uint64_t ms = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();

    return okResponse({}, ms, store_.lastNodeTraversals());
}

nlohmann::json Engine::handleSearch(const nlohmann::json& cmd) {
    if (!cmd.contains("table") || !cmd.contains("key")) {
        return errorResponse("search requires 'table' and 'key'");
    }

    auto t0 = std::chrono::steady_clock::now();

    std::string tableName = cmd["table"].get<std::string>();
    int64_t key = cmd["key"].get<int64_t>();

    auto result = store_.search(tableName, key);

    auto t1 = std::chrono::steady_clock::now();
    uint64_t ms = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();

    std::vector<nlohmann::json> rows;
    if (result.has_value()) {
        rows.push_back(result.value());
    }

    return okResponse(rows, ms, store_.lastNodeTraversals());
}

nlohmann::json Engine::handleRange(const nlohmann::json& cmd) {
    if (!cmd.contains("table") || !cmd.contains("start") || !cmd.contains("end")) {
        return errorResponse("range requires 'table', 'start', and 'end'");
    }

    auto t0 = std::chrono::steady_clock::now();

    std::string tableName = cmd["table"].get<std::string>();
    int64_t start = cmd["start"].get<int64_t>();
    int64_t end = cmd["end"].get<int64_t>();

    auto rows = store_.rangeQuery(tableName, start, end);

    auto t1 = std::chrono::steady_clock::now();
    uint64_t ms = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();

    return okResponse(rows, ms, store_.lastNodeTraversals());
}

nlohmann::json Engine::handleFullScan(const nlohmann::json& cmd) {
    if (!cmd.contains("table")) {
        return errorResponse("full_scan requires 'table'");
    }

    auto t0 = std::chrono::steady_clock::now();

    std::string tableName = cmd["table"].get<std::string>();
    auto rows = store_.fullScan(tableName);

    auto t1 = std::chrono::steady_clock::now();
    uint64_t ms = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();

    return okResponse(rows, ms, store_.lastNodeTraversals());
}

nlohmann::json Engine::okResponse(std::vector<nlohmann::json> rows, uint64_t timeMs, uint64_t diskReads) const {
    return {
        {"status", "ok"},
        {"rows", rows},
        {"error", nullptr},
        {"metrics", {
            {"time_ms", timeMs},
            {"disk_reads", diskReads},
            {"nodes_traversed", diskReads}
        }}
    };
}

nlohmann::json Engine::errorResponse(const std::string& message) const {
    return {
        {"status", "error"},
        {"rows", nlohmann::json::array()},
        {"error", message},
        {"metrics", {
            {"time_ms", 0},
            {"disk_reads", 0},
            {"nodes_traversed", 0}
        }}
    };
}

} // namespace arbor
