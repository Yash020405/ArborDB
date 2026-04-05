#include "engine.h"
#include <stdexcept>

namespace arbor {

Engine::Engine(const std::string& dataDir)
    : store_(dataDir), wal_(dataDir + "/wal.log") {}

nlohmann::json Engine::execute(const nlohmann::json& command) {
    try {
        if (!command.contains("operation") || !command["operation"].is_string()) {
            return errorResponse("Missing or invalid 'operation' field");
        }
        std::string op = command["operation"].get<std::string>();
        if (op == "create_table") return handleCreateTable(command);
        if (op == "insert")       return handleInsert(command);
        if (op == "search")       return handleSearch(command);
        if (op == "search_by_column") return handleSearchByColumn(command);
        if (op == "range")        return handleRange(command);
        if (op == "full_scan")    return handleFullScan(command);
        if (op == "update")       return handleUpdate(command);
        if (op == "delete")       return handleDelete(command);
        if (op == "drop_table")   return handleDropTable(command);
        return errorResponse("Unknown operation: " + op);
    } catch (const std::exception& e) {
        return errorResponse(e.what());
    }
}

nlohmann::json Engine::handleCreateTable(const nlohmann::json& cmd) {
    if (!cmd.contains("table") || !cmd.contains("schema")) {
        return errorResponse("create_table requires 'table' and 'schema'");
    }

    std::lock_guard<std::mutex> lock(mu_);

    TableSchema schema;
    schema.tableName  = cmd["table"].get<std::string>();
    schema.primaryKey = cmd.value("primary_key", std::string("id"));

    if (cmd.contains("columns") && cmd["columns"].is_array() && !cmd["columns"].empty()) {
        for (const auto& colDef : cmd["columns"]) {
            if (!colDef.contains("name") || !colDef.contains("type") ||
                !colDef["name"].is_string() || !colDef["type"].is_string()) {
                return errorResponse("create_table 'columns' must contain {name, type} objects");
            }

            Column col;
            col.name = colDef["name"].get<std::string>();
            col.type = parseColumnType(colDef["type"].get<std::string>());
            schema.columns.push_back(col);
        }
    } else {
        for (auto& [colName, colType] : cmd["schema"].items()) {
            Column col;
            col.name = colName;
            col.type = parseColumnType(colType.get<std::string>());
            schema.columns.push_back(col);
        }
    }

    wal_.logCreateTable(schema.tableName, cmd["schema"]);
    Metrics m = store_.createTable(schema);
    return okResponse({}, m);
}

nlohmann::json Engine::handleInsert(const nlohmann::json& cmd) {
    if (!cmd.contains("table") || !cmd.contains("key") || !cmd.contains("data")) {
        return errorResponse("insert requires 'table', 'key', and 'data'");
    }

    std::lock_guard<std::mutex> lock(mu_);

    std::string tableName = cmd["table"].get<std::string>();
    int64_t key           = cmd["key"].get<int64_t>();
    nlohmann::json row    = cmd["data"];

    wal_.logInsert(tableName, key, row);
    Metrics m = store_.insert(tableName, key, row);
    return okResponse({}, m);
}

nlohmann::json Engine::handleSearch(const nlohmann::json& cmd) {
    if (!cmd.contains("table") || !cmd.contains("key")) {
        return errorResponse("search requires 'table' and 'key'");
    }

    std::lock_guard<std::mutex> lock(mu_);

    std::string tableName = cmd["table"].get<std::string>();
    int64_t key           = cmd["key"].get<int64_t>();
    auto [result, m]      = store_.search(tableName, key);

    std::vector<nlohmann::json> rows;
    if (result.has_value()) rows.push_back(result.value());
    return okResponse(rows, m);
}

nlohmann::json Engine::handleRange(const nlohmann::json& cmd) {
    if (!cmd.contains("table") || !cmd.contains("start") || !cmd.contains("end")) {
        return errorResponse("range requires 'table', 'start', and 'end'");
    }

    std::lock_guard<std::mutex> lock(mu_);

    std::string tableName = cmd["table"].get<std::string>();
    int64_t start         = cmd["start"].get<int64_t>();
    int64_t end           = cmd["end"].get<int64_t>();
    auto [rows, m]        = store_.rangeQuery(tableName, start, end);
    return okResponse(rows, m);
}

nlohmann::json Engine::handleFullScan(const nlohmann::json& cmd) {
    if (!cmd.contains("table")) {
        return errorResponse("full_scan requires 'table'");
    }

    std::lock_guard<std::mutex> lock(mu_);

    std::string tableName = cmd["table"].get<std::string>();
    nlohmann::json filter = nlohmann::json();
    if (cmd.contains("filter")) {
        filter = cmd["filter"];
    }

    auto [rows, m] = store_.fullScan(tableName, filter);
    return okResponse(rows, m);
}

nlohmann::json Engine::handleSearchByColumn(const nlohmann::json& cmd) {
    if (!cmd.contains("table") || !cmd.contains("column") || !cmd.contains("value")) {
        return errorResponse("search_by_column requires 'table', 'column', and 'value'");
    }

    std::lock_guard<std::mutex> lock(mu_);

    std::string tableName = cmd["table"].get<std::string>();
    std::string column    = cmd["column"].get<std::string>();
    std::string value;
    
    // Value can be int, string, etc. - coerce to string for the secondary index map
    if (cmd["value"].is_string()) {
        value = cmd["value"].get<std::string>();
    } else {
        value = cmd["value"].dump();
    }

    auto [rows, m] = store_.searchByColumn(tableName, column, value);
    return okResponse(rows, m);
}

nlohmann::json Engine::handleUpdate(const nlohmann::json& cmd) {
    if (!cmd.contains("table") || !cmd.contains("column") || !cmd.contains("value")) {
        return errorResponse("update requires 'table', 'column', and 'value'");
    }

    std::lock_guard<std::mutex> lock(mu_);

    const std::string tableName = cmd["table"].get<std::string>();
    const std::string column = cmd["column"].get<std::string>();
    const nlohmann::json value = cmd["value"];
    const nlohmann::json filter = cmd.contains("filter") ? cmd["filter"] : nlohmann::json(nullptr);

    auto [affected, m] = store_.updateRows(tableName, column, value, filter);
    return {
        {"status", "ok"},
        {"rows",   nlohmann::json::array()},
        {"error",  nullptr},
        {"affected_rows", affected},
        {"metrics", m.toJson()}
    };
}

nlohmann::json Engine::handleDelete(const nlohmann::json& cmd) {
    if (!cmd.contains("table")) {
        return errorResponse("delete requires 'table'");
    }

    std::lock_guard<std::mutex> lock(mu_);

    const std::string tableName = cmd["table"].get<std::string>();
    const nlohmann::json filter = cmd.contains("filter") ? cmd["filter"] : nlohmann::json(nullptr);

    auto [affected, m] = store_.deleteRows(tableName, filter);
    return {
        {"status", "ok"},
        {"rows",   nlohmann::json::array()},
        {"error",  nullptr},
        {"affected_rows", affected},
        {"metrics", m.toJson()}
    };
}

nlohmann::json Engine::handleDropTable(const nlohmann::json& cmd) {
    if (!cmd.contains("table")) {
        return errorResponse("drop_table requires 'table'");
    }

    std::lock_guard<std::mutex> lock(mu_);

    const std::string tableName = cmd["table"].get<std::string>();
    Metrics m = store_.dropTable(tableName);
    return okResponse({}, m);
}

nlohmann::json Engine::okResponse(std::vector<nlohmann::json> rows, const Metrics& m) const {
    return {
        {"status", "ok"},
        {"rows",   rows},
        {"error",  nullptr},
        {"metrics", m.toJson()}
    };
}

nlohmann::json Engine::errorResponse(const std::string& message) const {
    return {
        {"status", "error"},
        {"rows",   nlohmann::json::array()},
        {"error",  message},
        {"metrics", Metrics{}.toJson()}
    };
}

} // namespace arbor
