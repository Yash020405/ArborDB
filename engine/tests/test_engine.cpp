#include "../src/engine.h"
#include <cassert>
#include <iostream>
#include <filesystem>

using namespace arbor;
using json = nlohmann::json;

int main() {
    const std::string testDir = "/tmp/arbor_engine_test";
    std::filesystem::remove_all(testDir);

    Engine engine(testDir);

    {
        json resp = engine.execute({
            {"operation", "create_table"},
            {"table", "users"},
            {"schema", {{"id", "INT"}, {"name", "STRING"}}},
            {"primary_key", "id"}
        });
        assert(resp["status"] == "ok");
    }

    {
        json resp = engine.execute({
            {"operation", "insert"},
            {"table", "users"},
            {"key", 1},
            {"data", {{"id", 1}, {"name", "Alice"}}}
        });
        assert(resp["status"] == "ok");

        resp = engine.execute({
            {"operation", "insert"},
            {"table", "users"},
            {"key", 2},
            {"data", {{"id", 2}, {"name", "Bob"}}}
        });
        assert(resp["status"] == "ok");

        resp = engine.execute({
            {"operation", "insert"},
            {"table", "users"},
            {"key", 3},
            {"data", {{"id", 3}, {"name", "Charlie"}}}
        });
        assert(resp["status"] == "ok");
    }

    {
        json resp = engine.execute({{"operation", "search"}, {"table", "users"}, {"key", 1}});
        assert(resp["status"] == "ok");
        assert(resp["rows"].size() == 1);
        assert(resp["rows"][0]["name"] == "Alice");
        assert(resp["metrics"]["nodes_traversed"] >= 0);
    }

    {
        json resp = engine.execute({{"operation", "search"}, {"table", "users"}, {"key", 99}});
        assert(resp["status"] == "ok");
        assert(resp["rows"].size() == 0);
    }

    {
        json resp = engine.execute({{"operation", "range"}, {"table", "users"}, {"start", 1}, {"end", 2}});
        assert(resp["status"] == "ok");
        assert(resp["rows"].size() == 2);
        assert(resp["rows"][0]["name"] == "Alice");
        assert(resp["rows"][1]["name"] == "Bob");
    }

    {
        json resp = engine.execute({{"operation", "full_scan"}, {"table", "users"}});
        assert(resp["status"] == "ok");
        assert(resp["rows"].size() == 3);
    }

    {
        json resp = engine.execute({{"operation", "bad_op"}, {"table", "users"}});
        assert(resp["status"] == "error");
        assert(!resp["error"].is_null());
    }

    {
        json resp = engine.execute({{"operation", "insert"}, {"table", "users"}, {"key", 1}, {"data", {{"id", 1}, {"name", "Dup"}}}});
        assert(resp["status"] == "error");
    }

    {
        json resp = engine.execute({{"operation", "search"}});
        assert(resp["status"] == "error");
    }

    std::filesystem::remove_all(testDir);
    std::cout << "PASS: all engine execution tests passed\n";
    return 0;
}
