#include "../src/storage/table_store.h"
#include <cassert>
#include <iostream>
#include <filesystem>

using namespace arbor;

int main() {
    const std::string testDir = "/tmp/arbor_secidx_test";
    std::filesystem::remove_all(testDir);

    TableStore store(testDir);

    TableSchema schema;
    schema.tableName  = "employees";
    schema.primaryKey = "id";
    schema.columns    = {{"id", ColumnType::INT}, {"name", ColumnType::STRING}, {"dept", ColumnType::STRING}};
    store.createTable(schema);

    store.insert("employees", 1, {{"id",1},{"name","Alice"},{"dept","Engineering"}});
    store.insert("employees", 2, {{"id",2},{"name","Bob"},  {"dept","Marketing"}});
    store.insert("employees", 3, {{"id",3},{"name","Carol"},{"dept","Engineering"}});
    store.insert("employees", 4, {{"id",4},{"name","Dave"}, {"dept","Engineering"}});
    store.insert("employees", 5, {{"id",5},{"name","Eve"},  {"dept","Marketing"}});

    {
        auto [rows, m] = store.searchByColumn("employees", "dept", "Engineering");
        assert(rows.size() == 3);
        assert(m.rows_returned == 3);
        assert(m.nodes_traversed > 0);
        bool foundAlice = false, foundCarol = false, foundDave = false;
        for (auto& r : rows) {
            if (r["name"] == "Alice") foundAlice = true;
            if (r["name"] == "Carol") foundCarol = true;
            if (r["name"] == "Dave")  foundDave  = true;
        }
        assert(foundAlice && foundCarol && foundDave);
    }

    {
        auto [rows, m] = store.searchByColumn("employees", "dept", "Marketing");
        assert(rows.size() == 2);
        assert(m.rows_returned == 2);
    }

    {
        auto [rows, m] = store.searchByColumn("employees", "dept", "NoSuchDept");
        assert(rows.size() == 0);
        assert(m.rows_returned == 0);
    }

    {
        auto [rows, m] = store.searchByColumn("employees", "name", "Bob");
        assert(rows.size() == 1);
        assert(rows[0]["id"] == 2);
    }

    std::filesystem::remove_all(testDir);
    std::cout << "PASS: all secondary index tests passed\n";
    return 0;
}
