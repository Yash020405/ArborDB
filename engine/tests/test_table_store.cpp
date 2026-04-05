#include "../src/storage/table_store.h"
#include <cassert>
#include <iostream>
#include <filesystem>

using namespace arbor;

int main() {
    const std::string testDir = "/tmp/arbor_tablestore_test";
    std::filesystem::remove_all(testDir);

    TableStore store(testDir);

    TableSchema schema;
    schema.tableName  = "products";
    schema.primaryKey = "id";
    schema.columns    = {{"id", ColumnType::INT}, {"name", ColumnType::STRING}, {"price", ColumnType::FLOAT}};
    store.createTable(schema);

    store.insert("products", 1, {{"id", 1}, {"name", "Apple"},  {"price", 1.5}});
    store.insert("products", 2, {{"id", 2}, {"name", "Banana"}, {"price", 0.5}});
    store.insert("products", 3, {{"id", 3}, {"name", "Cherry"}, {"price", 3.0}});

    {
        auto [row, m] = store.search("products", 2);
        assert(row.has_value());
        assert(row.value()["name"] == "Banana");
    }

    {
        auto [row, m] = store.search("products", 99);
        assert(!row.has_value());
    }

    {
        auto [rows, m] = store.rangeQuery("products", 1, 2);
        assert(rows.size() == 2);
        assert(rows[0]["name"] == "Apple");
        assert(rows[1]["name"] == "Banana");
    }

    {
        auto [rows, m] = store.fullScan("products");
        assert(rows.size() == 3);
    }

    bool threw = false;
    try {
        store.insert("products", 4, {{"id", 4}, {"name", 99}, {"price", 1.0}});
    } catch (...) { threw = true; }
    assert(threw);

    threw = false;
    try {
        store.insert("products", 5, {{"id", 5}, {"price", 1.0}});
    } catch (...) { threw = true; }
    assert(threw);

    threw = false;
    try { store.search("no_such_table", 1); } catch (...) { threw = true; }
    assert(threw);

    auto tables = store.listTables();
    assert(tables.size() == 1);
    assert(tables[0] == "products");

    std::filesystem::remove_all(testDir);
    std::cout << "PASS: all table store tests passed\n";
    return 0;
}
