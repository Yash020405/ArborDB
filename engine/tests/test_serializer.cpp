#include "../src/storage/table_store.h"
#include "../src/storage/serializer.h"
#include "../src/disk/pager.h"
#include "../src/storage/btree.h"
#include <cassert>
#include <iostream>
#include <filesystem>

using namespace arbor;

int main() {
    const std::string testDir = "/tmp/arbor_serial_test";
    std::filesystem::remove_all(testDir);

    {
        TableStore store(testDir);
        TableSchema schema;
        schema.tableName  = "orders";
        schema.primaryKey = "id";
        schema.columns    = {{"id", ColumnType::INT}, {"item", ColumnType::STRING}, {"qty", ColumnType::INT}};
        store.createTable(schema);

        for (int i = 1; i <= 15; i++) {
            store.insert("orders", i, {{"id", i}, {"item", "item_" + std::to_string(i)}, {"qty", i * 2}});
        }

        auto [row, m] = store.search("orders", 7);
        assert(row.has_value());
        assert(row.value()["item"] == "item_7");
    }

    {
        TableStore store(testDir);

        auto [row5, m1] = store.search("orders", 5);
        assert(row5.has_value());
        assert(row5.value()["item"] == "item_5");
        assert(row5.value()["qty"]  == 10);

        auto [row15, m2] = store.search("orders", 15);
        assert(row15.has_value());
        assert(row15.value()["item"] == "item_15");

        auto [missing, m3] = store.search("orders", 99);
        assert(!missing.has_value());

        auto [rangeRows, m4] = store.rangeQuery("orders", 3, 7);
        assert(rangeRows.size() == 5);
        assert(rangeRows[0]["id"] == 3);
        assert(rangeRows[4]["id"] == 7);

        auto [allRows, m5] = store.fullScan("orders");
        assert(allRows.size() == 15);
        for (int i = 0; i < 15; i++) {
            assert(allRows[i]["id"] == i + 1);
        }
    }

    {
        BTree tree;
        std::filesystem::create_directories(testDir);
        Pager pager(testDir + "/btree_direct.db");

        tree.insert(10, {{"x", 100}});
        tree.insert(5,  {{"x", 50}});
        tree.insert(20, {{"x", 200}});

        Serializer::save(tree, pager);

        BTree tree2;
        Pager pager2(testDir + "/btree_direct.db");
        Serializer::load(tree2, pager2);

        assert(tree2.search(10).value()["x"] == 100);
        assert(tree2.search(5).value()["x"]  == 50);
        assert(tree2.search(20).value()["x"] == 200);
        assert(!tree2.search(99).has_value());
    }

    std::filesystem::remove_all(testDir);
    std::cout << "PASS: all serializer tests passed\n";
    return 0;
}
