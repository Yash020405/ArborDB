#include "../src/storage/btree.h"
#include <cassert>
#include <iostream>

using namespace arbor;

int main() {
    {
        BTree tree;
        assert(!tree.search(1).has_value());

        tree.insert(5, {{"name", "Alice"}});
        tree.insert(3, {{"name", "Bob"}});
        tree.insert(8, {{"name", "Charlie"}});
        tree.insert(1, {{"name", "Dave"}});
        tree.insert(7, {{"name", "Eve"}});

        assert(tree.search(5).value()["name"] == "Alice");
        assert(tree.search(3).value()["name"] == "Bob");
        assert(tree.search(1).value()["name"] == "Dave");
        assert(!tree.search(99).has_value());

        bool threw = false;
        try { tree.insert(5, {{"name", "Dup"}}); } catch (...) { threw = true; }
        assert(threw);
    }

    {
        BTree tree;
        for (int i = 1; i <= 20; i++) {
            tree.insert(i, {{"id", i}});
        }
        auto results = tree.rangeQuery(5, 10);
        assert(results.size() == 6);
        assert(results[0]["id"] == 5);
        assert(results[5]["id"] == 10);

        auto all = tree.fullScan();
        assert(all.size() == 20);
        for (int i = 0; i < 20; i++) {
            assert(all[i]["id"] == i + 1);
        }
    }

    {
        BTree tree;
        for (int i = 1; i <= 100; i++) {
            tree.insert(i, {{"val", i * 10}});
        }
        for (int i = 1; i <= 100; i++) {
            assert(tree.search(i).value()["val"] == i * 10);
        }
        auto range = tree.rangeQuery(50, 60);
        assert(range.size() == 11);

        tree.resetMetrics();
        tree.search(50);
        assert(tree.nodeTraversals() > 0);
    }

    std::cout << "PASS: all B+ tree tests passed\n";
    return 0;
}
