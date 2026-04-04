#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <optional>
#include "../../vendor/json.hpp"

namespace arbor {

static constexpr int BTREE_ORDER = 4;

struct BTreeNode {
    bool isLeaf;
    std::vector<int64_t> keys;
    std::vector<nlohmann::json> values;
    std::vector<BTreeNode*> children;
    BTreeNode* next;

    explicit BTreeNode(bool leaf);
    ~BTreeNode();
};

class BTree {
public:
    BTree();
    ~BTree();

    void insert(int64_t key, const nlohmann::json& value);
    std::optional<nlohmann::json> search(int64_t key) const;
    std::vector<nlohmann::json> rangeQuery(int64_t start, int64_t end) const;
    std::vector<nlohmann::json> fullScan() const;

    uint64_t nodeTraversals() const;
    void resetMetrics();

private:
    BTreeNode* root_;
    mutable uint64_t nodeTraversals_;

    void insertNonFull(BTreeNode* node, int64_t key, const nlohmann::json& value);
    void splitChild(BTreeNode* parent, int index);
};

} // namespace arbor
