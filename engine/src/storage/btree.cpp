#include "btree.h"
#include <stdexcept>
#include <algorithm>

namespace arbor {

BTreeNode::BTreeNode(bool leaf) : isLeaf(leaf), next(nullptr) {}

BTreeNode::~BTreeNode() {
    for (BTreeNode* child : children) {
        delete child;
    }
}

BTree::BTree() : root_(new BTreeNode(true)), nodeTraversals_(0) {}

BTree::~BTree() {
    delete root_;
}

uint64_t BTree::nodeTraversals() const {
    return nodeTraversals_;
}

void BTree::resetMetrics() {
    nodeTraversals_ = 0;
}

void BTree::insert(int64_t key, const nlohmann::json& value) {
    if (search(key).has_value()) {
        throw std::runtime_error("Duplicate key: " + std::to_string(key));
    }

    if (static_cast<int>(root_->keys.size()) == 2 * BTREE_ORDER - 1) {
        BTreeNode* newRoot = new BTreeNode(false);
        newRoot->children.push_back(root_);
        splitChild(newRoot, 0);
        root_ = newRoot;
    }
    insertNonFull(root_, key, value);
}

void BTree::insertNonFull(BTreeNode* node, int64_t key, const nlohmann::json& value) {
    int i = static_cast<int>(node->keys.size()) - 1;

    if (node->isLeaf) {
        node->keys.push_back(0);
        node->values.push_back(nlohmann::json{});
        while (i >= 0 && key < node->keys[i]) {
            node->keys[i + 1] = node->keys[i];
            node->values[i + 1] = node->values[i];
            i--;
        }
        node->keys[i + 1] = key;
        node->values[i + 1] = value;
    } else {
        while (i >= 0 && key < node->keys[i]) {
            i--;
        }
        i++;
        if (static_cast<int>(node->children[i]->keys.size()) == 2 * BTREE_ORDER - 1) {
            splitChild(node, i);
            if (key > node->keys[i]) {
                i++;
            }
        }
        insertNonFull(node->children[i], key, value);
    }
}

void BTree::splitChild(BTreeNode* parent, int index) {
    BTreeNode* fullChild = parent->children[index];
    BTreeNode* newChild = new BTreeNode(fullChild->isLeaf);
    int mid = BTREE_ORDER - 1;

    int64_t midKey = fullChild->keys[mid];

    newChild->keys.assign(fullChild->keys.begin() + mid + (fullChild->isLeaf ? 0 : 1), fullChild->keys.end());
    fullChild->keys.resize(mid);

    if (fullChild->isLeaf) {
        newChild->values.assign(fullChild->values.begin() + mid, fullChild->values.end());
        fullChild->values.resize(mid);
        newChild->next = fullChild->next;
        fullChild->next = newChild;
    } else {
        newChild->values.assign(fullChild->values.begin() + mid + 1, fullChild->values.end());
        fullChild->values.resize(mid);
        newChild->children.assign(fullChild->children.begin() + mid + 1, fullChild->children.end());
        fullChild->children.resize(mid + 1);
    }

    parent->keys.insert(parent->keys.begin() + index, midKey);
    parent->values.insert(parent->values.begin() + index, nlohmann::json{});
    parent->children.insert(parent->children.begin() + index + 1, newChild);
}

std::optional<nlohmann::json> BTree::search(int64_t key) const {
    BTreeNode* node = root_;
    while (node != nullptr) {
        nodeTraversals_++;
        int i = 0;
        while (i < static_cast<int>(node->keys.size()) && key > node->keys[i]) {
            i++;
        }
        if (node->isLeaf) {
            if (i < static_cast<int>(node->keys.size()) && node->keys[i] == key) {
                return node->values[i];
            }
            return std::nullopt;
        }
        if (i < static_cast<int>(node->keys.size()) && node->keys[i] == key) {
            node = node->children[i + 1];
        } else {
            node = node->children[i];
        }
    }
    return std::nullopt;
}

std::vector<nlohmann::json> BTree::rangeQuery(int64_t start, int64_t end) const {
    std::vector<nlohmann::json> results;
    BTreeNode* node = root_;

    while (!node->isLeaf) {
        nodeTraversals_++;
        int i = 0;
        while (i < static_cast<int>(node->keys.size()) && start > node->keys[i]) {
            i++;
        }
        node = node->children[i];
    }

    while (node != nullptr) {
        nodeTraversals_++;
        for (int i = 0; i < static_cast<int>(node->keys.size()); i++) {
            if (node->keys[i] > end) return results;
            if (node->keys[i] >= start) {
                results.push_back(node->values[i]);
            }
        }
        node = node->next;
    }
    return results;
}

std::vector<nlohmann::json> BTree::fullScan() const {
    std::vector<nlohmann::json> results;
    BTreeNode* node = root_;

    while (!node->isLeaf) {
        nodeTraversals_++;
        node = node->children[0];
    }

    while (node != nullptr) {
        nodeTraversals_++;
        for (const auto& val : node->values) {
            results.push_back(val);
        }
        node = node->next;
    }
    return results;
}

} // namespace arbor
