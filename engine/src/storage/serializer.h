#pragma once

#include <string>
#include <unordered_map>
#include "btree.h"
#include "../disk/pager.h"
#include "../../vendor/json.hpp"

namespace arbor {

class Serializer {
public:
    static void save(BTree& tree, Pager& pager);
    static void load(BTree& tree, Pager& pager);

private:
    static uint32_t writeNode(BTreeNode* node,
                              Pager& pager,
                              std::unordered_map<BTreeNode*, uint32_t>& pageMap);

    static BTreeNode* readNode(uint32_t pageId,
                               Pager& pager,
                               std::unordered_map<uint32_t, BTreeNode*>& nodeMap);
};

} // namespace arbor
