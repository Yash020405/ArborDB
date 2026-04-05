#include "serializer.h"
#include <cstring>
#include <stdexcept>
#include <queue>

namespace arbor {

static void writeUint8(uint8_t* buf, size_t& off, uint8_t v) {
    buf[off++] = v;
}

static void writeUint32(uint8_t* buf, size_t& off, uint32_t v) {
    std::memcpy(buf + off, &v, sizeof(v));
    off += sizeof(v);
}

static void writeInt64(uint8_t* buf, size_t& off, int64_t v) {
    std::memcpy(buf + off, &v, sizeof(v));
    off += sizeof(v);
}

static uint8_t readUint8(const uint8_t* buf, size_t& off) {
    return buf[off++];
}

static uint32_t readUint32(const uint8_t* buf, size_t& off) {
    uint32_t v;
    std::memcpy(&v, buf + off, sizeof(v));
    off += sizeof(v);
    return v;
}

static int64_t readInt64(const uint8_t* buf, size_t& off) {
    int64_t v;
    std::memcpy(&v, buf + off, sizeof(v));
    off += sizeof(v);
    return v;
}

void Serializer::save(BTree& tree, Pager& pager) {
    std::vector<BTreeNode*> allNodes;
    std::queue<BTreeNode*> q;
    q.push(tree.root_);
    while (!q.empty()) {
        BTreeNode* n = q.front(); q.pop();
        allNodes.push_back(n);
        for (BTreeNode* c : n->children) q.push(c);
    }

    std::unordered_map<BTreeNode*, uint32_t> pageMap;
    uint32_t nextPage = 1;
    for (BTreeNode* n : allNodes) {
        pageMap[n] = nextPage++;
    }

    Page metaPage;
    std::memset(&metaPage, 0, sizeof(Page));
    size_t metaOff = 0;
    writeUint32(metaPage.data, metaOff, pageMap.at(tree.root_));
    pager.writePage(0, metaPage);

    for (BTreeNode* n : allNodes) {
        uint32_t pageId = pageMap[n];
        Page page;
        std::memset(&page, 0, sizeof(Page));
        uint8_t* buf = page.data;
        size_t off = 0;

        writeUint8(buf, off, n->isLeaf ? 1 : 0);

        uint32_t numKeys = static_cast<uint32_t>(n->keys.size());
        writeUint32(buf, off, numKeys);
        for (int64_t k : n->keys) writeInt64(buf, off, k);

        uint32_t nextPageId = NULL_PAGE;
        if (n->isLeaf && n->next != nullptr) {
            auto it = pageMap.find(n->next);
            if (it != pageMap.end()) nextPageId = it->second;
        }
        writeUint32(buf, off, nextPageId);

        uint32_t numChildren = static_cast<uint32_t>(n->children.size());
        writeUint32(buf, off, numChildren);
        for (BTreeNode* child : n->children) {
            writeUint32(buf, off, pageMap.at(child));
        }

        uint32_t numValues = static_cast<uint32_t>(n->values.size());
        writeUint32(buf, off, numValues);
        for (const auto& val : n->values) {
            std::string s = val.is_null() ? "null" : val.dump();
            uint32_t len = static_cast<uint32_t>(s.size());
            if (off + sizeof(uint32_t) + len > PAGE_SIZE) {
                throw std::runtime_error("Node too large to fit in one page");
            }
            writeUint32(buf, off, len);
            std::memcpy(buf + off, s.data(), len);
            off += len;
        }

        pager.writePage(pageId, page);
    }

    pager.flush();
}

BTreeNode* Serializer::readNode(uint32_t pageId,
                                 Pager& pager,
                                 std::unordered_map<uint32_t, BTreeNode*>& nodeMap)
{
    auto it = nodeMap.find(pageId);
    if (it != nodeMap.end()) return it->second;

    Page* page = pager.readPage(pageId);
    const uint8_t* buf = page->data;
    size_t off = 0;

    bool isLeaf = readUint8(buf, off) == 1;
    BTreeNode* node = new BTreeNode(isLeaf);
    nodeMap[pageId] = node;

    uint32_t numKeys = readUint32(buf, off);
    node->keys.resize(numKeys);
    for (uint32_t i = 0; i < numKeys; i++) {
        node->keys[i] = readInt64(buf, off);
    }

    uint32_t nextPageId = readUint32(buf, off);

    uint32_t numChildren = readUint32(buf, off);
    std::vector<uint32_t> childPageIds(numChildren);
    for (uint32_t i = 0; i < numChildren; i++) {
        childPageIds[i] = readUint32(buf, off);
    }

    uint32_t numValues = readUint32(buf, off);
    node->values.resize(numValues);
    for (uint32_t i = 0; i < numValues; i++) {
        uint32_t len = readUint32(buf, off);
        std::string s(reinterpret_cast<const char*>(buf + off), len);
        off += len;
        node->values[i] = nlohmann::json::parse(s);
    }

    for (uint32_t childPageId : childPageIds) {
        node->children.push_back(readNode(childPageId, pager, nodeMap));
    }

    if (isLeaf && nextPageId != NULL_PAGE) {
        node->next = readNode(nextPageId, pager, nodeMap);
    }

    return node;
}

void Serializer::load(BTree& tree, Pager& pager) {
    if (pager.totalPages() == 0) return;

    Page* metaPage = pager.readPage(0);
    size_t off = 0;
    uint32_t rootPageId = readUint32(metaPage->data, off);

    if (rootPageId == 0 || rootPageId == NULL_PAGE || rootPageId >= pager.totalPages()) return;

    std::unordered_map<uint32_t, BTreeNode*> nodeMap;
    BTreeNode* root = readNode(rootPageId, pager, nodeMap);

    delete tree.root_;
    tree.root_ = root;
}

} // namespace arbor
