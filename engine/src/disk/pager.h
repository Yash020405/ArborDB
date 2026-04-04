#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>
#include <fstream>
#include <stdexcept>

namespace arbor {

static constexpr uint32_t PAGE_SIZE = 4096;
static constexpr uint32_t NULL_PAGE = UINT32_MAX;

struct Page {
    uint8_t data[PAGE_SIZE];
};

class Pager {
public:
    explicit Pager(const std::string& filepath);
    ~Pager();

    Page* readPage(uint32_t pageId);
    void writePage(uint32_t pageId, const Page& page);
    uint32_t allocatePage();
    void flush();
    uint32_t totalPages() const;

private:
    std::string filepath_;
    std::fstream file_;
    uint32_t totalPages_;
    std::unordered_map<uint32_t, Page> cache_;

};

} // namespace arbor
