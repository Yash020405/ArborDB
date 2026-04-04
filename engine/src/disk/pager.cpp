#include "pager.h"
#include <cstring>
#include <iostream>

namespace arbor {

Pager::Pager(const std::string& filepath) : filepath_(filepath), totalPages_(0) {
    file_.open(filepath_, std::ios::in | std::ios::out | std::ios::binary);
    if (!file_.is_open()) {
        file_.open(filepath_, std::ios::out | std::ios::binary);
        file_.close();
        file_.open(filepath_, std::ios::in | std::ios::out | std::ios::binary);
    }
    if (!file_.is_open()) {
        throw std::runtime_error("Cannot open pager file: " + filepath_);
    }

    file_.seekg(0, std::ios::end);
    std::streamsize fileSize = file_.tellg();
    totalPages_ = static_cast<uint32_t>(fileSize / PAGE_SIZE);
}

Pager::~Pager() {
    flush();
    if (file_.is_open()) {
        file_.close();
    }
}

Page* Pager::readPage(uint32_t pageId) {
    auto it = cache_.find(pageId);
    if (it != cache_.end()) {
        return &it->second;
    }

    Page page;
    std::memset(&page, 0, sizeof(Page));

    if (pageId < totalPages_) {
        file_.seekg(static_cast<std::streamoff>(pageId) * PAGE_SIZE, std::ios::beg);
        file_.read(reinterpret_cast<char*>(page.data), PAGE_SIZE);
        if (file_.fail()) {
            file_.clear();
        }
    }

    cache_[pageId] = page;
    return &cache_[pageId];
}

void Pager::writePage(uint32_t pageId, const Page& page) {
    cache_[pageId] = page;
    if (pageId >= totalPages_) {
        totalPages_ = pageId + 1;
    }
}

uint32_t Pager::allocatePage() {
    uint32_t pageId = totalPages_;
    Page page;
    std::memset(&page, 0, sizeof(Page));
    writePage(pageId, page);
    return pageId;
}

void Pager::flush() {
    for (auto& [pageId, page] : cache_) {
        file_.seekp(static_cast<std::streamoff>(pageId) * PAGE_SIZE, std::ios::beg);
        file_.write(reinterpret_cast<const char*>(page.data), PAGE_SIZE);
    }
    file_.flush();
}

uint32_t Pager::totalPages() const {
    return totalPages_;
}

} // namespace arbor
