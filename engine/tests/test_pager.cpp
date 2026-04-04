#include "../src/disk/pager.h"
#include <cassert>
#include <cstring>
#include <iostream>
#include <cstdio>

using namespace arbor;

int main() {
    const std::string testFile = "/tmp/arbor_pager_test.db";
    std::remove(testFile.c_str());

    {
        Pager pager(testFile);
        assert(pager.totalPages() == 0);

        uint32_t p0 = pager.allocatePage();
        uint32_t p1 = pager.allocatePage();
        assert(p0 == 0);
        assert(p1 == 1);
        assert(pager.totalPages() == 2);

        Page page;
        std::memset(&page, 0, sizeof(Page));
        std::memcpy(page.data, "hello arbor", 11);
        pager.writePage(p0, page);

        Page* readBack = pager.readPage(p0);
        assert(std::memcmp(readBack->data, "hello arbor", 11) == 0);

        Page page2;
        std::memset(&page2, 0, sizeof(Page));
        page2.data[0] = 0xDE;
        page2.data[1] = 0xAD;
        pager.writePage(p1, page2);

        pager.flush();
    }

    {
        Pager pager(testFile);
        assert(pager.totalPages() == 2);

        Page* p0 = pager.readPage(0);
        assert(std::memcmp(p0->data, "hello arbor", 11) == 0);

        Page* p1 = pager.readPage(1);
        assert(p1->data[0] == 0xDE);
        assert(p1->data[1] == 0xAD);

        uint32_t p2 = pager.allocatePage();
        assert(p2 == 2);
    }

    std::remove(testFile.c_str());
    std::cout << "PASS: all pager tests passed\n";
    return 0;
}
