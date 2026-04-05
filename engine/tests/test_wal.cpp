#include "../src/wal.h"
#include "../src/engine.h"
#include <cassert>
#include <iostream>
#include <filesystem>
#include <thread>

using namespace arbor;
using json = nlohmann::json;

int main() {
    const std::string testDir = "/tmp/arbor_wal_test";
    std::filesystem::remove_all(testDir);
    std::filesystem::create_directories(testDir);

    {
        WAL wal(testDir + "/wal.log");
        wal.logCreateTable("users", {{"id","INT"},{"name","STRING"}});
        wal.logInsert("users", 1, {{"id",1},{"name","Alice"}});
        wal.logInsert("users", 2, {{"id",2},{"name","Bob"}});
    }

    {
        WAL wal(testDir + "/wal.log");
        auto entries = wal.recover();
        assert(entries.size() == 3);
        assert(entries[0].op    == WALOpType::CREATE_TABLE);
        assert(entries[0].table == "users");
        assert(entries[1].op    == WALOpType::INSERT);
        assert(entries[1].key   == 1);
        assert(entries[1].data["name"] == "Alice");
        assert(entries[2].op    == WALOpType::INSERT);
        assert(entries[2].key   == 2);
        assert(entries[2].data["name"] == "Bob");
    }

    {
        WAL wal(testDir + "/wal.log");
        wal.truncate();
        auto entries = wal.recover();
        assert(entries.size() == 0);
    }

    {
        const std::string engineDir = testDir + "/engine";
        std::filesystem::create_directories(engineDir);
        Engine engine(engineDir);

        engine.execute({{"operation","create_table"},{"table","t"},
                        {"schema",{{"id","INT"},{"val","STRING"}}},{"primary_key","id"}});

        constexpr int N = 20;
        std::vector<std::thread> threads;
        for (int i = 1; i <= N; i++) {
            threads.emplace_back([&engine, i]() {
                engine.execute({{"operation","insert"},{"table","t"},
                                {"key",i},{"data",{{"id",i},{"val","v"}}}});
            });
        }
        for (auto& t : threads) t.join();

        json r = engine.execute({{"operation","full_scan"},{"table","t"}});
        assert(r["status"] == "ok");
        assert(r["metrics"]["rows_returned"].get<int>() == N);

        assert(std::filesystem::exists(engineDir + "/wal.log"));
    }

    std::filesystem::remove_all(testDir);
    std::cout << "PASS: all WAL + mutex tests passed\n";
    return 0;
}
