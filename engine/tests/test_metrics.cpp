#include "../src/engine.h"
#include <cassert>
#include <iostream>
#include <filesystem>

using namespace arbor;
using json = nlohmann::json;

int main() {
    const std::string testDir = "/tmp/arbor_metrics_test";
    std::filesystem::remove_all(testDir);

    Engine engine(testDir);

    engine.execute({{"operation","create_table"},{"table","items"},
                    {"schema",{{"id","INT"},{"name","STRING"}}},{"primary_key","id"}});

    for (int i = 1; i <= 10; i++) {
        engine.execute({{"operation","insert"},{"table","items"},
                        {"key",i},{"data",{{"id",i},{"name","item"}}}});
    }

    {
        json r = engine.execute({{"operation","search"},{"table","items"},{"key",5}});
        assert(r["status"] == "ok");
        assert(r["metrics"]["nodes_traversed"].get<int>() > 0);
        assert(r["metrics"]["rows_returned"].get<int>() == 1);
        assert(r["metrics"].contains("time_ms"));
        assert(r["metrics"].contains("disk_reads"));
    }

    {
        json r = engine.execute({{"operation","search"},{"table","items"},{"key",99}});
        assert(r["status"] == "ok");
        assert(r["metrics"]["rows_returned"].get<int>() == 0);
    }

    {
        json r = engine.execute({{"operation","range"},{"table","items"},{"start",3},{"end",7}});
        assert(r["status"] == "ok");
        assert(r["metrics"]["rows_returned"].get<int>() == 5);
        assert(r["metrics"]["nodes_traversed"].get<int>() > 0);
    }

    {
        json r = engine.execute({{"operation","full_scan"},{"table","items"}});
        assert(r["status"] == "ok");
        assert(r["metrics"]["rows_returned"].get<int>() == 10);
    }

    {
        json r = engine.execute({{"operation","bad_op"},{"table","items"}});
        assert(r["status"] == "error");
        assert(r["metrics"]["time_ms"].get<int>() == 0);
        assert(r["metrics"]["disk_reads"].get<int>() == 0);
        assert(!r["error"].is_null());
    }

    std::filesystem::remove_all(testDir);
    std::cout << "PASS: all metrics tests passed\n";
    return 0;
}
