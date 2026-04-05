#include <iostream>
#include <string>
#include "src/engine.h"
#include "vendor/json.hpp"

namespace {

nlohmann::json parseCommandOrError(const std::string& input, bool& ok) {
    try {
        ok = true;
        return nlohmann::json::parse(input);
    } catch (const std::exception& e) {
        ok = false;
        return nlohmann::json({
            {"status", "error"},
            {"rows", nlohmann::json::array()},
            {"error", std::string("JSON parse error: ") + e.what()},
            {"metrics", {{"time_ms", 0}, {"disk_reads", 0}, {"nodes_traversed", 0}}}
        });
    }
}

void runServerMode(const std::string& dataDir) {
    arbor::Engine engine(dataDir);
    std::string line;

    while (std::getline(std::cin, line)) {
        if (line.empty()) {
            continue;
        }

        bool ok = false;
        nlohmann::json cmdOrError = parseCommandOrError(line, ok);
        if (!ok) {
            std::cout << cmdOrError.dump() << "\n";
            std::cout.flush();
            continue;
        }

        nlohmann::json response = engine.execute(cmdOrError);
        std::cout << response.dump() << "\n";
        std::cout.flush();
    }
}

} // namespace

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cout << nlohmann::json({
            {"status", "error"},
            {"rows", nlohmann::json::array()},
            {"error", "No command provided. Usage: engine '<json>' or engine --server [data_dir]"},
            {"metrics", {{"time_ms", 0}, {"disk_reads", 0}, {"nodes_traversed", 0}}}
        }).dump() << "\n";
        return 1;
    }

    const std::string arg1 = argv[1];
    if (arg1 == "--server") {
        std::string dataDir = "./data/tables";
        if (argc >= 3) {
            dataDir = argv[2];
        }

        runServerMode(dataDir);
        return 0;
    }

    std::string dataDir = "./data/tables";
    if (argc >= 3) {
        dataDir = argv[2];
    }

    bool ok = false;
    nlohmann::json command = parseCommandOrError(argv[1], ok);
    if (!ok) {
        std::cout << command.dump() << "\n";
        return 1;
    }

    arbor::Engine engine(dataDir);
    nlohmann::json response = engine.execute(command);
    std::cout << response.dump() << "\n";
    return 0;
}
