#include <iostream>
#include <string>
#include "src/engine.h"
#include "vendor/json.hpp"

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cout << nlohmann::json({
            {"status", "error"},
            {"rows", nlohmann::json::array()},
            {"error", "No command provided. Usage: engine '<json>'"},
            {"metrics", {{"time_ms", 0}, {"disk_reads", 0}, {"nodes_traversed", 0}}}
        }).dump() << "\n";
        return 1;
    }

    std::string dataDir = "./data/tables";
    if (argc >= 3) {
        dataDir = argv[2];
    }

    nlohmann::json command;
    try {
        command = nlohmann::json::parse(argv[1]);
    } catch (const std::exception& e) {
        std::cout << nlohmann::json({
            {"status", "error"},
            {"rows", nlohmann::json::array()},
            {"error", std::string("JSON parse error: ") + e.what()},
            {"metrics", {{"time_ms", 0}, {"disk_reads", 0}, {"nodes_traversed", 0}}}
        }).dump() << "\n";
        return 1;
    }

    arbor::Engine engine(dataDir);
    nlohmann::json response = engine.execute(command);
    std::cout << response.dump() << "\n";
    return 0;
}
