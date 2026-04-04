#pragma once

#include <cstdint>
#include <chrono>
#include "../vendor/json.hpp"

namespace arbor {

struct Metrics {
    uint64_t time_ms        = 0;
    uint64_t disk_reads     = 0;
    uint64_t nodes_traversed = 0;
    uint64_t rows_returned  = 0;

    nlohmann::json toJson() const {
        return {
            {"time_ms",         time_ms},
            {"disk_reads",      disk_reads},
            {"nodes_traversed", nodes_traversed},
            {"rows_returned",   rows_returned}
        };
    }
};

class Timer {
public:
    Timer() : start_(std::chrono::steady_clock::now()) {}

    uint64_t elapsedMs() const {
        auto now = std::chrono::steady_clock::now();
        return static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(now - start_).count()
        );
    }

private:
    std::chrono::steady_clock::time_point start_;
};

} // namespace arbor
