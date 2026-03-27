// backup/backup.h - Full and incremental backup engine.
#pragma once

#include "s3backup/format.h"
#include "s3backup/store.h"
#include <string>
#include <vector>

namespace backup {

    struct Config {
        store::ObjectStore* store = nullptr;
        std::string bucket;
        std::string new_name;
        std::string old_name;       // empty for full backup
        std::string dir;            // local directory to back up
        std::string tag = "--root--";
        bool verbose = false;
        bool noio = false;
        std::vector<std::string> exclude;
        int64_t stop_after = 0;     // 0 = unlimited
        int version_idx = 0;
    };

    struct Result {
        s3fs::StatFS stats;
        bool truncated = false;
    };

    Result run(const Config& cfg);

    // Parse a size string with K/M/G suffix.
    int64_t parse_size(const std::string& s);

    // SectorWriter definition
    class SectorWriter {
    public:
        SectorWriter(store::ObjectStore::Writer* w, bool noio);
        void write(const uint8_t* data, size_t len);

        [[nodiscard]] int64_t offset() const { return total_written_ / s3fs::kSectorSize; }
        [[nodiscard]] int64_t total_written() const { return total_written_; }

    private:
        store::ObjectStore::Writer* w_;
        bool noio_;
        int64_t total_written_ = 0;
    };

} // namespace backup