// mount.h - FUSE mount infrastructure including directory and data caching.
#pragma once

#include "s3backup/format.h"
#include "s3backup/store.h"
#include <unordered_map>
#include <vector>
#include <string>
#include <mutex>

namespace s3mount {

// DirCache: mmap-based directory metadata cache.
class DirCache {
public:
    DirCache() = default;
    ~DirCache() { close(); }
    DirCache(const DirCache&) = delete;
    DirCache& operator=(const DirCache&) = delete;

    void init(const std::vector<s3fs::DirLoc>& locs,
              const uint8_t* dir_data, size_t dir_data_len);

    [[nodiscard]] std::pair<const uint8_t*, size_t>
    find(s3fs::S3Offset off, uint64_t nbytes) const;

    void close();

private:
    struct Entry { size_t offset; uint32_t length; };
    uint8_t* data_ = nullptr;
    size_t data_len_ = 0;
    int fd_ = -1;
    std::unordered_map<uint64_t, Entry> index_;
};

// DataCache: LRU block-level file data cache.
class DataCache {
public:
    explicit DataCache(store::ObjectStore* st) : store_(st) {}

    void read(const std::string& key, int version,
              uint8_t* buf, int64_t offset, size_t len, int64_t max_offset);

private:
    static constexpr int kCacheSize = 16;
    static constexpr int64_t kCacheBlockSize = 16 * 1024 * 1024;

    struct CEntry {
        bool valid = false;
        int64_t seq = 0;
        int version = -1;
        int64_t base = 0;
        size_t length = 0;
        std::vector<uint8_t> data;
    };

    struct BlockResult { const uint8_t* data; size_t length; int64_t base; };
    BlockResult get_block(const std::string& key, int version,
                          int64_t offset, int64_t max_offset);

    store::ObjectStore* store_;
    std::mutex mu_;
    CEntry entries_[kCacheSize] = {};
    int64_t seq_ = 0;
};

// MountState: Global state structure for FUSE operations.
struct MountState {
    store::ObjectStore* store = nullptr;
    std::vector<std::string> names;
    std::vector<int64_t> nsectors;
    s3fs::Dirent root_de;
    s3fs::StatFS statfs_info;
    DirCache dir_cache;
    DataCache* data_cache = nullptr;
    bool no_cache = false;
};

// Initialize mount state from backup metadata.
void init_mount_state(MountState& state, store::ObjectStore* st,
                      const std::string& object_key, bool verbose);

// Execute fuse_main with defined operations.
int run_fuse(MountState& state, const std::string& mountpoint,
             bool allow_other, bool foreground);

} // namespace s3mount