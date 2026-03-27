// datacache.cpp - LRU data block cache for s3mount.
#include "s3backup/mount.h"
#include <algorithm>

namespace s3mount {

void DataCache::read(const std::string& key, int version,
                     uint8_t* buf, int64_t offset, size_t len,
                     int64_t max_offset) {
    size_t remaining = len;
    size_t pos = 0;
    while (remaining > 0) {
        auto [data, data_len, base] = get_block(key, version, offset, max_offset);
        if (!data || data_len == 0) break;
        int64_t in_block = offset - base;
        size_t avail = data_len - static_cast<size_t>(in_block);
        size_t to_copy = std::min(remaining, avail);
        std::memcpy(buf + pos, data + in_block, to_copy);
        pos += to_copy;
        offset += static_cast<int64_t>(to_copy);
        remaining -= to_copy;
    }
}

DataCache::BlockResult
DataCache::get_block(const std::string& key, int version,
                     int64_t offset, int64_t max_offset) {
    int64_t base = offset & ~(kCacheBlockSize - 1);
    std::unique_lock<std::mutex> lock(mu_);

    for (auto& e : entries_) {
        if (e.valid && e.base == base && e.version == version) {
            e.seq = seq_++;
            return {e.data.data(), e.length, e.base};
        }
    }

    int slot = -1;
    for (int i = 0; i < kCacheSize; ++i) {
        if (!entries_[i].valid) { slot = i; break; }
    }
    if (slot < 0) {
        int64_t min_seq = seq_;
        for (int i = 0; i < kCacheSize; ++i) {
            if (entries_[i].seq < min_seq) { min_seq = entries_[i].seq; slot = i; }
        }
    }

    int64_t read_len = kCacheBlockSize;
    if (base + read_len > max_offset) read_len = max_offset - base;
    if (read_len <= 0) return {nullptr, 0, base};

    lock.unlock();
    auto fetched = store_->get_range(key, base, read_len);
    lock.lock();

    auto& e = entries_[slot];
    e.data = std::move(fetched);
    e.valid = true;
    e.version = version;
    e.base = base;
    e.length = e.data.size();
    e.seq = seq_++;
    return {e.data.data(), e.length, e.base};
}

} // namespace s3mount