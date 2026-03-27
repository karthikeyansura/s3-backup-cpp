// dircache.cpp - mmap-based directory cache implementation for s3mount.
#include "s3backup/mount.h"
#include <unistd.h>
#include <sys/mman.h>
#include <vector>

namespace s3mount {

void DirCache::init(const std::vector<s3fs::DirLoc>& locs,
                    const uint8_t* dir_data, size_t dir_data_len) {
    if (dir_data_len == 0 || dir_data == nullptr) return;

    char tmpl[] = "/tmp/s3mount-dc.XXXXXX";
    fd_ = ::mkstemp(tmpl);
    if (fd_ < 0) return;

    // Ensure the file is unlinked immediately so it is deleted on close.
    ::unlink(tmpl);

    // Prepare the file size and write the directory data to the temporary store.
    if (::ftruncate(fd_, static_cast<off_t>(dir_data_len)) != 0) {
        ::close(fd_);
        fd_ = -1;
        return;
    }

    if (::write(fd_, dir_data, dir_data_len) != static_cast<ssize_t>(dir_data_len)) {
        ::close(fd_);
        fd_ = -1;
        return;
    }

    // Map the temporary file into memory for zero-copy access.
    void* map_addr = ::mmap(nullptr, dir_data_len, PROT_READ, MAP_SHARED, fd_, 0);
    if (map_addr == MAP_FAILED) {
        ::close(fd_);
        fd_ = -1;
        return;
    }

    data_ = static_cast<uint8_t*>(map_addr);
    data_len_ = dir_data_len;

    // Index the directory locations for rapid lookup by S3Offset.
    size_t byte_offset = 0;
    for (const auto& loc : locs) {
        if (loc.bytes > 0) {
            index_[loc.offset.raw()] = {byte_offset, loc.bytes};
            byte_offset += static_cast<size_t>(loc.bytes);
        }
    }
}

std::pair<const uint8_t*, size_t>
DirCache::find(s3fs::S3Offset off, uint64_t nbytes) const {
    if (nbytes == 0 || data_ == nullptr) return {nullptr, 0};

    auto it = index_.find(off.raw());
    if (it == index_.end()) return {nullptr, 0};

    return {data_ + it->second.offset, static_cast<size_t>(it->second.length)};
}

void DirCache::close() {
    if (data_ != nullptr) {
        ::munmap(data_, data_len_);
        data_ = nullptr;
    }
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
    index_.clear();
}

} // namespace s3mount