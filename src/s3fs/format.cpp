// format.cpp - On-disk format serialization.
#include "s3backup/format.h"
#include <stdexcept>

namespace s3fs {

namespace {
    // Little-endian helpers for explicit memory copies.
    uint16_t read16(const uint8_t* p) { uint16_t v; std::memcpy(&v, p, 2); return v; }
    uint32_t read32(const uint8_t* p) { uint32_t v; std::memcpy(&v, p, 4); return v; }
    uint64_t read64(const uint8_t* p) { uint64_t v; std::memcpy(&v, p, 8); return v; }
    void write16(uint8_t* p, uint16_t v) { std::memcpy(p, &v, 2); }
    void write32(uint8_t* p, uint32_t v) { std::memcpy(p, &v, 4); }
    void write64(uint8_t* p, uint64_t v) { std::memcpy(p, &v, 8); }
} // namespace

// Dirent implementation.

std::pair<Dirent, int> parse_dirent(const uint8_t* data, size_t len) {
    if (len < static_cast<size_t>(kDirentFixedSize))
        throw std::runtime_error("s3fs: buffer too small for dirent");

    Dirent d;
    d.mode    = read16(data + 0);
    d.uid     = read16(data + 2);
    d.gid     = read16(data + 4);
    d.ctime   = read32(data + 6);
    d.offset  = S3Offset(read64(data + 10));

    uint64_t bx = read64(data + 18);
    d.bytes   = unpack_bytes(bx);
    d.xattr   = unpack_xattr(bx);
    d.namelen = data[26];

    int total = kDirentFixedSize + d.namelen;
    if (len < static_cast<size_t>(total))
        throw std::runtime_error("s3fs: buffer too small for dirent name");

    d.name.assign(reinterpret_cast<const char*>(data + 27), d.namelen);
    return {d, total};
}

int marshal_dirent(const Dirent& d, uint8_t* buf) {
    write16(buf + 0, d.mode);
    write16(buf + 2, d.uid);
    write16(buf + 4, d.gid);
    write32(buf + 6, d.ctime);
    write64(buf + 10, d.offset.raw());
    write64(buf + 18, pack_bytes_xattr(d.bytes, d.xattr));
    buf[26] = d.namelen;
    std::memcpy(buf + 27, d.name.data(), d.namelen);
    return d.size();
}

void iter_dirents(const uint8_t* data, size_t len,
                  const std::function<bool(const Dirent&)>& fn) {
    size_t off = 0;
    while (off < len) {
        // Validation for remaining zero padding.
        bool all_zero = true;
        for (size_t i = off; i < len; ++i) {
            if (data[i] != 0) { all_zero = false; break; }
        }
        if (all_zero) break;

        auto [d, n] = parse_dirent(data + off, len - off);
        if (!fn(d)) break;
        off += n;
    }
}

std::optional<Dirent> lookup_dirent(const uint8_t* data, size_t len,
                                     std::string_view name) {
    std::optional<Dirent> found;
    iter_dirents(data, len, [&](const Dirent& d) -> bool {
        if (d.name == name) {
            found = d;
            return false;
        }
        return true;
    });
    return found;
}

// DirLoc implementation.

DirLoc parse_dirloc(const uint8_t* data) {
    return DirLoc{S3Offset(read64(data)), read32(data + 8)};
}

void marshal_dirloc(const DirLoc& dl, uint8_t* buf) {
    write64(buf, dl.offset.raw());
    write32(buf + 8, dl.bytes);
}

std::vector<DirLoc> parse_dirlocs(const uint8_t* data, size_t len) {
    size_t n = len / kDirLocSize;
    std::vector<DirLoc> locs(n);
    for (size_t i = 0; i < n; ++i)
        locs[i] = parse_dirloc(data + i * kDirLocSize);
    return locs;
}

std::vector<uint8_t> marshal_dirlocs(const std::vector<DirLoc>& locs) {
    std::vector<uint8_t> buf(locs.size() * kDirLocSize);
    for (size_t i = 0; i < locs.size(); ++i)
        marshal_dirloc(locs[i], buf.data() + i * kDirLocSize);
    return buf;
}

// StatFS implementation.

StatFS parse_statfs(const uint8_t* data) {
    StatFS s;
    s.total_sectors = read64(data + 0);
    s.files         = read32(data + 8);
    s.file_sectors  = read64(data + 12);
    s.dirs          = read32(data + 20);
    s.dir_sectors   = read64(data + 24);
    s.dir_bytes     = read64(data + 32);
    s.symlinks      = read32(data + 40);
    s.sym_sectors   = read64(data + 44);
    return s;
}

void marshal_statfs(const StatFS& s, uint8_t* buf) {
    write64(buf + 0,  s.total_sectors);
    write32(buf + 8,  s.files);
    write64(buf + 12, s.file_sectors);
    write32(buf + 20, s.dirs);
    write64(buf + 24, s.dir_sectors);
    write64(buf + 32, s.dir_bytes);
    write32(buf + 40, s.symlinks);
    write64(buf + 44, s.sym_sectors);
}

} // namespace s3fs