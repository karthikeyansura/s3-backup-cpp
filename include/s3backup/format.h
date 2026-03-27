// s3fs/format.h - On-disk binary format for S3 backup objects.
//
// All structures are packed (no alignment padding) and little-endian
// to match the original C implementation byte-for-byte.
#pragma once

#include <string>
#include <string_view>
#include <vector>
#include <functional>
#include <optional>

namespace s3fs {

constexpr uint32_t kSectorSize = 512;
constexpr uint32_t kMagic = 0x55423353; // 'S' | ('3'<<8) | ('B'<<16) | ('U'<<24)

// Flags
constexpr uint32_t kFlagUseInum   = 2;
constexpr uint32_t kFlagDirLoc    = 4;
constexpr uint32_t kFlagDirData   = 8;
constexpr uint32_t kFlagDirPacked = 16;
constexpr uint32_t kFlagHardLinks = 32;

// S3Offset: 48-bit sector + 16-bit object index
class S3Offset {
public:
    S3Offset() : raw_(0) {}
    explicit S3Offset(uint64_t raw) : raw_(raw) {}
    S3Offset(uint64_t sector, uint16_t object)
        : raw_((sector & 0x0000FFFFFFFFFFFF) | (static_cast<uint64_t>(object) << 48)) {}

    [[nodiscard]] uint64_t sector() const { return raw_ & 0x0000FFFFFFFFFFFF; }
    [[nodiscard]] uint16_t object() const { return static_cast<uint16_t>(raw_ >> 48); }
    [[nodiscard]] uint64_t raw() const { return raw_; }
    [[nodiscard]] bool is_zero() const { return raw_ == 0; }

    bool operator==(const S3Offset& o) const { return raw_ == o.raw_; }
    bool operator!=(const S3Offset& o) const { return raw_ != o.raw_; }

private:
    uint64_t raw_;
};

// Bytes/Xattr bitfield (52 + 12 bits in a uint64)
inline uint64_t pack_bytes_xattr(uint64_t bytes, uint16_t xattr) {
    return (bytes & 0x000FFFFFFFFFFFFF) | (static_cast<uint64_t>(xattr) << 52);
}
inline uint64_t unpack_bytes(uint64_t v) { return v & 0x000FFFFFFFFFFFFF; }
inline uint16_t unpack_xattr(uint64_t v) { return static_cast<uint16_t>(v >> 52); }

// Dirent: variable-length directory entry
//
// On-disk layout (packed, 27 bytes fixed + namelen):
//   mode:16  uid:16  gid:16  ctime:32  offset:64  bytes_xattr:64  namelen:8  name[namelen]

constexpr int kDirentFixedSize = 2 + 2 + 2 + 4 + 8 + 8 + 1; // = 27

struct Dirent {
    uint16_t mode = 0;
    uint16_t uid = 0;
    uint16_t gid = 0;
    uint32_t ctime = 0;
    S3Offset offset;
    uint64_t bytes = 0;   // 52-bit file size
    uint16_t xattr = 0;   // 12-bit xattr length
    uint8_t  namelen = 0;
    std::string name;

    [[nodiscard]] int size() const { return kDirentFixedSize + static_cast<int>(namelen); }
};

// Parse a single dirent from raw bytes. Returns {dirent, bytes_consumed}.
std::pair<Dirent, int> parse_dirent(const uint8_t* data, size_t len);

// Serialize a dirent into buf. Returns bytes written.
int marshal_dirent(const Dirent& d, uint8_t* buf);

// Iterate over dirents in a directory data buffer.
// fn returns false to stop early.
void iter_dirents(const uint8_t* data, size_t len,
                  const std::function<bool(const Dirent&)>& fn);

// Find a dirent by name in directory data.
std::optional<Dirent> lookup_dirent(const uint8_t* data, size_t len,
                                     std::string_view name);

// DirLoc: directory location entry (packed, 12 bytes)
struct DirLoc {
    S3Offset offset;
    uint32_t bytes = 0;
};

constexpr int kDirLocSize = 12;

DirLoc parse_dirloc(const uint8_t* data);
void marshal_dirloc(const DirLoc& dl, uint8_t* buf);
std::vector<DirLoc> parse_dirlocs(const uint8_t* data, size_t len);
std::vector<uint8_t> marshal_dirlocs(const std::vector<DirLoc>& locs);

// StatFS: filesystem statistics (52 bytes)
struct StatFS {
    uint64_t total_sectors = 0;
    uint32_t files = 0;
    uint64_t file_sectors = 0;
    uint32_t dirs = 0;
    uint64_t dir_sectors = 0;
    uint64_t dir_bytes = 0;
    uint32_t symlinks = 0;
    uint64_t sym_sectors = 0;
};

constexpr int kStatFSSize = 52;

StatFS parse_statfs(const uint8_t* data);
void marshal_statfs(const StatFS& s, uint8_t* buf);

// Helpers
inline int64_t round_up(int64_t a, int64_t b) {
    return b * ((a + b - 1) / b);
}

inline int64_t sectors_for(int64_t n) {
    return round_up(n, kSectorSize) / kSectorSize;
}

inline int64_t pad_to_sector(int64_t n) {
    return round_up(n, kSectorSize) - n;
}

} // namespace s3fs