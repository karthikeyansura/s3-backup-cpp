// test_format.cpp - Binary format round-trip validation.
#include "s3backup/format.h"
#include "s3backup/superblock.h"
#include <iostream>
#include <vector>
#include <string>
#include <optional>

static int tests_passed = 0;
static int tests_failed = 0;

// Assertion macro for tracking test pass/fail state.
#define CHECK(cond, msg) do { \
    if (!(cond)) { \
        std::cerr << "FAIL: " << msg << " (" << __FILE__ << ":" << __LINE__ << ")\n"; \
        tests_failed++; \
    } else { \
        tests_passed++; \
    } \
} while(0)

// Validation of S3Offset bit-packing logic.
void test_s3offset() {
    auto o = s3fs::S3Offset(1024, 3);
    CHECK(o.sector() == 1024, "sector");
    CHECK(o.object() == 3, "object");

    auto o2 = s3fs::S3Offset(0xFFFFFFFFFFFFULL, 0xFFFF);
    CHECK(o2.sector() == 0xFFFFFFFFFFFFULL, "max sector");
    CHECK(o2.object() == 0xFFFF, "max object");
}

// Validation of byte count and xattr bit-packing logic.
void test_bytes_xattr() {
    uint64_t packed = s3fs::pack_bytes_xattr(999999999ULL, 4095);
    CHECK(s3fs::unpack_bytes(packed) == 999999999ULL, "unpack bytes");
    CHECK(s3fs::unpack_xattr(packed) == 4095, "unpack xattr");
}

// Validation of Dirent serialization and deserialization.
void test_dirent_roundtrip() {
    s3fs::Dirent d;
    d.mode = 040755;
    d.uid = 1000;
    d.gid = 1000;
    d.ctime = 1700000000;
    d.offset = s3fs::S3Offset(42, 1);
    d.bytes = 8192;
    d.xattr = 0;
    d.namelen = 7;
    d.name = "testdir";

    std::vector<uint8_t> buf(d.size() + 10, 0);
    int n = s3fs::marshal_dirent(d, buf.data());
    CHECK(static_cast<size_t>(n) == d.size(), "marshal size");

    auto [parsed, consumed] = s3fs::parse_dirent(buf.data(), buf.size());
    CHECK(static_cast<size_t>(consumed) == d.size(), "consumed");
    CHECK(parsed.mode == d.mode, "mode");
    CHECK(parsed.uid == d.uid, "uid");
    CHECK(parsed.gid == d.gid, "gid");
    CHECK(parsed.ctime == d.ctime, "ctime");
    CHECK(parsed.offset.sector() == 42, "offset.sector");
    CHECK(parsed.offset.object() == 1, "offset.object");
    CHECK(parsed.bytes == 8192, "bytes");
    CHECK(parsed.name == "testdir", "name");
}

// Validation of directory entry stream iteration.
void test_dirent_iteration() {
    std::vector<s3fs::Dirent> entries = {
        {0100644, 1000, 1000, 1700000000, s3fs::S3Offset(10,0), 100, 0, 5, "file1"},
        {0100644, 1000, 1000, 1700000001, s3fs::S3Offset(20,0), 200, 0, 5, "file2"},
        {040755,  1000, 1000, 1700000002, s3fs::S3Offset(30,0), 4096, 0, 6, "subdir"},
    };

    size_t total = 0;
    for (const auto& e : entries) total += e.size();
    std::vector<uint8_t> buf(total + 512, 0);
    size_t off = 0;
    for (const auto& e : entries)
        off += static_cast<size_t>(s3fs::marshal_dirent(e, buf.data() + off));

    std::vector<std::string> found;
    s3fs::iter_dirents(buf.data(), off, [&](const s3fs::Dirent& d) -> bool {
        found.push_back(d.name);
        return true;
    });

    CHECK(found.size() == 3, "iter count");
    CHECK(found[0] == "file1", "iter[0]");
    CHECK(found[1] == "file2", "iter[1]");
    CHECK(found[2] == "subdir", "iter[2]");
}

// Validation of specific entry lookup within a directory block.
void test_lookup_dirent() {
    s3fs::Dirent d1{0100644, 1000, 1000, 1700000000, s3fs::S3Offset(10,0), 100, 0, 5, "file1"};
    s3fs::Dirent d2{0100644, 1000, 1000, 1700000001, s3fs::S3Offset(20,0), 200, 0, 5, "file2"};

    std::vector<uint8_t> buf(1024, 0);
    size_t off = 0;
    off += static_cast<size_t>(s3fs::marshal_dirent(d1, buf.data() + off));
    off += static_cast<size_t>(s3fs::marshal_dirent(d2, buf.data() + off));

    auto found = s3fs::lookup_dirent(buf.data(), off, "file2");
    CHECK(found.has_value(), "lookup found");
    CHECK(found->bytes == 200, "lookup bytes");

    auto miss = s3fs::lookup_dirent(buf.data(), off, "file3");
    CHECK(!miss.has_value(), "lookup miss");
}

// Validation of DirLoc structure serialization.
void test_dirloc_roundtrip() {
    std::vector<s3fs::DirLoc> locs = {
        {s3fs::S3Offset(100, 2), 4096},
        {s3fs::S3Offset(200, 3), 8192},
    };

    auto data = s3fs::marshal_dirlocs(locs);
    auto parsed = s3fs::parse_dirlocs(data.data(), data.size());

    CHECK(parsed.size() == 2, "dirloc count");
    CHECK(parsed[0].offset.raw() == locs[0].offset.raw(), "dirloc[0].offset");
    CHECK(parsed[0].bytes == 4096, "dirloc[0].bytes");
    CHECK(parsed[1].bytes == 8192, "dirloc[1].bytes");
}

// Validation of StatFS metadata serialization.
void test_statfs_roundtrip() {
    s3fs::StatFS orig{10000, 500, 8000, 50, 200, 90000, 10, 5};
    std::vector<uint8_t> buf(s3fs::kStatFSSize);
    s3fs::marshal_statfs(orig, buf.data());
    auto parsed = s3fs::parse_statfs(buf.data());

    CHECK(parsed.total_sectors == 10000, "statfs.total_sectors");
    CHECK(parsed.files == 500, "statfs.files");
    CHECK(parsed.file_sectors == 8000, "statfs.file_sectors");
    CHECK(parsed.dirs == 50, "statfs.dirs");
    CHECK(parsed.dir_bytes == 90000, "statfs.dir_bytes");
    CHECK(parsed.symlinks == 10, "statfs.symlinks");
}

// Validation of Superblock creation and versioning.
void test_superblock_roundtrip() {
    auto [buf, sectors] = s3fs::make_superblock("backup-2024-01-01", {});
    CHECK(sectors >= 1, "superblock sectors");
    CHECK(buf.size() == static_cast<size_t>(sectors) * s3fs::kSectorSize, "superblock size");

    auto sb = s3fs::parse_superblock(buf.data(), buf.size());
    CHECK(sb.magic == s3fs::kMagic, "magic");
    CHECK(sb.nvers == 1, "nvers");
    CHECK(sb.versions[0].name == "backup-2024-01-01", "version name");
    CHECK(sb.version_names().size() == 1, "version_names size");
    CHECK(sb.version_names()[0] == "backup-2024-01-01", "version_names[0]");
}

// Validation of sector alignment rounding utility.
void test_roundup() {
    CHECK(s3fs::round_up(0, 512) == 0, "roundup 0");
    CHECK(s3fs::round_up(1, 512) == 512, "roundup 1");
    CHECK(s3fs::round_up(511, 512) == 512, "roundup 511");
    CHECK(s3fs::round_up(512, 512) == 512, "roundup 512");
    CHECK(s3fs::round_up(513, 512) == 1024, "roundup 513");
}

int main() {
    test_s3offset();
    test_bytes_xattr();
    test_dirent_roundtrip();
    test_dirent_iteration();
    test_lookup_dirent();
    test_dirloc_roundtrip();
    test_statfs_roundtrip();
    test_superblock_roundtrip();
    test_roundup();

    std::cout << tests_passed << " passed, " << tests_failed << " failed\n";
    return tests_failed > 0 ? 1 : 0;
}