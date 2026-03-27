// test_backup.cpp - End-to-end backup and verification test.
#include "s3backup/backup.h"
#include "s3backup/format.h"
#include "s3backup/superblock.h"
#include "s3backup/store.h"
#include <fstream>
#include <filesystem>
#include <iostream>
#include <string>
#include <algorithm>
#include <vector>

namespace fs = std::filesystem;

static int tests_passed = 0;
static int tests_failed = 0;

// Assertion macro for test state tracking.
#define CHECK(cond, msg) do { \
    if (!(cond)) { \
        std::cerr << "FAIL: " << msg << " (" << __FILE__ << ":" << __LINE__ << ")\n"; \
        tests_failed++; \
    } else { \
        tests_passed++; \
    } \
} while(0)

// Helper to write string content to a binary file.
static void write_file(const fs::path& p, const std::string& content) {
    std::ofstream f(p, std::ios::binary);
    f << content;
}

// Verification of full backup functionality using LocalStore.
void test_full_backup_local() {
    auto src_dir = fs::temp_directory_path() / "s3bu_test_src";
    auto out_dir = fs::temp_directory_path() / "s3bu_test_out";
    fs::remove_all(src_dir);
    fs::remove_all(out_dir);
    fs::create_directories(src_dir);
    fs::create_directories(out_dir);
    fs::create_directories(src_dir / "subdir");

    write_file(src_dir / "hello.txt", "Hello, world!\n");
    write_file(src_dir / "data.bin", std::string("Some data\x00\x01\x02\x03", 13));
    write_file(src_dir / "subdir" / "nested.txt", "Nested file content\n");
    fs::create_symlink("hello.txt", src_dir / "link.txt");

    std::string out_file = (out_dir / "backup.img").string();

    store::LocalStore st;
    backup::Config cfg;
    cfg.store = &st;
    cfg.new_name = out_file;
    cfg.dir = src_dir.string();
    cfg.tag = "--root--";
    cfg.verbose = false;
    cfg.version_idx = 0;

    auto result = backup::run(cfg);

    // Validation of backup statistics.
    CHECK(result.stats.files >= 3, "files >= 3");
    CHECK(result.stats.dirs >= 1, "dirs >= 1");
    CHECK(result.stats.symlinks >= 1, "symlinks >= 1");

    // Validation of output file integrity and alignment.
    auto info = fs::file_size(out_file);
    CHECK(info > 0, "output not empty");
    CHECK(info % 512 == 0, "sector aligned");

    // Superblock verification.
    auto sb_data = st.get_range(out_file, 0, 4096);
    auto sb = s3fs::parse_superblock(sb_data.data(), sb_data.size());
    CHECK(sb.magic == s3fs::kMagic, "magic");
    CHECK(sb.nvers == 1, "nvers");
    CHECK(sb.versions[0].name == out_file, "version name");

    // Trailer and root directory entry verification.
    auto trailer = st.get_range(out_file, static_cast<int64_t>(info) - 512, 512);
    auto [root_de, root_n] = s3fs::parse_dirent(trailer.data(), trailer.size());
    CHECK(root_de.name == "--root--", "root name");
    CHECK((root_de.mode & 040000) != 0, "root is directory");

    // Directory content verification.
    auto root_dir_data = st.get_range(out_file,
        static_cast<int64_t>(root_de.offset.sector()) * 512,
        static_cast<int64_t>(root_de.bytes));

    std::vector<std::string> found;
    s3fs::iter_dirents(root_dir_data.data(), root_dir_data.size(),
        [&](const s3fs::Dirent& d) -> bool {
            found.push_back(d.name);
            return true;
        });

    auto has = [&](const std::string& n) {
        return std::find(found.begin(), found.end(), n) != found.end();
    };
    CHECK(has("hello.txt"), "has hello.txt");
    CHECK(has("data.bin"), "has data.bin");
    CHECK(has("subdir"), "has subdir");
    CHECK(has("link.txt"), "has link.txt");

    fs::remove_all(src_dir);
    fs::remove_all(out_dir);

    std::cout << "Backup verification successful: " << result.stats.files << " files, "
              << result.stats.dirs << " dirs, "
              << result.stats.symlinks << " symlinks, "
              << result.stats.total_sectors << " sectors\n";
}

// Verification of size string parsing logic.
void test_parse_size() {
    CHECK(backup::parse_size("1024") == 1024, "plain");
    CHECK(backup::parse_size("1K") == 1024, "1K");
    CHECK(backup::parse_size("1M") == 1024*1024, "1M");
    CHECK(backup::parse_size("1G") == 1024LL*1024*1024, "1G");
    CHECK(backup::parse_size("5M") == 5*1024*1024, "5M");
    CHECK(backup::parse_size("") == 0, "empty");
}

int main() {
    test_parse_size();
    test_full_backup_local();

    std::cout << tests_passed << " passed, " << tests_failed << " failed\n";
    return tests_failed > 0 ? 1 : 0;
}