#include "s3check/fsck.h"
#include <iostream>
#include <sys/stat.h>
#include <sstream>

namespace s3check {

void FsckReport::errorf(const std::string& path, const std::string& msg) {
    errors.push_back(path + ": " + msg);
}

void FsckReport::warnf(const std::string& path, const std::string& msg) {
    warnings.push_back(path + ": " + msg);
}

static void check_bounds(FsckReport& rep, s3mount::MountState& arch,
                         const s3fs::Dirent& de, const std::string& path) {
    int obj = static_cast<int>(de.offset.object());
    if (obj < 0 || obj >= static_cast<int>(arch.names.size())) {
        rep.errorf(path, "object index " + std::to_string(obj) + " out of range [0, " +
                   std::to_string(arch.names.size()) + ")");
        return;
    }

    int64_t max_sector = arch.nsectors.at(obj);
    int64_t start_sector = de.offset.sector();
    
    if (start_sector > max_sector) {
        rep.errorf(path, "sector " + std::to_string(start_sector) + " exceeds object size " + std::to_string(max_sector));
        return;
    }

    int kind = de.mode & S_IFMT;
    if ((kind == S_IFREG || kind == S_IFLNK) && de.bytes > 0) {
        int64_t data_sectors = s3fs::round_up(de.bytes, s3fs::kSectorSize) / s3fs::kSectorSize;
        int64_t end_sector = start_sector + data_sectors;
        if (end_sector > max_sector) {
            rep.errorf(path, "data span [" + std::to_string(start_sector) + ", " +
                       std::to_string(end_sector) + ") exceeds max sector " + std::to_string(max_sector));
        }
    }
}

static void check_data(FsckReport& rep, s3mount::MountState& arch,
                       const s3fs::Dirent& de, const std::string& path) {
    int kind = de.mode & S_IFMT;
    if (kind != S_IFREG && kind != S_IFLNK) return;
    if (de.bytes == 0) return;

    size_t obj_idx = de.offset.object();
    if (obj_idx >= arch.names.size()) return; // bounds checked already
    
    const std::string& key = arch.names[obj_idx];
    int64_t base_offset = static_cast<int64_t>(de.offset.sector()) * s3fs::kSectorSize;

    try {
        auto data = arch.store->get_range(key, base_offset, static_cast<int64_t>(de.bytes));
        if (data.size() != de.bytes) {
            rep.errorf(path, "size mismatch (record=" + std::to_string(de.bytes) + " read=" + std::to_string(data.size()) + ")");
        }
    } catch (const std::exception& e) {
        rep.errorf(path, "read data failed: " + std::string(e.what()));
    }
}

static void walk_dir(FsckReport& rep, s3mount::MountState& arch,
                     const s3fs::Dirent& de, const std::string& path) {
    rep.directories++;

    auto [dir_data, dir_len] = arch.dir_cache.find(de.offset, de.bytes);
    if (dir_data == nullptr) {
        if (de.bytes > 0) {
            rep.errorf(path, "directory data missing (bytes=" + std::to_string(de.bytes) + ")");
        }
        return;
    }

    size_t offset = 0;
    while (offset < dir_len) {
        bool all_zero = true;
        for (size_t i = offset; i < dir_len; ++i) {
            if (dir_data[i] != 0) { all_zero = false; break; }
        }
        if (all_zero) break;

        auto [child, n] = s3fs::parse_dirent(dir_data + offset, dir_len - offset);
        if (n == 0) {
            rep.errorf(path, "parse dirent failed at byte " + std::to_string(offset));
            return;
        }
        offset += n;

        std::string child_path = path.empty() ? child.name : path + "/" + child.name;
        check_bounds(rep, arch, child, child_path);
        check_data(rep, arch, child, child_path);

        int kind = child.mode & S_IFMT;
        if (kind == S_IFDIR) {
            walk_dir(rep, arch, child, child_path);
        } else if (kind == S_IFREG) {
            rep.files++;
        } else if (kind == S_IFLNK) {
            rep.symlinks++;
        } else {
            rep.specials++;
        }
    }
}

FsckReport fsck(s3mount::MountState& arch) {
    FsckReport rep;
    rep.versions = static_cast<int>(arch.names.size());

    if (!(arch.root_de.mode & S_IFDIR)) {
        rep.errorf("<root>", "root entry is not a directory");
        return rep;
    }

    check_bounds(rep, arch, arch.root_de, "<root>");
    walk_dir(rep, arch, arch.root_de, "");

    int expected_files = rep.files + 2; // _dirloc_, _dirdat_
    int statfs_files = static_cast<int>(arch.statfs_info.files);
    if (statfs_files != expected_files) {
        rep.warnf("<root>", "statfs.files=" + std::to_string(statfs_files) +
                            " expected " + std::to_string(expected_files));
    }
    int statfs_dirs = static_cast<int>(arch.statfs_info.dirs);
    if (statfs_dirs != rep.directories) {
        rep.warnf("<root>", "statfs.dirs=" + std::to_string(statfs_dirs) +
                            " but walked " + std::to_string(rep.directories));
    }
    int statfs_symlinks = static_cast<int>(arch.statfs_info.symlinks);
    if (statfs_symlinks != rep.symlinks) {
        rep.warnf("<root>", "statfs.symlinks=" + std::to_string(statfs_symlinks) +
                            " but walked " + std::to_string(rep.symlinks));
    }

    return rep;
}

} // namespace s3check
