// backup.cpp - Full and incremental backup engine.
#include "s3backup/backup.h"
#include "s3backup/superblock.h"
#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <cstdio>
#include <algorithm>
#include <iostream>
#include <cstddef>

namespace backup {

// Platform compatibility

static uint32_t get_ctime(const struct stat& sb) {
#ifdef PLATFORM_DARWIN
    return static_cast<uint32_t>(sb.st_ctimespec.tv_sec);
#else
    return static_cast<uint32_t>(sb.st_ctim.tv_sec);
#endif
}

// Directory cache for incremental backups
using DirCache = std::unordered_map<uint64_t, std::vector<uint8_t>>;

// Internal backup state
struct BackupState {
    const Config& cfg;
    SectorWriter& sw;
    DirCache cache;
    std::vector<s3fs::DirLoc> dir_locs;
    std::FILE* tmp_file = nullptr;
    s3fs::StatFS stats{};
    std::string cur_path;

    // Check if a file is unchanged from the old backup
    static bool unchanged(const s3fs::Dirent* old_de, const struct stat& sb) {
        if (!old_de) return false;
        return old_de->mode == static_cast<uint16_t>(sb.st_mode) &&
               old_de->bytes == static_cast<uint64_t>(sb.st_size) &&
               old_de->ctime == get_ctime(sb) &&
               old_de->uid == static_cast<uint16_t>(sb.st_uid) &&
               old_de->gid == static_cast<uint16_t>(sb.st_gid);
    }

    [[nodiscard]] bool is_excluded(const std::string& dir, const std::string& file) const {
        std::string full = dir.empty() ? file : dir + "/" + file;
        return std::any_of(cfg.exclude.begin(), cfg.exclude.end(),
                           [&](const std::string& pat) { 
                               return pat == full || pat == file; 
                           });
    }

    // Store a regular file
    std::pair<s3fs::Dirent, int64_t>
    store_file(int64_t offset, const std::string& path,
               const std::string& name, const struct stat& sb)
    {
        int64_t nbytes = 0;
        uint8_t buf[16 * 1024];

        if (!cfg.noio) {
            int fd = ::open(path.c_str(), O_RDONLY);
            if (fd >= 0) {
                ssize_t n;
                while ((n = ::read(fd, buf, sizeof(buf))) > 0) {
                    sw.write(buf, n);
                    nbytes += n;
                }
                ::close(fd);
            }
        } else {
            nbytes = sb.st_size;
            sw.write(nullptr, 0); // track size only
        }

        // Pad to sector boundary
        int64_t pad = s3fs::pad_to_sector(nbytes);
        if (pad > 0 && !cfg.noio) {
            std::vector<uint8_t> zeros(pad, 0);
            sw.write(zeros.data(), pad);
        }
        if (cfg.noio) {
            // Account for padded size manually
            [[maybe_unused]] int64_t padded = s3fs::round_up(nbytes, s3fs::kSectorSize);
            // SectorWriter already counted nbytes via the size tracking
            // but in noio mode the padded total is required
        }

        int64_t total_sectors = s3fs::round_up(nbytes, s3fs::kSectorSize) / s3fs::kSectorSize;

        s3fs::Dirent de;
        de.mode = static_cast<uint16_t>(sb.st_mode);
        de.uid = static_cast<uint16_t>(sb.st_uid);
        de.gid = static_cast<uint16_t>(sb.st_gid);
        de.ctime = get_ctime(sb);
        de.offset = s3fs::S3Offset(static_cast<uint64_t>(offset),
                                    static_cast<uint16_t>(cfg.version_idx));
        de.bytes = static_cast<uint64_t>(nbytes);
        de.namelen = static_cast<uint8_t>(name.size());
        de.name = name;

        stats.files++;
        stats.file_sectors += total_sectors;

        if (cfg.verbose)
            std::cout << "F " << total_sectors << " " << sb.st_size
                      << " " << cur_path << "/" << name << "\n";

        return {de, total_sectors};
    }

    // Store a symlink
    std::pair<s3fs::Dirent, int64_t>
    store_link(int64_t offset, const std::string& path,
               const std::string& name, const struct stat& sb)
    {
        char target[4096];
        ssize_t len = ::readlink(path.c_str(), target, sizeof(target));
        if (len < 0)
            throw std::runtime_error("readlink " + path + ": " + strerror(errno));

        int64_t nbytes = len;
        int64_t padded = s3fs::round_up(nbytes, s3fs::kSectorSize);
        std::vector<uint8_t> buf(padded, 0);
        std::memcpy(buf.data(), target, len);

        sw.write(buf.data(), buf.size());
        int64_t total_sectors = padded / s3fs::kSectorSize;

        s3fs::Dirent de;
        de.mode = static_cast<uint16_t>(sb.st_mode);
        de.uid = static_cast<uint16_t>(sb.st_uid);
        de.gid = static_cast<uint16_t>(sb.st_gid);
        de.ctime = get_ctime(sb);
        de.offset = s3fs::S3Offset(static_cast<uint64_t>(offset),
                                    static_cast<uint16_t>(cfg.version_idx));
        de.bytes = static_cast<uint64_t>(nbytes);
        de.namelen = static_cast<uint8_t>(name.size());
        de.name = name;

        stats.symlinks++;
        stats.sym_sectors += total_sectors;

        if (cfg.verbose)
            std::cout << "L " << total_sectors << " " << nbytes
                      << " " << cur_path << "/" << name << "\n";

        return {de, total_sectors};
    }

    // Store raw data (dirloc table)
    std::pair<s3fs::Dirent, int64_t>
    store_data(int64_t offset, const std::string& name,
               const uint8_t* data, size_t nbytes)
    {
        int64_t padded = s3fs::round_up(static_cast<int64_t>(nbytes), s3fs::kSectorSize);
        if (!cfg.noio) {
            std::vector<uint8_t> buf(padded, 0);
            std::memcpy(buf.data(), data, nbytes);
            sw.write(buf.data(), buf.size());
        }
        int64_t total_sectors = padded / s3fs::kSectorSize;

        s3fs::Dirent de;
        de.mode = 0; de.uid = 0; de.gid = 0; de.ctime = 0;
        de.offset = s3fs::S3Offset(static_cast<uint64_t>(offset),
                                    static_cast<uint16_t>(cfg.version_idx));
        de.bytes = nbytes;
        de.namelen = static_cast<uint8_t>(name.size());
        de.name = name;

        stats.files++;
        stats.file_sectors += total_sectors;
        return {de, total_sectors};
    }

    // Store a device node or empty mountpoint directory
    static s3fs::Dirent store_node(const std::string& name, const struct stat& sb) {
        s3fs::Dirent de;
        de.mode = static_cast<uint16_t>(sb.st_mode);
        de.uid = static_cast<uint16_t>(sb.st_uid);
        de.gid = static_cast<uint16_t>(sb.st_gid);
        de.ctime = get_ctime(sb);
        de.offset = s3fs::S3Offset(0, 0);
        de.bytes = 0;
        if (S_ISCHR(sb.st_mode) || S_ISBLK(sb.st_mode))
            de.bytes = sb.st_rdev;
        de.namelen = static_cast<uint8_t>(name.size());
        de.name = name;
        return de;
    }

    // Recursively store a directory
    std::pair<s3fs::Dirent, int64_t>
    store_dir(int64_t offset, const std::string& dir_path,
              const std::string& name, const struct stat& dir_sb,
              const s3fs::Dirent* old_de)
    {
        std::string prev_path = cur_path;
        cur_path = cur_path.empty() ? name : cur_path + "/" + name;

        dev_t dir_dev = dir_sb.st_dev;

        // Load old directory data for incremental comparison
        const uint8_t* old_dir_data = nullptr;
        size_t old_dir_len = 0;
        std::vector<uint8_t>* old_dir_vec = nullptr;
        if (old_de) {
            auto it = cache.find(old_de->offset.raw());
            if (it != cache.end()) {
                old_dir_vec = &it->second;
                old_dir_data = old_dir_vec->data();
                old_dir_len = old_dir_vec->size();
            }
        }

        // Read local directory entries
        DIR* d = ::opendir(dir_path.c_str());
        if (!d) {
            cur_path = prev_path;
            throw std::runtime_error("opendir " + dir_path + ": " + strerror(errno));
        }

        // Accumulate child dirents
        std::vector<uint8_t> child_buf;

        struct dirent* entry;
        while ((entry = ::readdir(d)) != nullptr) {
            std::string child_name = entry->d_name;
            if (child_name == "." || child_name == "..") continue;

            std::string child_path = dir_path;
            child_path += "/";
            child_path += child_name;

            if (is_excluded(cur_path, child_name)) {
                if (cfg.verbose)
                    std::cerr << "excluding " << cur_path << "/" << child_name << "\n";
                continue;
            }

            if (offset >= cfg.stop_after) break;

            struct stat sb2{};
            if (::lstat(child_path.c_str(), &sb2) < 0) {
                std::cerr << "skipping " << child_path << ": " << strerror(errno) << "\n";
                continue;
            }

            // Skip FIFOs
            if (S_ISFIFO(sb2.st_mode)) continue;

            // Lookup old dirent
            std::optional<s3fs::Dirent> old_child;
            if (old_dir_data)
                old_child = s3fs::lookup_dirent(old_dir_data, old_dir_len, child_name);

            s3fs::Dirent de;

            if (S_ISREG(sb2.st_mode)) {
                if (old_child && unchanged(&*old_child, sb2)) {
                    de = *old_child;
                } else {
                    auto [fde, sectors] = store_file(offset, child_path, child_name, sb2);
                    de = fde;
                    offset += sectors;
                }
            } else if (S_ISDIR(sb2.st_mode)) {
                if (sb2.st_dev != dir_dev) {
                    de = store_node(child_name, sb2);
                } else {
                    const s3fs::Dirent* optr = old_child ? &*old_child : nullptr;
                    auto [dde, new_off] = store_dir(offset, child_path, child_name, sb2, optr);
                    de = dde;
                    offset = new_off;
                }
            } else if (S_ISLNK(sb2.st_mode)) {
                auto [lde, sectors] = store_link(offset, child_path, child_name, sb2);
                de = lde;
                offset += sectors;
            } else if (S_ISCHR(sb2.st_mode) || S_ISBLK(sb2.st_mode)) {
                de = store_node(child_name, sb2);
            } else {
                continue; // skip sockets etc.
            }

            // Serialize and append
            std::vector<uint8_t> de_buf(de.size());
            s3fs::marshal_dirent(de, de_buf.data());
            child_buf.insert(child_buf.end(), de_buf.begin(), de_buf.end());
        }
        ::closedir(d);

        // Pad directory data to sector boundary and write
        auto dir_data_len = static_cast<int64_t>(child_buf.size());
        int64_t padded = s3fs::round_up(dir_data_len, s3fs::kSectorSize);
        child_buf.resize(padded, 0);
        sw.write(child_buf.data(), child_buf.size());

        int64_t dir_sectors = padded / s3fs::kSectorSize;

        // Build dirent for THIS directory
        auto loc = s3fs::S3Offset(static_cast<uint64_t>(offset),
                                   static_cast<uint16_t>(cfg.version_idx));
        s3fs::Dirent this_de;
        this_de.mode = static_cast<uint16_t>(dir_sb.st_mode);
        this_de.uid = static_cast<uint16_t>(dir_sb.st_uid);
        this_de.gid = static_cast<uint16_t>(dir_sb.st_gid);
        this_de.ctime = get_ctime(dir_sb);
        this_de.offset = loc;
        this_de.bytes = static_cast<uint64_t>(dir_data_len);
        this_de.namelen = static_cast<uint8_t>(name.size());
        this_de.name = name;

        // Save dir location
        dir_locs.push_back(s3fs::DirLoc{loc, static_cast<uint32_t>(dir_data_len)});

        // Write raw dir data (no padding!) to temp file
        if (dir_data_len > 0 && tmp_file) {
            std::fwrite(child_buf.data(), 1, dir_data_len, tmp_file);
        }

        offset += dir_sectors;

        stats.dirs++;
        stats.dir_sectors += dir_sectors;
        stats.dir_bytes += dir_data_len;

        if (cfg.verbose)
            std::cout << "D " << dir_sectors << " " << dir_sb.st_size
                      << " " << cur_path << "\n";

        cur_path = prev_path;
        return {this_de, offset};
    }
};

// Build trailer sector

static std::vector<uint8_t> build_trailer(
    const std::vector<s3fs::Dirent>& dirents, const s3fs::StatFS& stats)
{
    std::vector<uint8_t> buf(s3fs::kSectorSize, 0);
    int off = 0;
    for (const auto& de : dirents)
        off += s3fs::marshal_dirent(de, buf.data() + off);

    s3fs::marshal_statfs(stats, buf.data() + s3fs::kSectorSize - s3fs::kStatFSSize);
    return buf;
}

// Load prior backup for incremental

static std::pair<std::vector<s3fs::Version>, s3fs::Dirent>
load_prior_backup(const Config& cfg, DirCache& cache) {
    auto sb_data = cfg.store->get_range(cfg.old_name, 0, 4096);
    auto sb = s3fs::parse_superblock(sb_data.data(), sb_data.size());

    // Read trailer
    int64_t old_size = cfg.store->size(cfg.old_name);
    auto trailer = cfg.store->get_range(cfg.old_name, old_size - 512, 512);

    // Parse 3 dirents
    auto [root_de, root_n] = s3fs::parse_dirent(trailer.data(), trailer.size());
    auto [dirloc_de, dirloc_n] = s3fs::parse_dirent(trailer.data() + root_n, trailer.size() - root_n);
    auto [dirdat_de, _] = s3fs::parse_dirent(trailer.data() + root_n + dirloc_n,
                                              trailer.size() - root_n - dirloc_n);

    // Read dir locs
    auto loc_data = cfg.store->get_range(cfg.old_name,
        static_cast<int64_t>(dirloc_de.offset.sector()) * 512,
        static_cast<int64_t>(dirloc_de.bytes));
    auto locs = s3fs::parse_dirlocs(loc_data.data(), loc_data.size());

    // Read packed dir data
    auto dir_data = cfg.store->get_range(cfg.old_name,
        static_cast<int64_t>(dirdat_de.offset.sector()) * 512,
        static_cast<int64_t>(dirdat_de.bytes));

    // Populate cache
    size_t byte_offset = 0;
    for (const auto& loc : locs) {
        if (loc.bytes > 0) {
            size_t end = byte_offset + loc.bytes;
            if (end > dir_data.size()) end = dir_data.size();
            cache[loc.offset.raw()] = std::vector<uint8_t>(
                dir_data.begin() + static_cast<std::ptrdiff_t>(byte_offset),
                dir_data.begin() + static_cast<std::ptrdiff_t>(end));
            byte_offset = end;
        }
    }

    return {sb.versions, root_de};
}

// Main entry point

Result run(const Config& cfg_in) {
    Config cfg = cfg_in;
    if (cfg.tag.empty()) cfg.tag = "--root--";
    if (cfg.stop_after == 0) cfg.stop_after = 1LL << 50;

    auto writer = cfg.store->new_writer(cfg.new_name);
    SectorWriter sw(writer.get(), cfg.noio);

    DirCache cache;
    s3fs::Dirent* old_root_de_ptr = nullptr;
    s3fs::Dirent old_root_de;
    std::vector<s3fs::Version> prev_versions;

    // Incremental setup
    if (!cfg.old_name.empty()) {
        auto [pv, rde] = load_prior_backup(cfg, cache);
        prev_versions = std::move(pv);
        old_root_de = rde;
        old_root_de_ptr = &old_root_de;
    }

    // Verify source
    struct stat dir_sb{};
    if (::lstat(cfg.dir.c_str(), &dir_sb) < 0)
        throw std::runtime_error("stat " + cfg.dir + ": " + strerror(errno));
    if (!S_ISDIR(dir_sb.st_mode))
        throw std::runtime_error(cfg.dir + " is not a directory");

    // Write superblock
    auto [sb_buf, sb_sectors] = s3fs::make_superblock(cfg.new_name, prev_versions);
    sw.write(sb_buf.data(), sb_buf.size());
    int64_t offset = sb_sectors;

    // Temp file for packed dir data
    char tmp_template[] = "/tmp/s3bu-cpp.XXXXXX";
    int tmp_fd = ::mkstemp(tmp_template);
    ::unlink(tmp_template);
    std::FILE* tmp_file = ::fdopen(tmp_fd, "w+b");

    BackupState state{cfg, sw, std::move(cache), {}, tmp_file, {}, ""};

    // Traverse
    auto [root_de, new_offset] = state.store_dir(offset, cfg.dir, cfg.tag, dir_sb, old_root_de_ptr);
    offset = new_offset;
    std::vector<s3fs::Dirent> trailer_dirents = {root_de};

    // Write dirloc table
    auto dirloc_data = s3fs::marshal_dirlocs(state.dir_locs);
    auto [dirloc_de, dirloc_sectors] = state.store_data(offset, "_dirloc_",
        dirloc_data.data(), dirloc_data.size());
    offset += dirloc_sectors;
    trailer_dirents.push_back(dirloc_de);

    // Write packed dir data from temp file
    ::fseek(tmp_file, 0, SEEK_END);
    long tmp_size = ::ftell(tmp_file);
    ::fseek(tmp_file, 0, SEEK_SET);

    std::vector<uint8_t> tmp_data(static_cast<size_t>(tmp_size));
    ::fread(tmp_data.data(), 1, static_cast<size_t>(tmp_size), tmp_file);
    ::fclose(tmp_file);

    auto [dirdat_de, dirdat_sectors] = state.store_data(offset, "_dirdat_",
        tmp_data.data(), tmp_data.size());
    offset += dirdat_sectors;
    trailer_dirents.push_back(dirdat_de);

    // Write trailer
    state.stats.total_sectors = static_cast<uint64_t>(offset + 1);
    auto trailer = build_trailer(trailer_dirents, state.stats);
    sw.write(trailer.data(), trailer.size());

    writer->close();

    bool truncated = offset >= cfg.stop_after;

    if (cfg.verbose) {
        std::cout << state.stats.files << " files (" << state.stats.file_sectors << " sectors)\n";
        std::cout << state.stats.dirs << " directories (" << state.stats.dir_sectors
                  << " sectors, " << state.stats.dir_bytes << " bytes)\n";
        std::cout << state.stats.symlinks << " symlinks\n";
        std::cout << state.stats.total_sectors << " total sectors ("
                  << sw.total_written() << " bytes)\n";
        std::cout << "truncated: " << (truncated ? "YES" : "NO") << "\n";
    }

    return Result{state.stats, truncated};
}

int64_t parse_size(const std::string& s) {
    if (s.empty()) return 0;
    int64_t multiplier = 1;
    std::string num = s;
    char last = static_cast<char>(std::toupper(static_cast<unsigned char>(s.back())));
    if (last == 'G') { multiplier = 1024LL * 1024 * 1024; num.pop_back(); }
    else if (last == 'M') { multiplier = 1024LL * 1024; num.pop_back(); }
    else if (last == 'K') { multiplier = 1024LL; num.pop_back(); }
    return std::stoll(num) * multiplier;
}

} // namespace backup