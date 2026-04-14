#include "s3check/compare.h"
#include <iostream>
#include <sys/stat.h>
#include <dirent.h>
#include <unistd.h>
#include <sstream>
#include <fstream>
#include <unordered_map>
#include <openssl/sha.h>
#include <iomanip>
#include <algorithm>

// Define missing Platform Compatibility in compare.cpp
static uint32_t get_ctime_compat(const struct stat& sb) {
#ifdef PLATFORM_DARWIN
    return static_cast<uint32_t>(sb.st_ctimespec.tv_sec);
#else
    return static_cast<uint32_t>(sb.st_ctim.tv_sec);
#endif
}

namespace s3check {

void Report::mismatch(const std::string& path, const std::string& msg) {
    mismatches.push_back(path + ": " + msg);
}

static std::string hash_local_file(const std::string& path) {
    std::ifstream file(path, std::ios::binary);
    if (!file) return "";
    
    SHA256_CTX ctx;
    SHA256_Init(&ctx);
    char buf[32768];
    while (file.read(buf, sizeof(buf))) {
        SHA256_Update(&ctx, buf, file.gcount());
    }
    if (file.gcount() > 0) {
        SHA256_Update(&ctx, buf, file.gcount());
    }
    
    unsigned char hash[SHA256_DIGEST_LENGTH];
    SHA256_Final(hash, &ctx);
    
    std::stringstream ss;
    for (int i = 0; i < SHA256_DIGEST_LENGTH; i++) {
        ss << std::hex << std::setw(2) << std::setfill('0') << (int)hash[i];
    }
    return ss.str();
}

static std::string hash_backup_file(s3mount::MountState& arch, const s3fs::Dirent& de) {
    size_t obj_idx = de.offset.object();
    if (obj_idx >= arch.names.size()) return "";
    
    const std::string& key = arch.names[obj_idx];
    int64_t base_offset = static_cast<int64_t>(de.offset.sector()) * s3fs::kSectorSize;
    int64_t remaining = de.bytes;
    int64_t cur = base_offset;
    
    SHA256_CTX ctx;
    SHA256_Init(&ctx);
    
    const int64_t chunk_size = 4 * 1024 * 1024;
    while (remaining > 0) {
        int64_t n = std::min(remaining, chunk_size);
        auto data = arch.store->get_range(key, cur, n);
        if (data.empty()) break;
        SHA256_Update(&ctx, data.data(), data.size());
        cur += static_cast<int64_t>(data.size());
        remaining -= static_cast<int64_t>(data.size());
    }
    
    unsigned char hash[SHA256_DIGEST_LENGTH];
    SHA256_Final(hash, &ctx);
    
    std::stringstream ss;
    for (int i = 0; i < SHA256_DIGEST_LENGTH; i++) {
        ss << std::hex << std::setw(2) << std::setfill('0') << (int)hash[i];
    }
    return ss.str();
}

static void compare_meta(Report& rep, const std::string& path,
                         const struct stat& sb, const s3fs::Dirent& de) {
    if (static_cast<uint16_t>(sb.st_mode) != de.mode) {
        rep.mismatch(path, "mode differs: local=" + std::to_string(sb.st_mode) + " backup=" + std::to_string(de.mode));
    }
    if (static_cast<uint16_t>(sb.st_uid) != de.uid || static_cast<uint16_t>(sb.st_gid) != de.gid) {
        rep.mismatch(path, "owner differs: local=" + std::to_string(sb.st_uid) + ":" + std::to_string(sb.st_gid) +
                           " backup=" + std::to_string(de.uid) + ":" + std::to_string(de.gid));
    }
    uint32_t ctime = get_ctime_compat(sb);
    if (ctime != de.ctime) {
        rep.mismatch(path, "ctime differs: local=" + std::to_string(ctime) + " backup=" + std::to_string(de.ctime));
    }
    if (S_ISREG(sb.st_mode) && static_cast<uint64_t>(sb.st_size) != de.bytes) {
        rep.mismatch(path, "size differs: local=" + std::to_string(sb.st_size) + " backup=" + std::to_string(de.bytes));
    }
}

static void compare_dir(Report& rep, s3mount::MountState& arch,
                        const std::string& abs_path, const std::string& rel_path,
                        const s3fs::Dirent& backup_de) {
    rep.compared++;

    struct stat dir_sb{};
    if (lstat(abs_path.c_str(), &dir_sb) < 0) {
        rep.mismatch(rel_path, "lstat failed: " + std::string(strerror(errno)));
        return;
    }
    dev_t this_dev = dir_sb.st_dev;

    std::unordered_map<std::string, s3fs::Dirent> backup_children;
    auto [dir_data, dir_len] = arch.dir_cache.find(backup_de.offset, backup_de.bytes);
    if (dir_data != nullptr) {
        s3fs::iter_dirents(dir_data, dir_len, [&](const s3fs::Dirent& d) -> bool {
            backup_children[d.name] = d;
            return true;
        });
    }

    DIR* d = opendir(abs_path.c_str());
    if (!d) {
        rep.mismatch(rel_path, "opendir failed: " + std::string(strerror(errno)));
        return;
    }

    std::vector<std::string> local_entries;
    struct dirent* entry;
    while ((entry = readdir(d)) != nullptr) {
        std::string name = entry->d_name;
        if (name == "." || name == "..") continue;
        local_entries.push_back(name);
    }
    closedir(d);

    for (const auto& kv : backup_children) {
        if (std::find(local_entries.begin(), local_entries.end(), kv.first) == local_entries.end()) {
            rep.missing_in_local.push_back(rel_path.empty() ? kv.first : rel_path + "/" + kv.first);
        }
    }

    for (const auto& name : local_entries) {
        std::string child_abs = abs_path + "/" + name;
        std::string child_rel = rel_path.empty() ? name : rel_path + "/" + name;

        struct stat sb{};
        if (lstat(child_abs.c_str(), &sb) < 0) {
            rep.mismatch(child_rel, "lstat failed: " + std::string(strerror(errno)));
            continue;
        }

        if (S_ISFIFO(sb.st_mode)) continue; // FIFOs are skipped by backup
        
        auto it = backup_children.find(name);
        if (it == backup_children.end()) {
            rep.missing_in_backup.push_back(child_rel);
            continue;
        }
        
        rep.compared++;
        const s3fs::Dirent& bde = it->second;

        if (S_ISDIR(sb.st_mode)) {
            if (!(bde.mode & S_IFDIR)) {
                rep.mismatch(child_rel, "local is dir, backup is not");
                continue;
            }
            compare_meta(rep, child_rel, sb, bde);
            
            if (sb.st_dev != this_dev) {
                if (bde.bytes != 0) {
                    rep.mismatch(child_rel, "mount point should be empty in backup");
                }
                continue;
            }
            compare_dir(rep, arch, child_abs, child_rel, bde);
        } else if (S_ISLNK(sb.st_mode)) {
            if ((bde.mode & S_IFMT) != S_IFLNK) {
                rep.mismatch(child_rel, "local is symlink, backup is not");
                continue;
            }
            compare_meta(rep, child_rel, sb, bde);
            
            char target[4096];
            ssize_t len = readlink(child_abs.c_str(), target, sizeof(target));
            if (len >= 0) {
                target[len] = '\0';
                std::string local_tgt = target;
                
                size_t obj_idx = bde.offset.object();
                if (obj_idx < arch.names.size()) {
                    auto bk_data = arch.store->get_range(arch.names[obj_idx], 
                                    static_cast<int64_t>(bde.offset.sector()) * s3fs::kSectorSize,
                                    static_cast<int64_t>(bde.bytes));
                    std::string backup_tgt(bk_data.begin(), bk_data.end());
                    if (local_tgt != backup_tgt) {
                        rep.mismatch(child_rel, "symlink target differs");
                    }
                }
            }
        } else if (S_ISREG(sb.st_mode)) {
            if ((bde.mode & S_IFMT) != S_IFREG) {
                rep.mismatch(child_rel, "local is regular file, backup is not");
                continue;
            }
            compare_meta(rep, child_rel, sb, bde);
            
            if (static_cast<uint64_t>(sb.st_size) == bde.bytes) {
                std::string local_hash = hash_local_file(child_abs);
                std::string backup_hash = hash_backup_file(arch, bde);
                if (local_hash != backup_hash) {
                    rep.mismatch(child_rel, "content hash differs");
                }
            }
        } else {
            compare_meta(rep, child_rel, sb, bde);
        }
    }
}

Report compare_tree(s3mount::MountState& arch, const std::string& local_root) {
    Report rep;
    struct stat sb{};
    if (lstat(local_root.c_str(), &sb) < 0 || !S_ISDIR(sb.st_mode)) {
        throw std::runtime_error("compare: " + local_root + " is not a directory");
    }
    
    compare_dir(rep, arch, local_root, "", arch.root_de);
    return rep;
}

} // namespace s3check
