// fs.cpp - FUSE filesystem implementation for s3mount.
#include "s3backup/mount.h"
#include "s3backup/superblock.h"
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <iostream>
#include <sstream>

#ifndef FUSE_USE_VERSION
#define FUSE_USE_VERSION 26
#endif

#include <fuse.h>

namespace s3mount {

static MountState* g_state = nullptr;

// Path translation
static int translate(const char* path, s3fs::Dirent& result) {
    if (g_state == nullptr) return -EIO;
    result = g_state->root_de;

    if (std::strcmp(path, "/") == 0) return 0;

    std::string p(path + 1);
    std::istringstream ss(p);
    std::string component;

    while (std::getline(ss, component, '/')) {
        if (component.empty()) continue;
        if (!(result.mode & S_IFDIR)) return -ENOTDIR;

        auto [dir_data, dir_len] = g_state->dir_cache.find(
            result.offset, result.bytes);
        if (dir_data == nullptr) return -ENOENT;

        auto found = s3fs::lookup_dirent(dir_data, dir_len, component);
        if (!found.has_value()) return -ENOENT;
        result = *found;
    }
    return 0;
}

// Convert Dirent to struct stat
static void dirent_to_stat(const s3fs::Dirent& de, struct stat* sb) {
    std::memset(sb, 0, sizeof(*sb));
    sb->st_mode = de.mode;
    sb->st_nlink = 1;
    sb->st_uid = de.uid;
    sb->st_gid = de.gid;

    if (S_ISCHR(de.mode) || S_ISBLK(de.mode)) {
        sb->st_rdev = static_cast<dev_t>(de.bytes);
        sb->st_size = 0;
    } else {
        sb->st_size = static_cast<off_t>(de.bytes);
    }

    sb->st_blocks = static_cast<blkcnt_t>((de.bytes + 511) / 512);
#ifdef PLATFORM_DARWIN
    sb->st_atimespec.tv_sec = static_cast<time_t>(de.ctime);
    sb->st_mtimespec.tv_sec = static_cast<time_t>(de.ctime);
    sb->st_ctimespec.tv_sec = static_cast<time_t>(de.ctime);
#else
    sb->st_atim.tv_sec = static_cast<time_t>(de.ctime);
    sb->st_mtim.tv_sec = static_cast<time_t>(de.ctime);
    sb->st_ctim.tv_sec = static_cast<time_t>(de.ctime);
#endif
}

// FUSE callbacks
static int fs_getattr(const char* path, struct stat* stbuf) {
    s3fs::Dirent de;
    int rc = translate(path, de);
    if (rc < 0) return rc;
    dirent_to_stat(de, stbuf);
    return 0;
}

static int fs_readdir(const char* path, void* buf, fuse_fill_dir_t filler,
                      off_t /*offset*/, struct fuse_file_info* /*fi*/) {
    s3fs::Dirent de;
    int rc = translate(path, de);
    if (rc < 0) return rc;
    if (!(de.mode & S_IFDIR)) return -ENOTDIR;

    auto [dir_data, dir_len] = g_state->dir_cache.find(de.offset, de.bytes);
    if (dir_data == nullptr) return 0;

    filler(buf, ".", nullptr, 0);
    filler(buf, "..", nullptr, 0);

    s3fs::iter_dirents(dir_data, dir_len, [&](const s3fs::Dirent& child) -> bool {
        struct stat sb{};
        dirent_to_stat(child, &sb);
        filler(buf, child.name.c_str(), &sb, 0);
        return true;
    });
    return 0;
}

static int fs_open(const char* path, struct fuse_file_info* fi) {
    s3fs::Dirent de;
    int rc = translate(path, de);
    if (rc < 0) return rc;
    auto* stored = new s3fs::Dirent(de);
    fi->fh = reinterpret_cast<uint64_t>(stored);
    return 0;
}

static int fs_release(const char* /*path*/, struct fuse_file_info* fi) {
    if (fi->fh != 0) {
        delete reinterpret_cast<s3fs::Dirent*>(fi->fh);
        fi->fh = 0;
    }
    return 0;
}

static int fs_read(const char* /*path*/, char* buf, size_t size, off_t offset,
                   struct fuse_file_info* fi) {
    if (fi == nullptr || fi->fh == 0) return -EBADF;
    auto* de = reinterpret_cast<s3fs::Dirent*>(fi->fh);
    if (!S_ISREG(de->mode)) return -EISDIR;

    auto file_size = static_cast<int64_t>(de->bytes);
    if (offset >= file_size) return 0;

    int64_t end = offset + static_cast<int64_t>(size);
    if (end > file_size) end = file_size;
    auto read_len = static_cast<size_t>(end - offset);

    auto obj_idx = static_cast<size_t>(de->offset.object());
    const std::string& key = g_state->names.at(obj_idx);
    auto base_offset = static_cast<int64_t>(de->offset.sector()) * 512;

    if (g_state->no_cache || g_state->data_cache == nullptr) {
        auto data = g_state->store->get_range(key, base_offset + offset,
                                               static_cast<int64_t>(read_len));
        std::memcpy(buf, data.data(), std::min(data.size(), read_len));
    } else {
        int64_t max_off = g_state->nsectors.at(obj_idx) * 512;
        g_state->data_cache->read(key, static_cast<int>(obj_idx),
                                   reinterpret_cast<uint8_t*>(buf),
                                   base_offset + offset, read_len, max_off);
    }
    return static_cast<int>(read_len);
}

static int fs_readlink(const char* path, char* buf, size_t size) {
    s3fs::Dirent de;
    int rc = translate(path, de);
    if (rc < 0) return rc;
    if (!S_ISLNK(de.mode)) return -EINVAL;

    auto link_len = static_cast<size_t>(de.bytes);
    size_t n = (size - 1 < link_len) ? size - 1 : link_len;

    auto obj_idx = static_cast<size_t>(de.offset.object());
    const std::string& key = g_state->names.at(obj_idx);
    auto base_offset = static_cast<int64_t>(de.offset.sector()) * 512;

    auto data = g_state->store->get_range(key, base_offset, static_cast<int64_t>(n));
    std::memcpy(buf, data.data(), std::min(data.size(), n));
    buf[std::min(data.size(), n)] = '\0';
    return 0;
}

static int fs_statfs_op(const char* /*path*/, struct statvfs* stbuf) {
    if (g_state == nullptr) return -EIO;
    std::memset(stbuf, 0, sizeof(*stbuf));
    stbuf->f_bsize = stbuf->f_frsize = 512;
    stbuf->f_blocks = static_cast<fsblkcnt_t>(g_state->statfs_info.total_sectors);
    for (auto ns : g_state->nsectors)
        stbuf->f_blocks += static_cast<fsblkcnt_t>(ns);
    stbuf->f_bfree = stbuf->f_bavail = 0;
    stbuf->f_files = g_state->statfs_info.files + g_state->statfs_info.dirs
                     + g_state->statfs_info.symlinks;
    stbuf->f_ffree = stbuf->f_favail = 0;
    stbuf->f_namemax = 255;
    return 0;
}

static struct fuse_operations s3_ops = {};

// Init and run
void init_mount_state(MountState& state, store::ObjectStore* st,
                      const std::string& object_key, bool verbose) {
    state.store = st;

    auto sb_data = st->get_range(object_key, 0, 4096);
    auto sb = s3fs::parse_superblock(sb_data.data(), sb_data.size());

    auto nvers = static_cast<size_t>(sb.nvers);
    state.names = sb.version_names();
    state.nsectors.resize(nvers);

    auto uuids = sb.version_uuids();
    for (size_t i = 0; i < nvers; ++i) {
        state.nsectors.at(i) = st->size(state.names.at(i)) / 512;
        auto vsb_data = st->get_range(state.names.at(i), 0, 4096);
        auto vsb = s3fs::parse_superblock(vsb_data.data(), vsb_data.size());
        if (vsb.versions[0].uuid != uuids.at(i))
            throw std::runtime_error("UUID mismatch for " + state.names.at(i));
    }

    int64_t obj_size = st->size(object_key);
    auto trailer = st->get_range(object_key, obj_size - 512, 512);

    auto [root_de, root_n] = s3fs::parse_dirent(trailer.data(), trailer.size());
    auto [dirloc_de, dirloc_n] = s3fs::parse_dirent(
        trailer.data() + root_n, trailer.size() - root_n);
    auto [dirdat_de, _] = s3fs::parse_dirent(
        trailer.data() + root_n + dirloc_n,
        trailer.size() - root_n - dirloc_n);

    state.root_de = root_de;
    state.statfs_info = s3fs::parse_statfs(
        trailer.data() + s3fs::kSectorSize - s3fs::kStatFSSize);

    auto loc_data = st->get_range(object_key,
        static_cast<int64_t>(dirloc_de.offset.sector()) * 512,
        static_cast<int64_t>(dirloc_de.bytes));
    auto locs = s3fs::parse_dirlocs(loc_data.data(), loc_data.size());

    auto dir_data = st->get_range(object_key,
        static_cast<int64_t>(dirdat_de.offset.sector()) * 512,
        static_cast<int64_t>(dirdat_de.bytes));

    state.dir_cache.init(locs, dir_data.data(), dir_data.size());

    if (verbose) {
        std::cout << "mounted: " << nvers << " versions, "
                  << locs.size() << " directories\n";
    }
}

int run_fuse(MountState& state, const std::string& mountpoint,
             bool /*allow_other*/, bool foreground) {
    g_state = &state;

    std::memset(&s3_ops, 0, sizeof(s3_ops));
    s3_ops.getattr  = fs_getattr;
    s3_ops.readdir  = fs_readdir;
    s3_ops.open     = fs_open;
    s3_ops.release  = fs_release;
    s3_ops.read     = fs_read;
    s3_ops.readlink = fs_readlink;
    s3_ops.statfs   = fs_statfs_op;

    std::vector<const char*> argv;
    argv.push_back("s3mount");
    if (foreground) argv.push_back("-f");
    argv.push_back("-o"); argv.push_back("ro,default_permissions");
    argv.push_back(mountpoint.c_str());

    return fuse_main(static_cast<int>(argv.size()),
                     const_cast<char**>(argv.data()),
                     &s3_ops, nullptr);
}

} // namespace s3mount