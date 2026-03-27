# S3 Incremental Backup and FUSE Client — C++ Implementation

**s3backup** performs a full or incremental backup of a directory hierarchy to a single S3 object; **s3mount** mounts a backup as a read-only FUSE file system.

This implementation is a ground-up port of [pjd-nu/s3-backup](https://github.com/pjd-nu/s3-backup) from C to modern C++17, replacing the unmaintained `libs3` library with a minimal libcurl-based S3 client (swappable for minio-cpp or the AWS SDK), eliminating `libavl` in favor of `std::unordered_map`, and implementing the mmap-based directory cache optimization described in the original project's cleanup notes.

The on-disk binary format is fully preserved — backups created by any of the C, Go, or C++ versions can be mounted by any other.

Original project by Peter Desnoyers, Northeastern University, Solid-State Storage Lab.

## Building

Requires CMake 3.20+ and a C++17 compiler. The only system dependency is libcurl (present on macOS and most Linux distributions by default).

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build
```

To skip building `s3mount` (avoids the FUSE dependency):

```bash
cmake -B build -DBUILD_MOUNT=OFF
cmake --build build
```

### Running Tests

```bash
cd build
ctest --output-on-failure
```

Or run directly:

```bash
./build/test_format     # binary format round-trip tests
./build/test_backup     # end-to-end backup + verification (local mode)
```

### macOS Notes

- `s3backup` works without any extra dependencies.
- `s3mount` requires [FUSE-T](https://github.com/macos-fuse-t/fuse-t) (`brew install fuse-t fuse-t-lib`).
- UUID generation uses the system `uuid/uuid.h` (available in both macOS and `libuuid-dev` on Linux).

## What Changed from the C Version

| Component | C Original | C++ Implementation |
|-----------|-----------|-------------------|
| S3 library | libs3 (unmaintained, required 64-bit patch) | libcurl (minimal S3 client; swappable for minio-cpp / AWS SDK) |
| Map / tree | libavl | `std::unordered_map` |
| Directory cache | In-memory heap buffers | mmap'd temp file (zero-copy lookups) |
| Data cache | Fixed 16-entry LRU, 16 MiB blocks | Same algorithm, `std::mutex`-protected |
| CLI parsing | argp | CLI11 (header-only, fetched via CMake FetchContent) |
| UUID | libuuid (C) | libuuid (same, via `uuid/uuid.h`) |
| Build system | Makefile + manual deps | CMake with FetchContent |
| Platform compat | `#ifdef` / gcc attributes | `#ifdef PLATFORM_DARWIN` / `PLATFORM_LINUX` |

## Project Structure

```
include/s3backup/   Public headers (format.h, superblock.h, store.h, backup.h, mount.h)
src/s3fs/           On-disk format: packed struct serialization, dirent/version iteration
src/store/          ObjectStore interface with S3 (libcurl) and local file backends
src/backup/         Backup engine: directory traversal, incremental diffing, sector-aligned I/O
src/mount/          FUSE filesystem, mmap-based directory cache, LRU data block cache
tests/              Format round-trip tests and end-to-end backup test
```

## Usage

```bash
# Full backup (local mode — no S3 needed)
./build/s3backup --bucket unused --local backup.img /path/to/dir

# Full backup to S3
export S3_HOSTNAME=play.min.io
export S3_ACCESS_KEY_ID=...
export S3_SECRET_ACCESS_KEY=...
./build/s3backup --bucket mybucket backup-2024-01-01 /home/user

# Incremental backup
./build/s3backup --bucket mybucket --incremental backup-2024-01-01 backup-2024-01-02 /home/user

# Mount (requires FUSE)
mkdir /mnt/backup
./build/s3mount --local backup.img /mnt/backup
```

## Binary Format Compatibility

The C++ implementation reads and writes the exact same binary format as the original C version. All multi-byte integers are little-endian. Structures are serialized field-by-field with explicit `memcpy` to avoid alignment padding — no `#pragma pack` or `__attribute__((packed))` is used in the C++ code; instead, portable serialization functions handle the byte layout.

## Dependencies

| Dependency | Purpose | Source                                                           |
|-----------|---------|------------------------------------------------------------------|
| libcurl | S3 HTTP operations (GET range, HEAD, PUT) | System package                                                   |
| CLI11 | Command-line parsing | CMake FetchContent                                               |
| libuuid | UUID generation | System (`uuid/uuid.h`)                                           |
| FUSE-T / libfuse | FUSE mount (optional) | `brew install fuse-t fuse-t-lib` (macOS) / `libfuse-dev` (Linux) |
