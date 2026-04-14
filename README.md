# S3 Incremental Backup and FUSE Client — C++ Implementation

**s3backup** performs a full or incremental backup of a directory hierarchy to a single S3 object; **s3mount** mounts a backup as a read-only FUSE file system; **s3check** validates backup integrity and compares backups against source trees.

This is a modern C++17 port of the original [pjd-nu/s3-backup](https://github.com/pjd-nu/s3-backup) project. It replaces the unmaintained `libs3` with the official **[MinIO C++ SDK (`minio-cpp`)](https://github.com/minio/minio-cpp)**, and entirely eliminates the Out-Of-Memory (OOM) footprint that previously choked the C-based FUSE daemon on large datasets. The C++ implementation features a highly-optimized pipelined streaming layout for S3 multipart uploads. 

The on-disk binary format is fully preserved — backups created by either the C version, Go version, or this C++ version can be mounted interchangeably.

Original project by Peter Desnoyers, Northeastern University, Solid-State Storage Lab.

## Quick Start / Build Guide

This project leverages modern CMake (`>= 3.20`) and relies on standard macOS environments to pull dependencies.

### 1. Install Dependencies (macOS)
The dependencies are natively sourced via Homebrew instead of utilizing heavy Windows-based package managers like `vcpkg`.

First, ensure you have Homebrew installed, then run:
```bash
brew install cmake nlohmann-json pugixml curlpp inih openssl zlib
```

*Note: For mounting capabilities using `s3mount`, you must also have macOS `FUSE-T` (or `macFUSE`) installed.*

### 2. Build the Project
Configure and compile the project using standard CMake tools. Building forces an out-of-source structure ensuring safety.

```bash
cmake -B cmake-build-debug -DCMAKE_EXPORT_COMPILE_COMMANDS=ON
cmake --build cmake-build-debug -j 4
```

> **IDE Warnings / Setup Notice**: 
> If you are using CLion or VSCode (with clangd) and see red lines or auto-completion failures due to missing includes, the `-DCMAKE_EXPORT_COMPILE_COMMANDS=ON` flag fixes this! It generates a `compile_commands.json` file inside `cmake-build-debug/`. You can copy or symlink this back to your project root `ln -s cmake-build-debug/compile_commands.json .` letting your IDE seamlessly resolve the `<miniocpp/client.h>` and `OpenSSL` headers.

## Usage

Environment variables: `S3_HOSTNAME`, `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`
(Alternatively available as command-line flags).

After building, the binaries will be located inside the `cmake-build-debug` directory.

```bash
./cmake-build-debug/s3backup --bucket BUCKET [--incremental OBJECT] [--max SIZE] [--exclude PATH] OBJECT /path
./cmake-build-debug/s3mount  [--local] bucket/key /mountpoint
./cmake-build-debug/s3check  fsck [--local] TARGET
./cmake-build-debug/s3check  diff [--local] TARGET DIRECTORY
```

## Description

`s3backup` stores a snapshot of a file system as a single S3 object, using a simplified log-structured file system with 512-byte sectors. It supports incremental backups by chaining a sequence of these objects — each incremental backup stores only files whose metadata (mode, size, ctime, uid, gid) has changed since the previous version.

The FUSE client (`s3mount`) aggressively caches data and directories, stores symbolic links, and preserves owners, timestamps, and permissions. Directory metadata is cached via `mmap` on a temporary file rather than held in heap memory, allowing the OS to page it in and out as needed — significantly reducing resident memory for large datasets.

### Traversal Semantics

During the backup procedure, the directory tree traversal handles these complex edge-cases:

- **Symbolic links**: restored faithfully on mount via FUSE `Readlink`.
- **Cross-device mount points**: detected by comparing `st_dev` of child directories against the parent. Unmounted devices are stored as empty entries.
- **FIFOs**: skipped entirely to avoid blocking the daemon traversing thread.

### Features

- S3 hostname, access key, and secret key can be provided by flags (`--hostname`) as well as environment variables.
- The `--local` flag forces object paths to be interpreted as local filesystem paths—essential for offline debugging or unit testing without an external S3 bucket API.
- Native multithreaded `minio-cpp` bindings completely sidestep memory bloat by buffering and flushing S3 chunks concurrently.
- Highly resilient `diff` and `fsck` utilities that provide deep algorithmic insights and consistency evaluations.

## Running Tests

Integration and Unit Test scripts ported natively from the original implementations run directly off the generated bins using local test mounts.

```bash
# Validates incremental tracking, sector alignments, exclusions, and file diffing.
./test_local.sh

# Builds sandbox trees and FUSE mounts them checking block translations.
./test_mount.sh
```
