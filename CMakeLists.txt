cmake_minimum_required(VERSION 3.20)
project(s3-backup-cpp VERSION 1.0.0 LANGUAGES CXX
        DESCRIPTION "S3 incremental backup and FUSE client")

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(BUILD_MOUNT "Build s3mount (requires FUSE)" ON)
option(BUILD_TESTS "Build tests" ON)

# Platform detection
if(APPLE)
    add_compile_definitions(PLATFORM_DARWIN)
else()
    add_compile_definitions(PLATFORM_LINUX)
endif()

# Dependencies
include(FetchContent)
FetchContent_Declare(cli11
        GIT_REPOSITORY https://github.com/CLIUtils/CLI11.git
        GIT_TAG v2.4.2)
FetchContent_MakeAvailable(cli11)

# macOS Homebrew prefix hint for dependencies
if(APPLE)
    list(APPEND CMAKE_PREFIX_PATH "/opt/homebrew")
endif()

find_package(CURL REQUIRED)
find_package(OpenSSL REQUIRED)
add_subdirectory(minio-cpp)

# Library: s3fs (format)
add_library(s3fs STATIC src/s3fs/format.cpp src/s3fs/superblock.cpp)
target_include_directories(s3fs PUBLIC include)

# Library: store
add_library(s3store STATIC
        src/store/local_store.cpp src/store/s3_store.cpp)
target_include_directories(s3store PUBLIC include)
target_link_libraries(s3store PUBLIC s3fs CURL::libcurl miniocpp::miniocpp OpenSSL::SSL OpenSSL::Crypto)

# Library: backup
add_library(s3backup_lib STATIC src/backup/backup.cpp src/backup/writer.cpp)
target_include_directories(s3backup_lib PUBLIC include)
target_link_libraries(s3backup_lib PUBLIC s3fs s3store)

# Executable: s3backup
add_executable(s3backup src/backup/main.cpp)
target_link_libraries(s3backup PRIVATE s3backup_lib CLI11::CLI11)

# Executable: s3check
add_executable(s3check src/check/main.cpp src/check/fsck.cpp src/check/compare.cpp)
target_include_directories(s3check PRIVATE include)
target_link_libraries(s3check PRIVATE s3mount_lib s3backup_lib s3store s3fs CLI11::CLI11 OpenSSL::Crypto minio-cpp::miniocpp)

# Executable: s3mount
if(BUILD_MOUNT)
    find_package(PkgConfig REQUIRED)

    # Locate FUSE-T on macOS or legacy fuse on Linux
    if(APPLE)
        pkg_check_modules(FUSE REQUIRED fuse-t)
    else()
        pkg_check_modules(FUSE REQUIRED fuse)
    endif()

    if(FUSE_FOUND)
        add_library(s3mount_lib STATIC
                src/mount/fs.cpp src/mount/dircache.cpp src/mount/datacache.cpp)
        target_include_directories(s3mount_lib PUBLIC include ${FUSE_INCLUDE_DIRS})
        target_link_directories(s3mount_lib PUBLIC ${FUSE_LIBRARY_DIRS})
        target_link_libraries(s3mount_lib PUBLIC s3fs s3store ${FUSE_LIBRARIES})

        target_compile_definitions(s3mount_lib PUBLIC
                FUSE_USE_VERSION=26 _FILE_OFFSET_BITS=64)

        add_executable(s3mount src/mount/main.cpp)
        target_link_libraries(s3mount PRIVATE s3mount_lib CLI11::CLI11)

        message(STATUS "FUSE-T found. Building s3mount.")
    else()
        message(FATAL_ERROR "FUSE-T not found. Install via 'brew install --cask fuse-t'.")
    endif()
endif()

# Tests
if(BUILD_TESTS)
    enable_testing()
    add_executable(test_format tests/test_format.cpp)
    target_link_libraries(test_format PRIVATE s3fs)
    add_test(NAME format_tests COMMAND test_format)

    add_executable(test_backup tests/test_backup.cpp)
    target_link_libraries(test_backup PRIVATE s3backup_lib)
    add_test(NAME backup_tests COMMAND test_backup)
endif()