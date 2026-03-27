// main.cpp - s3backup CLI entry point.
#include "s3backup/backup.h"
#include "s3backup/store.h"
#include <CLI/CLI.hpp>
#include <iostream>

int main(int argc, char** argv) {
    store::StoreEnv::Initialize();

    CLI::App app{"s3backup - backup a directory tree to a single S3 object"};

    std::string bucket, incremental, max_size, hostname, access_key, secret_key;
    std::string protocol = "https", tag = "--root--";
    std::vector<std::string> exclude;
    bool local = false, verbose = false, noio = false;
    std::string object_name, dir_path;

    app.add_option("-b,--bucket", bucket, "S3 bucket name")->required();
    app.add_option("-i,--incremental", incremental, "Previous backup object");
    app.add_option("-m,--max", max_size, "Stop after SIZE (K/M/G)");
    app.add_option("--hostname", hostname, "S3 hostname");
    app.add_option("--access-key", access_key, "S3 access key");
    app.add_option("--secret-key", secret_key, "S3 secret key");
    app.add_option("-p,--protocol", protocol, "http or https");
    app.add_option("-t,--tag", tag, "Root entry tag");
    app.add_option("-e,--exclude", exclude, "Exclude paths");
    app.add_flag("-l,--local", local, "Use local files");
    app.add_flag("-v,--verbose", verbose, "Verbose output");
    app.add_flag("-n,--noio", noio, "No output (test only)");
    app.add_option("object", object_name, "Object name")->required();
    app.add_option("directory", dir_path, "Directory to back up")->required();

    CLI11_PARSE(app, argc, argv);

    auto env_or = [](const std::string& val, const char* env) -> std::string {
        if (!val.empty()) return val;
        const char* e = std::getenv(env);
        return e != nullptr ? e : "";
    };

    hostname   = env_or(hostname, "S3_HOSTNAME");
    access_key = env_or(access_key, "S3_ACCESS_KEY_ID");
    secret_key = env_or(secret_key, "S3_SECRET_ACCESS_KEY");

    std::unique_ptr<store::ObjectStore> st;
    if (local) {
        st = std::make_unique<store::LocalStore>();
    } else {
        st = std::make_unique<store::S3Store>(store::S3Config{
            hostname, access_key, secret_key, bucket, protocol != "http"
        });
    }

    int64_t stop_after = 0;
    if (!max_size.empty())
        stop_after = backup::parse_size(max_size) / s3fs::kSectorSize;

    int version_idx = 0;
    if (!incremental.empty()) {
        try {
            auto sb_data = st->get_range(incremental, 0, 20);
            if (sb_data.size() >= 20) {
                uint32_t nvers;
                std::memcpy(&nvers, sb_data.data() + 16, 4);
                version_idx = static_cast<int>(nvers);
            }
        } catch (...) {
            std::cerr << "warning: could not read old superblock\n";
        }
    }

    backup::Config cfg;
    cfg.store = st.get();
    cfg.bucket = bucket;
    cfg.new_name = object_name;
    cfg.old_name = incremental;
    cfg.dir = dir_path;
    cfg.tag = tag;
    cfg.verbose = verbose;
    cfg.noio = noio;
    cfg.exclude = exclude;
    cfg.stop_after = stop_after;
    cfg.version_idx = version_idx;

    int exit_code = 0;
    try {
        auto result = backup::run(cfg);
        std::cout << result.stats.files << " files (" << result.stats.file_sectors << " sectors)\n"
                  << result.stats.dirs << " directories (" << result.stats.dir_sectors << " sectors)\n"
                  << result.stats.total_sectors << " total sectors\n";
    } catch (const std::exception& e) {
        std::cerr << "s3backup error: " << e.what() << "\n";
        exit_code = 1;
    }

    store::StoreEnv::Cleanup();
    return exit_code;
}