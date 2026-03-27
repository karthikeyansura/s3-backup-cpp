// main.cpp - s3mount CLI entry point.
#include "s3backup/mount.h"
#include "s3backup/store.h"
#include <CLI/CLI.hpp>
#include <iostream>
#include <string>
#include <memory>

int main(int argc, char** argv) {
    CLI::App app{"s3mount - mount an S3 backup as a read-only FUSE filesystem"};

    std::string hostname, access_key, secret_key;
    bool local = false, use_http = false, no_cache = false, verbose = false;
    bool foreground = false;
    std::string target, mountpoint;

    app.add_option("--hostname", hostname, "S3 hostname");
    app.add_option("--access-key", access_key, "S3 access key");
    app.add_option("--secret-key", secret_key, "S3 secret key");
    app.add_flag("--local", local, "Local file mode");
    app.add_flag("--http", use_http, "Use HTTP instead of HTTPS");
    app.add_flag("--nocache", no_cache, "Disable data block cache");
    app.add_flag("-v,--verbose", verbose, "Verbose output");
    app.add_flag("-f,--foreground", foreground, "Run in foreground");
    app.add_option("target", target, "bucket/key or local path")->required();
    app.add_option("mountpoint", mountpoint, "Mount point directory")->required();

    CLI11_PARSE(app, argc, argv);

    auto env_or = [](const std::string& val, const char* env) -> std::string {
        if (!val.empty()) return val;
        const char* e = std::getenv(env);
        return e != nullptr ? e : "";
    };

    hostname   = env_or(hostname, "S3_HOSTNAME");
    access_key = env_or(access_key, "S3_ACCESS_KEY_ID");
    secret_key = env_or(secret_key, "S3_SECRET_ACCESS_KEY");

    try {
        std::unique_ptr<store::ObjectStore> st;
        std::string object_key;

        if (local) {
            st = std::make_unique<store::LocalStore>();
            object_key = target;
        } else {
            auto slash = target.find('/');
            if (slash == std::string::npos) {
                std::cerr << "Error: target must be in 'bucket/key' format\n";
                return 1;
            }
            std::string bucket = target.substr(0, slash);
            object_key = target.substr(slash + 1);

            store::S3Config config{hostname, access_key, secret_key, bucket, !use_http};
            st = std::make_unique<store::S3Store>(config);
        }

        s3mount::MountState state;
        state.no_cache = no_cache;
        state.store = st.get();

        s3mount::init_mount_state(state, st.get(), object_key, verbose);

        if (!no_cache) {
            state.data_cache = new s3mount::DataCache(st.get());
        }

        if (verbose) {
            std::cout << "Mounting " << target << " at " << mountpoint << "\n";
        }

        // FUSE_USE_VERSION 26 requires allow_other to be handled carefully in args
        int rc = s3mount::run_fuse(state, mountpoint, true, foreground || verbose);

        delete state.data_cache;
        return rc;

    } catch (const std::exception& e) {
        std::cerr << "s3mount error: " << e.what() << "\n";
        return 1;
    }
}