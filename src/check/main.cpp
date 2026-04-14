#include "CLI/CLI.hpp"
#include "s3check/fsck.h"
#include "s3check/compare.h"
#include "s3backup/store.h"
#include "s3backup/mount.h"
#include <iostream>
#include <memory>

static std::unique_ptr<store::ObjectStore> resolve_target(bool local, bool use_http, const std::string& target) {
    if (local) {
        return std::make_unique<store::LocalStore>();
    }
    
    size_t pos = target.find('/');
    if (pos == std::string::npos) {
        throw std::runtime_error("target must be bucket/key, got " + target);
    }
    std::string bucket = target.substr(0, pos);
    
    store::S3Config cfg;
    cfg.bucket = bucket;
    
    const char* env_host = std::getenv("S3_HOSTNAME");
    if (env_host) cfg.endpoint = env_host;
    
    const char* env_access = std::getenv("S3_ACCESS_KEY_ID");
    if (env_access) cfg.access_key = env_access;
    
    const char* env_secret = std::getenv("S3_SECRET_ACCESS_KEY");
    if (env_secret) cfg.secret_key = env_secret;
    
    cfg.use_ssl = !use_http;
    
    return std::make_unique<store::S3Store>(cfg);
}

static std::string extract_key(bool local, const std::string& target) {
    if (local) return target;
    size_t pos = target.find('/');
    if (pos == std::string::npos) return ""; // Won't happen if resolve_target passed
    return target.substr(pos + 1);
}

int main(int argc, char** argv) {
    CLI::App app{"Validate and compare s3backup objects"};
    app.fallthrough();

    bool local = false;
    app.add_flag("--local", local, "Interpret target as a local file path");
    
    bool use_http = false;
    app.add_flag("--http", use_http, "Use HTTP instead of HTTPS");

    auto fsck_cmd = app.add_subcommand("fsck", "Validate internal consistency of a backup object");
    std::string fsck_target;
    fsck_cmd->add_option("target", fsck_target, "Target object")->required();

    auto diff_cmd = app.add_subcommand("diff", "Compare a backup object against a local directory tree");
    std::string diff_target;
    std::string diff_dir;
    diff_cmd->add_option("target", diff_target, "Target object")->required();
    diff_cmd->add_option("directory", diff_dir, "Directory to compare")->required();

    CLI11_PARSE(app, argc, argv);

    store::StoreEnv env;

    try {
        if (app.got_subcommand(fsck_cmd)) {
            auto st = resolve_target(local, use_http, fsck_target);
            std::string key = extract_key(local, fsck_target);
            
            s3mount::MountState state;
            s3mount::init_mount_state(state, st.get(), key, false);
            
            auto rep = s3check::fsck(state);
            std::cout << "versions:    " << rep.versions << "\n";
            std::cout << "directories: " << rep.directories << "\n";
            std::cout << "files:       " << rep.files << "\n";
            std::cout << "symlinks:    " << rep.symlinks << "\n";
            std::cout << "specials:    " << rep.specials << "\n";
            
            for (const auto& w : rep.warnings) std::cout << "WARNING: " << w << "\n";
            for (const auto& e : rep.errors) std::cout << "ERROR:   " << e << "\n";
            
            if (rep.ok()) {
                std::cout << "fsck: OK\n";
                return 0;
            } else {
                std::cout << "fsck: FAILED (" << rep.errors.size() << " errors)\n";
                return 1;
            }
        } 
        else if (app.got_subcommand(diff_cmd)) {
            auto st = resolve_target(local, use_http, diff_target);
            std::string key = extract_key(local, diff_target);
            
            s3mount::MountState state;
            s3mount::init_mount_state(state, st.get(), key, false);
            
            auto rep = s3check::compare_tree(state, diff_dir);
            std::cout << "compared: " << rep.compared << " entries\n";
            
            for (const auto& m : rep.missing_in_backup) std::cout << "MISSING IN BACKUP: " << m << "\n";
            for (const auto& m : rep.missing_in_local) std::cout << "MISSING LOCALLY:   " << m << "\n";
            for (const auto& m : rep.mismatches) std::cout << "MISMATCH: " << m << "\n";
            
            if (rep.ok()) {
                std::cout << "diff: MATCH\n";
                return 0;
            } else {
                std::cout << "diff: DIFFER (" << rep.missing_in_backup.size() << " missing in backup, "
                          << rep.missing_in_local.size() << " missing locally, "
                          << rep.mismatches.size() << " mismatches)\n";
                return 1;
            }
        } else {
            std::cerr << app.help() << std::endl;
            return 1;
        }
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
}
