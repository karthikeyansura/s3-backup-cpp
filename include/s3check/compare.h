#pragma once

#include "s3backup/mount.h"
#include <string>
#include <vector>

namespace s3check {

struct Report {
    int compared = 0;
    std::vector<std::string> missing_in_backup;
    std::vector<std::string> missing_in_local;
    std::vector<std::string> mismatches;

    bool ok() const {
        return missing_in_backup.empty() && missing_in_local.empty() && mismatches.empty();
    }
    
    void mismatch(const std::string& path, const std::string& msg);
};

// Tree compares a local directory tree against a loaded backup archive.
Report compare_tree(s3mount::MountState& arch, const std::string& local_root);

} // namespace s3check
