#pragma once

#include "s3backup/mount.h"
#include <string>
#include <vector>

namespace s3check {

struct FsckReport {
    int versions = 0;
    int directories = 0;
    int files = 0;
    int symlinks = 0;
    int specials = 0;
    std::vector<std::string> warnings;
    std::vector<std::string> errors;

    bool ok() const { return errors.empty(); }
    void errorf(const std::string& path, const std::string& msg);
    void warnf(const std::string& path, const std::string& msg);
};

// Check validates structural consistency of a backup object.
FsckReport fsck(s3mount::MountState& arch);

} // namespace s3check
