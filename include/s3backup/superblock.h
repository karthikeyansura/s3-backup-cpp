// s3fs/superblock.h - Superblock and version chain.
#pragma once

#include "s3backup/format.h"
#include <array>
#include <vector>
#include <string>

namespace s3fs {

    // UUID as a 16-byte array (matching uuid_t)
    using UUID = std::array<uint8_t, 16>;

    UUID generate_uuid();

    struct Version {
        UUID     uuid;
        uint16_t namelen = 0;
        std::string name;

        [[nodiscard]] int size() const { return 16 + 2 + static_cast<int>(namelen); }
    };

    constexpr int kVersionFixedSize = 18; // uuid(16) + namelen(2)

    std::pair<Version, int> parse_version(const uint8_t* data, size_t len);
    int marshal_version(const Version& v, uint8_t* buf);

    // Superblock fixed fields: magic(4) + version(4) + flags(4) + len(4) + nvers(4) = 20
    constexpr int kSuperblockFixedSize = 20;

    struct Superblock {
        uint32_t magic = 0;
        uint32_t version = 0;
        uint32_t flags = 0;
        uint32_t len = 0;          // length in sectors
        uint32_t nvers = 0;
        std::vector<Version> versions; // newest first

        // Return names in oldest-to-newest order
        [[nodiscard]] std::vector<std::string> version_names() const;
        [[nodiscard]] std::vector<UUID> version_uuids() const;
    };

    Superblock parse_superblock(const uint8_t* data, size_t len);

    // Create a new superblock. Returns {buffer, num_sectors}.
    std::pair<std::vector<uint8_t>, int> make_superblock(
        const std::string& new_name,
        const std::vector<Version>& prev_versions);

} // namespace s3fs