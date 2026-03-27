// superblock.cpp - Superblock and version chain implementation.
#include "s3backup/superblock.h"
#include <stdexcept>
#include <random>
#include <uuid/uuid.h>

namespace s3fs {

namespace {
    // Memory access helpers for fixed-width integers.
    uint16_t read16(const uint8_t* p) { uint16_t v; std::memcpy(&v, p, 2); return v; }
    uint32_t read32(const uint8_t* p) { uint32_t v; std::memcpy(&v, p, 4); return v; }
    void write16(uint8_t* p, uint16_t v) { std::memcpy(p, &v, 2); }
    void write32(uint8_t* p, uint32_t v) { std::memcpy(p, &v, 4); }
}

UUID generate_uuid() {
    UUID u;
    uuid_t raw;
    uuid_generate(raw);
    std::memcpy(u.data(), raw, 16);
    return u;
}

// Version parsing and serialization.

std::pair<Version, int> parse_version(const uint8_t* data, size_t len) {
    if (len < static_cast<size_t>(kVersionFixedSize))
        throw std::runtime_error("s3fs: buffer too small for version");

    Version v;
    std::memcpy(v.uuid.data(), data, 16);
    v.namelen = read16(data + 16);

    int total = kVersionFixedSize + v.namelen;
    if (len < static_cast<size_t>(total))
        throw std::runtime_error("s3fs: buffer too small for version name");

    v.name.assign(reinterpret_cast<const char*>(data + 18), v.namelen);
    return {v, total};
}

int marshal_version(const Version& v, uint8_t* buf) {
    std::memcpy(buf, v.uuid.data(), 16);
    write16(buf + 16, v.namelen);
    std::memcpy(buf + 18, v.name.data(), v.namelen);
    return v.size();
}

// Superblock parsing and creation.

Superblock parse_superblock(const uint8_t* data, size_t len) {
    if (len < static_cast<size_t>(kSuperblockFixedSize))
        throw std::runtime_error("s3fs: buffer too small for superblock");

    Superblock sb;
    sb.magic   = read32(data + 0);
    sb.version = read32(data + 4);
    sb.flags   = read32(data + 8);
    sb.len     = read32(data + 12);
    sb.nvers   = read32(data + 16);

    if (sb.magic != kMagic)
        throw std::runtime_error("s3fs: bad magic");

    size_t off = kSuperblockFixedSize;
    for (uint32_t i = 0; i < sb.nvers; ++i) {
        auto [v, n] = parse_version(data + off, len - off);
        sb.versions.push_back(std::move(v));
        off += n;
    }
    return sb;
}

std::vector<std::string> Superblock::version_names() const {
    std::vector<std::string> names(versions.size());
    for (size_t i = 0; i < versions.size(); ++i)
        names[names.size() - 1 - i] = versions[i].name;
    return names;
}

std::vector<UUID> Superblock::version_uuids() const {
    std::vector<UUID> uuids(versions.size());
    for (size_t i = 0; i < versions.size(); ++i)
        uuids[uuids.size() - 1 - i] = versions[i].uuid;
    return uuids;
}

std::pair<std::vector<uint8_t>, int> make_superblock(
    const std::string& new_name,
    const std::vector<Version>& prev_versions)
{
    std::vector<uint8_t> buf(4096, 0);

    write32(buf.data() + 0, kMagic);
    write32(buf.data() + 4, 1); // internal format version
    write32(buf.data() + 8, kFlagDirLoc | kFlagDirData | kFlagDirPacked);

    size_t off = kSuperblockFixedSize;
    uint32_t nvers = 1;

    // Version chain ordered with newest first.
    Version nv;
    nv.uuid = generate_uuid();
    nv.namelen = static_cast<uint16_t>(new_name.size());
    nv.name = new_name;
    off += marshal_version(nv, buf.data() + off);

    for (const auto& pv : prev_versions) {
        off += marshal_version(pv, buf.data() + off);
        ++nvers;
    }

    write32(buf.data() + 16, nvers);

    auto len_sectors = static_cast<uint32_t>(round_up(static_cast<int64_t>(off), kSectorSize) / kSectorSize);
    write32(buf.data() + 12, static_cast<uint32_t>(len_sectors));

    auto total_bytes = static_cast<size_t>(len_sectors) * kSectorSize;
    buf.resize(total_bytes);
    return {buf, len_sectors};
}

} // namespace s3fs