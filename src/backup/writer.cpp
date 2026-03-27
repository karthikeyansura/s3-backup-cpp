// writer.cpp - Sector-aligned writer.
#include "s3backup/backup.h"

namespace backup {

    SectorWriter::SectorWriter(store::ObjectStore::Writer* w, bool noio)
        : w_(w), noio_(noio) {}

    void SectorWriter::write(const uint8_t* data, size_t len) {
        total_written_ += static_cast<int64_t>(len);
        if (len == 0 || noio_) return;
        w_->write(data, len);
    }

} // namespace backup
