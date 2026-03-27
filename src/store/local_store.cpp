// local_store.cpp - Local file backend implementation.
#include "s3backup/store.h"
#include <fstream>
#include <sys/stat.h>
#include <stdexcept>

namespace store {

    std::vector<uint8_t> LocalStore::get_range(
        const std::string& key, int64_t offset, int64_t length)
    {
        std::ifstream f(key, std::ios::binary);
        if (!f) throw std::runtime_error("store: cannot open " + key);

        f.seekg(offset);

        // Type deduction for streamsize to avoid narrowing conversion.
        auto read_size = static_cast<std::streamsize>(length);
        std::vector<uint8_t> buf(static_cast<size_t>(length));

        f.read(reinterpret_cast<char*>(buf.data()), read_size);
        auto got = f.gcount();
        buf.resize(static_cast<size_t>(got));
        return buf;
    }

    int64_t LocalStore::size(const std::string& key) {
        // Value initialization to satisfy uninitialized record type check.
        struct stat sb {};
        if (::stat(key.c_str(), &sb) < 0)
            throw std::runtime_error("store: cannot stat " + key);
        return static_cast<int64_t>(sb.st_size);
    }

    // Local filesystem writer implementation.
    class LocalWriter : public ObjectStore::Writer {
    public:
        explicit LocalWriter(const std::string& path)
            : f_(path, std::ios::binary | std::ios::trunc) {
            if (!f_) throw std::runtime_error("store: cannot create " + path);
        }

        void write(const uint8_t* data, size_t len) override {
            // Cast to streamsize for standard stream compliance.
            f_.write(reinterpret_cast<const char*>(data), static_cast<std::streamsize>(len));
        }

        void close() override {
            f_.close();
        }

    private:
        std::ofstream f_;
    };

    std::unique_ptr<ObjectStore::Writer> LocalStore::new_writer(const std::string& key) {
        return std::make_unique<LocalWriter>(key);
    }

} // namespace store