// store.h - Object store abstraction and environment control.
#pragma once

#include <memory>
#include <string>
#include <vector>

namespace store {

    // Manages global environment initialization for object stores.
    class StoreEnv {
    public:
        static void Initialize();
        static void Cleanup();
    };

    // ObjectStore abstracts S3 and local file I/O.
    class ObjectStore {
    public:
        virtual ~ObjectStore() = default;

        [[nodiscard]] virtual std::vector<uint8_t> get_range(
            const std::string& key, int64_t offset, int64_t length) = 0;

        [[nodiscard]] virtual int64_t size(const std::string& key) = 0;

        class Writer {
        public:
            virtual ~Writer() = default;
            virtual void write(const uint8_t* data, size_t len) = 0;
            virtual void close() = 0;
        };

        [[nodiscard]] virtual std::unique_ptr<Writer> new_writer(const std::string& key) = 0;
    };

    class LocalStore : public ObjectStore {
    public:
        [[nodiscard]] std::vector<uint8_t> get_range(
            const std::string& key, int64_t offset, int64_t length) override;

        [[nodiscard]] int64_t size(const std::string& key) override;

        [[nodiscard]] std::unique_ptr<Writer> new_writer(const std::string& key) override;
    };

    struct S3Config {
        std::string endpoint;
        std::string access_key;
        std::string secret_key;
        std::string bucket;
        bool use_ssl = true;
    };

    class S3Store : public ObjectStore {
    public:
        explicit S3Store(S3Config cfg);

        [[nodiscard]] std::vector<uint8_t> get_range(
            const std::string& key, int64_t offset, int64_t length) override;

        [[nodiscard]] int64_t size(const std::string& key) override;

        [[nodiscard]] std::unique_ptr<Writer> new_writer(const std::string& key) override;

    private:
        S3Config cfg_;
    };

} // namespace store