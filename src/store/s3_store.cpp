// s3_store.cpp - S3 backend implementation using libcurl.
#include "s3backup/store.h"
#include <curl/curl.h>
#include <sstream>
#include <stdexcept>
#include <algorithm>

namespace store {

void StoreEnv::Initialize() {
    curl_global_init(CURL_GLOBAL_DEFAULT);
}

void StoreEnv::Cleanup() {
    curl_global_cleanup();
}

namespace {

size_t write_callback(char* ptr, size_t size, size_t nmemb, void* userdata) {
    auto* buf = static_cast<std::vector<uint8_t>*>(userdata);
    size_t bytes = size * nmemb;
    buf->insert(buf->end(), reinterpret_cast<uint8_t*>(ptr), reinterpret_cast<uint8_t*>(ptr) + bytes);
    return bytes;
}

size_t header_callback(const char* buffer, size_t size, size_t nitems, void* userdata) {
    auto* content_length = static_cast<int64_t*>(userdata);
    std::string line(buffer, size * nitems);
    if (line.compare(0, 16, "Content-Length: ") == 0 ||
        line.compare(0, 16, "content-length: ") == 0) {
        try {
            *content_length = std::stoll(line.substr(16));
        } catch (...) {
            // Parsing failed for Content-Length header.
        }
    }
    return size * nitems;
}

std::string make_url(const S3Config& cfg, const std::string& key) {
    std::string scheme = cfg.use_ssl ? "https" : "http";
    return scheme + "://" + cfg.endpoint + "/" + cfg.bucket + "/" + key;
}

} // namespace

S3Store::S3Store(S3Config cfg) : cfg_(std::move(cfg)) {}

std::vector<uint8_t> S3Store::get_range(const std::string& key, int64_t offset, int64_t length) {
    CURL* curl = curl_easy_init();
    if (curl == nullptr) throw std::runtime_error("store: curl init failed");

    std::string url = make_url(cfg_, key);
    std::string range = std::to_string(offset) + "-" + std::to_string(offset + length - 1);

    std::vector<uint8_t> buf;
    buf.reserve(static_cast<size_t>(length));

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_RANGE, range.c_str());
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &buf);

    if (!cfg_.access_key.empty()) {
        std::string userpwd = cfg_.access_key + ":" + cfg_.secret_key;
        curl_easy_setopt(curl, CURLOPT_USERPWD, userpwd.c_str());
    }

    CURLcode res = curl_easy_perform(curl);
    curl_easy_cleanup(curl);

    if (res != CURLE_OK)
        throw std::runtime_error(std::string("store: curl GET failed: ") + curl_easy_strerror(res));

    return buf;
}

int64_t S3Store::size(const std::string& key) {
    CURL* curl = curl_easy_init();
    if (curl == nullptr) throw std::runtime_error("store: curl init failed");

    std::string url = make_url(cfg_, key);
    int64_t content_length = 0;

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_NOBODY, 1L);
    curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, header_callback);
    curl_easy_setopt(curl, CURLOPT_HEADERDATA, &content_length);

    if (!cfg_.access_key.empty()) {
        std::string userpwd = cfg_.access_key + ":" + cfg_.secret_key;
        curl_easy_setopt(curl, CURLOPT_USERPWD, userpwd.c_str());
    }

    CURLcode res = curl_easy_perform(curl);
    curl_easy_cleanup(curl);

    if (res != CURLE_OK)
        throw std::runtime_error(std::string("store: curl HEAD failed: ") + curl_easy_strerror(res));

    return content_length;
}

class S3Writer : public ObjectStore::Writer {
public:
    S3Writer(S3Config cfg, std::string key)
        : cfg_(std::move(cfg)), key_(std::move(key)), read_offset_(0) {}

    void write(const uint8_t* data, size_t len) override {
        buf_.insert(buf_.end(), data, data + len);
    }

    void close() override {
        CURL* curl = curl_easy_init();
        if (curl == nullptr) throw std::runtime_error("store: curl init failed");

        std::string url = make_url(cfg_, key_);
        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_UPLOAD, 1L);
        curl_easy_setopt(curl, CURLOPT_INFILESIZE_LARGE, static_cast<curl_off_t>(buf_.size()));

        read_offset_ = 0;
        curl_easy_setopt(curl, CURLOPT_READFUNCTION, read_cb);
        curl_easy_setopt(curl, CURLOPT_READDATA, this);

        if (!cfg_.access_key.empty()) {
            std::string userpwd = cfg_.access_key + ":" + cfg_.secret_key;
            curl_easy_setopt(curl, CURLOPT_USERPWD, userpwd.c_str());
        }

        CURLcode res = curl_easy_perform(curl);
        curl_easy_cleanup(curl);

        if (res != CURLE_OK)
            throw std::runtime_error(std::string("store: curl PUT failed: ") + curl_easy_strerror(res));
    }

private:
    static size_t read_cb(char* ptr, size_t size, size_t nmemb, void* userdata) {
        auto* self = static_cast<S3Writer*>(userdata);
        size_t remaining = self->buf_.size() - self->read_offset_;
        size_t to_copy = std::min(size * nmemb, remaining);
        if (to_copy > 0) {
            std::memcpy(ptr, self->buf_.data() + self->read_offset_, to_copy);
            self->read_offset_ += to_copy;
        }
        return to_copy;
    }

    S3Config cfg_;
    std::string key_;
    std::vector<uint8_t> buf_;
    size_t read_offset_;
};

std::unique_ptr<ObjectStore::Writer> S3Store::new_writer(const std::string& key) {
    return std::make_unique<S3Writer>(cfg_, key);
}

} // namespace store