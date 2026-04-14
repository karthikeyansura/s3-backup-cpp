// s3_store.cpp - S3 backend implementation using minio-cpp.
#include "s3backup/store.h"
#include <miniocpp/client.h>
#include <sstream>
#include <stdexcept>
#include <algorithm>
#include <thread>
#include <mutex>
#include <condition_variable>

namespace store {

void StoreEnv::Initialize() {
}

void StoreEnv::Cleanup() {
}

struct MinioS3Context {
    minio::s3::BaseUrl base_url;
    minio::creds::StaticProvider provider;
    minio::s3::Client client;

    MinioS3Context(const S3Config& cfg) 
      : base_url(cfg.endpoint, cfg.use_ssl), 
        provider(cfg.access_key, cfg.secret_key),
        client(base_url, &provider) {
    }
};

S3Store::S3Store(S3Config cfg) : cfg_(std::move(cfg)) {}

std::vector<uint8_t> S3Store::get_range(const std::string& key, int64_t offset, int64_t length) {
    MinioS3Context ctx(cfg_);

    minio::s3::GetObjectArgs args;
    args.bucket = cfg_.bucket;
    args.object = key;
    args.extra_headers.Add("Range", "bytes=" + std::to_string(offset) + "-" + std::to_string(offset + length - 1));
    
    std::vector<uint8_t> buf;
    args.datafunc = [&buf](minio::http::DataFunctionArgs dargs) -> bool {
        buf.insert(buf.end(), dargs.datachunk.begin(), dargs.datachunk.end());
        return true;
    };

    minio::s3::GetObjectResponse resp = ctx.client.GetObject(args);
    if (!resp) {
        throw std::runtime_error("store: minio GET failed: " + resp.Error().String());
    }
    
    return buf;
}

int64_t S3Store::size(const std::string& key) {
    MinioS3Context ctx(cfg_);

    minio::s3::StatObjectArgs args;
    args.bucket = cfg_.bucket;
    args.object = key;

    minio::s3::StatObjectResponse resp = ctx.client.StatObject(args);
    if (!resp) {
        throw std::runtime_error("store: minio HEAD failed: " + resp.Error().String());
    }

    return static_cast<int64_t>(resp.size);
}

class PipeStreamBuf : public std::streambuf {
public:
    PipeStreamBuf() : eof_(false) {}

    void write(const uint8_t* data, size_t len) {
        std::unique_lock<std::mutex> lock(mu_);
        buf_.insert(buf_.end(), data, data + len);
        cv_.notify_all();
    }

    void finish() {
        std::unique_lock<std::mutex> lock(mu_);
        eof_ = true;
        cv_.notify_all();
    }
    
    void throw_if_error(std::exception_ptr e) {
        std::unique_lock<std::mutex> lock(mu_);
        err_ = e;
        cv_.notify_all();
    }

protected:
    int underflow() override {
        std::unique_lock<std::mutex> lock(mu_);
        cv_.wait(lock, [this]() { return !buf_.empty() || eof_ || err_; });
        
        if (err_) std::rethrow_exception(err_);
        if (buf_.empty() && eof_) return traits_type::eof();

        setg(reinterpret_cast<char*>(buf_.data()), 
             reinterpret_cast<char*>(buf_.data()), 
             reinterpret_cast<char*>(buf_.data() + buf_.size()));
        return traits_type::to_int_type(*gptr());
    }

    int uflow() override {
        int ret = underflow();
        if (ret != traits_type::eof()) {
            buf_.erase(buf_.begin());
        }
        return ret;
    }
    
    std::streamsize xsgetn(char* s, std::streamsize n) override {
        std::streamsize remaining = n;
        char* dest = s;
        while (remaining > 0) {
            std::unique_lock<std::mutex> lock(mu_);
            cv_.wait(lock, [this]() { return !buf_.empty() || eof_ || err_; });
            
            if (err_) std::rethrow_exception(err_);
            if (buf_.empty() && eof_) break;
            
            size_t avail = buf_.size();
            size_t copy_size = std::min(static_cast<size_t>(remaining), avail);
            
            std::memcpy(dest, buf_.data(), copy_size);
            buf_.erase(buf_.begin(), buf_.begin() + copy_size);
            
            dest += copy_size;
            remaining -= static_cast<std::streamsize>(copy_size);
        }
        return n - remaining;
    }


private:
    std::mutex mu_;
    std::condition_variable cv_;
    std::vector<uint8_t> buf_;
    bool eof_;
    std::exception_ptr err_;
};

class S3Writer : public ObjectStore::Writer {
public:
    S3Writer(S3Config cfg, std::string key)
        : ctx_(std::make_unique<MinioS3Context>(cfg)), key_(std::move(key)), pipe_(), stream_(&pipe_) {
        // Run Minio backend in thread while the frontend writes blocks.
        // To avoid storing everything in memory, minio client reads from stream.
        S3Config thread_cfg = cfg;
        std::string thread_key = key_;
        
        uploader_ = std::thread([this, thread_cfg, thread_key]() {
            try {
                minio::s3::PutObjectArgs args(stream_, -1, 5 * 1024 * 1024);
                args.bucket = thread_cfg.bucket;
                args.object = thread_key;
                
                minio::s3::PutObjectResponse resp = ctx_->client.PutObject(args);
                if (!resp) {
                    pipe_.throw_if_error(std::make_exception_ptr(
                        std::runtime_error("minio PUT failed: " + resp.Error().String())));
                } else {
                    res_err_ = "";
                }
            } catch (const std::exception& e) {
                res_err_ = e.what();
            }
        });
    }
    
    ~S3Writer() {
        if (uploader_.joinable()) {
            pipe_.finish();
            uploader_.join();
        }
    }

    void write(const uint8_t* data, size_t len) override {
        if (len > 0) {
            pipe_.write(data, len);
        }
    }

    void close() override {
        pipe_.finish();
        if (uploader_.joinable()) uploader_.join();
        if (!res_err_.empty()) {
            throw std::runtime_error(res_err_);
        }
    }

private:
   std::unique_ptr<MinioS3Context> ctx_;
   std::string key_;
   PipeStreamBuf pipe_;
   std::istream stream_;
   std::thread uploader_;
   std::string res_err_;
};

std::unique_ptr<ObjectStore::Writer> S3Store::new_writer(const std::string& key) {
    return std::make_unique<S3Writer>(cfg_, key);
}

} // namespace store