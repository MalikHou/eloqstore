#include "eloq_store.h"

#include <glog/logging.h>

#include <atomic>
#include <cassert>
#include <cstddef>
#include <filesystem>
#include <memory>
#include <string_view>
#include <utility>
#include <vector>

#include "archive_crond.h"
#include "file_gc.h"
#include "shard.h"

namespace fs = std::filesystem;

namespace kvstore
{

EloqStore::EloqStore(const KvOptions &opts) : options_(opts), stopped_(true)
{
    CHECK((options_.data_page_size & (page_align - 1)) == 0);

    // Align stack size
    options_.coroutine_stack_size =
        ((options_.coroutine_stack_size + page_align - 1) & ~(page_align - 1));

    if (options_.overflow_pointers == 0 ||
        options_.overflow_pointers > max_overflow_pointers)
    {
        LOG(FATAL) << "Invalid option overflow_pointers";
    }

    if (options_.max_write_batch_pages == 0 ||
        options_.max_write_batch_pages > max_write_pages_batch)
    {
        LOG(FATAL) << "Invalid option max_write_batch_pages";
    }
}

EloqStore::~EloqStore()
{
    if (!IsStopped())
    {
        Stop();
    }
}

KvError EloqStore::Start()
{
    if (!options_.db_path.empty())
    {
        KvError err = InitDBDir();
        CHECK_KV_ERR(err);
    }

    shards_.resize(options_.num_threads);
    for (size_t i = 0; i < options_.num_threads; i++)
    {
        if (shards_[i] == nullptr)
        {
            shards_[i] = std::make_unique<Shard>(this);
        }
        KvError err = shards_[i]->Init(dir_fd_);
        CHECK_KV_ERR(err);
    }

    stopped_.store(false, std::memory_order_relaxed);

    if (options_.data_append_mode)
    {
        if (options_.num_gc_threads > 0)
        {
            if (file_gc_ == nullptr)
            {
                file_gc_ = std::make_unique<FileGarbageCollector>(&options_);
            }
            file_gc_->Start(options_.num_gc_threads);
            file_garbage_collector = file_gc_.get();
        }
        if (options_.num_retained_archives > 0 &&
            options_.archive_interval_secs > 0)
        {
            if (archive_crond_ == nullptr)
            {
                archive_crond_ = std::make_unique<ArchiveCrond>(this);
            }
            archive_crond_->Start();
        }
    }

    for (auto &shard : shards_)
    {
        shard->Start();
    }

    LOG(INFO) << "EloqStore is started.";
    return KvError::NoError;
}

KvError EloqStore::InitDBDir()
{
    const fs::path &db_path = options_.db_path;
    if (fs::exists(db_path))
    {
        if (!fs::is_directory(db_path))
        {
            LOG(ERROR) << "path " << db_path << " is not directory";
            return KvError::InvalidArgs;
        }
        for (auto &ent : fs::directory_iterator{db_path})
        {
            if (!ent.is_directory())
            {
                LOG(ERROR) << ent.path() << " is not directory";
                return KvError::InvalidArgs;
            }
            const std::string name = ent.path().filename().string();
            TableIdent tbl_id = TableIdent::FromString(name);
            if (tbl_id.tbl_name_.empty())
            {
                LOG(ERROR) << "unexpected partition name " << name;
                return KvError::InvalidArgs;
            }
            fs::path wal_path = ent.path() / FileNameManifest;
            if (!fs::exists(wal_path))
            {
                LOG(WARNING) << "clear incomplete partition " << name;
                fs::remove_all(ent.path());
            }
        }
    }
    else
    {
        fs::create_directories(db_path);
    }
    dir_fd_ = open(db_path.c_str(), IouringMgr::oflags_dir);
    if (dir_fd_ < 0)
    {
        return KvError::IoFail;
    }
    return KvError::NoError;
}

bool EloqStore::ExecAsyn(KvRequest *req)
{
    req->user_data_ = 0;
    req->callback_ = nullptr;
    return SendRequest(req);
}

void EloqStore::ExecSync(KvRequest *req)
{
    req->user_data_ = 0;
    req->callback_ = nullptr;
    if (SendRequest(req))
    {
        req->Wait();
    }
    else
    {
        req->SetDone(KvError::NotRunning);
    }
}

bool EloqStore::SendRequest(KvRequest *req)
{
    if (stopped_.load(std::memory_order_relaxed))
    {
        return false;
    }

    req->err_ = KvError::NoError;
    req->done_.store(false, std::memory_order_relaxed);

    Shard *shard = shards_[req->TableId().partition_id_ % shards_.size()].get();
    return shard->AddKvRequest(req);
}

void EloqStore::Stop()
{
    if (archive_crond_ != nullptr)
    {
        archive_crond_->Stop();
    }

    stopped_.store(true, std::memory_order_relaxed);
    for (auto &shard : shards_)
    {
        shard->Stop();
    }
    shards_.clear();

    if (file_gc_ != nullptr)
    {
        file_garbage_collector = nullptr;
        file_gc_->Stop();
    }

    if (dir_fd_ >= 0)
    {
        close(dir_fd_);
        dir_fd_ = -1;
    }
    LOG(INFO) << "EloqStore is stopped.";
}

const KvOptions &EloqStore::Options() const
{
    return options_;
}

bool EloqStore::IsStopped() const
{
    return stopped_.load(std::memory_order_relaxed);
}

void KvRequest::SetTableId(TableIdent tbl_id)
{
    tbl_id_ = std::move(tbl_id);
}

KvError KvRequest::Error() const
{
    return err_;
}

const char *KvRequest::ErrMessage() const
{
    return ErrorString(err_);
}

uint64_t KvRequest::UserData() const
{
    return user_data_;
}

void KvRequest::Wait() const
{
    CHECK(callback_ == nullptr);
    done_.wait(false, std::memory_order_acquire);
}

void ReadRequest::SetArgs(TableIdent tbl_id, std::string_view key)
{
    SetTableId(std::move(tbl_id));
    key_ = key;
}

void ScanRequest::SetArgs(TableIdent tbl_id,
                          std::string_view begin,
                          std::string_view end,
                          bool begin_inclusive)
{
    SetTableId(std::move(tbl_id));
    begin_key_ = begin;
    end_key_ = end;
    begin_inclusive_ = begin_inclusive;
}

void ScanRequest::SetPagination(size_t entries, size_t size)
{
    page_entries_ = entries != 0 ? entries : SIZE_MAX;
    page_size_ = size != 0 ? size : SIZE_MAX;
}

size_t ScanRequest::ResultSize() const
{
    size_t size = 0;
    for (const auto &[k, v, _] : entries_)
    {
        size += k.size() + v.size() + sizeof(uint64_t);
    }
    return size;
}

void WriteRequest::SetArgs(TableIdent tbl_id,
                           std::vector<WriteDataEntry> &&batch)
{
    SetTableId(std::move(tbl_id));
    batch_ = std::move(batch);
}

void WriteRequest::AddWrite(std::string key,
                            std::string value,
                            uint64_t ts,
                            WriteOp op)
{
    batch_.push_back({std::move(key), std::move(value), ts, op});
}

void TruncateRequest::SetArgs(TableIdent tbl_id, std::string_view position)
{
    SetTableId(std::move(tbl_id));
    position_ = position;
}

void ArchiveRequest::SetArgs(TableIdent tbl_id)
{
    SetTableId(std::move(tbl_id));
}

const TableIdent &KvRequest::TableId() const
{
    return tbl_id_;
}

bool KvRequest::IsDone() const
{
    return done_.load(std::memory_order_acquire);
}

void KvRequest::SetDone(KvError err)
{
    err_ = err;
    done_.store(true, std::memory_order_release);
    if (callback_)
    {
        // Asynchronous request
        callback_(this);
    }
    else
    {
        // Synchronous request
        done_.notify_one();
    }
}

}  // namespace kvstore