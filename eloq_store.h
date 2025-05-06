#pragma once

#include <atomic>

#include "batch_write_task.h"
#include "error.h"
#include "scan_task.h"
#include "types.h"

namespace kvstore
{
class Shard;

enum class RequestType : uint8_t
{
    Read,
    Scan,
    Write,
    Truncate,
    Archive
};

class KvRequest
{
public:
    virtual RequestType Type() const = 0;
    KvError Error() const;
    const char *ErrMessage() const;
    void SetTableId(TableIdent tbl_id);
    const TableIdent &TableId() const;
    uint64_t UserData() const;

    /**
     * @brief Test if this request is done.
     */
    bool IsDone() const;
    void Wait() const;

protected:
    void SetDone(KvError err);

    TableIdent tbl_id_;
    uint64_t user_data_{0};
    std::function<void(KvRequest *)> callback_{nullptr};
    std::atomic<bool> done_{true};
    KvError err_{KvError::NoError};

    friend class Shard;
    friend class EloqStore;
};

class ReadRequest : public KvRequest
{
public:
    RequestType Type() const override
    {
        return RequestType::Read;
    }
    void SetArgs(TableIdent tid, std::string_view key);

    // input
    std::string_view key_;
    // output
    std::string value_;
    uint64_t ts_{0};
};

class ScanRequest : public KvRequest
{
public:
    RequestType Type() const override
    {
        return RequestType::Scan;
    }
    /**
     * @brief Set the scan range.
     * @param tbl_id Table partition identifier.
     * @param begin The begin key of the scan range.
     * @param end The end key of the scan range (not inclusive).
     * @param begin_inclusive Whether the begin key is inclusive.
     */
    void SetArgs(TableIdent tbl_id,
                 std::string_view begin,
                 std::string_view end,
                 bool begin_inclusive = true);

    /**
     * @brief Set the pagination of the scan result.
     * @param entries Limit the number of entries in one page.
     * @param size Limit the page size (byte).
     */
    void SetPagination(size_t entries, size_t size);

    size_t ResultSize() const;

    // input
    bool begin_inclusive_;
    std::string_view begin_key_;
    std::string_view end_key_;
    size_t page_entries_{SIZE_MAX};
    size_t page_size_{SIZE_MAX};
    // output
    std::vector<KvEntry> entries_;
    bool has_remaining_;
};

class WriteRequest : public KvRequest
{
public:
    RequestType Type() const override
    {
        return RequestType::Write;
    }
    void SetArgs(TableIdent tid, std::vector<WriteDataEntry> &&batch);
    void AddWrite(std::string key, std::string value, uint64_t ts, WriteOp op);

    // input
    std::vector<WriteDataEntry> batch_;
};

class TruncateRequest : public KvRequest
{
public:
    RequestType Type() const override
    {
        return RequestType::Truncate;
    }
    void SetArgs(TableIdent tid, std::string_view position);

    // input
    std::string_view position_;
};

class ArchiveRequest : public KvRequest
{
public:
    RequestType Type() const override
    {
        return RequestType::Archive;
    }
    void SetArgs(TableIdent tid);
};

class FileGarbageCollector;
class ArchiveCrond;

class EloqStore
{
public:
    EloqStore(const KvOptions &opts);
    EloqStore(const EloqStore &) = delete;
    EloqStore(EloqStore &&) = delete;
    ~EloqStore();
    KvError Start();
    void Stop();
    bool IsStopped() const;
    const KvOptions &Options() const;

    template <typename F>
    bool ExecAsyn(KvRequest *req, uint64_t data, F callback)
    {
        req->user_data_ = data;
        req->callback_ = std::move(callback);
        return SendRequest(req);
    }
    bool ExecAsyn(KvRequest *req);
    void ExecSync(KvRequest *req);

private:
    bool SendRequest(KvRequest *req);
    KvError InitDBDir();

    KvOptions options_;
    int dir_fd_{-1};
    std::vector<std::unique_ptr<Shard>> shards_;
    std::atomic<bool> stopped_{true};

    std::unique_ptr<FileGarbageCollector> file_gc_{nullptr};
    std::unique_ptr<ArchiveCrond> archive_crond_{nullptr};
    friend Shard;
};
}  // namespace kvstore