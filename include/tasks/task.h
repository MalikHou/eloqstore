#pragma once

#include <array>
#include <boost/context/continuation.hpp>
#include <string>
#include <string_view>
#include <utility>

#include "comparator.h"
#include "compression.h"
#include "error.h"
#include "storage/data_page.h"
#include "types.h"

namespace eloqstore
{
class KvRequest;
class KvTask;
class IndexPageManager;
class AsyncIoManager;
struct KvOptions;
class TaskManager;
class PagesPool;
class MappingSnapshot;
class Shard;
class EloqStore;

inline EloqStore *eloq_store;
inline thread_local Shard *shard;
KvTask *ThdTask();
AsyncIoManager *IoMgr();
const KvOptions *Options();
const Comparator *Comp();

enum class TaskStatus : uint8_t
{
    Idle = 0,
    Ongoing,
    Blocked,
    BlockedIO,
    Finished
};

enum struct TaskType
{
    Read = 0,
    Scan,
    EvictFile,
    Prewarm,
    ListObject,
    BatchWrite,
    BackgroundWrite
};

std::pair<Page, KvError> LoadPage(const TableIdent &tbl_id,
                                  FilePageId file_page_id);
std::pair<DataPage, KvError> LoadDataPage(const TableIdent &tbl_id,
                                          PageId page_id,
                                          FilePageId file_page_id);
std::pair<OverflowPage, KvError> LoadOverflowPage(const TableIdent &tbl_id,
                                                  PageId page_id,
                                                  FilePageId file_page_id);

/**
 * @brief Get overflow value.
 * @param tbl_id The table partition identifier.
 * @param mapping The mapping snapshot of this table partition.
 * @param encoded_ptrs The encoded overflow pointers.
 */
std::pair<std::string, KvError> GetOverflowValue(const TableIdent &tbl_id,
                                                 const MappingSnapshot *mapping,
                                                 std::string_view encoded_ptrs);

std::pair<std::string_view, KvError> ResolveValue(
    const TableIdent &tbl_id,
    MappingSnapshot *mapping,
    DataPageIter &iter,
    std::string &storage,
    const compression::DictCompression *compression);
/**
 * @brief Decode overflow pointers.
 * @param encoded The encoded overflow pointers.
 * @param pointers The buffer to store the decoded overflow pointers.
 * @return The number of decoded overflow pointers.
 */
uint8_t DecodeOverflowPointers(
    std::string_view encoded,
    std::span<PageId, max_overflow_pointers> pointers);
uint8_t DecodeOverflowPointers(
    std::string_view encoded,
    std::span<FilePageId, max_overflow_pointers> pointers,
    const MappingSnapshot *mapping);

using boost::context::continuation;
class KvTask
{
public:
    static constexpr uint16_t kMaxTrackedIoUserData = 160;

    virtual ~KvTask() = default;
    virtual TaskType Type() const = 0;
    virtual void Abort() {};
    bool ReadOnly() const
    {
        return Type() < TaskType::BatchWrite;
    }
    void Yield();
    void YieldToLowPQ();
    /**
     * @brief Re-schedules the task to run. Note: the resumed task does not run
     * in place.
     *
     */
    void Resume();

    int WaitIoResult();
    void WaitIo();
    void FinishIo();
    void ResetIoTimeoutState();
    bool TrackIoUserData(uint64_t user_data);
    void UntrackIoUserData(uint64_t user_data);

    uint32_t inflight_io_{0};
    int io_res_{0};
    uint32_t io_flags_{0};
    bool io_timeout_enabled_{false};
    bool io_cancel_requested_{false};
    bool io_timeout_pending_cancel_{false};
    uint64_t io_timeout_deadline_ms_{0};
    uint32_t io_timeout_timer_idx_{0};
    uint32_t io_timeout_timer_gen_{0};
    uint64_t io_timeout_seq_{0};
    AsyncIoManager *io_timeout_owner_{nullptr};
    std::array<uint64_t, kMaxTrackedIoUserData> tracked_io_user_data_{};
    uint16_t tracked_io_user_data_count_{0};

    TaskStatus status_{TaskStatus::Idle};
    KvRequest *req_{nullptr};
    continuation coro_;
    KvTask *next_{nullptr};
};

class WaitingZone
{
public:
    WaitingZone() = default;
    void Wait(KvTask *task);
    void WakeOne();
    void WakeN(size_t n);
    void WakeAll();
    bool Empty() const;

private:
    void PushBack(KvTask *task);
    KvTask *PopFront();

    KvTask *head_{nullptr}, *tail_{nullptr};
};

class WaitingSeat
{
public:
    void Wait(KvTask *task);
    void Wake();

private:
    KvTask *task_{nullptr};
};

class Mutex
{
public:
    void Lock();
    void Unlock();

private:
    bool locked_{false};
    WaitingZone waiting_;
};
}  // namespace eloqstore
