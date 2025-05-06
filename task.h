#pragma once

#include <boost/context/continuation.hpp>
#include <cassert>
#include <cstdint>

#include "circular_queue.h"
#include "comparator.h"
#include "data_page.h"
#include "error.h"
#include "types.h"

namespace kvstore
{
class KvRequest;
class KvTask;
class IndexPageManager;
class AsyncIoManager;
class KvOptions;
class TaskManager;
class PagesPool;
class MappingSnapshot;
class Shard;

inline thread_local Shard *shard;
inline thread_local KvTask *thd_task;

AsyncIoManager *IoMgr();
const KvOptions *Options();
const Comparator *Comp();

enum class TaskStatus : uint8_t
{
    Idle = 0,
    Ongoing,
    Blocked,
    WaitSyncIo,
    WaitAllAsynIo
};

enum struct TaskType
{
    Read = 0,
    Scan,
    BatchWrite,
    Truncate,
    Compact,
    Archive
};

using boost::context::continuation;

class KvTask
{
public:
    virtual ~KvTask() = default;
    virtual TaskType Type() const = 0;
    void Yield();
    /**
     * @brief Re-schedules the task to run. Note: the resumed task does not run
     * in place.
     *
     */
    void Resume();

    int WaitSyncIo();
    void WaitAsynIo();
    void FinishIo(bool is_sync_io);

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
    std::pair<std::string, KvError> GetOverflowValue(
        const TableIdent &tbl_id,
        const MappingSnapshot *mapping,
        std::string_view encoded_ptrs);
    /**
     * @brief Decode overflow pointers.
     * @param encoded The encoded overflow pointers.
     * @param pointers The buffer to store the decoded overflow pointers.
     * @return The number of decoded overflow pointers.
     */
    static uint8_t DecodeOverflowPointers(
        std::string_view encoded,
        std::span<PageId, max_overflow_pointers> pointers);
    static uint8_t DecodeOverflowPointers(
        std::string_view encoded,
        std::span<FilePageId, max_overflow_pointers> pointers,
        const MappingSnapshot *mapping);

    TaskStatus status_{TaskStatus::Idle};

    uint32_t inflight_io_{0};
    int io_res_{0};
    uint32_t io_flags_{0};

    KvRequest *req_{nullptr};
    boost::context::continuation coro_;
};

class WaitingZone
{
public:
    WaitingZone(size_t capacity = 8) : tasks_(capacity) {};
    void Sleep(KvTask *task);
    void WakeOne();
    void WakeN(size_t n);
    void WakeAll();

private:
    CircularQueue<KvTask *> tasks_;
};
}  // namespace kvstore