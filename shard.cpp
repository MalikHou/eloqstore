#include "shard.h"

namespace kvstore
{
Shard::Shard(const EloqStore *store, uint32_t fd_limit)
    : store_(store),
      page_pool_(&store->options_),
      io_mgr_(AsyncIoManager::Instance(store, fd_limit)),
      index_mgr_(io_mgr_.get()),
      stack_pool_(store->options_.coroutine_stack_size)
{
}

KvError Shard::Init()
{
    return io_mgr_->Init(this);
}

void Shard::WorkLoop()
{
    while (true)
    {
        KvRequest *reqs[128];
        size_t nreqs = requests_.try_dequeue_bulk(reqs, std::size(reqs));
        for (size_t i = 0; i < nreqs; i++)
        {
            OnReceivedReq(reqs[i]);
        }

        if (nreqs == 0 && task_mgr_.NumActive() == 0)
        {
            if (store_->IsStopped())
            {
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            continue;
        }

        io_mgr_->Submit();
        io_mgr_->PollComplete();

        ResumeScheduled();
        PollFinished();
    }
}

void Shard::Start()
{
    thd_ = std::thread(
        [this]
        {
            shard = this;
            io_mgr_->Start();
            WorkLoop();
        });
}

void Shard::Stop()
{
    thd_.join();
}

bool Shard::AddKvRequest(KvRequest *req)
{
    return requests_.enqueue(req);
}

void Shard::AddPendingCompact(const TableIdent &tbl_id)
{
    // Send CompactRequest from internal.
    assert(!HasPendingCompact(tbl_id));
    auto it = pending_queues_.find(tbl_id);
    assert(it != pending_queues_.end());
    PendingWriteQueue &pending_q = it->second;
    CompactRequest &req = pending_q.compact_req_;
    req.SetTableId(tbl_id);
    req.done_.store(false, std::memory_order_relaxed);
    pending_q.PushBack(&req);
}

bool Shard::HasPendingCompact(const TableIdent &tbl_id)
{
    auto it = pending_queues_.find(tbl_id);
    assert(it != pending_queues_.end());
    PendingWriteQueue &pending_q = it->second;
    return !pending_q.compact_req_.done_.load(std::memory_order_relaxed);
}

void Shard::AddPendingTTL(const TableIdent &tbl_id)
{
    // Send CleanExpiredRequest from internal.
    assert(!HasPendingTTL(tbl_id));
    auto it = pending_queues_.find(tbl_id);
    assert(it != pending_queues_.end());
    PendingWriteQueue &pending_q = it->second;
    CleanExpiredRequest &req = pending_q.expire_req_;
    req.SetTableId(tbl_id);
    req.done_.store(false, std::memory_order_relaxed);
    pending_q.PushBack(&req);
}

bool Shard::HasPendingTTL(const TableIdent &tbl_id)
{
    auto it = pending_queues_.find(tbl_id);
    assert(it != pending_queues_.end());
    PendingWriteQueue &pending_q = it->second;
    return !pending_q.expire_req_.done_.load(std::memory_order_relaxed);
}

IndexPageManager *Shard::IndexManager()
{
    return &index_mgr_;
}

AsyncIoManager *Shard::IoManager()
{
    return io_mgr_.get();
}

TaskManager *Shard::TaskMgr()
{
    return &task_mgr_;
}

PagesPool *Shard::PagePool()
{
    return &page_pool_;
}

const KvOptions *Shard::Options() const
{
    return &store_->Options();
}

void Shard::OnReceivedReq(KvRequest *req)
{
    if (auto wreq = dynamic_cast<WriteRequest *>(req); wreq != nullptr)
    {
        // Try acquire lock to ensure write operation is executed
        // sequentially on each table partition.
        auto [it, ok] = pending_queues_.try_emplace(req->tbl_id_);
        if (!ok)
        {
            // Wait on pending write queue because of other write task.
            it->second.PushBack(wreq);
            return;
        }
    }

    ProcessReq(req);
}

void Shard::ProcessReq(KvRequest *req)
{
    switch (req->Type())
    {
    case RequestType::Read:
    {
        ReadTask *task = task_mgr_.GetReadTask();
        auto lbd = [task, req]() -> KvError
        {
            auto read_req = static_cast<ReadRequest *>(req);
            KvError err = task->Read(req->TableId(),
                                     read_req->key_,
                                     read_req->value_,
                                     read_req->ts_,
                                     read_req->expire_ts_);
            return err;
        };
        StartTask(task, req, lbd);
        break;
    }
    case RequestType::Floor:
    {
        ReadTask *task = task_mgr_.GetReadTask();
        auto lbd = [task, req]() -> KvError
        {
            auto floor_req = static_cast<FloorRequest *>(req);
            KvError err = task->Floor(req->TableId(),
                                      floor_req->key_,
                                      floor_req->floor_key_,
                                      floor_req->value_,
                                      floor_req->ts_,
                                      floor_req->expire_ts_);
            return err;
        };
        StartTask(task, req, lbd);
        break;
    }
    case RequestType::Scan:
    {
        ScanTask *task = task_mgr_.GetScanTask();
        auto lbd = [task, req]() -> KvError
        {
            auto scan_req = static_cast<ScanRequest *>(req);
            return task->Scan(req->TableId(),
                              scan_req->begin_key_,
                              scan_req->end_key_,
                              scan_req->begin_inclusive_,
                              scan_req->page_entries_,
                              scan_req->page_size_,
                              scan_req->entries_,
                              scan_req->has_remaining_);
        };
        StartTask(task, req, lbd);
        break;
    }
    case RequestType::BatchWrite:
    {
        BatchWriteTask *task = task_mgr_.GetBatchWriteTask(req->TableId());
        auto lbd = [task, req]() -> KvError
        {
            auto write_req = static_cast<BatchWriteRequest *>(req);
            if (write_req->batch_.empty())
            {
                return KvError::NoError;
            }
            if (!task->SetBatch(write_req->batch_))
            {
                return KvError::InvalidArgs;
            }
            return task->Apply();
        };
        StartTask(task, req, lbd);
        break;
    }
    case RequestType::Truncate:
    {
        BatchWriteTask *task = task_mgr_.GetBatchWriteTask(req->TableId());
        auto lbd = [task, req]() -> KvError
        {
            auto trunc_req = static_cast<TruncateRequest *>(req);
            return task->Truncate(trunc_req->position_);
        };
        StartTask(task, req, lbd);
        break;
    }
    case RequestType::Archive:
    {
        ArchiveTask *task = task_mgr_.GetArchiveTask(req->TableId());
        auto lbd = [task]() -> KvError { return task->CreateArchive(); };
        StartTask(task, req, lbd);
        break;
    }
    case RequestType::Compact:
    {
        CompactTask *task = task_mgr_.GetCompactTask(req->TableId());
        auto lbd = [task]() -> KvError { return task->CompactDataFile(); };
        StartTask(task, req, lbd);
        break;
    }
    case RequestType::CleanExpired:
    {
        BatchWriteTask *task = task_mgr_.GetBatchWriteTask(req->TableId());
        auto lbd = [task]() -> KvError { return task->CleanExpiredKeys(); };
        StartTask(task, req, lbd);
        break;
    }
    }
}

void Shard::ResumeScheduled()
{
    while (scheduled_.Size() > 0)
    {
        running_ = scheduled_.Peek();
        scheduled_.Dequeue();
        running_->coro_ = running_->coro_.resume();
    }
    running_ = nullptr;
}

void Shard::PollFinished()
{
    while (finished_.Size() > 0)
    {
        KvTask *task = finished_.Peek();
        finished_.Dequeue();

        if (auto *wtask = dynamic_cast<WriteTask *>(task); wtask != nullptr)
        {
            OnWriteFinished(wtask->TableId());
        }

        // Note: You can recycle the stack of this coroutine here if needed.
        task_mgr_.FreeTask(task);
    }
}

void Shard::OnWriteFinished(const TableIdent &tbl_id)
{
    auto it = pending_queues_.find(tbl_id);
    assert(it != pending_queues_.end());
    PendingWriteQueue &pending_q = it->second;
    if (pending_q.Empty())
    {
        // No more write requests, remove the pending queue.
        pending_queues_.erase(it);
        return;
    }

    WriteRequest *req = pending_q.PopFront();
    // Continue execute the next pending write request.
    ProcessReq(req);
}

void Shard::PendingWriteQueue::PushBack(WriteRequest *req)
{
    if (tail_ == nullptr)
    {
        assert(head_ == nullptr);
        head_ = tail_ = req;
    }
    else
    {
        assert(head_ != nullptr);
        req->next_ = nullptr;
        tail_->next_ = req;
        tail_ = req;
    }
}

WriteRequest *Shard::PendingWriteQueue::PopFront()
{
    WriteRequest *req = head_;
    if (req != nullptr)
    {
        head_ = req->next_;
        if (head_ == nullptr)
        {
            tail_ = nullptr;
        }
        req->next_ = nullptr;  // Clear next pointer for safety.
    }
    return req;
}

bool Shard::PendingWriteQueue::Empty() const
{
    return head_ == nullptr;
}

}  // namespace kvstore