#include "shard.h"

namespace kvstore
{
Shard::Shard(const EloqStore *store)
    : store_(store),
      page_pool_(store->options_.data_page_size),
      io_mgr_(AsyncIoManager::New(&store->options_)),
      index_mgr_(io_mgr_.get()),
      stack_pool_(store->options_.coroutine_stack_size)
{
}

Shard::~Shard()
{
    if (thd_.joinable())
    {
        thd_.join();
    }
}

KvError Shard::Init(int dir_fd)
{
    return io_mgr_->Init(dir_fd);
}

void Shard::Loop()
{
    while (true)
    {
        KvRequest *reqs[128];
        size_t nreqs = requests_.try_dequeue_bulk(reqs, std::size(reqs));
        for (size_t i = 0; i < nreqs; i++)
        {
            OnReceivedReq(reqs[i]);
        }

        if (nreqs == 0 && task_mgr_.IsIdle())
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
            Loop();
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

void Shard::AddCompactRequest(const TableIdent &tbl_id)
{
    write_queue_[tbl_id].Enqueue(uint64_t(QueueElement::Compact));
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
    if (req->Type() == RequestType::Write ||
        req->Type() == RequestType::Truncate ||
        req->Type() == RequestType::Archive)
    {
        // Try acquire lock to ensure write operation is executed
        // sequentially on each table partition.
        auto [it, ok] = write_queue_.try_emplace(req->tbl_id_);
        if (!ok)
        {
            // Blocked on queue because of other write task.
            it->second.Enqueue(uint64_t(req));
            return;
        }
    }

    HandleReq(req);
}

void Shard::HandleReq(KvRequest *req)
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
                                     read_req->ts_);
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
                                      floor_req->ts_);
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
    case RequestType::Write:
    {
        BatchWriteTask *task = task_mgr_.GetBatchWriteTask(req->TableId());
        auto lbd = [task, req]() -> KvError
        {
            auto write_req = static_cast<WriteRequest *>(req);
            if (write_req->batch_.empty())
            {
                return KvError::NoError;
            }
            if (!task->SetBatch(std::move(write_req->batch_)))
            {
                return KvError::InvalidArgs;
            }
            KvError err = task->Apply();
            if (err != KvError::NoError)
            {
                task->Abort();
            }
            return err;
        };
        StartTask(task, req, lbd);
        break;
    }
    case RequestType::Truncate:
    {
        TruncateTask *task = task_mgr_.GetTruncateTask(req->TableId());
        auto lbd = [task, req]() -> KvError
        {
            auto trunc_req = static_cast<TruncateRequest *>(req);
            KvError err = task->Truncate(trunc_req->position_);
            if (err != KvError::NoError)
            {
                task->Abort();
            }
            return err;
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
    }
}

void Shard::StartCompact(const TableIdent &tbl_id)
{
    CompactTask *task = task_mgr_.GetCompactTask(tbl_id);
    auto lbd = [task]() -> KvError
    {
        KvError err = task->CompactDataFile();
        if (err != KvError::NoError)
        {
            task->Abort();
        }
        return err;
    };
    StartTask(task, nullptr, lbd);
}

void Shard::ResumeScheduled()
{
    while (scheduled_.Size() > 0)
    {
        thd_task = scheduled_.Peek();
        scheduled_.Dequeue();
        thd_task->coro_ = thd_task->coro_.resume();
    }
}

void Shard::PollFinished()
{
    while (finished_.Size() > 0)
    {
        KvTask *task = finished_.Peek();
        finished_.Dequeue();

        if (WriteTask *wtask = dynamic_cast<WriteTask *>(task);
            wtask != nullptr)
        {
            OnWriteFinished(wtask->TableId());
        }

        // Note: You can recycle the stack of this coroutine here if needed.
        task_mgr_.FreeTask(task);
    }
}

void Shard::OnWriteFinished(const TableIdent &tbl_id)
{
    auto it = write_queue_.find(tbl_id);
    assert(it != write_queue_.end());
    if (it->second.Size() == 0)
    {
        // Clear lock queue.
        write_queue_.erase(it);
        return;
    }
    uint64_t val = it->second.Peek();
    it->second.Dequeue();
    switch (val & uint64_t(QueueElement::Mask))
    {
    case uint64_t(QueueElement::KvRequest):
    {
        // Continue execute the next write request.
        KvRequest *req = reinterpret_cast<KvRequest *>(val);
        HandleReq(req);
        break;
    }
    case uint64_t(QueueElement::Compact):
    {
        StartCompact(tbl_id);
        break;
    }
    default:
        LOG(FATAL) << "Invalid write queue element type";
    }
}

}  // namespace kvstore