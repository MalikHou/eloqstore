#pragma once

#include <boost/context/pooled_fixedsize_stack.hpp>

#include "eloq_store.h"
#include "task_manager.h"

// https://github.com/cameron314/concurrentqueue/issues/280
#undef BLOCK_SIZE
#include "concurrentqueue/concurrentqueue.h"

namespace kvstore
{
class Shard
{
public:
    Shard(const EloqStore *store, uint32_t fd_limit);
    KvError Init();
    void Start();
    void Stop();
    bool AddKvRequest(KvRequest *req);

    void AddPendingCompact(const TableIdent &tbl_id);
    bool HasPendingCompact(const TableIdent &tbl_id);
    void AddPendingTTL(const TableIdent &tbl_id);
    bool HasPendingTTL(const TableIdent &tbl_id);

    const KvOptions *Options() const;
    AsyncIoManager *IoManager();
    IndexPageManager *IndexManager();
    TaskManager *TaskMgr();
    PagesPool *PagePool();

    const EloqStore *store_;
    boost::context::continuation main_;
    KvTask *running_;
    CircularQueue<KvTask *> scheduled_;
    CircularQueue<KvTask *> finished_;

private:
    void WorkLoop();
    void ResumeScheduled();
    void PollFinished();

    void OnReceivedReq(KvRequest *req);
    void ProcessReq(KvRequest *req);
    void OnWriteFinished(const TableIdent &tbl_id);

    template <typename F>
    void StartTask(KvTask *task, KvRequest *req, F lbd)
    {
        task->req_ = req;
        task->status_ = TaskStatus::Ongoing;
        running_ = task;
        task->coro_ = boost::context::callcc(std::allocator_arg,
                                             stack_pool_,
                                             [lbd](continuation &&sink)
                                             {
                                                 shard->main_ = std::move(sink);
                                                 KvError err = lbd();
                                                 KvTask *task = ThdTask();
                                                 if (err != KvError::NoError)
                                                 {
                                                     task->Abort();
                                                 }
                                                 if (task->req_ != nullptr)
                                                 {
                                                     task->req_->SetDone(err);
                                                     task->req_ = nullptr;
                                                 }
                                                 task->status_ =
                                                     TaskStatus::Idle;
                                                 shard->finished_.Enqueue(task);
                                                 return std::move(shard->main_);
                                             });
    }

    moodycamel::ConcurrentQueue<KvRequest *> requests_;
    std::thread thd_;
    PagesPool page_pool_;
    std::unique_ptr<AsyncIoManager> io_mgr_;
    IndexPageManager index_mgr_;
    TaskManager task_mgr_;
    boost::context::pooled_fixedsize_stack stack_pool_;

    class PendingWriteQueue
    {
    public:
        void PushBack(WriteRequest *req);
        WriteRequest *PopFront();
        bool Empty() const;

        // Requests from internal
        CompactRequest compact_req_;
        CleanExpiredRequest expire_req_;

    private:
        WriteRequest *head_{nullptr};
        WriteRequest *tail_{nullptr};
    };
    std::unordered_map<TableIdent, PendingWriteQueue> pending_queues_;
};
}  // namespace kvstore