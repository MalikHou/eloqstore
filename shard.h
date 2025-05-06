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
    Shard(const EloqStore *store);
    ~Shard();
    KvError Init(int dir_fd);
    void Start();
    void Stop();
    bool AddKvRequest(KvRequest *req);
    void AddCompactRequest(const TableIdent &tbl_id);

    const KvOptions *Options() const;
    AsyncIoManager *IoManager();
    IndexPageManager *IndexManager();
    TaskManager *TaskMgr();
    PagesPool *PagePool();

    boost::context::continuation main_;
    CircularQueue<KvTask *> scheduled_;
    CircularQueue<KvTask *> finished_;

private:
    void Loop();
    void ResumeScheduled();
    void PollFinished();

    void OnReceivedReq(KvRequest *req);
    void HandleReq(KvRequest *req);
    void StartCompact(const TableIdent &tbl_id);
    void OnWriteFinished(const TableIdent &tbl_id);

    template <typename F>
    void StartTask(KvTask *task, KvRequest *req, F lbd)
    {
        task->req_ = req;
        task->status_ = TaskStatus::Ongoing;
        thd_task = task;
        task->coro_ = boost::context::callcc(std::allocator_arg,
                                             stack_pool_,
                                             [task, lbd](continuation &&sink)
                                             {
                                                 shard->main_ = std::move(sink);
                                                 KvError err = lbd();
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

    const EloqStore *store_;
    moodycamel::ConcurrentQueue<KvRequest *> requests_;
    std::thread thd_;
    PagesPool page_pool_;
    std::unique_ptr<AsyncIoManager> io_mgr_;
    IndexPageManager index_mgr_;
    TaskManager task_mgr_;
    boost::context::pooled_fixedsize_stack stack_pool_;

    enum class QueueElement : uint8_t
    {
        KvRequest = 0,
        Compact,
        Mask = 7
    };
    std::unordered_map<TableIdent, CircularQueue<uint64_t>> write_queue_;
};
}  // namespace kvstore