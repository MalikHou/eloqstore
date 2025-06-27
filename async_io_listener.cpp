#include "async_io_listener.h"

#include "eloqstore_module.h"
#include "shard.h"

namespace kvstore
{
#ifdef ELOQ_MODULE_ENABLED
AsyncIOListener::AsyncIOListener(const Shard *shard)
    : shard_(shard), poll_status_(PollStatus::Sleep)
{
    pool_thd_ = std::thread([&]() { Run(); });
}

AsyncIOListener::~AsyncIOListener()
{
    poll_status_.store(PollStatus::Terminated, std::memory_order_release);
    {
        std::unique_lock<std::mutex> lk(mux_);
        cv_.notify_one();
    }

    if (pool_thd_.joinable())
    {
        pool_thd_.join();
    }
}

void AsyncIOListener::Run()
{
    while (poll_status_.load(std::memory_order_relaxed) !=
           PollStatus::Terminated)
    {
        std::unique_lock<std::mutex> lk(mux_);
        cv_.wait(lk,
                 [this]()
                 {
                     return (!shard_->ext_proc_running_.load(
                                 std::memory_order_acquire) &&
                             shard_->task_mgr_.NumActive() > 0) ||
                            poll_status_.load(std::memory_order_relaxed) ==
                                PollStatus::Terminated;
                 });
        lk.unlock();

        if (!shard_->ext_proc_running_.load(std::memory_order_acquire) &&
            shard_->task_mgr_.NumActive() > 0)
        {
            PollStatus status = PollStatus::Sleep;
            bool success = poll_status_.compare_exchange_strong(
                status, PollStatus::Listening, std::memory_order_acq_rel);
            if (success)
            {
                PollAndNotify();
            }
        }
    }
}

void AsyncIOListener::StartListening()
{
    if (poll_status_.load(std::memory_order_relaxed) == PollStatus::Sleep)
    {
        std::unique_lock<std::mutex> lk(mux_);
        cv_.notify_one();
    }
}

void AsyncIOListener::PollAndNotify()
{
    int ret = shard_->io_mgr_->WaitCompletedIO();
    if (ret != 0)
    {
        LOG(ERROR) << "Listener wait completed io errno: " << ret;
        return;
    }

    shard_->io_mgr_->CheckAndSetIOStatus(true);
    poll_status_.store(PollStatus::Sleep, std::memory_order_relaxed);
    eloq::EloqModule::NotifyWorker(shard_->shard_id_);
}
#endif
}  // namespace kvstore
