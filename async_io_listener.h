#pragma once

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>

namespace kvstore
{
#ifdef ELOQ_MODULE_ENABLED
class Shard;

class AsyncIOListener
{
public:
    explicit AsyncIOListener(const Shard *shard);
    ~AsyncIOListener();

    void StartListening();

private:
    void Run();
    void PollAndNotify();

    enum struct PollStatus : uint8_t
    {
        Listening = 0,
        Sleep,
        Terminated
    };

    const Shard *const shard_{nullptr};
    std::atomic<PollStatus> poll_status_{PollStatus::Sleep};
    std::mutex mux_;
    std::condition_variable cv_;
    std::thread pool_thd_;
};
#endif
}  // namespace kvstore