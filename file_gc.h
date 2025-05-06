#pragma once

#include <filesystem>
#include <thread>
#include <vector>

#include "error.h"
#include "kv_options.h"
#include "types.h"

// https://github.com/cameron314/concurrentqueue/issues/280
#undef BLOCK_SIZE
#include "concurrentqueue/blockingconcurrentqueue.h"

namespace kvstore
{
class MappingSnapshot;

class FileGarbageCollector
{
public:
    FileGarbageCollector(const KvOptions *opts) : options_(opts) {};
    ~FileGarbageCollector();
    void Start(uint16_t n_workers);
    void Stop();
    bool AddTask(std::filesystem::path partition_path,
                 std::shared_ptr<MappingSnapshot> mapping,
                 uint64_t ts,
                 FileId max_file_id);

    static KvError Execute(const KvOptions *opts,
                           const std::filesystem::path &dir_path,
                           const MappingSnapshot *mapping,
                           uint64_t mapping_ts,
                           FileId max_file_id);

private:
    void GCRoutine();

    struct GcTask
    {
        GcTask() = default;
        GcTask(std::filesystem::path path,
               std::shared_ptr<MappingSnapshot> mapping,
               uint64_t ts,
               FileId max_file_id);
        bool IsStopSignal() const;
        std::filesystem::path partition_path_;
        std::shared_ptr<MappingSnapshot> mapping_{nullptr};
        uint64_t mapping_ts_{0};
        FileId max_file_id_{0};
    };

    const KvOptions *options_;
    moodycamel::BlockingConcurrentQueue<GcTask> tasks_;
    std::vector<std::thread> workers_;
};

inline FileGarbageCollector *file_garbage_collector;

}  // namespace kvstore