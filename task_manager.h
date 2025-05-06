
#pragma once

#include <memory>
#include <vector>

#include "archive_task.h"
#include "batch_write_task.h"
#include "compact_task.h"
#include "read_task.h"
#include "scan_task.h"
#include "truncate_task.h"
#include "types.h"

namespace kvstore
{
class TaskManager
{
public:
    BatchWriteTask *GetBatchWriteTask(const TableIdent &tbl_id);
    TruncateTask *GetTruncateTask(const TableIdent &tbl_id);
    CompactTask *GetCompactTask(const TableIdent &tbl_id);
    ArchiveTask *GetArchiveTask(const TableIdent &tbl_id);
    ReadTask *GetReadTask();
    ScanTask *GetScanTask();
    void FreeTask(KvTask *task);

    uint32_t NumActive() const;
    bool IsIdle() const;

private:
    template <typename T>
    class TaskPool
    {
    public:
        T *GetTask()
        {
            if (free_tasks_.empty())
            {
                auto task = std::make_unique<T>();
                free_tasks_.push_back(task.get());
                tasks_pool_.emplace_back(std::move(task));
            }
            T *task = free_tasks_.back();
            free_tasks_.pop_back();
            return task;
        }

        void FreeTask(T *task)
        {
            free_tasks_.push_back(task);
        }

        uint32_t NumActive() const
        {
            return tasks_pool_.size() - free_tasks_.size();
        }
        bool IsIdle() const
        {
            return tasks_pool_.size() == free_tasks_.size();
        }

    private:
        std::vector<std::unique_ptr<T>> tasks_pool_;
        std::vector<T *> free_tasks_;
    };

    TaskPool<BatchWriteTask> batch_write_pool_;
    TaskPool<TruncateTask> truncate_pool_;
    TaskPool<CompactTask> compact_pool_;
    TaskPool<ArchiveTask> archive_pool_;
    TaskPool<ReadTask> read_pool_;
    TaskPool<ScanTask> scan_pool_;
};
}  // namespace kvstore