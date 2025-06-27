#include "task_manager.h"

#include <boost/context/continuation_fcontext.hpp>
#include <cassert>

#include "read_task.h"
#include "task.h"

using namespace boost::context;

namespace kvstore
{
BatchWriteTask *TaskManager::GetBatchWriteTask(const TableIdent &tbl_id)
{
    num_active_++;
    BatchWriteTask *task = batch_write_pool_.GetTask();
    task->Reset(tbl_id);
    return task;
}

CompactTask *TaskManager::GetCompactTask(const TableIdent &tbl_id)
{
    num_active_++;
    CompactTask *task = compact_pool_.GetTask();
    task->Reset(tbl_id);
    return task;
}

ArchiveTask *TaskManager::GetArchiveTask(const TableIdent &tbl_id)
{
    num_active_++;
    ArchiveTask *task = archive_pool_.GetTask();
    task->Reset(tbl_id);
    return task;
}

ReadTask *TaskManager::GetReadTask()
{
    num_active_++;
    return read_pool_.GetTask();
}

ScanTask *TaskManager::GetScanTask()
{
    num_active_++;
    return scan_pool_.GetTask();
}

void TaskManager::FreeTask(KvTask *task)
{
    num_active_--;
    switch (task->Type())
    {
    case TaskType::Read:
        read_pool_.FreeTask(static_cast<ReadTask *>(task));
        break;
    case TaskType::Scan:
        scan_pool_.FreeTask(static_cast<ScanTask *>(task));
        break;
    case TaskType::BatchWrite:
        batch_write_pool_.FreeTask(static_cast<BatchWriteTask *>(task));
        break;
    case TaskType::Compact:
        compact_pool_.FreeTask(static_cast<CompactTask *>(task));
        break;
    case TaskType::Archive:
        archive_pool_.FreeTask(static_cast<ArchiveTask *>(task));
        break;
    case TaskType::EvictFile:
        assert(false && "EvictFile task should not be freed here");
        break;
    }
}

size_t TaskManager::NumActive() const
{
    return num_active_;
}

}  // namespace kvstore