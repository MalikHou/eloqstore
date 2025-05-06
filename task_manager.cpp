#include "task_manager.h"

#include <boost/context/continuation_fcontext.hpp>
#include <cassert>

#include "read_task.h"
#include "task.h"
#include "write_task.h"

using namespace boost::context;

namespace kvstore
{
BatchWriteTask *TaskManager::GetBatchWriteTask(const TableIdent &tbl_id)
{
    BatchWriteTask *task = batch_write_pool_.GetTask();
    task->Reset(tbl_id);
    return task;
}

TruncateTask *TaskManager::GetTruncateTask(const TableIdent &tbl_id)
{
    TruncateTask *task = truncate_pool_.GetTask();
    task->Reset(tbl_id);
    return task;
}

CompactTask *TaskManager::GetCompactTask(const TableIdent &tbl_id)
{
    CompactTask *task = compact_pool_.GetTask();
    task->Reset(tbl_id);
    return task;
}

ArchiveTask *TaskManager::GetArchiveTask(const TableIdent &tbl_id)
{
    ArchiveTask *task = archive_pool_.GetTask();
    task->Reset(tbl_id);
    return task;
}

ReadTask *TaskManager::GetReadTask()
{
    return read_pool_.GetTask();
}

ScanTask *TaskManager::GetScanTask()
{
    return scan_pool_.GetTask();
}

void TaskManager::FreeTask(KvTask *task)
{
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
    case TaskType::Truncate:
        truncate_pool_.FreeTask(static_cast<TruncateTask *>(task));
        break;
    case TaskType::Compact:
        compact_pool_.FreeTask(static_cast<CompactTask *>(task));
        break;
    case TaskType::Archive:
        archive_pool_.FreeTask(static_cast<ArchiveTask *>(task));
        break;
    }
}

uint32_t TaskManager::NumActive() const
{
    return read_pool_.NumActive() + scan_pool_.NumActive() +
           batch_write_pool_.NumActive() + truncate_pool_.NumActive() +
           compact_pool_.NumActive() + archive_pool_.NumActive();
}

bool TaskManager::IsIdle() const
{
    return read_pool_.IsIdle() && scan_pool_.IsIdle() &&
           batch_write_pool_.IsIdle() && truncate_pool_.IsIdle() &&
           compact_pool_.IsIdle() && archive_pool_.IsIdle();
}

}  // namespace kvstore