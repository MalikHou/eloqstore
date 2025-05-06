#pragma once

#include "write_task.h"

namespace kvstore
{
class ArchiveTask : public WriteTask
{
public:
    TaskType Type() const override
    {
        return TaskType::Archive;
    }
    KvError CreateArchive();
};
}  // namespace kvstore