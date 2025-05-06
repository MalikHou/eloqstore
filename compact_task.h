#pragma once

#include "write_task.h"

namespace kvstore
{
class CompactTask : public WriteTask
{
public:
    TaskType Type() const override
    {
        return TaskType::Compact;
    }
    /**
     * @brief Compact data files with a low utilization rate. Copy all pages
     * referenced by the latest mapping and append them to the latest data file.
     */
    KvError CompactDataFile();
};
}  // namespace kvstore