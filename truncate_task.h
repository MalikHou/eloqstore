#pragma once

#include "write_task.h"

namespace kvstore
{
class TruncateTask : public WriteTask
{
public:
    TaskType Type() const override
    {
        return TaskType::Truncate;
    }
    KvError Truncate(std::string_view trunc_pos);

private:
    /**
     * @brief Truncate the data page at page_id from the trunc_pos.
     * @return true if the page is empty after truncation.
     */
    std::pair<bool, KvError> TruncateDataPage(PageId page_id,
                                              std::string_view trunc_pos);
    std::pair<MemIndexPage *, KvError> TruncateIndexPage(
        PageId page_id, std::string_view trunc_pos);
};
}  // namespace kvstore