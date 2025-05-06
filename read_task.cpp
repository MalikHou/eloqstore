#include "read_task.h"

#include "error.h"
#include "mem_index_page.h"
#include "page_mapper.h"
#include "shard.h"

namespace kvstore
{

KvError ReadTask::Read(const TableIdent &tbl_id,
                       std::string_view search_key,
                       std::string &value,
                       uint64_t &timestamp)
{
    auto [meta, err] = shard->IndexManager()->FindRoot(tbl_id);
    CHECK_KV_ERR(err);
    if (meta->root_page_ == nullptr)
    {
        return KvError::NotFound;
    }
    auto mapping = meta->mapper_->GetMappingSnapshot();

    PageId page_id;
    err = shard->IndexManager()->SeekIndex(
        mapping.get(), tbl_id, meta->root_page_, search_key, page_id);
    CHECK_KV_ERR(err);
    FilePageId file_page = mapping->ToFilePage(page_id);
    auto [page, err_load] = LoadDataPage(tbl_id, page_id, file_page);
    CHECK_KV_ERR(err_load);

    DataPageIter iter{&page, Options()};
    iter.Seek(search_key);
    std::string_view seek_key = iter.Key();
    if (!seek_key.empty() && seek_key == search_key)
    {
        if (iter.IsOverflow())
        {
            auto ret = GetOverflowValue(tbl_id, mapping.get(), iter.Value());
            CHECK_KV_ERR(ret.second);
            value = std::move(ret.first);
        }
        else
        {
            value = iter.Value();
        }
        timestamp = iter.Timestamp();
        return KvError::NoError;
    }
    else
    {
        return KvError::NotFound;
    }
}

}  // namespace kvstore