#include "scan_task.h"

#include <cassert>
#include <cstdint>
#include <memory>

#include "error.h"
#include "page_mapper.h"
#include "shard.h"

namespace kvstore
{
ScanIterator::ScanIterator(const TableIdent &tbl_id)
    : tbl_id_(tbl_id), iter_(nullptr, Options())
{
}

KvError ScanIterator::Seek(std::string_view key, bool ttl)
{
    auto [meta, err] = shard->IndexManager()->FindRoot(tbl_id_);
    CHECK_KV_ERR(err);
    PageId root_id = ttl ? meta->ttl_root_id_ : meta->root_id_;
    if (root_id == MaxPageId)
    {
        return KvError::EndOfFile;
    }
    mapping_ = meta->mapper_->GetMappingSnapshot();

    PageId page_id;
    err =
        shard->IndexManager()->SeekIndex(mapping_.get(), root_id, key, page_id);
    CHECK_KV_ERR(err);
    assert(page_id != MaxPageId);
    FilePageId file_page = mapping_->ToFilePage(page_id);
    auto [page, err_load] = LoadDataPage(tbl_id_, page_id, file_page);
    CHECK_KV_ERR(err_load);

    data_page_ = std::move(page);
    iter_.Reset(&data_page_, Options()->data_page_size);

    if (!iter_.Seek(key))
    {
        err = Next();
        CHECK_KV_ERR(err);
    }
    return KvError::NoError;
}

KvError ScanIterator::Next()
{
    if (!iter_.HasNext())
    {
        PageId page_id = data_page_.NextPageId();
        if (page_id == MaxPageId)
        {
            return KvError::EndOfFile;
        }
        FilePageId file_page = mapping_->ToFilePage(page_id);
        assert(file_page != MaxFilePageId);
        auto [page, err] = LoadDataPage(tbl_id_, page_id, file_page);
        CHECK_KV_ERR(err);

        data_page_ = std::move(page);
        iter_.Reset(&data_page_, Options()->data_page_size);
        assert(iter_.HasNext());
    }
    iter_.Next();
    return KvError::NoError;
}

std::string_view ScanIterator::Key() const
{
    return iter_.Key();
}

std::string_view ScanIterator::Value() const
{
    return iter_.Value();
}

bool ScanIterator::IsOverflow() const
{
    return iter_.IsOverflow();
}

uint64_t ScanIterator::ExpireTs() const
{
    return iter_.ExpireTs();
}

uint64_t ScanIterator::Timestamp() const
{
    return iter_.Timestamp();
}

bool ScanIterator::HasNext() const
{
    return iter_.HasNext() || data_page_.NextPageId() != MaxPageId;
}

MappingSnapshot *ScanIterator::Mapping() const
{
    return mapping_.get();
}

KvError ScanTask::Scan(const TableIdent &tbl_id,
                       std::string_view begin_key,
                       std::string_view end_key,
                       bool begin_inclusive,
                       size_t page_entries,
                       size_t page_size,
                       std::vector<KvEntry> &result,
                       bool &has_remaining)
{
    assert(page_entries > 0 && page_size > 0);
    result.clear();
    has_remaining = false;
    size_t result_size = 0;

    ScanIterator iter(tbl_id);
    KvError err = iter.Seek(begin_key);
    if (err != KvError::NoError)
    {
        return err == KvError::EndOfFile ? KvError::NoError : err;
    }

    if (!begin_inclusive && Comp()->Compare(iter.Key(), begin_key) == 0)
    {
        err = iter.Next();
        if (err != KvError::NoError)
        {
            return err == KvError::EndOfFile ? KvError::NoError : err;
        }
    }

    while (end_key.empty() || Comp()->Compare(iter.Key(), end_key) < 0)
    {
        // Check entries number limit.
        if (result.size() == page_entries)
        {
            has_remaining = true;
            break;
        }

        // Fetch value
        std::string value;
        if (iter.IsOverflow())
        {
            auto ret = GetOverflowValue(tbl_id, iter.Mapping(), iter.Value());
            err = ret.second;
            assert(err != KvError::EndOfFile);
            CHECK_KV_ERR(err);
            value = std::move(ret.first);
        }
        else
        {
            value = iter.Value();
        }

        // Check result size limit.
        size_t entry_size = iter.Key().size() + value.size() +
                            sizeof(iter.Timestamp()) + sizeof(iter.ExpireTs());
        if (result_size > 0 && result_size + entry_size > page_size)
        {
            has_remaining = true;
            break;
        }
        result_size += entry_size;

        std::string key(iter.Key());
        uint64_t ts = iter.Timestamp();
        uint64_t expire_ts = iter.ExpireTs();
        result.emplace_back(std::move(key), std::move(value), ts, expire_ts);

        err = iter.Next();
        if (err != KvError::NoError)
        {
            return err == KvError::EndOfFile ? KvError::NoError : err;
        }
    }
    return KvError::NoError;
}
}  // namespace kvstore