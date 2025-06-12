#include "write_task.h"

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>
#include <utility>

#include "async_io_manager.h"
#include "data_page.h"
#include "error.h"
#include "file_gc.h"
#include "index_page_manager.h"
#include "mem_index_page.h"
#include "page_mapper.h"
#include "shard.h"
#include "types.h"
#include "utils.h"

namespace kvstore
{
WriteTask::WriteTask()
    : idx_page_builder_(Options()), data_page_builder_(Options())
{
    overflow_ptrs_.reserve(Options()->overflow_pointers * 4);

    if (Options()->data_append_mode)
    {
        batch_pages_.reserve(Options()->max_write_batch_pages);
    }
}

const TableIdent &WriteTask::TableId() const
{
    return tbl_ident_;
}

void WriteTask::Reset(const TableIdent &tbl_id)
{
    tbl_ident_ = tbl_id;
    stack_.clear();
    write_err_ = KvError::NoError;
    wal_builder_.Reset();
}

void WriteTask::Abort()
{
    if (!Options()->data_append_mode)
    {
        IoMgr()->AbortWrite(tbl_ident_);
    }

    // Unpins all index pages in the stack.
    while (!stack_.empty())
    {
        IndexStackEntry *stack_entry = stack_.back().get();
        if (stack_entry->idx_page_)
        {
            stack_entry->idx_page_->Unpin();
        }
        stack_.pop_back();
    }

    if (cow_meta_.old_mapping_ != nullptr)
    {
        // Cancel all free file page operations.
        cow_meta_.old_mapping_->ClearFreeFilePage();
        cow_meta_.old_mapping_ = nullptr;
    }
    cow_meta_.mapper_ = nullptr;
    if (cow_meta_.manifest_size_ == 0)
    {
        assert(cow_meta_.root_ == nullptr);
        // MakeCowRoot will create a empty RootMeta if partition not found
        shard->IndexManager()->EvictRootIfEmpty(tbl_ident_);
    }
}

KvError WriteTask::DeleteTree(PageId page_id)
{
    auto [idx_page, err] = shard->IndexManager()->FindPage(
        cow_meta_.mapper_->GetMapping(), page_id);
    CHECK_KV_ERR(err);
    idx_page->Pin();
    IndexPageIter iter(idx_page, Options());

    if (idx_page->IsPointingToLeaf())
    {
        while (iter.Next())
        {
            err = DeleteDataPage(iter.GetPageId());
            if (err != KvError::NoError)
            {
                idx_page->Unpin();
                return err;
            }
        }
        idx_page->Unpin();
        FreePage(page_id);
        return KvError::NoError;
    }

    while (iter.Next())
    {
        KvError err = DeleteTree(iter.GetPageId());
        if (err != KvError::NoError)
        {
            idx_page->Unpin();
            return err;
        }
    }
    idx_page->Unpin();
    FreePage(page_id);
    return KvError::NoError;
}

KvError WriteTask::DeleteDataPage(PageId page_id)
{
    auto [page, err] = LoadDataPage(page_id);
    CHECK_KV_ERR(err);
    DataPageIter iter(&page, Options());
    while (iter.Next())
    {
        if (iter.IsOverflow())
        {
            err = DelOverflowValue(iter.Value());
            CHECK_KV_ERR(err);
        }
    }
    FreePage(page_id);
    return KvError::NoError;
}

std::string_view WriteTask::LeftBound(bool is_data_page)
{
    size_t level = is_data_page ? 0 : 1;
    auto stack_it = stack_.crbegin() + level;
    while (stack_it != stack_.crend())
    {
        IndexPageIter &idx_iter = (*stack_it)->idx_page_iter_;
        std::string_view idx_key = idx_iter.Key();
        if (!idx_key.empty())
        {
            return idx_key;
        }
        ++stack_it;
    }

    // An empty string for left bound means negative infinity.
    return std::string_view{};
}

std::string WriteTask::RightBound(bool is_data_page)
{
    size_t level = is_data_page ? 0 : 1;
    auto stack_it = stack_.crbegin() + level;
    while (stack_it != stack_.crend())
    {
        IndexPageIter &idx_iter = (*stack_it)->idx_page_iter_;
        std::string next_key = idx_iter.PeekNextKey();
        if (!next_key.empty())
        {
            return next_key;
        }
        ++stack_it;
    }

    // An empty string for right bound means positive infinity.
    return std::string{};
}

void WriteTask::AdvanceDataPageIter(DataPageIter &iter, bool &is_valid)
{
    is_valid = iter.HasNext() ? iter.Next() : false;
}

void WriteTask::AdvanceIndexPageIter(IndexPageIter &iter, bool &is_valid)
{
    is_valid = iter.HasNext() ? iter.Next() : false;
}

KvError WriteTask::WritePage(DataPage &&page)
{
    SetPageChecksum(page.PagePtr(), Options()->data_page_size);
    auto [_, fp_id] = AllocatePage(page.GetPageId());
    return WritePage(std::move(page), fp_id);
}

KvError WriteTask::WritePage(OverflowPage &&page)
{
    SetPageChecksum(page.PagePtr(), Options()->data_page_size);
    auto [_, fp_id] = AllocatePage(page.GetPageId());
    return WritePage(std::move(page), fp_id);
}

KvError WriteTask::WritePage(MemIndexPage *page)
{
    SetPageChecksum(page->PagePtr(), Options()->data_page_size);
    auto [page_id, file_page_id] = AllocatePage(page->GetPageId());
    page->SetPageId(page_id);
    page->SetFilePageId(file_page_id);
    return WritePage(page, file_page_id);
}

KvError WriteTask::WritePage(VarPage page, FilePageId file_page_id)
{
    const KvOptions *opts = Options();
    assert(ValidatePageChecksum(VarPagePtr(page), opts->data_page_size));
    KvError err;
    if (opts->data_append_mode)
    {
        batch_pages_.emplace_back(std::move(page));
        if (batch_pages_.size() == 1)
        {
            batch_fp_id_ = file_page_id;
        }
        // Flush the current batch when it is full, or when a data file switch
        // is required.
        size_t mask = opts->FilePageOffsetMask();
        if (batch_pages_.size() >= opts->max_write_batch_pages ||
            (file_page_id & mask) == mask)
        {
            err = FlushBatchPages();
            CHECK_KV_ERR(err);
        }
    }
    else
    {
        err = IoMgr()->WritePage(tbl_ident_, std::move(page), file_page_id);
        CHECK_KV_ERR(err);
        if (inflight_io_ >= opts->max_write_batch_pages)
        {
            // Avoid long running WriteTask block ReadTask/ScanTask
            err = WaitWrite();
            CHECK_KV_ERR(err);
        }
    }
    return KvError::NoError;
}

void WriteTask::WritePageCallback(VarPage page, KvError err)
{
    if (err != KvError::NoError)
    {
        write_err_ = err;
    }

    switch (VarPageType(page.index()))
    {
    case VarPageType::MemIndexPage:
    {
        MemIndexPage *idx_page = std::get<MemIndexPage *>(page);
        if (err == KvError::NoError)
        {
            shard->IndexManager()->FinishIo(cow_meta_.mapper_->GetMapping(),
                                            idx_page);
        }
        else
        {
            shard->IndexManager()->FreeIndexPage(idx_page);
        }
        break;
    }
    case VarPageType::DataPage:
    case VarPageType::OverflowPage:
    case VarPageType::Page:
        break;
    }
}

KvError WriteTask::FlushBatchPages()
{
    assert(!batch_pages_.empty());
    assert(batch_fp_id_ != MaxFilePageId);
    assert(Options()->data_append_mode);
    KvError err = IoMgr()->WritePages(tbl_ident_, batch_pages_, batch_fp_id_);
    for (VarPage &page : batch_pages_)
    {
        WritePageCallback(std::move(page), err);
    }
    batch_pages_.clear();
    batch_fp_id_ = MaxFilePageId;
    return err;
}

KvError WriteTask::WaitWrite()
{
    WaitIo();
    KvError err = write_err_;
    write_err_ = KvError::NoError;
    return err;
}

std::pair<PageId, KvError> WriteTask::Seek(std::string_view key)
{
    if (stack_.back()->idx_page_ == nullptr)
    {
        stack_.back()->is_leaf_index_ = true;
        return {MaxPageId, KvError::NoError};
    }

    while (true)
    {
        IndexStackEntry *idx_entry = stack_.back().get();
        IndexPageIter &idx_iter = idx_entry->idx_page_iter_;
        idx_iter.Seek(key);
        PageId page_id = idx_iter.GetPageId();
        assert(page_id != MaxPageId);
        if (idx_entry->idx_page_->IsPointingToLeaf())
        {
            break;
        }
        assert(!stack_.back()->is_leaf_index_);
        auto [node, err] = shard->IndexManager()->FindPage(
            cow_meta_.mapper_->GetMapping(), page_id);
        if (err != KvError::NoError)
        {
            return {MaxPageId, err};
        }
        node->Pin();
        stack_.emplace_back(std::make_unique<IndexStackEntry>(node, Options()));
    }
    stack_.back()->is_leaf_index_ = true;
    return {stack_.back()->idx_page_iter_.GetPageId(), KvError::NoError};
}

std::pair<PageId, FilePageId> WriteTask::AllocatePage(PageId page_id)
{
    if (!Options()->data_append_mode && page_id != MaxPageId)
    {
        FilePageId old_fp_id = ToFilePage(page_id);
        if (old_fp_id != MaxFilePageId)
        {
            // The page is mapped to a new file page. The old file page will be
            // recycled. However, the old file page shall only be recycled when
            // the old mapping snapshot is destructed, i.e., no one is using the
            // old mapping.
            cow_meta_.old_mapping_->AddFreeFilePage(old_fp_id);
        }
    }

    if (page_id == MaxPageId)
    {
        page_id = cow_meta_.mapper_->GetPage();
    }
    FilePageId file_page_id = cow_meta_.mapper_->FilePgAllocator()->Allocate();
    cow_meta_.mapper_->UpdateMapping(page_id, file_page_id);
    wal_builder_.UpdateMapping(page_id, file_page_id);
    return {page_id, file_page_id};
}

void WriteTask::FreePage(PageId page_id)
{
    if (!Options()->data_append_mode)
    {
        // Free file page.
        FilePageId file_page = ToFilePage(page_id);
        cow_meta_.old_mapping_->AddFreeFilePage(file_page);
    }
    cow_meta_.mapper_->FreePage(page_id);
    wal_builder_.DeleteMapping(page_id);
}

FilePageId WriteTask::ToFilePage(PageId page_id)
{
    return cow_meta_.mapper_->GetMapping()->ToFilePage(page_id);
}

KvError WriteTask::FlushManifest(PageId root_page_id)
{
    if (wal_builder_.Empty())
    {
        return KvError::NoError;
    }

    const KvOptions *opts = Options();
    KvError err;
    uint64_t manifest_size = cow_meta_.manifest_size_;
    std::string_view snapshot;
    if (manifest_size > 0 &&
        manifest_size + wal_builder_.CurrentSize() <= opts->manifest_limit)
    {
        std::string_view blob = wal_builder_.Finalize(root_page_id);
        err = IoMgr()->AppendManifest(tbl_ident_, blob, manifest_size);
        CHECK_KV_ERR(err);
        cow_meta_.manifest_size_ += blob.size();
    }
    else
    {
        MappingSnapshot *mapping = cow_meta_.mapper_->GetMapping();
        FilePageId max_fp_id =
            cow_meta_.mapper_->FilePgAllocator()->MaxFilePageId();
        snapshot = wal_builder_.Snapshot(root_page_id, mapping, max_fp_id);
        err = IoMgr()->SwitchManifest(tbl_ident_, snapshot);
        CHECK_KV_ERR(err);
        cow_meta_.manifest_size_ = snapshot.size();
    }
    return KvError::NoError;
}

KvError WriteTask::UpdateMeta(MemIndexPage *root)
{
    KvError err;
    const KvOptions *opts = Options();
    // Flush data pages.
    if (opts->data_append_mode)
    {
        if (!batch_pages_.empty())
        {
            err = FlushBatchPages();
            CHECK_KV_ERR(err);
        }
    }
    else
    {
        err = WaitWrite();
        CHECK_KV_ERR(err);
    }

    PageId root_pg_id = MaxPageId;
    if (root != nullptr)
    {
        // Prevent the root page from being evicted before update RootMeta.
        root->Pin();
        root_pg_id = root->GetPageId();
    }

    err = IoMgr()->SyncData(tbl_ident_);
    if (err != KvError::NoError)
    {
        if (root != nullptr)
        {
            root->Unpin();
        }
        return err;
    }

    // Update meta data in storage and then in memory.
    err = FlushManifest(root_pg_id);
    if (err != KvError::NoError)
    {
        if (root != nullptr)
        {
            root->Unpin();
        }
        return err;
    }

    if (opts->data_append_mode)
    {
        CompactIfNeeded(cow_meta_.mapper_.get());
    }

    cow_meta_.root_ = root;
    shard->IndexManager()->UpdateRoot(tbl_ident_, std::move(cow_meta_));
    if (root != nullptr)
    {
        root->Unpin();
    }
    return KvError::NoError;
}

void WriteTask::CompactIfNeeded(PageMapper *mapper) const
{
    const KvOptions *opts = Options();
    if (opts->file_amplify_factor != 0 && Type() != TaskType::Compact)
    {
        auto allocator =
            static_cast<AppendAllocator *>(mapper->FilePgAllocator());
        uint32_t mapping_cnt = mapper->MappingCount();
        if (mapping_cnt == 0)
        {
            // Update statistic.
            allocator->UpdateStat(MaxFileId, 0);
        }
        else
        {
            size_t space_size = allocator->SpaceSize();
            assert(space_size >= mapping_cnt);
            if (space_size >= allocator->PagesPerFile() &&
                double(space_size) / double(mapping_cnt) >
                    double(opts->file_amplify_factor))
            {
                shard->AddCompactRequest(tbl_ident_);
            }
        }
    }
}

KvError WriteTask::TriggerFileGC() const
{
    if (eloqstore->file_gc_ == nullptr)
    {
        // File garbage collector is not enabled.
        return KvError::NoError;
    }

    auto [meta, err] = shard->IndexManager()->FindRoot(tbl_ident_);
    CHECK_KV_ERR(err);

    const PageMapper *mapper = meta->mapper_.get();
    auto mapping = mapper->GetMappingSnapshot();
    uint64_t ts = utils::UnixTs<chrono::microseconds>();
    FileId cur_file_id = mapper->FilePgAllocator()->CurrentFileId();
    eloqstore->file_gc_->AddTask(std::move(mapping), ts, cur_file_id);
    return KvError::NoError;
}

KvError WriteTask::WriteOverflowValue(std::string_view value)
{
    const KvOptions *opts = Options();
    const uint16_t page_cap = OverflowPage::Capacity(opts, false);
    const uint16_t end_page_cap = OverflowPage::Capacity(opts, true);
    overflow_ptrs_.clear();
    uint32_t end_page_id = MaxPageId;
    std::string_view page_val;
    KvError err;
    std::array<uint32_t, max_overflow_pointers> buf;
    std::span<uint32_t> pointers;

    while (!value.empty())
    {
        // Calculates how many page ids are needed for the next group.
        // Round upward and up to KvOptions::overflow_pointers.
        size_t next_group_size = (value.size() + page_cap - 1) / page_cap;
        next_group_size =
            std::min(next_group_size, size_t(opts->overflow_pointers));
        // Allocate pointers to the next group.
        for (uint8_t i = 0; i < next_group_size; i++)
        {
            buf[i] = cow_meta_.mapper_->GetPage();
        }
        pointers = {buf.data(), next_group_size};
        if (end_page_id == MaxPageId)
        {
            // Store head pointers of this link list in overflow_ptrs_.
            for (uint32_t pg_id : pointers)
            {
                PutFixed32(&overflow_ptrs_, pg_id);
            }
        }
        else
        {
            // Write end page of the previous group that contains pointers to
            // the next group.
            err =
                WritePage(OverflowPage(end_page_id, opts, page_val, pointers));
            CHECK_KV_ERR(err);
        }

        // Write the next overflow pages group.
        uint8_t i = 0;
        for (uint32_t pg_id : pointers)
        {
            i++;
            if (i == opts->overflow_pointers && value.size() > page_cap)
            {
                // The end page of this group can't hold the remaining value.
                // So we need at least one more group.
                end_page_id = pg_id;
                // The end page of a group has a smaller capacity.
                uint16_t page_val_size =
                    std::min(size_t(end_page_cap), value.size());
                page_val = value.substr(0, page_val_size);
                value = value.substr(page_val_size);
                break;
            }
            uint16_t page_val_size = std::min(size_t(page_cap), value.size());
            page_val = value.substr(0, page_val_size);
            value = value.substr(page_val_size);
            err = WritePage(OverflowPage(pg_id, opts, page_val));
            CHECK_KV_ERR(err);
        }
        assert(i == pointers.size());
    }
    return KvError::NoError;
}

KvError WriteTask::DelOverflowValue(std::string_view encoded_ptrs)
{
    std::array<PageId, max_overflow_pointers> pointers;
    uint8_t n_ptrs = DecodeOverflowPointers(encoded_ptrs, pointers);
    for (uint8_t i = 0; i < n_ptrs;)
    {
        PageId page_id = pointers[i];
        if (i == (Options()->overflow_pointers - 1))
        {
            auto [page, err] = LoadOverflowPage(page_id);
            CHECK_KV_ERR(err);
            encoded_ptrs = page.GetEncodedPointers(Options());
            n_ptrs = DecodeOverflowPointers(encoded_ptrs, pointers);
            i = 0;
        }
        else
        {
            i++;
        }
        FreePage(page_id);
    }
    return KvError::NoError;
}

std::pair<DataPage, KvError> WriteTask::LoadDataPage(PageId page_id)
{
    return KvTask::LoadDataPage(tbl_ident_, page_id, ToFilePage(page_id));
}

std::pair<OverflowPage, KvError> WriteTask::LoadOverflowPage(PageId page_id)
{
    return KvTask::LoadOverflowPage(tbl_ident_, page_id, ToFilePage(page_id));
}

}  // namespace kvstore