#include "batch_write_task.h"

#include "shard.h"

namespace kvstore
{
KvError BatchWriteTask::SeekStack(std::string_view search_key)
{
    const Comparator *cmp = shard->IndexManager()->GetComparator();

    auto entry_contains = [](std::string_view start,
                             std::string_view end,
                             std::string_view search_key,
                             const Comparator *cmp)
    {
        return (start.empty() || cmp->Compare(search_key, start) >= 0) &&
               (end.empty() || cmp->Compare(search_key, end) < 0);
    };

    // The bottom index entry (i.e., the tree root) ranges from negative
    // infinity to positive infinity.
    while (stack_.size() > 1)
    {
        IndexPageIter &idx_iter = stack_.back()->idx_page_iter_;

        if (idx_iter.HasNext())
        {
            idx_iter.Next();
            std::string_view idx_entry_start = idx_iter.Key();
            std::string idx_entry_end = RightBound(false);

            if (entry_contains(idx_entry_start, idx_entry_end, search_key, cmp))
            {
                break;
            }
            else
            {
                auto [_, err] = Pop();
                CHECK_KV_ERR(err);
            }
        }
        else
        {
            auto [_, err] = Pop();
            CHECK_KV_ERR(err);
        }
    }
    return KvError::NoError;
}

inline DataPage *BatchWriteTask::TripleElement(uint8_t idx)
{
    return leaf_triple_[idx].IsEmpty() ? nullptr : &leaf_triple_[idx];
}

KvError BatchWriteTask::LoadTripleElement(uint8_t idx, PageId page_id)
{
    if (TripleElement(idx))
    {
        return KvError::NoError;
    }
    assert(page_id != MaxPageId);
    auto [page, err] = LoadDataPage(page_id);
    CHECK_KV_ERR(err);
    leaf_triple_[idx] = std::move(page);
    return KvError::NoError;
}

KvError BatchWriteTask::ShiftLeafLink()
{
    if (TripleElement(0))
    {
        KvError err = WritePage(std::move(leaf_triple_[0]));
        CHECK_KV_ERR(err);
    }
    leaf_triple_[0] = std::move(leaf_triple_[1]);
    return KvError::NoError;
}

KvError BatchWriteTask::LeafLinkUpdate(DataPage &&page)
{
    page.SetNextPageId(applying_page_.NextPageId());
    page.SetPrevPageId(applying_page_.PrevPageId());
    leaf_triple_[1] = std::move(page);
    return ShiftLeafLink();
}

KvError BatchWriteTask::LeafLinkInsert(DataPage &&page)
{
    assert(!TripleElement(1));
    leaf_triple_[1] = std::move(page);
    DataPage &new_elem = leaf_triple_[1];
    if (TripleElement(0) == nullptr)
    {
        // Add first element into empty link list
        assert(stack_.back()->idx_page_iter_.GetPageId() == MaxPageId);
        new_elem.SetNextPageId(MaxPageId);
        new_elem.SetPrevPageId(MaxPageId);
        return ShiftLeafLink();
    }
    DataPage *prev_page = TripleElement(0);
    assert(stack_.back()->idx_page_iter_.GetPageId() == MaxPageId ||
           prev_page->NextPageId() == applying_page_.NextPageId());
    if (prev_page->NextPageId() != MaxPageId)
    {
        KvError err = LoadTripleElement(2, prev_page->NextPageId());
        CHECK_KV_ERR(err);
        TripleElement(2)->SetPrevPageId(new_elem.GetPageId());
    }
    new_elem.SetPrevPageId(prev_page->GetPageId());
    new_elem.SetNextPageId(prev_page->NextPageId());
    prev_page->SetNextPageId(new_elem.GetPageId());
    return ShiftLeafLink();
}

KvError BatchWriteTask::LeafLinkDelete()
{
    if (applying_page_.PrevPageId() != MaxPageId)
    {
        KvError err = LoadTripleElement(0, applying_page_.PrevPageId());
        CHECK_KV_ERR(err);
        TripleElement(0)->SetNextPageId(applying_page_.NextPageId());
    }
    if (applying_page_.NextPageId() != MaxPageId)
    {
        assert(!TripleElement(2));
        KvError err = LoadTripleElement(2, applying_page_.NextPageId());
        CHECK_KV_ERR(err);
        TripleElement(2)->SetPrevPageId(applying_page_.PrevPageId());
    }
    return KvError::NoError;
}

bool BatchWriteTask::SetBatch(std::vector<WriteDataEntry> &&entries)
{
#ifndef NDEBUG
    const Comparator *cmp = Comp();
    if (entries.size() > 1)
    {
        // Ensure the input batch keys are unique and ordered.
        for (uint64_t i = 1; i < entries.size(); i++)
        {
            if (cmp->Compare(entries[i - 1].key_, entries[i].key_) >= 0)
            {
                return false;
            }
        }
    }

    // Limit max key length to half of page size.
    uint16_t max_key_len = Options()->data_page_size >> 1;
    for (WriteDataEntry &ent : entries)
    {
        if (ent.key_.empty() || ent.key_.size() > max_key_len)
        {
            return false;
        }
    }
#endif

    batch_ = std::move(entries);
    return true;
}

void BatchWriteTask::Abort()
{
    WriteTask::Abort();

    for (DataPage &page : leaf_triple_)
    {
        page.Clear();
    }
    batch_.clear();
}

KvError BatchWriteTask::Apply()
{
    KvError err = shard->IndexManager()->MakeCowRoot(tbl_ident_, cow_meta_);
    CHECK_KV_ERR(err);

    stack_.emplace_back(
        std::make_unique<IndexStackEntry>(cow_meta_.root_, Options()));
    if (cow_meta_.root_ != nullptr)
    {
        cow_meta_.root_->Pin();
    }

    size_t cidx = 0;
    while (cidx < batch_.size())
    {
        std::string_view batch_start_key = {batch_[cidx].key_.data(),
                                            batch_[cidx].key_.size()};
        if (stack_.size() > 1)
        {
            err = SeekStack(batch_start_key);
            CHECK_KV_ERR(err);
        }
        auto [page_id, err] = Seek(batch_start_key);
        CHECK_KV_ERR(err);
        if (page_id != MaxPageId)
        {
            err = LoadApplyingPage(page_id);
            CHECK_KV_ERR(err);
        }
        err = ApplyOnePage(cidx);
        CHECK_KV_ERR(err);
    }
    // Flush all dirty leaf data pages in leaf_triple_ .
    assert(TripleElement(2) == nullptr);
    err = ShiftLeafLink();
    CHECK_KV_ERR(err);
    err = ShiftLeafLink();
    CHECK_KV_ERR(err);
    batch_.clear();

    assert(!stack_.empty());
    MemIndexPage *new_root = nullptr;
    while (!stack_.empty())
    {
        auto [new_page, err] = Pop();
        CHECK_KV_ERR(err);
        new_root = new_page;
    }
    err = UpdateMeta(new_root);
    return err;
}

KvError BatchWriteTask::LoadApplyingPage(PageId page_id)
{
    assert(page_id != MaxPageId);
    // Now we are going to fetch a data page before execute ApplyOnePage.
    // But this page may already exists at leaf_triple_[1], because it may be
    // loaded by previous ApplyOnePage for linking purpose.
    if (TripleElement(1) && TripleElement(1)->GetPageId() == page_id)
    {
        // Fast path: leaf_triple_[1] is exactly the page we want. Just move it
        // to avoid a disk access.
        applying_page_.Clear();
        applying_page_ = std::move(*TripleElement(1));
    }
    else
    {
        auto [page, err] = LoadDataPage(page_id);
        CHECK_KV_ERR(err);
        applying_page_ = std::move(page);
    }
    assert(TypeOfPage(applying_page_.PagePtr()) == PageType::Data);

    if (TripleElement(1))
    {
        assert(TripleElement(1)->GetPageId() != applying_page_.GetPageId());
        KvError err = ShiftLeafLink();
        CHECK_KV_ERR(err);
    }
    if (TripleElement(0) &&
        TripleElement(0)->GetPageId() != applying_page_.PrevPageId())
    {
        // leaf_triple_[0] is not the previously adjacent page of the
        // applying page.
        KvError err = ShiftLeafLink();
        CHECK_KV_ERR(err);
    }
    return KvError::NoError;
}

KvError BatchWriteTask::ApplyOnePage(size_t &cidx)
{
    assert(!stack_.empty());

    DataPage *base_page = nullptr;
    std::string_view page_left_bound{};
    std::string page_right_key;
    std::string_view page_right_bound{};

    if (stack_.back()->idx_page_iter_.GetPageId() != MaxPageId)
    {
        assert(stack_.back()->idx_page_iter_.GetPageId() ==
               applying_page_.GetPageId());
        base_page = &applying_page_;
        page_left_bound = LeftBound(true);
        page_right_key = RightBound(true);
        page_right_bound = {page_right_key.data(), page_right_key.size()};
    }

    const Comparator *cmp = shard->IndexManager()->GetComparator();
    DataPageIter base_page_iter{base_page, Options()};
    bool is_base_iter_valid = false;
    AdvanceDataPageIter(base_page_iter, is_base_iter_valid);

    data_page_builder_.Reset();

    assert(cidx < batch_.size());
    std::string_view change_key = {batch_[cidx].key_.data(),
                                   batch_[cidx].key_.size()};
    assert(cmp->Compare(page_left_bound, change_key) <= 0);
    assert(page_right_bound.empty() ||
           cmp->Compare(page_left_bound, page_right_bound) < 0);

    auto change_it = batch_.begin() + cidx;
    auto change_end_it = std::lower_bound(
        change_it,
        batch_.end(),
        page_right_bound,
        [&](const WriteDataEntry &change_item, std::string_view key)
        {
            if (key.empty())
            {
                // An empty-string right bound represents positive infinity.
                return true;
            }

            std::string_view ckey{change_item.key_.data(),
                                  change_item.key_.size()};
            return cmp->Compare(ckey, key) < 0;
        });

    std::string prev_key;
    std::string_view page_key = stack_.back()->idx_page_iter_.Key();
    std::string curr_page_key{page_key.data(), page_key.size()};

    PageId page_id = MaxPageId;
    if (base_page != nullptr)
    {
        page_id = base_page->GetPageId();
        assert(page_key <= base_page_iter.Key());
    }

    std::function<KvError(std::string_view, std::string_view, bool, uint64_t)>
        add_to_page = [&](std::string_view key,
                          std::string_view val,
                          bool is_ptr,
                          uint64_t ts) -> KvError
    {
        bool success = data_page_builder_.Add(key, val, ts, is_ptr);
        if (!success)
        {
            if (!is_ptr &&
                DataPageBuilder::IsOverflowKV(key, val.size(), ts, Options()))
            {
                // The key-value pair is too large to fit in a single data page.
                // Split it into multiple overflow pages.
                KvError err = WriteOverflowValue(val);
                CHECK_KV_ERR(err);
                return add_to_page(key, overflow_ptrs_, true, ts);
            }

            // Finishes the current page.
            std::string_view page_view = data_page_builder_.Finish();
            KvError err =
                FinishDataPage(page_view, std::move(curr_page_key), page_id);
            CHECK_KV_ERR(err);
            // Starts a new page.
            curr_page_key = cmp->FindShortestSeparator(
                {prev_key.data(), prev_key.size()}, key);
            assert(!prev_key.empty() && prev_key < curr_page_key);
            data_page_builder_.Reset();
            success = data_page_builder_.Add(key, val, ts, is_ptr);
            assert(success);
            page_id = MaxPageId;
        }
        assert(curr_page_key <= key);
        prev_key = key;
        return KvError::NoError;
    };

    while (is_base_iter_valid && change_it != change_end_it)
    {
        std::string_view base_key = base_page_iter.Key();
        std::string_view base_val = base_page_iter.Value();
        uint64_t base_ts = base_page_iter.Timestamp();
        bool is_overflow_ptr = false;

        change_key = {change_it->key_.data(), change_it->key_.size()};
        std::string_view change_val = {change_it->val_.data(),
                                       change_it->val_.size()};
        uint64_t change_ts = change_it->timestamp_;

        enum struct AdvanceType
        {
            PageIter,
            Changes,
            Both
        };

        std::string_view new_key;
        std::string_view new_val;
        uint64_t new_ts;
        AdvanceType adv_type;

        int cmp_ret = cmp->Compare(base_key, change_key);
        if (cmp_ret < 0)
        {
            new_key = base_key;
            new_val = base_val;
            new_ts = base_ts;
            is_overflow_ptr = base_page_iter.IsOverflow();
            adv_type = AdvanceType::PageIter;
        }
        else if (cmp_ret == 0)
        {
            adv_type = AdvanceType::Both;
            if (change_ts > base_ts)
            {
                if (base_page_iter.IsOverflow())
                {
                    KvError err = DelOverflowValue(base_val);
                    CHECK_KV_ERR(err);
                }
                if (change_it->op_ == WriteOp::Delete)
                {
                    new_key = std::string_view{};
                }
                else
                {
                    new_key = change_key;
                    new_val = change_val;
                    new_ts = change_ts;
                }
            }
            else
            {
                new_key = base_key;
                new_val = base_val;
                new_ts = base_ts;
                is_overflow_ptr = base_page_iter.IsOverflow();
            }
        }
        else
        {
            adv_type = AdvanceType::Changes;
            if (change_it->op_ == WriteOp::Delete)
            {
                new_key = std::string_view{};
            }
            else
            {
                new_key = change_key;
                new_val = change_val;
                new_ts = change_ts;
            }
        }

        if (!new_key.empty())
        {
            KvError err =
                add_to_page(new_key, new_val, is_overflow_ptr, new_ts);
            CHECK_KV_ERR(err);
        }

        switch (adv_type)
        {
        case AdvanceType::PageIter:
            AdvanceDataPageIter(base_page_iter, is_base_iter_valid);
            break;
        case AdvanceType::Changes:
            ++change_it;
            break;
        default:
            AdvanceDataPageIter(base_page_iter, is_base_iter_valid);
            ++change_it;
            break;
        }
    }

    while (is_base_iter_valid)
    {
        std::string_view new_key = base_page_iter.Key();
        std::string_view new_val = base_page_iter.Value();
        bool overflow = base_page_iter.IsOverflow();
        uint64_t new_ts = base_page_iter.Timestamp();
        KvError err = add_to_page(new_key, new_val, overflow, new_ts);
        CHECK_KV_ERR(err);
        AdvanceDataPageIter(base_page_iter, is_base_iter_valid);
    }

    while (change_it != change_end_it)
    {
        if (change_it->op_ != WriteOp::Delete)
        {
            std::string_view new_key{change_it->key_.data(),
                                     change_it->key_.size()};
            std::string_view new_val{change_it->val_.data(),
                                     change_it->val_.size()};
            uint64_t new_ts = change_it->timestamp_;
            KvError err = add_to_page(new_key, new_val, false, new_ts);
            CHECK_KV_ERR(err);
        }
        ++change_it;
    }

    if (data_page_builder_.IsEmpty())
    {
        if (base_page)
        {
            KvError err = LeafLinkDelete();
            CHECK_KV_ERR(err);
            FreePage(applying_page_.GetPageId());
            assert(stack_.back()->changes_.empty() ||
                   stack_.back()->changes_.back().key_ < curr_page_key);
            stack_.back()->changes_.emplace_back(
                std::move(curr_page_key), page_id, WriteOp::Delete);
        }
    }
    else
    {
        std::string_view page_view = data_page_builder_.Finish();
        KvError err =
            FinishDataPage(page_view, std::move(curr_page_key), page_id);
        CHECK_KV_ERR(err);
    }
    assert(!TripleElement(1));
    leaf_triple_[1] = std::move(leaf_triple_[2]);

    cidx = cidx + std::distance(batch_.begin() + cidx, change_end_it);
    return KvError::NoError;
}

std::pair<MemIndexPage *, KvError> BatchWriteTask::Pop()
{
    if (stack_.empty())
    {
        return {nullptr, KvError::NoError};
    }

    IndexStackEntry *stack_entry = stack_.back().get();
    // There is no change at this level.
    if (stack_entry->changes_.empty())
    {
        MemIndexPage *page = stack_entry->idx_page_;
        if (page != nullptr)
        {
            page->Unpin();
        }
        stack_.pop_back();
        return {page, KvError::NoError};
    }

    idx_page_builder_.Reset();

    const Comparator *cmp = shard->IndexManager()->GetComparator();
    std::vector<IndexOp> &changes = stack_entry->changes_;
    MemIndexPage *stack_page = stack_entry->idx_page_;
    // If the change op contains no index page pointer, this is the lowest level
    // index page.
    bool is_leaf_index = stack_entry->is_leaf_index_;
    IndexPageIter base_page_iter{stack_page, Options()};
    bool is_base_iter_valid = false;
    AdvanceIndexPageIter(base_page_iter, is_base_iter_valid);

    idx_page_builder_.Reset();

    // Merges index entries in the page with the change vector.
    auto cit = changes.begin();

    // We keep the previous built page in the pipeline before flushing it to
    // storage. This is to redistribute between last two pages in case the last
    // page is sparse.
    MemIndexPage *prev_page = nullptr;
    MemIndexPage *curr_page = nullptr;
    std::string prev_page_key{};
    std::string_view page_key =
        stack_.size() == 1 ? std::string_view{}
                           : stack_[stack_.size() - 2]->idx_page_iter_.Key();
    std::string curr_page_key{page_key};

    PageId page_id = MaxPageId;
    if (stack_page != nullptr)
    {
        page_id = stack_page->GetPageId();
    }

    auto add_to_page = [&](std::string_view new_key,
                           uint32_t new_page_id) -> KvError
    {
        bool success =
            idx_page_builder_.Add(new_key, new_page_id, is_leaf_index);
        if (!success)
        {
            curr_page = shard->IndexManager()->AllocIndexPage();
            if (curr_page == nullptr)
            {
                return KvError::OutOfMem;
            }
            // The page is full.
            std::string_view page_view = idx_page_builder_.Finish();
            memcpy(curr_page->PagePtr(), page_view.data(), page_view.size());

            if (prev_page != nullptr)
            {
                // The update results in an index page split, because there is
                // at least one new index page to come. Flushes the previously
                // built index page and elevates it to the parent in the stack.
                KvError err = FinishIndexPage(
                    prev_page, std::move(prev_page_key), page_id, true);
                if (err != KvError::NoError)
                {
                    shard->IndexManager()->FreeIndexPage(prev_page);
                    prev_page = nullptr;
                    return err;
                }

                // The first split index page shares the same page Id with the
                // original one. The following index pages will have new page
                // Id's.
                page_id = MaxPageId;
            }

            prev_page = curr_page;
            prev_page_key = std::move(curr_page_key);
            curr_page_key = new_key;
            idx_page_builder_.Reset();
            // The first index entry is the leftmost pointer w/o the key.
            idx_page_builder_.Add(
                std::string_view{}, new_page_id, is_leaf_index);
        }
        return KvError::NoError;
    };

    while (is_base_iter_valid && cit != changes.end())
    {
        std::string_view base_key = base_page_iter.Key();
        uint32_t base_page_id = base_page_iter.GetPageId();
        std::string_view change_key{cit->key_.data(), cit->key_.size()};
        uint32_t change_page = cit->page_id_;
        int cmp_ret = cmp->Compare(base_key, change_key);

        enum struct AdvanceType
        {
            PageIter,
            Changes,
            Both
        };

        std::string_view new_key;
        uint32_t new_page_id;
        AdvanceType adv_type;

        if (cmp_ret < 0)
        {
            new_key = base_key;
            new_page_id = base_page_id;
            adv_type = AdvanceType::PageIter;
        }
        else if (cmp_ret == 0)
        {
            adv_type = AdvanceType::Both;
            if (cit->op_ == WriteOp::Delete)
            {
                new_key = std::string_view{};
                new_page_id = MaxPageId;
            }
            else
            {
                new_key = change_key;
                new_page_id = change_page;
            }
        }
        else
        {
            // base_key > change_key
            assert(cit->op_ == WriteOp::Upsert);
            adv_type = AdvanceType::Changes;
            new_key = change_key;
            new_page_id = change_page;
        }

        // The first inserted entry is the leftmost pointer whose key is empty.
        if (!new_key.empty() || new_page_id != MaxPageId)
        {
            KvError err = add_to_page(new_key, new_page_id);
            if (err != KvError::NoError)
            {
                return {nullptr, err};
            }
        }

        switch (adv_type)
        {
        case AdvanceType::PageIter:
            AdvanceIndexPageIter(base_page_iter, is_base_iter_valid);
            break;
        case AdvanceType::Changes:
            cit++;
            break;
        default:
            AdvanceIndexPageIter(base_page_iter, is_base_iter_valid);
            cit++;
            break;
        }
    }

    while (is_base_iter_valid)
    {
        std::string_view new_key = base_page_iter.Key();
        uint32_t new_page_id = base_page_iter.GetPageId();
        KvError err = add_to_page(new_key, new_page_id);
        if (err != KvError::NoError)
        {
            return {nullptr, err};
        }
        AdvanceIndexPageIter(base_page_iter, is_base_iter_valid);
    }

    while (cit != changes.end())
    {
        if (cit->op_ != WriteOp::Delete)
        {
            std::string_view new_key{cit->key_.data(), cit->key_.size()};
            uint32_t new_page = cit->page_id_;
            KvError err = add_to_page(new_key, new_page);
            if (err != KvError::NoError)
            {
                return {nullptr, err};
            }
        }
        ++cit;
    }

    bool elevate;
    if (prev_page != nullptr)
    {
        KvError err =
            FinishIndexPage(prev_page, std::move(prev_page_key), page_id, true);
        if (err != KvError::NoError)
        {
            return {nullptr, err};
        }
        page_id = MaxPageId;
        // The index page is split into two or more pages, all of which are
        // elevated.
        elevate = true;
    }
    else
    {
        // The update does not yield a page split. The new page has the same
        // page Id as the original one. It is mapped to a new file page and is
        // not elevated to its parent page.
        elevate = false;
    }

    if (idx_page_builder_.IsEmpty())
    {
        FreePage(stack_.back()->idx_page_->GetPageId());
        if (stack_.size() > 1)
        {
            IndexStackEntry *parent = stack_[stack_.size() - 2].get();
            std::string_view page_key = parent->idx_page_iter_.Key();
            parent->changes_.emplace_back(
                std::string(page_key), page_id, WriteOp::Delete);
        }
    }
    else
    {
        curr_page = shard->IndexManager()->AllocIndexPage();
        if (curr_page == nullptr)
        {
            return {nullptr, KvError::OutOfMem};
        }
        std::string_view page_view = idx_page_builder_.Finish();
        memcpy(curr_page->PagePtr(), page_view.data(), page_view.size());
        KvError err = FinishIndexPage(
            curr_page, std::move(curr_page_key), page_id, elevate);
        if (err != KvError::NoError)
        {
            shard->IndexManager()->FreeIndexPage(curr_page);
            return {nullptr, err};
        }
    }

    if (stack_page != nullptr)
    {
        stack_page->Unpin();
    }
    stack_.pop_back();
    return {curr_page, KvError::NoError};
}

KvError BatchWriteTask::FinishIndexPage(MemIndexPage *idx_page,
                                        std::string idx_page_key,
                                        PageId page_id,
                                        bool elevate)
{
    // Flushes the built index page.
    idx_page->SetPageId(page_id);
    KvError err = WritePage(idx_page);
    CHECK_KV_ERR(err);

    // The index page is linked to the parent.
    if (elevate)
    {
        assert(stack_.size() >= 1);
        if (stack_.size() == 1)
        {
            stack_.emplace(
                stack_.begin(),
                std::make_unique<IndexStackEntry>(nullptr, Options()));
        }

        IndexStackEntry *parent_entry = stack_[stack_.size() - 2].get();
        parent_entry->changes_.emplace_back(
            std::move(idx_page_key), idx_page->GetPageId(), WriteOp::Upsert);
    }
    return KvError::NoError;
}

KvError BatchWriteTask::FinishDataPage(std::string_view page_view,
                                       std::string page_key,
                                       PageId page_id)
{
    PageId new_page_id =
        page_id == MaxPageId ? cow_meta_.mapper_->GetPage() : page_id;
    DataPage new_page(new_page_id);
    memcpy(new_page.PagePtr(), page_view.data(), page_view.size());

    if (page_id == MaxPageId)
    {
        // This is a new data page that does not exist in the tree and has a new
        // page Id.
        KvError err = LeafLinkInsert(std::move(new_page));
        CHECK_KV_ERR(err);

        // This is a new page that does not exist in the parent index page.
        // Elevates to the parent index page.
        assert(stack_.back()->changes_.empty() ||
               stack_.back()->changes_.back().key_ < page_key);
        stack_.back()->changes_.emplace_back(
            std::move(page_key), new_page_id, WriteOp::Upsert);
    }
    else
    {
        // This is an existing data page with updated content.
        KvError err = LeafLinkUpdate(std::move(new_page));
        CHECK_KV_ERR(err);
    }
    return KvError::NoError;
}

}  // namespace kvstore