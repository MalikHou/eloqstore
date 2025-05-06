#include "truncate_task.h"

#include "shard.h"

namespace kvstore
{
std::pair<MemIndexPage *, KvError> TruncateTask::TruncateIndexPage(
    PageId page_id, std::string_view trunc_pos)
{
    auto [idx_page, err] = shard->IndexManager()->FindPage(
        cow_meta_.mapper_->GetMapping(), page_id);
    if (err != KvError::NoError)
    {
        return {nullptr, err};
    }

    const bool is_leaf_idx = idx_page->IsPointingToLeaf();
    IndexPageBuilder builder(Options());

    auto truncate_sub_node = [&](std::string_view sub_node_key,
                                 PageId sub_node_id) -> KvError
    {
        // truncate sub-node
        std::pair<bool, KvError> ret;
        if (is_leaf_idx)
        {
            ret = TruncateDataPage(sub_node_id, trunc_pos);
        }
        else
        {
            ret = TruncateIndexPage(sub_node_id, trunc_pos);
        }
        CHECK_KV_ERR(ret.second);
        if (ret.first)
        {
            // This sub-node is partially truncated
            builder.Add(sub_node_key, sub_node_id, is_leaf_idx);
        }
        return KvError::NoError;
    };

    auto delete_sub_node = [this, is_leaf_idx](std::string_view sub_node_key,
                                               PageId sub_node_id) -> KvError
    {
        // delete whole sub-node
        if (is_leaf_idx)
        {
            return DeleteDataPage(sub_node_id);
        }
        else
        {
            return DeleteTree(sub_node_id);
        }
    };

    IndexPageIter iter(idx_page, Options());
    CHECK(iter.Next());
    std::string sub_node_key = {};
    PageId sub_node_id = iter.GetPageId();

    // Depth first search recursively
    bool cut_point_found = false;
    idx_page->Pin();
    while (iter.Next())
    {
        std::string_view next_node_key = iter.Key();
        PageId next_node_id = iter.GetPageId();

        if (cut_point_found)
        {
            // Delete all sub-nodes bigger than cutting point.
            err = delete_sub_node(sub_node_key, sub_node_id);
            if (err != KvError::NoError)
            {
                idx_page->Unpin();
                return {nullptr, err};
            }
        }
        else if (Comp()->Compare(trunc_pos, next_node_key) >= 0)
        {
            // Preserve this sub-node smaller than cutting point.
            builder.Add(sub_node_key, sub_node_id, is_leaf_idx);
        }
        else
        {
            // Mark cutting point has been found to reduce string comparison.
            cut_point_found = true;
            if (Comp()->Compare(trunc_pos, sub_node_key) > 0)
            {
                // Truncate the biggest sub-node smaller than trunc_pos.
                err = truncate_sub_node(sub_node_key, sub_node_id);
            }
            else
            {
                // Delete sub-node equal to trunc_pos.
                assert(Comp()->Compare(trunc_pos, sub_node_key) == 0);
                err = delete_sub_node(sub_node_key, sub_node_id);
            }
            if (err != KvError::NoError)
            {
                idx_page->Unpin();
                return {nullptr, err};
            }
        }

        sub_node_key = next_node_key;
        sub_node_id = next_node_id;
    }
    if (cut_point_found)
    {
        err = delete_sub_node(sub_node_key, sub_node_id);
    }
    else if (Comp()->Compare(trunc_pos, sub_node_key) > 0)
    {
        err = truncate_sub_node(sub_node_key, sub_node_id);
    }
    else
    {
        err = delete_sub_node(sub_node_key, sub_node_id);
    }
    idx_page->Unpin();
    if (err != KvError::NoError)
    {
        return {nullptr, err};
    }

    if (builder.IsEmpty())
    {
        // This index page is wholly deleted
        return {nullptr, KvError::NoError};
    }
    // This index page is partially truncated
    MemIndexPage *new_page = shard->IndexManager()->AllocIndexPage();
    if (new_page == nullptr)
    {
        return {nullptr, KvError::OutOfMem};
    }
    std::string_view page_view = builder.Finish();
    memcpy(new_page->PagePtr(), page_view.data(), page_view.size());
    new_page->SetPageId(page_id);
    err = WritePage(new_page);
    if (err != KvError::NoError)
    {
        shard->IndexManager()->FreeIndexPage(new_page);
        return {nullptr, err};
    }
    return {new_page, KvError::NoError};
}

KvError TruncateTask::Truncate(std::string_view trunc_pos)
{
    KvError err = shard->IndexManager()->MakeCowRoot(tbl_ident_, cow_meta_);
    CHECK_KV_ERR(err);
    if (cow_meta_.root_ == nullptr)
    {
        return KvError::NotFound;
    }

    if (trunc_pos.empty())
    {
        DeleteTree(cow_meta_.root_->GetPageId());
        return UpdateMeta(nullptr);
    }

    auto ret = TruncateIndexPage(cow_meta_.root_->GetPageId(), trunc_pos);
    CHECK_KV_ERR(ret.second);
    MemIndexPage *new_root = ret.first;

    if (new_root != nullptr)
    {
        // Prevent the root page from being evicted before update RootMeta.
        new_root->Pin();
        err = UpdateMeta(new_root);
        new_root->Unpin();
    }
    else
    {
        err = UpdateMeta(nullptr);
    }
    return err;
}

std::pair<bool, KvError> TruncateTask::TruncateDataPage(
    PageId page_id, std::string_view trunc_pos)
{
    auto [page, err] = LoadDataPage(page_id);
    if (err != KvError::NoError)
    {
        return {true, err};
    }

    const Comparator *cmp = shard->IndexManager()->GetComparator();
    DataPageIter iter{&page, Options()};
    data_page_builder_.Reset();
    while (iter.Next() && cmp->Compare(iter.Key(), trunc_pos) < 0)
    {
        bool ok =
            data_page_builder_.Add(iter.Key(), iter.Value(), iter.Timestamp());
        assert(ok);
    }
    while (iter.Next())
    {
        if (iter.IsOverflow())
        {
            err = DelOverflowValue(iter.Value());
            if (err != KvError::NoError)
            {
                return {true, err};
            }
        }
    }

    if (data_page_builder_.IsEmpty())
    {
        FreePage(page.GetPageId());

        uint32_t prev_page_id = page.PrevPageId();
        if (prev_page_id == MaxPageId)
        {
            return {false, KvError::NoError};
        }
        // The previous data page will become the new tail data page.
        // We don't need to update the previous page id of the next data page.
        auto [prev_page, err] = LoadDataPage(prev_page_id);
        if (err != KvError::NoError)
        {
            return {false, err};
        }
        prev_page.SetNextPageId(MaxPageId);
        err = WritePage(std::move(prev_page));
        return {false, err};
    }
    else
    {
        // This currently updated data page will become the new tail data page.
        DataPage new_page(page_id, Options()->data_page_size);
        std::string_view page_view = data_page_builder_.Finish();
        memcpy(new_page.PagePtr(), page_view.data(), page_view.size());
        new_page.SetNextPageId(MaxPageId);
        new_page.SetPrevPageId(page.PrevPageId());
        err = WritePage(std::move(new_page));
        return {true, err};
    }
}
}  // namespace kvstore