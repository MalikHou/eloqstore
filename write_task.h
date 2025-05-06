#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "data_page.h"
#include "data_page_builder.h"
#include "error.h"
#include "index_page_builder.h"
#include "index_page_manager.h"
#include "mem_index_page.h"
#include "page_mapper.h"
#include "root_meta.h"
#include "task.h"
#include "types.h"
#include "write_tree_stack.h"

namespace kvstore
{
class WriteTask : public KvTask
{
public:
    WriteTask();
    WriteTask(const WriteTask &) = delete;

    virtual void Abort();
    void Reset(const TableIdent &tbl_id);
    const TableIdent &TableId() const;

    /**
     * @brief The index/data page has been flushed.
     * Enqueues the page into the cache replacement list (so that the page is
     * allowed to be evicted) if it is a index page.
     */
    void WritePageCallback(VarPage page, KvError err);

    KvError WaitWrite();
    // write_err_ record the result of the last failed write
    // request.
    KvError write_err_{KvError::NoError};

protected:
    KvError DeleteTree(PageId page_id);
    KvError DeleteDataPage(PageId page_id);

    /**
     * @brief Split and write overflow value into multiple pages.
     * @return overflow_ptrs_ store the encoded overflow pointers.
     */
    KvError WriteOverflowValue(std::string_view value);
    /**
     * @brief Delete overflow value.
     * @param encoded_ptrs The encoded overflow pointers.
     */
    KvError DelOverflowValue(std::string_view encoded_ptrs);

    /**
     * @brief Calculates the left boundary of the data page or the top index
     * page in the stack.
     *
     * @param is_data_page
     * @return std::string_view
     */
    std::string_view LeftBound(bool is_data_page);

    /**
     * @brief Calculates the right boundary of the data page or the top index
     * page in the stack.
     *
     * @param is_data_page
     * @return std::string
     */
    std::string RightBound(bool is_data_page);

    static void AdvanceDataPageIter(DataPageIter &iter, bool &is_valid);
    static void AdvanceIndexPageIter(IndexPageIter &iter, bool &is_valid);

    KvError FlushManifest(PageId root_page_id);
    KvError UpdateMeta(MemIndexPage *root);

    /**
     * @brief Request shard to create a compaction task if space amplification
     * factor is too big.
     */
    void CompactIfNeeded(PageMapper *mapper) const;
    KvError TriggerFileGC() const;

    std::pair<uint32_t, KvError> Seek(std::string_view key);
    std::pair<DataPage, KvError> LoadDataPage(PageId page_id);
    std::pair<OverflowPage, KvError> LoadOverflowPage(PageId page_id);

    std::pair<PageId, FilePageId> AllocatePage(PageId page_id);
    void FreePage(PageId page_id);

    FilePageId ToFilePage(PageId page_id);

    KvError WritePage(DataPage &&page);
    KvError WritePage(OverflowPage &&page);
    KvError WritePage(MemIndexPage *page);
    KvError WritePage(VarPage page, FilePageId file_page_id);

    TableIdent tbl_ident_;
    std::vector<std::unique_ptr<IndexStackEntry>> stack_;

    IndexPageBuilder idx_page_builder_;
    DataPageBuilder data_page_builder_;
    std::string overflow_ptrs_;

    CowRootMeta cow_meta_;
    ManifestBuilder wal_builder_;

    KvError FlushBatchPages();
    /**
     * @brief When the append-only mode is enabled, the pages ready to be
     * written are put into this batch. The batch is then sequentially
     * written to the disk when it is full or when a data file switch is
     * required.
     */
    std::vector<VarPage> batch_pages_;
    /**
     * @brief First file page id of this batch of pages.
     */
    FilePageId batch_fp_id_{MaxFilePageId};
};

}  // namespace kvstore