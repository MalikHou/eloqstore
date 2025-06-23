#include "archive_task.h"

#include "shard.h"
#include "utils.h"

namespace kvstore
{
KvError ArchiveTask::CreateArchive()
{
    assert(Options()->data_append_mode);
    assert(Options()->num_retained_archives > 0);
    auto [meta, err] = shard->IndexManager()->FindRoot(tbl_ident_);
    CHECK_KV_ERR(err);
    PageId root = meta->root_id_;
    if (root == MaxPageId)
    {
        return KvError::NotFound;
    }

    PageId ttl_root = meta->ttl_root_id_;
    MappingSnapshot *mapping = meta->mapper_->GetMapping();
    FilePageId max_fp_id = meta->mapper_->FilePgAllocator()->MaxFilePageId();
    std::string_view snapshot =
        wal_builder_.Snapshot(root, ttl_root, mapping, max_fp_id);

    uint64_t current_ts = utils::UnixTs<chrono::microseconds>();
    err = IoMgr()->CreateArchive(tbl_ident_, snapshot, current_ts);
    CHECK_KV_ERR(err);
    LOG(INFO) << "created archive for partition " << tbl_ident_ << " at "
              << current_ts;
    return KvError::NoError;
}
}  // namespace kvstore