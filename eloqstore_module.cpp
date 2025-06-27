#include "eloqstore_module.h"

namespace kvstore
{
#ifdef ELOQ_MODULE_ENABLED
void EloqStoreModule::ExtThdStart(int thd_id)
{
    assert(static_cast<size_t>(thd_id) < shards_->size());
    Shard *shard = shards_->at(thd_id).get();
    shard->ext_proc_running_.store(true);
    shard->BindExtThd();
}

void EloqStoreModule::ExtThdEnd(int thd_id)
{
    assert(static_cast<size_t>(thd_id) < shards_->size());
    // Notify the io listener to wait the completed io
    Shard *shard = shards_->at(thd_id).get();
    shard->ext_proc_running_.store(false);
    shard->NotifyListener();
}

void EloqStoreModule::Process(int thd_id)
{
    // Process tasks on this shard
    assert(static_cast<size_t>(thd_id) < shards_->size());
    Shard *shard = shards_->at(thd_id).get();
    size_t req_cnt = 0;
    shard->WorkOneRound(req_cnt);
}

bool EloqStoreModule::HasTask(int thd_id) const
{
    assert(static_cast<size_t>(thd_id) < shards_->size());
    return !shards_->at(thd_id)->IsIdle();
}
#endif

}  // namespace kvstore
