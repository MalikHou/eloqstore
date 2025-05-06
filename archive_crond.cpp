#include "archive_crond.h"

#include <cassert>
#include <filesystem>
#include <mutex>

#include "eloq_store.h"
#include "utils.h"

namespace fs = std::filesystem;

namespace kvstore
{

ArchiveCrond::ArchiveCrond(EloqStore *store) : store_(store)
{
}

void ArchiveCrond::Start()
{
    assert(!thd_.joinable());
    stopped_ = false;
    thd_ = std::thread(&ArchiveCrond::Crond, this);
    LOG(INFO) << "Archive crond started";
}

void ArchiveCrond::Stop()
{
    mu_.lock();
    stopped_ = true;
    mu_.unlock();
    if (thd_.joinable())
    {
        cond_var_.notify_one();
        thd_.join();
        LOG(INFO) << "Archive crond stopped";
    }
}

bool ArchiveCrond::IsStopped()
{
    std::scoped_lock lk(mu_);
    return stopped_;
}

void ArchiveCrond::Crond()
{
    const uint64_t interval_secs = store_->Options().archive_interval_secs;
    last_archive_ts_ = utils::UnixTs<std::chrono::seconds>();
    while (!IsStopped())
    {
        // Loop required to prevent spurious wakeups
        auto elapsed = utils::UnixTs<std::chrono::seconds>() - last_archive_ts_;
        while (elapsed < interval_secs)
        {
            auto wait_period = std::chrono::seconds(interval_secs - elapsed);
            std::unique_lock lk(mu_);
            cond_var_.wait_for(lk, wait_period, [this] { return stopped_; });
            if (stopped_)
            {
                // Stopped during wait.
                return;
            }
            elapsed = utils::UnixTs<std::chrono::seconds>() - last_archive_ts_;
        }

        StartArchiving();
        last_archive_ts_ = utils::UnixTs<std::chrono::seconds>();
    }
}

void ArchiveCrond::StartArchiving()
{
    LOG(INFO) << "Start archiving all partitions";
    const uint32_t archive_batch = store_->Options().max_archive_tasks;
    std::vector<TableIdent> table_ids;
    table_ids.reserve(archive_batch);
    std::vector<ArchiveRequest> requests(archive_batch);
    size_t total_partitions = 0;
    size_t fail_cnt = 0;

    auto do_archiving = [&](std::span<TableIdent> tbl_ids)
    {
        const size_t batch_size = tbl_ids.size();
        total_partitions += batch_size;
        for (size_t i = 0; i < batch_size; i++)
        {
            requests[i].SetArgs(std::move(tbl_ids[i]));
            bool ok = store_->ExecAsyn(&requests[i]);
            LOG_IF(FATAL, !ok) << "Failed to send archive request";
        }
        for (size_t i = 0; i < batch_size; i++)
        {
            const ArchiveRequest &req = requests[i];
            req.Wait();
            if (req.Error() != KvError::NoError)
            {
                fail_cnt++;
            }
        }
    };

    for (auto &ent : fs::directory_iterator{store_->Options().db_path})
    {
        if (!ent.is_directory())
        {
            continue;
        }
        TableIdent tbl_id = TableIdent::FromString(ent.path().filename());
        if (tbl_id.tbl_name_.empty())
        {
            LOG(WARNING) << "unexpected partition " << ent.path();
            continue;
        }
        table_ids.emplace_back(std::move(tbl_id));

        assert(table_ids.size() <= archive_batch);
        if (table_ids.size() == archive_batch)
        {
            do_archiving(table_ids);
            table_ids.clear();
        }
    }
    if (!table_ids.empty())
    {
        do_archiving(table_ids);
    }

    LOG(INFO) << "Finished archiving " << total_partitions << " partitions, "
              << fail_cnt << " failed";
}
}  // namespace kvstore