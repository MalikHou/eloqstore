#include "object_store.h"

#include <glog/logging.h>

namespace kvstore
{
ObjectStore::~ObjectStore()
{
    Stop();
}

void ObjectStore::Start()
{
    const uint16_t n_workers = options_->rclone_threads;
    workers_.reserve(n_workers);
    for (int i = 0; i < n_workers; i++)
    {
        workers_.emplace_back([this] { WorkLoop(); });
    }
    LOG(INFO) << "object store syncer started";
}

void ObjectStore::Stop()
{
    if (workers_.empty())
    {
        return;
    }
    // Send stop signal to all workers.
    StopSignal stop;
    for (auto &w : workers_)
    {
        submit_q_.enqueue(&stop);
    }
    for (auto &w : workers_)
    {
        w.join();
    }
    workers_.clear();
    LOG(INFO) << "object store syncer stopped";
}

bool ObjectStore::IsRunning() const
{
    return !workers_.empty();
}

void ObjectStore::WorkLoop()
{
    std::string command;
    command.reserve(512);
    while (true)
    {
        Task *task;
        submit_q_.wait_dequeue(task);
        if (task->TaskType() == Task::Type::Stop)
        {
            return;
        }

        fs::path dir_path = task->tbl_id_->StorePath(options_->store_path);
        switch (task->TaskType())
        {
        case Task::Type::Download:
        {
            auto dl_task = static_cast<DownloadTask *>(task);
            command = "rclone copyto --error-on-no-transfer ";

            command.append(options_->cloud_store_path);
            command.push_back('/');
            command.append(dl_task->tbl_id_->ToString());
            command.push_back('/');
            command.append(dl_task->filename_);

            command.push_back(' ');

            command.append(dir_path);
            command.push_back('/');
            command.append(dl_task->filename_);
            break;
        }
        case Task::Type::Upload:
        {
            auto up_task = static_cast<UploadTask *>(task);
            assert(!up_task->filenames_.empty());
            command = "rclone copy --no-check-dest ";

            command.append("--transfers ");
            size_t transfers = std::min(up_task->filenames_.size(), size_t(32));
            command.append(std::to_string(transfers));
            command.push_back(' ');

            for (const std::string &filename : up_task->filenames_)
            {
                command.append("--include ");
                command.append(filename);
                command.push_back(' ');
            }

            command.append(dir_path);

            command.push_back(' ');

            command.append(options_->cloud_store_path);
            command.push_back('/');
            command.append(up_task->tbl_id_->ToString());
            break;
        }
        default:
            LOG(FATAL) << "Unknown task type " << int(task->TaskType());
        }

        task->error_ = ExecRclone(command);

        complete_q_.enqueue(task);
    }
}

KvError ObjectStore::ExecRclone(std::string_view cmd) const
{
    // rclone retry params are set to low-level-retries=10 and retries=3 by
    // default.
    uint8_t retry_cnt = 0;
    while (true)
    {
        int res = system(cmd.data());
        DLOG(INFO) << cmd << " => " << res;

        // See https://rclone.org/docs/#list-of-exit-codes
        switch (WEXITSTATUS(res))
        {
        case 0:  // Success
            return KvError::NoError;
        case 2:  // Syntax or usage error
            LOG(FATAL) << "Rclone syntax or usage error : " << cmd;
        case 3:  // Directory not found
        case 4:  // File not found
            return KvError::NotFound;
        case 5:  // Temporary error
            if (retry_cnt++ < 3)
            {
                continue;  // Try again.
            }
            return KvError::TryAgain;
        case 9:  // Operation successful, but no files transferred
            // Download source not found. Reported by '--error-on-no-transfer'
            return KvError::NotFound;
        case 10:  // Duration exceeded
            // This should not happen because --max-duration defaults to off.
            return KvError::Timeout;
        case 1:  // Error not otherwise categorised
        case 6:  // Less serious errors (NoRetry errors)
        case 7:  // Fatal error
        case 8:  // Transfer exceeded (--max-transfer defaults to off)
        default:
            return KvError::CloudErr;
        }
    }
}
}  // namespace kvstore