#include "common.h"

#include <cassert>
#include <catch2/catch_test_macros.hpp>
#include <cstdlib>
#include <filesystem>

kvstore::EloqStore *InitStore(const kvstore::KvOptions &opts)
{
    static std::unique_ptr<kvstore::EloqStore> eloqstore = nullptr;

    if (eloqstore)
    {
        const kvstore::KvOptions &old_opts = eloqstore->Options();
        if (old_opts == opts)
        {
            // Fast path: reuse the existing store
            if (eloqstore->IsStopped())
            {
                kvstore::KvError err = eloqstore->Start();
                CHECK(err == kvstore::KvError::NoError);
            }
            return eloqstore.get();
        }
        // Required options not equal to the options of the existing store, so
        // we need to stop and remove it.
        eloqstore->Stop();
        for (const std::string &db_path : old_opts.store_path)
        {
            std::filesystem::remove_all(db_path);
        }
        if (!old_opts.cloud_store_path.empty())
        {
            std::string command = "rclone delete ";
            command.append(old_opts.cloud_store_path);
            int res = system(command.c_str());
        }
    }

    for (const std::string &db_path : opts.store_path)
    {
        std::filesystem::remove_all(db_path);
    }
    if (!opts.cloud_store_path.empty())
    {
        std::string command = "rclone delete ";
        command.append(opts.cloud_store_path);
        int res = system(command.c_str());
    }

    eloqstore = std::make_unique<kvstore::EloqStore>(opts);
    kvstore::KvError err = eloqstore->Start();
    CHECK(err == kvstore::KvError::NoError);
    return eloqstore.get();
}
