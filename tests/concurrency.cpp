#include <glog/logging.h>

#include <cassert>
#include <catch2/catch_test_macros.hpp>
#include <cstdlib>
#include <thread>

#include "common.h"
#include "test_utils.h"

using namespace test_util;

TEST_CASE("concurrently write to partition", "[concurrency]")
{
    kvstore::EloqStore *store = InitStore(mem_store_opts);
    kvstore::TableIdent tbl_id("concurrent-write", 1);
    kvstore::WriteRequest requests[128];
    const uint32_t batch = 100;
    for (int i = 0; i < std::size(requests); i++)
    {
        std::vector<kvstore::WriteDataEntry> entries;
        for (int j = 0; j < batch; j++)
        {
            kvstore::WriteDataEntry &ent = entries.emplace_back();
            ent.key_ = Key(j);
            ent.val_ = Value(i);
            ent.timestamp_ = i;
            ent.op_ = kvstore::WriteOp::Upsert;
        }

        kvstore::WriteRequest &req = requests[i];
        req.SetArgs(tbl_id, std::move(entries));
        bool ok = store->ExecAsyn(&req, 0, [](kvstore::KvRequest *req) {});
        REQUIRE(ok);
    }

    for (kvstore::WriteRequest &req : requests)
    {
        while (!req.IsDone())
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }

    {
        kvstore::ScanRequest scan_req;
        std::string begin = Key(0);
        std::string end = Key(batch);
        scan_req.SetArgs(tbl_id, begin, end);
        store->ExecSync(&scan_req);
        for (auto [_, val, ts] : scan_req.entries_)
        {
            REQUIRE(val == Value(std::size(requests) - 1));
            REQUIRE(ts == std::size(requests) - 1);
        }
    }
}

TEST_CASE("easy concurrency test", "[persist][concurrency]")
{
    kvstore::EloqStore *store = InitStore(default_opts);
    ConcurrencyTester tester(store, "t1", 1, 32);
    tester.Init();
    tester.Run(5, 32, 20);
    tester.Clear();
}

TEST_CASE("hard concurrency test", "[persist][concurrency]")
{
    kvstore::KvOptions options = {
        .db_path = test_path,
        .num_threads = 4,
    };
    kvstore::EloqStore *store = InitStore(options);
    ConcurrencyTester tester(store, "t1", 10, 1000);
    tester.Init();
    tester.Run(10, 600, 5000);
    tester.Clear();
}

TEST_CASE("stress append only mode", "[persist][append]")
{
    kvstore::KvOptions options{
        .db_path = test_path,
        .data_append_mode = true,
    };
    kvstore::EloqStore *store = InitStore(options);

    ConcurrencyTester tester(store, "t1", 4, 1024);
    tester.Init();
    tester.Run(10, 10, 1000);
    tester.Clear();
}