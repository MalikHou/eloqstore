#include <glog/logging.h>

#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common.h"
#include "error.h"
#include "kv_options.h"
#include "test_utils.h"
#include "types.h"

using namespace test_util;

namespace fs = std::filesystem;

TEST_CASE("simple persist", "[persist]")
{
    kvstore::EloqStore *store = InitStore(default_opts);
    MapVerifier verify(test_tbl_id, store);
    verify.Upsert(100, 200);
    verify.Delete(100, 150);
    verify.Upsert(0, 50);
    verify.WriteRnd(0, 200);
    verify.WriteRnd(0, 200);
}

TEST_CASE("complex persist", "[persist]")
{
    kvstore::EloqStore *store = InitStore(default_opts);
    MapVerifier verify(test_tbl_id, store);
    for (int i = 0; i < 5; i++)
    {
        verify.WriteRnd(0, 2000);
    }
}

TEST_CASE("persist with restart", "[persist]")
{
    kvstore::EloqStore *store = InitStore(default_opts);

    std::vector<std::unique_ptr<MapVerifier>> tbls;
    for (uint32_t i = 0; i < 3; i++)
    {
        kvstore::TableIdent tbl_id{"t1", i};
        tbls.push_back(std::make_unique<MapVerifier>(tbl_id, store));
    }

    for (int i = 0; i < 5; i++)
    {
        for (auto &tbl : tbls)
        {
            tbl->WriteRnd(0, 1000);
        }
        store->Stop();
        store->Start();
    }
}

TEST_CASE("simple LRU for opened fd", "[persist]")
{
    kvstore::KvOptions options{
        .fd_limit = 12,
        .store_path = {test_path},
        .data_page_size = static_cast<uint16_t>(kvstore::page_align),
        .pages_per_file_shift = 1,
    };
    kvstore::EloqStore *store = InitStore(options);

    MapVerifier verify(test_tbl_id, store);
    verify.Upsert(1, 5000);
    verify.Upsert(5000, 10000);
    verify.Upsert(1, 10000);
}

TEST_CASE("complex LRU for opened fd", "[persist]")
{
    kvstore::KvOptions options{
        .fd_limit = 12,
        .store_path = {test_path},
        .data_page_size = static_cast<uint16_t>(kvstore::page_align),
        .pages_per_file_shift = 1,
    };
    kvstore::EloqStore *store = InitStore(options);

    std::vector<std::unique_ptr<MapVerifier>> tbls;
    for (uint32_t i = 0; i < 10; i++)
    {
        kvstore::TableIdent tbl_id{"t1", i};
        tbls.push_back(std::make_unique<MapVerifier>(tbl_id, store));
    }

    for (uint32_t i = 0; i < 3; i++)
    {
        for (auto &tbl : tbls)
        {
            tbl->Upsert(0, 5000);
        }
    }
}

TEST_CASE("detect corrupted page", "[persist][checksum]")
{
    kvstore::EloqStore *store = InitStore(default_opts);
    kvstore::TableIdent tbl_id = {"detect-corrupted", 1};
    {
        std::vector<kvstore::WriteDataEntry> entries;
        for (size_t idx = 0; idx < 10; ++idx)
        {
            entries.emplace_back(
                Key(idx), std::to_string(idx), 1, kvstore::WriteOp::Upsert);
        }
        kvstore::BatchWriteRequest req;
        req.SetArgs(tbl_id, std::move(entries));
        store->ExecSync(&req);
    }

    // corrupt it
    std::string datafile = std::string(test_path) + '/' + tbl_id.ToString() +
                           '/' + kvstore::DataFileName(0);
    std::fstream file(datafile,
                      std::ios::binary | std::ios::out | std::ios::in);
    REQUIRE(file);
    char c;
    file.seekg(10, std::ios::beg);
    file.read(&c, 1);
    REQUIRE(file);
    c += 1;
    file.seekp(10, std::ios::beg);
    file.write(&c, 1);
    REQUIRE(file);
    file.sync();
    REQUIRE(file);
    file.close();

    {
        kvstore::ScanRequest req;
        req.SetArgs(tbl_id, Key(0), Key(10));
        store->ExecSync(&req);
        REQUIRE(req.Error() == kvstore::KvError::Corrupted);
    }

    {
        // can't read success if the target key locate on the same page
        kvstore::ReadRequest req;
        req.SetArgs(tbl_id, Key(0));
        store->ExecSync(&req);
        REQUIRE(req.Error() == kvstore::KvError::Corrupted);
    }
}

TEST_CASE("overflow kv", "[persist][overflow_kv]")
{
    kvstore::EloqStore *store = InitStore(default_opts);

    const kvstore::TableIdent tbl_id("overflow", 0);
    const uint32_t biggest = (128 << 20);
    MapVerifier verifier(tbl_id, store);

    kvstore::BatchWriteRequest write_req;
    write_req.SetTableId(tbl_id);

    for (uint32_t sz = 1; sz <= biggest; sz <<= 1)
    {
        write_req.AddWrite(Key(sz), Value(sz, sz), 1, kvstore::WriteOp::Upsert);
    }
    verifier.ExecWrite(&write_req);

    verifier.Read(Key(1 << 20));

    verifier.Scan(Key(2 << 10), Key(16 << 20));

    for (uint32_t i : {1, 100, 1024, 5000, 131072})
    {
        write_req.AddWrite(
            Key(i), Value(i + 1, i), 2, kvstore::WriteOp::Upsert);
    }
    verifier.ExecWrite(&write_req);

    verifier.Read(Key(5000));
}

TEST_CASE("random overflow kv", "[persist][overflow_kv]")
{
    kvstore::EloqStore *store = InitStore(default_opts);
    MapVerifier verifier(test_tbl_id, store);
    verifier.SetValueSize(5000);
    verifier.WriteRnd(1, 100);
    verifier.SetValueSize(10000);
    verifier.WriteRnd(1, 100);
    verifier.SetValueSize(1 << 20);
    verifier.WriteRnd(1, 100, 20, 10);
    verifier.WriteRnd(1, 100, 20, 10);
    verifier.SetValueSize(5000);
    verifier.WriteRnd(1, 100);
}

TEST_CASE("easy append only mode", "[persist][append]")
{
    kvstore::KvOptions options{
        .store_path = {test_path},
        .data_append_mode = true,
    };
    kvstore::EloqStore *store = InitStore(options);

    MapVerifier verify(test_tbl_id, store);
    verify.SetValueSize(1000);

    verify.WriteRnd(0, 1000);
    verify.WriteRnd(1000, 2000);
    verify.WriteRnd(500, 1500);
}

TEST_CASE("hard append only mode", "[persist][append]")
{
    kvstore::KvOptions options{
        .file_amplify_factor = 2,
        .store_path = {test_path},
        .pages_per_file_shift = 8,
        .data_append_mode = true,
    };
    kvstore::EloqStore *store = InitStore(options);

    MapVerifier verify(test_tbl_id, store, false);
    verify.SetValueSize(10000);

    for (int i = 0; i < 50; i += 5)
    {
        verify.WriteRnd(0, 1000, i);
    }
    verify.Validate();
}

size_t GetDirEntriesCount(const fs::path &dir_path)
{
    if (!fs::exists(dir_path) || !fs::is_directory(dir_path))
    {
        LOG(FATAL) << "Invalid directory";
    }

    size_t count = 0;
    for (const auto &entry : fs::directory_iterator(dir_path))
    {
        count++;
    }
    return count;
};

TEST_CASE("file garbage collector", "[GC]")
{
    kvstore::KvOptions options{
        .num_retained_archives = 0,
        .file_amplify_factor = 2,
        .store_path = {test_path},
        .pages_per_file_shift = 8,
        .data_append_mode = true,
    };
    kvstore::EloqStore *store = InitStore(options);
    const fs::path dir_path = fs::path(test_path) / test_tbl_id.ToString();

    MapVerifier tester(test_tbl_id, store, false);
    tester.SetValueSize(10000);

    tester.Upsert(0, 1000);
    tester.Upsert(0, 1000);
    size_t max_cnt = GetDirEntriesCount(dir_path);
    for (int i = 0; i < 20; i++)
    {
        tester.Upsert(500, 1000);
        // Do validate after each write to try to ensure the file GC is
        // finished.
        tester.Validate();
        size_t cnt = GetDirEntriesCount(dir_path);
        CHECK(cnt <= max_cnt);
    }
}

TEST_CASE("append mode with restart", "[persist]")
{
    kvstore::KvOptions options{
        .num_retained_archives = 0,
        .file_amplify_factor = 2,
        .store_path = {test_path},
        .pages_per_file_shift = 8,
        .data_append_mode = true,
    };

    kvstore::EloqStore *store = InitStore(options);

    std::vector<std::unique_ptr<MapVerifier>> tbls;
    for (uint32_t i = 0; i < 3; i++)
    {
        kvstore::TableIdent tbl_id{"t1", i};
        auto tester = std::make_unique<MapVerifier>(tbl_id, store, false);
        tester->SetValueSize(10000);
        tbls.push_back(std::move(tester));
    }

    for (int i = 0; i < 5; i++)
    {
        for (auto &tbl : tbls)
        {
            tbl->WriteRnd(0, 1000, 10, 90);
        }
        store->Stop();
        store->Start();
        for (auto &tbl : tbls)
        {
            tbl->Validate();
        }
    }
}