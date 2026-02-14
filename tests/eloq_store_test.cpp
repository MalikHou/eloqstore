#include "eloq_store.h"

#include <atomic>
#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <limits>
#include <memory>
#include <thread>

#include "circular_queue.h"
#include "common.h"
#include "test_utils.h"
#include "time_wheel.h"

namespace fs = std::filesystem;

eloqstore::KvOptions CreateValidOptions(const fs::path &test_dir)
{
    eloqstore::KvOptions options;
    options.store_path = {test_dir};
    options.num_threads = 2;
    options.data_page_size = 4096;
    options.coroutine_stack_size = 8192;
    options.overflow_pointers = 4;
    options.max_write_batch_pages = 16;
    options.fd_limit = 100;
    return options;
}

fs::path CreateTestDir(const std::string &suffix = "")
{
    fs::path test_dir = fs::temp_directory_path() / ("eloqstore_test" + suffix);
    fs::create_directories(test_dir);
    return test_dir;
}

void CleanupTestDir(const fs::path &test_dir)
{
    if (fs::exists(test_dir))
    {
        fs::remove_all(test_dir);
    }
}
TEST_CASE("EloqStore ValidateOptions validates all parameters", "[eloq_store]")
{
    auto test_dir = CreateTestDir("_validate_options");
    auto options = CreateValidOptions(test_dir);

    // Test valid configuration
    REQUIRE(eloqstore::EloqStore::ValidateOptions(options) == true);

    // Test data_page_size that is not page-aligned
    options.data_page_size = 4097;  // not page-aligned
    REQUIRE(eloqstore::EloqStore::ValidateOptions(options) == false);
    options = CreateValidOptions(test_dir);  // restore valid value

    // Test coroutine_stack_size that is not page-aligned
    options.coroutine_stack_size = 8193;  // not page-aligned
    REQUIRE(eloqstore::EloqStore::ValidateOptions(options) == false);
    options = CreateValidOptions(test_dir);  // restore valid value

    // Test invalid overflow_pointers
    options.overflow_pointers = 0;
    REQUIRE(eloqstore::EloqStore::ValidateOptions(options) == false);
    options = CreateValidOptions(test_dir);  // restore valid value

    // Test invalid max_write_batch_pages
    options.max_write_batch_pages = 0;
    REQUIRE(eloqstore::EloqStore::ValidateOptions(options) == false);
    options = CreateValidOptions(test_dir);  // restore valid value

    // Test invalid read_request_timeout_ms (> time wheel max delay)
    options.read_request_timeout_ms = eloqstore::TimeWheel::kMaxDelayMs + 1;
    REQUIRE(eloqstore::EloqStore::ValidateOptions(options) == false);
    options = CreateValidOptions(test_dir);  // restore valid value

    // Test invalid max_cloud_concurrency (cloud mode)
    options.cloud_store_path = "test";
    options.max_cloud_concurrency = 0;
    REQUIRE(eloqstore::EloqStore::ValidateOptions(options) == false);
    options = CreateValidOptions(test_dir);  // restore valid value

    // Test invalid cloud_request_threads (cloud mode)
    options.cloud_store_path = "test";
    options.cloud_request_threads = 0;
    REQUIRE(eloqstore::EloqStore::ValidateOptions(options) == false);
    options = CreateValidOptions(test_dir);  // restore valid value

    // Test invalid max_upload_batch (cloud mode)
    options.cloud_store_path = "test";
    options.max_upload_batch = 0;
    REQUIRE(eloqstore::EloqStore::ValidateOptions(options) == false);
    options = CreateValidOptions(test_dir);  // restore valid value

    // Test invalid cloud_upload_max_retries (cloud mode)
    options.cloud_store_path = "test";
    options.cloud_upload_max_retries = 0;
    REQUIRE(eloqstore::EloqStore::ValidateOptions(options) == false);
    options = CreateValidOptions(test_dir);  // restore valid value

    // Cloud storage configuration: auto fix local space limit and append mode
    options.cloud_store_path = "test";
    options.local_space_limit = 0;
    options.data_append_mode = false;
    REQUIRE(eloqstore::EloqStore::ValidateOptions(options) == true);
    REQUIRE(options.local_space_limit == size_t(1) * eloqstore::TB);
    REQUIRE(options.data_append_mode == true);

    // fd_limit is clamped when exceeding local space budget in cloud mode
    options = CreateValidOptions(test_dir);
    options.local_space_limit = 10ULL * 1024 * 1024 * 1024;
    options.fd_limit = 1000000;
    options.pages_per_file_shift = 12;
    options.data_page_size = 4096;
    options.data_append_mode = true;
    options.cloud_store_path = "test";
    REQUIRE(eloqstore::EloqStore::ValidateOptions(options) == true);
    const uint64_t per_file = static_cast<uint64_t>(options.data_page_size) *
                              (1ULL << options.pages_per_file_shift);
    REQUIRE(options.fd_limit ==
            static_cast<uint32_t>(options.local_space_limit / per_file));

    // prewarm without cloud path is disabled automatically
    options = CreateValidOptions(test_dir);
    options.prewarm_cloud_cache = true;
    REQUIRE(eloqstore::EloqStore::ValidateOptions(options) == true);
    REQUIRE(options.prewarm_cloud_cache == false);

    CleanupTestDir(test_dir);
}

TEST_CASE("EloqStore Start validates local store paths", "[eloq_store]")
{
    auto test_dir = CreateTestDir("_start_store_space");
    auto options = CreateValidOptions(test_dir);

    // test safe path
    {
        eloqstore::EloqStore store(options);
        auto err = store.Start();
        REQUIRE(err == eloqstore::KvError::NoError);
        store.Stop();
    }

    // test the non exist path
    fs::path nonexistent_path =
        fs::temp_directory_path() / "nonexistent_eloqstore_test";
    options.store_path = {nonexistent_path};
    {
        eloqstore::EloqStore store(options);
        auto err = store.Start();
        REQUIRE(err == eloqstore::KvError::NoError);
        REQUIRE(fs::exists(nonexistent_path));
        REQUIRE(fs::is_directory(nonexistent_path));
        store.Stop();
    }

    // the path is file
    fs::path file_path = fs::temp_directory_path() / "eloqstore_file_test";
    std::ofstream file(file_path);
    file.close();
    options.store_path = {file_path};
    {
        eloqstore::EloqStore store(options);
        auto err = store.Start();
        REQUIRE(err == eloqstore::KvError::InvalidArgs);
        store.Stop();
    }
    fs::remove(file_path);

    // not directory
    auto test_dir_with_file = CreateTestDir("_with_file");
    fs::path file_in_dir = test_dir_with_file / "not_a_directory.txt";
    std::ofstream file_in_dir_stream(file_in_dir);
    file_in_dir_stream.close();
    options.store_path = {test_dir_with_file};
    {
        eloqstore::EloqStore store(options);
        auto err = store.Start();
        REQUIRE(err == eloqstore::KvError::InvalidArgs);
        store.Stop();
    }

    CleanupTestDir(test_dir);
    CleanupTestDir(nonexistent_path);
    CleanupTestDir(test_dir_with_file);
}

// test the basic life cycle
TEST_CASE("EloqStore basic lifecycle management", "[eloq_store]")
{
    auto test_dir = CreateTestDir("_lifecycle");
    auto options = CreateValidOptions(test_dir);
    eloqstore::EloqStore store(options);

    REQUIRE(store.IsStopped());

    auto err = store.Start();
    REQUIRE(err == eloqstore::KvError::NoError);
    REQUIRE_FALSE(store.IsStopped());

    store.Stop();
    REQUIRE(store.IsStopped());

    CleanupTestDir(test_dir);
}

// test repeat start
TEST_CASE("EloqStore handles multiple start calls", "[eloq_store]")
{
    auto test_dir = CreateTestDir("_multi_start");
    auto options = CreateValidOptions(test_dir);
    eloqstore::EloqStore store(options);

    // first start
    auto err1 = store.Start();
    REQUIRE(err1 == eloqstore::KvError::NoError);
    REQUIRE_FALSE(store.IsStopped());

    // the second should be safe
    auto err2 = store.Start();

    store.Stop();
    CleanupTestDir(test_dir);
}

// test repeat stop
TEST_CASE("EloqStore handles multiple stop calls", "[eloq_store]")
{
    auto test_dir = CreateTestDir("_multi_stop");
    auto options = CreateValidOptions(test_dir);
    eloqstore::EloqStore store(options);

    auto err = store.Start();
    REQUIRE(err == eloqstore::KvError::NoError);

    // first stop
    store.Stop();
    REQUIRE(store.IsStopped());

    // the second time should be safe
    REQUIRE_NOTHROW(store.Stop());
    REQUIRE(store.IsStopped());

    CleanupTestDir(test_dir);
}

TEST_CASE("EloqStore handles requests when stopped", "[eloq_store]")
{
    auto test_dir = CreateTestDir("_stopped_requests");
    auto options = CreateValidOptions(test_dir);
    eloqstore::EloqStore store(options);

    REQUIRE(store.IsStopped());

    eloqstore::ReadRequest request;
    eloqstore::TableIdent tbl_id("test_table", 0);
    request.SetArgs(tbl_id, "test_key");

    store.ExecSync(&request);
    REQUIRE(request.Error() == eloqstore::KvError::NotRunning);

    CleanupTestDir(test_dir);
}

TEST_CASE("CircularQueue rejects zero capacity", "[cqueue]")
{
    REQUIRE_THROWS_AS(eloqstore::CircularQueue<int>(0), std::invalid_argument);
}

TEST_CASE("CircularQueue reset rejects zero capacity", "[cqueue]")
{
    eloqstore::CircularQueue<int> q;
    REQUIRE_THROWS_AS(q.Reset(0), std::invalid_argument);
}

TEST_CASE("CircularQueue reset assigns new capacity", "[cqueue]")
{
    eloqstore::CircularQueue<int> q;
    q.Enqueue(1);
    q.Enqueue(2);

    q.Reset(4);
    REQUIRE(q.Size() == 0);
    REQUIRE(q.Capacity() == 4);

    q.Enqueue(3);
    q.Enqueue(4);
    REQUIRE(q.Size() == 2);
    REQUIRE(q.Get(0) == 3);
    REQUIRE(q.Get(1) == 4);
}

TEST_CASE("CircularQueue EnqueueAsFirst prepends items", "[cqueue]")
{
    eloqstore::CircularQueue<int> q;
    q.Enqueue(3);
    q.Enqueue(4);
    q.Enqueue(5);

    q.EnqueueAsFirst(2);
    q.EnqueueAsFirst(1);

    REQUIRE(q.Size() == 5);
    REQUIRE(q.Get(0) == 1);
    REQUIRE(q.Get(1) == 2);
    REQUIRE(q.Get(2) == 3);
    REQUIRE(q.Get(3) == 4);
    REQUIRE(q.Get(4) == 5);
}

TEST_CASE("KvOptions loads read_request_timeout_ms from ini",
          "[eloq_store][timeout]")
{
    auto ini_path = fs::temp_directory_path() / "eloqstore_timeout_opts.ini";
    {
        std::ofstream ini_file(ini_path);
        REQUIRE(ini_file.is_open());
        ini_file << "[run]\n";
        ini_file << "read_request_timeout_ms = 123\n";
        ini_file << "cloud_upload_max_retries = 17\n";
        ini_file << "[permanent]\n";
        ini_file << "data_page_size = 4KB\n";
        ini_file << "data_file_size = 8MB\n";
    }

    eloqstore::KvOptions options;
    REQUIRE(options.LoadFromIni(ini_path.c_str()) == 0);
    REQUIRE(options.read_request_timeout_ms == 123);
    REQUIRE(options.cloud_upload_max_retries == 17);
    fs::remove(ini_path);
}

TEST_CASE("Read/Floor/Scan request timeout constructors",
          "[eloq_store][timeout]")
{
    eloqstore::ReadRequest read_req(10);
    eloqstore::FloorRequest floor_req(20);
    eloqstore::ScanRequest scan_req(30);

    REQUIRE(read_req.TimeOutMs() == 10);
    REQUIRE(floor_req.TimeOutMs() == 20);
    REQUIRE(scan_req.TimeOutMs() == 30);
}

TEST_CASE("Read/Floor/Scan request timeout rejects values over 60s",
          "[eloq_store][timeout]")
{
    eloqstore::KvOptions options;
    options.num_threads = 1;

    eloqstore::EloqStore store(options);
    REQUIRE(store.Start() == eloqstore::KvError::NoError);

    eloqstore::TableIdent tbl_id("timeout_limit_table", 0);

    eloqstore::ReadRequest read_req(60'001);
    read_req.SetArgs(tbl_id, test_util::Key(0, 12));
    store.ExecSync(&read_req);
    REQUIRE(read_req.Error() == eloqstore::KvError::InvalidArgs);

    eloqstore::FloorRequest floor_req(60'001);
    floor_req.SetArgs(tbl_id, test_util::Key(0, 12));
    store.ExecSync(&floor_req);
    REQUIRE(floor_req.Error() == eloqstore::KvError::InvalidArgs);

    eloqstore::ScanRequest scan_req(60'001);
    scan_req.SetArgs(tbl_id, std::string_view{}, std::string_view{});
    scan_req.SetPagination(std::numeric_limits<size_t>::max(),
                           std::numeric_limits<size_t>::max());
    store.ExecSync(&scan_req);
    REQUIRE(scan_req.Error() == eloqstore::KvError::InvalidArgs);

    store.Stop();
}

TEST_CASE("EloqStore read timeout applies to scan and request timeout can "
          "override default",
          "[eloq_store][timeout]")
{
    eloqstore::KvOptions options;
    options.num_threads = 1;
    options.read_request_timeout_ms = 1;

    eloqstore::EloqStore store(options);
    REQUIRE(store.Start() == eloqstore::KvError::NoError);

    eloqstore::TableIdent tbl_id("timeout_table", 0);
    constexpr uint32_t kBatchCount = 50;
    constexpr uint32_t kBatchSize = 512;
    uint64_t sequence = 0;
    for (uint32_t batch = 0; batch < kBatchCount; ++batch)
    {
        std::vector<eloqstore::WriteDataEntry> entries;
        entries.reserve(kBatchSize);
        for (uint32_t i = 0; i < kBatchSize; ++i)
        {
            entries.emplace_back(test_util::Key(sequence, 12),
                                 std::string(512, 'v'),
                                 sequence,
                                 eloqstore::WriteOp::Upsert);
            ++sequence;
        }
        eloqstore::BatchWriteRequest write_req;
        write_req.SetArgs(tbl_id, std::move(entries));
        store.ExecSync(&write_req);
        REQUIRE(write_req.Error() == eloqstore::KvError::NoError);
    }

    bool saw_timeout = false;
    for (int attempt = 0; attempt < 5 && !saw_timeout; ++attempt)
    {
        eloqstore::ScanRequest scan_req;
        scan_req.SetArgs(tbl_id, std::string_view{}, std::string_view{});
        scan_req.SetPagination(std::numeric_limits<size_t>::max(),
                               std::numeric_limits<size_t>::max());
        store.ExecSync(&scan_req);
        saw_timeout = scan_req.Error() == eloqstore::KvError::Timeout;
    }
    REQUIRE(saw_timeout);

    eloqstore::ReadRequest read_req(10'000);
    read_req.SetArgs(tbl_id, test_util::Key(0, 12));
    store.ExecSync(&read_req);
    REQUIRE(read_req.Error() == eloqstore::KvError::NoError);

    store.Stop();
}
