#include <bvar/bvar.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <limits>
#include <memory>
#include <random>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "coding.h"
#include "eloq_store.h"
#include "time_wheel.h"
#include "utils.h"

// https://github.com/cameron314/concurrentqueue/issues/280
#undef BLOCK_SIZE
#include "../external/concurrentqueue/blockingconcurrentqueue.h"

DEFINE_string(kvoptions, "", "Path to config file of EloqStore options");
DEFINE_string(workload,
              "scan",
              "workload type for timeout storm (read/scan/mixed)");
DEFINE_uint32(partitions, 128, "number of partitions");
DEFINE_uint32(clients_per_part,
              1,
              "number of concurrent clients per partition per thread");
DEFINE_uint32(client_threads, 1, "number of client threads");
DEFINE_uint32(test_secs, 30, "benchmark duration in seconds");
DEFINE_double(report_interval_sec,
              1.0,
              "minimum seconds between periodic reports");
DEFINE_uint32(request_timeout_ms,
              1,
              "request timeout in milliseconds for each read/scan request");
DEFINE_uint32(max_key, 1'000'000, "max random key value");
DEFINE_uint32(kv_size, 256, "KV bytes used by optional data prepare");
DEFINE_uint64(scan_bytes,
              0,
              "bytes to scan per request; 0 means scan to end of partition");
DEFINE_bool(prepare_data,
            false,
            "if true, pre-load keys before running timeout storm");
DEFINE_uint32(prepare_keys_per_part,
              200'000,
              "number of upserted keys per partition when prepare_data=true");
DEFINE_uint32(prepare_batch_size,
              4096,
              "batch size used by data pre-load when prepare_data=true");

namespace
{
using namespace std::chrono;

constexpr char kTableName[] = "timeout_storm_bm";

bvar::LatencyRecorder g_all_latency("timeout_storm_all_latency");
bvar::LatencyRecorder g_timeout_latency("timeout_storm_timeout_latency");
std::atomic<uint64_t> g_all_max_latency_us{0};
std::atomic<uint64_t> g_timeout_max_latency_us{0};

void UpdateMaxLatency(std::atomic<uint64_t> &target, uint64_t latency_us)
{
    uint64_t prev = target.load(std::memory_order_relaxed);
    while (prev < latency_us &&
           !target.compare_exchange_weak(
               prev, latency_us, std::memory_order_relaxed))
    {
    }
}

uint64_t NowMicroseconds()
{
    return utils::UnixTs<microseconds>();
}

void EncodeKey(char *dst, uint64_t key)
{
    eloqstore::EncodeFixed64(dst, eloqstore::ToBigEndian(key));
}

enum class Workload
{
    Read,
    Scan,
    Mixed
};

Workload ParseWorkload(const std::string &name)
{
    if (name == "read")
    {
        return Workload::Read;
    }
    if (name == "scan")
    {
        return Workload::Scan;
    }
    if (name == "mixed")
    {
        return Workload::Mixed;
    }
    LOG(FATAL) << "unsupported workload: " << name
               << ", expected read/scan/mixed";
}

class BaseClient
{
public:
    virtual ~BaseClient() = default;
    virtual void Prepare(std::mt19937_64 &rng) = 0;
    virtual eloqstore::KvRequest *Request() = 0;

    uint64_t start_ts_us_{0};
    uint64_t latency_us_{0};
};

class ReadClient : public BaseClient
{
public:
    ReadClient(uint32_t timeout_ms, uint32_t partition)
        : request_(timeout_ms), table_id_(kTableName, partition)
    {
        EncodeKey(key_, 0);
        request_.SetArgs(table_id_, std::string_view(key_, sizeof(key_)));
    }

    void Prepare(std::mt19937_64 &rng) override
    {
        EncodeKey(key_, rng() % FLAGS_max_key);
    }

    eloqstore::KvRequest *Request() override
    {
        return &request_;
    }

private:
    eloqstore::ReadRequest request_;
    eloqstore::TableIdent table_id_;
    char key_[sizeof(uint64_t)];
};

class ScanClient : public BaseClient
{
public:
    ScanClient(uint32_t timeout_ms, uint32_t partition)
        : request_(timeout_ms), table_id_(kTableName, partition)
    {
        request_.SetPagination(std::numeric_limits<size_t>::max(),
                               std::numeric_limits<size_t>::max());
        EncodeKey(begin_key_, 0);
        request_.SetArgs(table_id_,
                         std::string_view(begin_key_, sizeof(begin_key_)),
                         std::string_view{});
    }

    void Prepare(std::mt19937_64 &rng) override
    {
        if (FLAGS_scan_bytes == 0)
        {
            uint64_t begin = rng() % FLAGS_max_key;
            EncodeKey(begin_key_, begin);
            request_.SetArgs(table_id_,
                             std::string_view(begin_key_, sizeof(begin_key_)),
                             std::string_view{});
            return;
        }

        const uint64_t value_bytes =
            std::max<uint64_t>(1, FLAGS_kv_size - sizeof(uint64_t));
        const uint64_t scan_kvs =
            std::max<uint64_t>(1, FLAGS_scan_bytes / value_bytes);
        const uint64_t max_begin =
            FLAGS_max_key > scan_kvs ? (FLAGS_max_key - scan_kvs) : 0;
        const uint64_t begin = max_begin == 0 ? 0 : (rng() % max_begin);
        const uint64_t end = begin + scan_kvs;
        EncodeKey(begin_key_, begin);
        EncodeKey(end_key_, end);
        request_.SetArgs(table_id_,
                         std::string_view(begin_key_, sizeof(begin_key_)),
                         std::string_view(end_key_, sizeof(end_key_)));
    }

    eloqstore::KvRequest *Request() override
    {
        return &request_;
    }

private:
    eloqstore::ScanRequest request_;
    eloqstore::TableIdent table_id_;
    char begin_key_[sizeof(uint64_t)];
    char end_key_[sizeof(uint64_t)];
};

struct ThreadStats
{
    uint64_t total{0};
    uint64_t timeout{0};
    uint64_t non_timeout{0};
    uint64_t other_errors{0};
    uint64_t send_fail{0};
    uint64_t elapsed_us{0};
};

bool SubmitRequest(eloqstore::EloqStore *store,
                   BaseClient *client,
                   const std::function<void(eloqstore::KvRequest *)> &callback,
                   std::mt19937_64 &rng)
{
    client->Prepare(rng);
    client->start_ts_us_ = NowMicroseconds();
    for (uint32_t retry = 0; retry < 64; ++retry)
    {
        if (store->ExecAsyn(
                client->Request(), reinterpret_cast<uint64_t>(client), callback))
        {
            return true;
        }
        std::this_thread::yield();
    }
    return false;
}

void PrepareData(eloqstore::EloqStore *store)
{
    CHECK_GT(FLAGS_prepare_keys_per_part, 0u);
    CHECK_GT(FLAGS_prepare_batch_size, 0u);
    CHECK_GT(FLAGS_kv_size, sizeof(uint64_t));

    const size_t value_size = FLAGS_kv_size - sizeof(uint64_t);
    LOG(INFO) << "prepare_data=1, writing " << FLAGS_prepare_keys_per_part
              << " keys per partition across " << FLAGS_partitions
              << " partitions, batch_size=" << FLAGS_prepare_batch_size;

    const uint32_t progress_step = std::max<uint32_t>(1, FLAGS_partitions / 10);
    for (uint32_t part = 0; part < FLAGS_partitions; ++part)
    {
        const eloqstore::TableIdent tbl_id(kTableName, part);
        uint64_t next_key = 0;
        uint64_t written = 0;
        while (written < FLAGS_prepare_keys_per_part)
        {
            const uint32_t batch =
                std::min<uint32_t>(FLAGS_prepare_batch_size,
                                   static_cast<uint32_t>(FLAGS_prepare_keys_per_part -
                                                         written));
            std::vector<eloqstore::WriteDataEntry> entries;
            entries.reserve(batch);
            const uint64_t ts = utils::UnixTs<milliseconds>();
            for (uint32_t i = 0; i < batch; ++i)
            {
                std::string key(sizeof(uint64_t), '\0');
                EncodeKey(key.data(), next_key++);
                std::string value(value_size, static_cast<char>('a' + (next_key % 26)));
                entries.emplace_back(std::move(key),
                                     std::move(value),
                                     ts,
                                     eloqstore::WriteOp::Upsert);
            }

            eloqstore::BatchWriteRequest req;
            req.SetArgs(tbl_id, std::move(entries));
            store->ExecSync(&req);
            if (req.Error() != eloqstore::KvError::NoError)
            {
                LOG(FATAL) << "prepare_data failed for partition " << part
                           << ", error=" << req.ErrMessage();
            }
            written += batch;
        }

        if ((part + 1) % progress_step == 0 || part + 1 == FLAGS_partitions)
        {
            LOG(INFO) << "prepare_data progress: " << (part + 1) << "/"
                      << FLAGS_partitions << " partitions";
        }
    }
}

ThreadStats RunStormThread(eloqstore::EloqStore *store,
                           uint32_t thd_id,
                           Workload workload)
{
    ThreadStats stats;
    const size_t client_count =
        static_cast<size_t>(FLAGS_clients_per_part) * FLAGS_partitions;
    std::vector<std::unique_ptr<BaseClient>> clients;
    clients.reserve(client_count);
    for (size_t i = 0; i < client_count; ++i)
    {
        const uint32_t partition = static_cast<uint32_t>(i % FLAGS_partitions);
        const bool use_scan =
            workload == Workload::Scan ||
            (workload == Workload::Mixed && ((i & 1u) == 1u));
        if (use_scan)
        {
            clients.emplace_back(
                std::make_unique<ScanClient>(FLAGS_request_timeout_ms, partition));
        }
        else
        {
            clients.emplace_back(
                std::make_unique<ReadClient>(FLAGS_request_timeout_ms, partition));
        }
    }

    moodycamel::BlockingConcurrentQueue<BaseClient *> finished;
    auto callback = [&finished](eloqstore::KvRequest *req)
    {
        auto *client = reinterpret_cast<BaseClient *>(req->UserData());
        client->latency_us_ = NowMicroseconds() - client->start_ts_us_;
        finished.enqueue(client);
    };

    std::mt19937_64 rng(0x9e3779b97f4a7c15ULL + static_cast<uint64_t>(thd_id));
    uint64_t inflight = 0;
    for (auto &client : clients)
    {
        if (SubmitRequest(store, client.get(), callback, rng))
        {
            ++inflight;
        }
        else
        {
            ++stats.send_fail;
        }
    }

    const auto start = high_resolution_clock::now();
    const auto deadline = start + seconds(FLAGS_test_secs);
    auto last_report = start;
    const double interval_ms = std::max(100.0, FLAGS_report_interval_sec * 1000.0);

    while (inflight > 0)
    {
        BaseClient *client = nullptr;
        finished.wait_dequeue(client);
        --inflight;

        ++stats.total;
        const uint64_t latency_us = client->latency_us_;
        g_all_latency << static_cast<int64_t>(latency_us);
        UpdateMaxLatency(g_all_max_latency_us, latency_us);

        const auto err = client->Request()->Error();
        if (err == eloqstore::KvError::Timeout)
        {
            ++stats.timeout;
            g_timeout_latency << static_cast<int64_t>(latency_us);
            UpdateMaxLatency(g_timeout_max_latency_us, latency_us);
        }
        else if (err == eloqstore::KvError::NoError ||
                 err == eloqstore::KvError::NotFound)
        {
            ++stats.non_timeout;
        }
        else
        {
            ++stats.other_errors;
        }

        const auto now = high_resolution_clock::now();
        if (now < deadline)
        {
            if (SubmitRequest(store, client, callback, rng))
            {
                ++inflight;
            }
            else
            {
                ++stats.send_fail;
            }
        }

        if (thd_id == 0 &&
            duration_cast<milliseconds>(now - last_report).count() >= interval_ms)
        {
            const double timeout_ratio =
                stats.total == 0
                    ? 0.0
                    : (100.0 * static_cast<double>(stats.timeout) /
                       static_cast<double>(stats.total));
            LOG(INFO) << "storm qps=" << g_all_latency.qps()
                      << " timeout_qps=" << g_timeout_latency.qps()
                      << " timeout_ratio=" << timeout_ratio
                      << "% all_avg_us=" << g_all_latency.latency()
                      << " all_p99_us=" << g_all_latency.latency_percentile(0.99)
                      << " timeout_p99_us="
                      << g_timeout_latency.latency_percentile(0.99)
                      << " all_max_us="
                      << g_all_max_latency_us.load(std::memory_order_relaxed);
            last_report = now;
        }
    }

    const auto end = high_resolution_clock::now();
    stats.elapsed_us = duration_cast<microseconds>(end - start).count();
    return stats;
}

void PrintSummary(const ThreadStats &sum, uint64_t elapsed_us)
{
    const double elapsed_sec =
        std::max(1e-6, static_cast<double>(elapsed_us) / 1'000'000.0);
    const double qps = static_cast<double>(sum.total) / elapsed_sec;
    const double timeout_ratio =
        sum.total == 0
            ? 0.0
            : 100.0 * static_cast<double>(sum.timeout) /
                  static_cast<double>(sum.total);

    LOG(INFO) << "========== timeout storm summary ==========";
    LOG(INFO) << "workload=" << FLAGS_workload
              << " partitions=" << FLAGS_partitions
              << " clients_per_part=" << FLAGS_clients_per_part
              << " client_threads=" << FLAGS_client_threads
              << " request_timeout_ms=" << FLAGS_request_timeout_ms
              << " elapsed_s=" << elapsed_sec;
    LOG(INFO) << "requests total=" << sum.total
              << " timeout=" << sum.timeout
              << " non_timeout=" << sum.non_timeout
              << " other_errors=" << sum.other_errors
              << " send_fail=" << sum.send_fail
              << " qps=" << qps << " timeout_ratio=" << timeout_ratio << "%";

    if (sum.total > 0)
    {
        LOG(INFO) << "all latency(us): avg=" << g_all_latency.latency()
                  << " p50=" << g_all_latency.latency_percentile(0.50)
                  << " p90=" << g_all_latency.latency_percentile(0.90)
                  << " p99=" << g_all_latency.latency_percentile(0.99)
                  << " p99.9=" << g_all_latency.latency_percentile(0.999)
                  << " max="
                  << g_all_max_latency_us.load(std::memory_order_relaxed);
    }
    if (sum.timeout > 0)
    {
        LOG(INFO) << "timeout latency(us): avg=" << g_timeout_latency.latency()
                  << " p50=" << g_timeout_latency.latency_percentile(0.50)
                  << " p90=" << g_timeout_latency.latency_percentile(0.90)
                  << " p99=" << g_timeout_latency.latency_percentile(0.99)
                  << " p99.9=" << g_timeout_latency.latency_percentile(0.999)
                  << " max="
                  << g_timeout_max_latency_us.load(std::memory_order_relaxed);
    }
}
}  // namespace

int main(int argc, char *argv[])
{
    google::ParseCommandLineFlags(&argc, &argv, true);
    CHECK(!FLAGS_kvoptions.empty()) << "kvoptions is required";
    CHECK_GT(FLAGS_partitions, 0u);
    CHECK_GT(FLAGS_clients_per_part, 0u);
    CHECK_GT(FLAGS_client_threads, 0u);
    CHECK_GT(FLAGS_test_secs, 0u);
    CHECK_GT(FLAGS_max_key, 0u);
    CHECK_GT(FLAGS_request_timeout_ms, 0u);
    CHECK_LE(FLAGS_request_timeout_ms, eloqstore::TimeWheel::kMaxDelayMs)
        << "request_timeout_ms exceeds max supported timeout "
        << eloqstore::TimeWheel::kMaxDelayMs;

    const Workload workload = ParseWorkload(FLAGS_workload);

    eloqstore::KvOptions options;
    if (int res = options.LoadFromIni(FLAGS_kvoptions.c_str()); res != 0)
    {
        LOG(FATAL) << "Failed to parse " << FLAGS_kvoptions << " at " << res;
    }

    eloqstore::EloqStore store(options);
    const auto start_err = store.Start();
    if (start_err != eloqstore::KvError::NoError)
    {
        LOG(FATAL) << "failed to start store: " << static_cast<uint32_t>(start_err);
    }

    if (FLAGS_prepare_data)
    {
        PrepareData(&store);
    }

    std::vector<ThreadStats> thread_stats(FLAGS_client_threads);
    std::vector<std::thread> threads;
    threads.reserve(FLAGS_client_threads);
    const auto begin = high_resolution_clock::now();
    for (uint32_t i = 0; i < FLAGS_client_threads; ++i)
    {
        threads.emplace_back(
            [&, i]() { thread_stats[i] = RunStormThread(&store, i, workload); });
    }
    for (auto &thd : threads)
    {
        thd.join();
    }
    const auto end = high_resolution_clock::now();

    ThreadStats sum;
    for (const auto &s : thread_stats)
    {
        sum.total += s.total;
        sum.timeout += s.timeout;
        sum.non_timeout += s.non_timeout;
        sum.other_errors += s.other_errors;
        sum.send_fail += s.send_fail;
    }
    const uint64_t elapsed_us = duration_cast<microseconds>(end - begin).count();
    PrintSummary(sum, elapsed_us);

    store.Stop();
    return 0;
}
