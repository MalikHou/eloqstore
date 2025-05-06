#include <gflags/gflags.h>
#include <glog/logging.h>

#include <cassert>
#include <cstdlib>

#include "kill_point.h"
#include "kv_options.h"
#include "test_utils.h"

// params for db_stress
DEFINE_string(db_path, "/tmp/stress-test", "Path to database");
DEFINE_int32(n_partitions, 20, "nums of partitions");
DEFINE_int32(seg_size, 8, "size of each segment");
DEFINE_int32(seg_count, 50, "count of segments");
DEFINE_int32(num_readers, 500, "Amount of readers");
DEFINE_int32(rounds, 100000, "rounds of batch write on each partition");
DEFINE_int32(write_interval, 50, "pause interval between each round of write");
DEFINE_uint32(kill_odds, 0, "odds (1/this) of each killpoint to crash");
DEFINE_uint32(num_client_threads, 1, "Amount of threads");

// random params for crash_test
DEFINE_uint32(num_threads, 1, "Amount of threads");
DEFINE_uint32(data_page_restart_interval, 16, "interval of datapage restart");
DEFINE_uint32(index_page_restart_interval, 16, "interval of indexpage restart");
DEFINE_uint32(index_page_read_queue, 1024, "size of index_page_read_queue");
DEFINE_uint32(init_page_count, 1 << 15, "nums of init page");
DEFINE_uint32(index_buffer_pool_size, UINT32_MAX, "size of index buffer pool");
DEFINE_uint64(manifest_limit, 8 << 20, "limit of manifest");
DEFINE_uint32(fd_limit, 1024, "limit of fd");
DEFINE_uint32(io_queue_size, 4096, "size of io_queue");
DEFINE_uint32(buf_ring_size, 1 << 10, "size of buf_ring");
DEFINE_uint32(coroutine_stack_size, 1 << 17, "size of coroutine stack");
DEFINE_uint32(data_page_size, 1 << 12, "size of datapage");
DEFINE_uint32(pages_per_file_shift, 11, "nums of filepage shift");

int main(int argc, char **argv)
{
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostdout = true;
    FLAGS_colorlogtostdout = true;
    FLAGS_alsologtostderr = true;
    FLAGS_stderrthreshold = google::GLOG_WARNING;

    kvstore::KvOptions opts;
    opts.db_path = FLAGS_db_path;
    opts.buf_ring_size = FLAGS_buf_ring_size;
    opts.coroutine_stack_size = FLAGS_coroutine_stack_size;
    opts.data_page_size = FLAGS_data_page_size;
    opts.data_page_restart_interval = FLAGS_data_page_restart_interval;
    opts.index_page_restart_interval = FLAGS_index_page_restart_interval;
    opts.index_page_read_queue = FLAGS_index_page_read_queue;
    opts.index_buffer_pool_size = FLAGS_index_buffer_pool_size;
    opts.io_queue_size = FLAGS_io_queue_size;
    opts.fd_limit = FLAGS_fd_limit;
    opts.manifest_limit = FLAGS_manifest_limit;
    opts.init_page_count = FLAGS_init_page_count;
    opts.num_threads = FLAGS_num_threads;
    opts.pages_per_file_shift = FLAGS_pages_per_file_shift;

    kvstore::KillPoint::GetInstance().kill_odds_ = FLAGS_kill_odds;

    kvstore::EloqStore store(opts);
    store.Start();

    std::vector<std::thread> threads;
    for (uint32_t i = 0; i < FLAGS_num_client_threads; i++)
    {
        threads.emplace_back(
            [&store, i]
            {
                std::string tbl_name = "concurrency_test" + std::to_string(i);
                test_util::ConcurrencyTester tester(&store,
                                                    std::move(tbl_name),
                                                    FLAGS_n_partitions,
                                                    FLAGS_seg_count,
                                                    FLAGS_seg_size);
                tester.Init();
                tester.Run(
                    FLAGS_rounds, FLAGS_write_interval, FLAGS_num_readers);
                tester.Clear();
            });
    }
    for (auto &thd : threads)
    {
        thd.join();
    }

    store.Stop();

    google::ShutdownGoogleLogging();
}