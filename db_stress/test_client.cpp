#include <gflags/gflags.h>

#include <iostream>

#include "../test_utils.h"

DEFINE_string(db_path, "", "path to database");
DEFINE_string(partition, "", "table partition id");
DEFINE_uint32(scan_begin, 0, "scan begin key");
DEFINE_uint32(scan_end, UINT32_MAX, "scan end key");

int main(int argc, char **argv)
{
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    auto tbl_id = kvstore::TableIdent::FromString(FLAGS_partition);
    if (!tbl_id.IsValid())
    {
        std::cerr << "Invalid argument: " << FLAGS_partition << std::endl;
        exit(-1);
    }

    kvstore::KvOptions options;
    options.store_path = {FLAGS_db_path};
    kvstore::EloqStore store(options);
    kvstore::KvError err = store.Start();
    if (err != kvstore::KvError::NoError)
    {
        std::cerr << kvstore::ErrorString(err) << std::endl;
        exit(-1);
    }

    auto [kvs, e] =
        test_util::Scan(&store, tbl_id, FLAGS_scan_begin, FLAGS_scan_end);
    if (e != kvstore::KvError::NoError)
    {
        std::cerr << kvstore::ErrorString(e) << std::endl;
        exit(-1);
    }
    std::cout << kvs << std::endl;

    store.Stop();
}