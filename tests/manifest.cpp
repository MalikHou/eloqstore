#include <cassert>
#include <catch2/catch_test_macros.hpp>
#include <cstdlib>

#include "kv_options.h"
#include "test_utils.h"
#include "tests/common.h"

using namespace test_util;

TEST_CASE("simple manifest recovery", "[manifest]")
{
    kvstore::KvOptions opts;
    opts.init_page_count = 100;
    ManifestVerifier verifier(opts);

    verifier.NewMapping();
    verifier.NewMapping();
    verifier.UpdateMapping();
    verifier.FreeMapping();
    verifier.Finish();
    verifier.Verify();

    verifier.FreeMapping();
    verifier.Finish();
    verifier.Verify();
}

TEST_CASE("medium manifest recovery", "[manifest]")
{
    kvstore::KvOptions opts;
    opts.init_page_count = 100;
    ManifestVerifier verifier(opts);

    for (int i = 0; i < 100; i++)
    {
        verifier.NewMapping();
        verifier.NewMapping();
        verifier.FreeMapping();
        verifier.NewMapping();
        verifier.UpdateMapping();
        verifier.Finish();
    }
    verifier.Verify();

    verifier.Snapshot();
    verifier.Verify();

    for (int i = 0; i < 10; i++)
    {
        verifier.NewMapping();
        verifier.NewMapping();
        verifier.FreeMapping();
        verifier.NewMapping();
        verifier.UpdateMapping();
        verifier.Finish();

        verifier.Verify();
    }
}

TEST_CASE("detect manifest corruption", "[manifest]")
{
    // TODO:
}

TEST_CASE("create archives", "[archive][slow]")
{
    kvstore::KvOptions options{
        .db_path = test_path,
        .num_retained_archives = 1,
        .archive_interval_secs = 1,
        .file_amplify_factor = 2,
        .pages_per_file_shift = 8,
        .data_append_mode = true,
    };
    kvstore::EloqStore *store = InitStore(options);

    MapVerifier tester(test_tbl_id, store, false);
    tester.SetValueSize(10000);

    for (int i = 0; i < 10; i++)
    {
        tester.WriteRnd(0, 1000, 50, 80);
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    tester.Validate();
}

TEST_CASE("rollback to archive", "[archive]")
{
    // TODO:
}
