#include <condition_variable>

#include "eloq_store.h"

std::mutex m;
std::condition_variable cv;
bool ready = false;

void wake_up(kvstore::KvRequest *req)
{
    std::unique_lock lk(m);
    ready = true;
    lk.unlock();
    cv.notify_one();
}

int main()
{
    kvstore::KvOptions opts;
    opts.store_path = {"/tmp/eloq_store"};
    opts.num_threads = 1;
    kvstore::TableIdent tbl_id("t1", 1);

    kvstore::EloqStore store(opts);
    kvstore::KvError err = store.Start();
    assert(err == kvstore::KvError::NoError);

    {
        kvstore::BatchWriteRequest req;
        std::vector<kvstore::WriteDataEntry> entries;
        entries.emplace_back("key1", "val1", 1, kvstore::WriteOp::Upsert);
        entries.emplace_back("key2", "val2", 1, kvstore::WriteOp::Upsert);
        entries.emplace_back("key3", "val3", 1, kvstore::WriteOp::Upsert);
        req.SetArgs(tbl_id, std::move(entries));
        bool ok = store.ExecAsyn(&req, 0, wake_up);
        assert(ok);
        {
            std::unique_lock lk(m);
            cv.wait(lk, [] { return ready; });
        }
        assert(req.Error() == kvstore::KvError::NoError);
    }

    {
        ready = false;
        kvstore::ReadRequest req;
        req.SetArgs(tbl_id, "key2");
        store.ExecAsyn(&req, 0, wake_up);
        {
            std::unique_lock lk(m);
            cv.wait(lk, [] { return ready; });
        }
        assert(req.Error() == kvstore::KvError::NoError);
        assert(req.value_ == "val2");
        assert(req.ts_ == 1);
    }

    {
        // Execute asynchronously
        ready = false;
        kvstore::ScanRequest req;
        req.SetArgs(tbl_id, "key1", "key3");
        store.ExecAsyn(&req, 0, wake_up);
        {
            std::unique_lock lk(m);
            cv.wait(lk, [] { return ready; });
        }
        assert(req.entries_.size() == 2);
        kvstore::KvEntry &ent0 = req.entries_[0];
        assert(ent0.key_ == "key1");
        assert(ent0.value_ == "val1");
        assert(ent0.timestamp_ == 1);
        kvstore::KvEntry &ent1 = req.entries_[1];
        assert(ent1.key_ == "key2");
        assert(ent1.value_ == "val2");
        assert(ent1.timestamp_ == 1);
    }

    {
        // Execute synchronously
        kvstore::BatchWriteRequest req;
        std::vector<kvstore::WriteDataEntry> entries;
        entries.emplace_back("key1", "", 2, kvstore::WriteOp::Delete);
        entries.emplace_back("key3", "val33", 2, kvstore::WriteOp::Upsert);
        req.SetArgs(tbl_id, std::move(entries));
        store.ExecSync(&req);
        assert(req.Error() == kvstore::KvError::NoError);
    }

    {
        kvstore::ScanRequest req;
        req.SetArgs(tbl_id, "key3", "");
        store.ExecAsyn(&req, 0, wake_up);
        while (!req.IsDone())
            ;
        assert(req.entries_.size() == 1);
        kvstore::KvEntry &ent0 = req.entries_[0];
        assert(ent0.key_ == "key3");
        assert(ent0.value_ == "val33");
        assert(ent0.timestamp_ == 2);
    }

    store.Stop();
}