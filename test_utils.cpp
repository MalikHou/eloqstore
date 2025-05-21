#include "test_utils.h"

#include <sys/types.h>

#include <cstdint>
#include <cstdlib>
#include <string>
#include <utility>

#include "error.h"
#include "replayer.h"
#include "scan_task.h"
#include "types.h"
#include "utils.h"

namespace test_util
{
std::string Key(uint64_t k)
{
    constexpr int sz = 12;
    std::stringstream ss;
    ss << std::setw(sz) << std::setfill('0') << k;
    std::string kstr = ss.str();
    CHECK(kstr.size() == sz);
    return kstr;
}

std::string Value(uint64_t val, uint32_t len)
{
    std::string s = std::to_string(val);
    if (s.size() < len)
    {
        s.resize(len, '#');
    }
    return s;
}

void CheckKvEntry(const kvstore::KvEntry &left, const kvstore::KvEntry &right)
{
    CHECK(std::get<0>(left) == std::get<0>(right));
    {
        std::string_view lval(std::get<1>(left));
        std::string_view rval(std::get<1>(right));
        CHECK(lval == rval);
    }
    CHECK(std::get<2>(left) == std::get<2>(right));
}

std::string FormatEntries(const std::vector<kvstore::KvEntry> &entries)
{
    std::string kvs_str;
    for (auto &[k, v, _] : entries)
    {
        uint32_t key = decode_key(k.data());
        uint32_t val = kvstore::DecodeFixed32(v.data());
        kvs_str.push_back('{');
        kvs_str.append(std::to_string(key));
        kvs_str.push_back(':');
        kvs_str.append(std::to_string(val));
        kvs_str.push_back('}');
    }
    return kvs_str;
}

std::pair<std::string, kvstore::KvError> Scan(kvstore::EloqStore *store,
                                              const kvstore::TableIdent &tbl_id,
                                              uint32_t begin,
                                              uint32_t end)
{
    char begin_buf[sizeof(uint32_t)];
    char end_buf[sizeof(uint32_t)];
    kvstore::EncodeFixed32(begin_buf, kvstore::ToBigEndian(begin));
    kvstore::EncodeFixed32(end_buf, kvstore::ToBigEndian(end));
    std::string_view begin_key(begin_buf, sizeof(uint32_t));
    std::string_view end_key(end_buf, sizeof(uint32_t));
    kvstore::ScanRequest req;
    req.SetArgs(tbl_id, begin_key, end_key);
    store->ExecSync(&req);
    if (req.Error() != kvstore::KvError::NoError)
    {
        return {{}, req.Error()};
    }
    return {test_util::FormatEntries(req.entries_), kvstore::KvError::NoError};
}

MapVerifier::MapVerifier(kvstore::TableIdent tid,
                         kvstore::EloqStore *store,
                         bool validate)
    : tid_(std::move(tid)), eloq_store_(store), auto_validate_(validate)
{
}

MapVerifier::~MapVerifier()
{
    if (!answer_.empty())
    {
        Clean();
    }
}

void MapVerifier::Upsert(uint64_t key)
{
    Upsert(key, key + 1);
}

void MapVerifier::Upsert(uint64_t begin, uint64_t end)
{
    LOG(INFO) << "Upsert(" << begin << ',' << end << ')';

    std::vector<kvstore::WriteDataEntry> entries;
    for (size_t idx = begin; idx < end; ++idx)
    {
        std::string key = Key(idx);
        std::string val = Value(ts_ + idx, val_size_);
        entries.emplace_back(key, val, ts_, kvstore::WriteOp::Upsert);
    }
    kvstore::WriteRequest req;
    req.SetArgs(tid_, std::move(entries));
    ExecWrite(&req);
}

void MapVerifier::Delete(uint64_t begin, uint64_t end)
{
    LOG(INFO) << "Delete(" << begin << ',' << end << ')';

    std::vector<kvstore::WriteDataEntry> entries;
    for (size_t idx = begin; idx < end; ++idx)
    {
        std::string key = Key(idx);
        entries.emplace_back(key, "", ts_, kvstore::WriteOp::Delete);
    }
    kvstore::WriteRequest req;
    req.SetArgs(tid_, std::move(entries));
    ExecWrite(&req);
}

void MapVerifier::Truncate(uint64_t position)
{
    LOG(INFO) << "Truncate(" << position << ')';

    kvstore::TruncateRequest req;
    std::string key = Key(position);
    if (answer_.empty())
    {
        req.SetArgs(tid_, key);
        eloq_store_->ExecSync(&req);
        CHECK(req.Error() == kvstore::KvError::NotFound);
        return;
    }

    req.SetArgs(tid_, key);
    ExecWrite(&req);
}

void MapVerifier::WriteRnd(uint64_t begin,
                           uint64_t end,
                           uint8_t del,
                           uint8_t density)
{
    constexpr uint8_t max = 100;
    del = del > max ? max : del;
    density = density > max ? max : density;
    LOG(INFO) << "WriteRnd(" << begin << ',' << end << ',' << int(del) << ','
              << int(density) << ')';

    std::vector<kvstore::WriteDataEntry> entries;
    for (size_t idx = begin; idx < end; ++idx)
    {
        if ((rand() % max) >= density)
        {
            continue;
        }

        std::string key = Key(idx);
        uint64_t ts = ts_;
        if ((rand() % max) < del)
        {
            entries.emplace_back(
                std::move(key), std::string(), ts, kvstore::WriteOp::Delete);
        }
        else
        {
            uint32_t len = (rand() % val_size_) + 1;
            std::string val = Value(ts + idx, len);
            entries.emplace_back(
                std::move(key), std::move(val), ts, kvstore::WriteOp::Upsert);
        }
    }
    kvstore::WriteRequest req;
    req.SetArgs(tid_, std::move(entries));
    ExecWrite(&req);
}

void MapVerifier::Clean()
{
    LOG(INFO) << "Clean()";

    kvstore::TruncateRequest req;
    req.SetArgs(tid_, {});
    ExecWrite(&req);
}

void MapVerifier::Read(uint64_t key)
{
    Read(Key(key));
}

void MapVerifier::Read(std::string_view key)
{
    LOG(INFO) << "Read(" << key << ')';

    kvstore::ReadRequest req;
    req.SetArgs(tid_, key);
    eloq_store_->ExecSync(&req);
    if (req.Error() == kvstore::KvError::NoError)
    {
        kvstore::KvEntry ret(key, req.value_, req.ts_);
        CheckKvEntry(answer_.at(std::string(key)), ret);
    }
    else
    {
        CHECK(req.Error() == kvstore::KvError::NotFound);
        CHECK(answer_.find(std::string(key)) == answer_.end());
    }
}

void MapVerifier::Floor(std::string_view key)
{
    LOG(INFO) << "Floor(" << key << ')';

    kvstore::FloorRequest req;
    req.SetArgs(tid_, key);
    eloq_store_->ExecSync(&req);
    auto it_lb = answer_.upper_bound(std::string(key));
    if (it_lb != answer_.begin())
    {
        it_lb--;
        CHECK(req.Error() == kvstore::KvError::NoError);
        kvstore::KvEntry ret(req.floor_key_, req.value_, req.ts_);
        CheckKvEntry(it_lb->second, ret);
    }
    else
    {
        CHECK(req.Error() == kvstore::KvError::NotFound);
    }
}

void MapVerifier::Scan(uint64_t begin,
                       uint64_t end,
                       size_t page_entries,
                       size_t page_size)
{
    Scan(Key(begin), Key(end), page_entries, page_size);
}

void MapVerifier::Scan(std::string_view begin,
                       std::string_view end,
                       size_t page_entries,
                       size_t page_size)
{
    LOG(INFO) << "Scan(" << begin << ',' << end << ')';

    kvstore::ScanRequest req;
    req.SetPagination(page_entries, page_size);
    std::string begin_key(begin);
    std::string end_key(end);

    req.SetArgs(tid_, begin_key, end_key);
    while (true)
    {
        eloq_store_->ExecSync(&req);
        if (req.Error() != kvstore::KvError::NoError)
        {
            CHECK(req.Error() == kvstore::KvError::NotFound);
            CHECK(answer_.empty());
            break;
        }

        // Verify scan result
        CHECK(req.entries_.size() <= req.page_entries_);
        CHECK(req.ResultSize() <= req.page_size_ || req.entries_.size() == 1);
        auto it = answer_.lower_bound(begin_key);
        if (!req.begin_inclusive_)
        {
            assert(it->first == begin_key);
            it++;
        }
        for (auto &t : req.entries_)
        {
            CheckKvEntry(t, it->second);
            it++;
        }

        if (!req.has_remaining_)
        {
            if (it != answer_.end())
            {
                CHECK(it->first >= end_key);
            }
            break;
        }
        // Continue scan the next page.
        CHECK(!req.entries_.empty());
        begin_key = std::get<0>(req.entries_.back());
        req.SetArgs(tid_, begin_key, end_key, false);
    }
}

void MapVerifier::Validate()
{
    kvstore::ScanRequest req;
    req.SetArgs(tid_, {}, {});
    eloq_store_->ExecSync(&req);
    if (req.Error() == kvstore::KvError::NotFound)
    {
        CHECK(answer_.empty());
        CHECK(req.entries_.empty());
        return;
    }
    CHECK(req.Error() == kvstore::KvError::NoError);
    CHECK(answer_.size() == req.entries_.size());
    auto it = answer_.begin();
    for (auto &t : req.entries_)
    {
        CheckKvEntry(t, it->second);
        it++;
    }
    CHECK(it == answer_.end());
}

void MapVerifier::ExecWrite(kvstore::KvRequest *req)
{
    switch (req->Type())
    {
    case kvstore::RequestType::Write:
    {
        const auto wreq = static_cast<kvstore::WriteRequest *>(req);
        for (const kvstore::WriteDataEntry &ent : wreq->batch_)
        {
            auto it = answer_.find(ent.key_);
            if (it == answer_.end())
            {
                if (ent.op_ == kvstore::WriteOp::Delete)
                {
                    continue;
                }
                auto ret = answer_.try_emplace(ent.key_);
                assert(ret.second);
                it = ret.first;
            }
            else
            {
                if (ent.timestamp_ <= std::get<2>(it->second))
                {
                    continue;
                }
            }
            assert(it != answer_.end());

            if (ent.op_ == kvstore::WriteOp::Upsert)
            {
                it->second =
                    kvstore::KvEntry(ent.key_, ent.val_, ent.timestamp_);
            }
            else if (ent.op_ == kvstore::WriteOp::Delete)
            {
                answer_.erase(it);
            }
            else
            {
                assert(false);
            }
        }
        break;
    }
    case kvstore::RequestType::Truncate:
    {
        const auto treq = static_cast<kvstore::TruncateRequest *>(req);
        auto it = answer_.lower_bound(std::string(treq->position_));
        answer_.erase(it, answer_.end());
        break;
    }
    default:
        assert(false);
    }

    eloq_store_->ExecSync(req);
    CHECK(req->Error() == kvstore::KvError::NoError);

    if (auto_validate_)
    {
        Validate();
    }
    ts_++;
}

void MapVerifier::SetAutoValidate(bool v)
{
    auto_validate_ = v;
}

void MapVerifier::SetValueSize(uint32_t val_size)
{
    val_size_ = val_size;
}

void MapVerifier::SetStore(kvstore::EloqStore *store)
{
    eloq_store_ = store;
}

void MapVerifier::SetTimestamp(uint64_t ts)
{
    ts_ = ts;
}

bool ConcurrencyTester::Partition::IsWriting() const
{
    return ticks_ & 1;
}

uint32_t ConcurrencyTester::Partition::FinishedRounds() const
{
    return ticks_ >> 1;
}

void ConcurrencyTester::Partition::FinishWrite()
{
    CHECK(req_.Error() == kvstore::KvError::NoError);
    verify_cnt_ = 0;
    ticks_++;
}

ConcurrencyTester::ConcurrencyTester(kvstore::EloqStore *store,
                                     std::string tbl_name,
                                     uint32_t n_partitions,
                                     uint16_t seg_count,
                                     uint8_t seg_size)
    : seg_size_(seg_size),
      seg_count_(seg_count),
      seg_sum_(seg_size * average_v),
      tbl_name_(std::move(tbl_name)),
      partitions_(n_partitions),
      finished_reqs_(n_partitions),
      store_(store)
{
    for (uint32_t i = 0; i < n_partitions; i++)
    {
        partitions_[i].id_ = i;
    }
}

void ConcurrencyTester::Wake(kvstore::KvRequest *req)
{
    bool ok = finished_reqs_.enqueue(req->UserData());
    CHECK(ok);
}

void ConcurrencyTester::ExecRead(Reader *reader)
{
    Partition &partition = partitions_[reader->id_ % partitions_.size()];
    reader->start_tick_ = partition.ticks_;
    reader->partition_id_ = partition.id_;
    reader->begin_ = (rand() % seg_count_) * seg_size_;
    reader->end_ = reader->begin_ + seg_size_;
    kvstore::EncodeFixed32(reader->begin_key_,
                           kvstore::ToBigEndian(reader->begin_));
    kvstore::EncodeFixed32(reader->end_key_,
                           kvstore::ToBigEndian(reader->end_));
    std::string_view begin_key(reader->begin_key_, sizeof(uint32_t));
    std::string_view end_key(reader->end_key_, sizeof(uint32_t));
    reader->req_.SetArgs({tbl_name_, partition.id_}, begin_key, end_key);
    uint64_t user_data = reader->id_;
    bool ok = store_->ExecAsyn(&reader->req_,
                               user_data,
                               [this](kvstore::KvRequest *req) { Wake(req); });
    CHECK(ok);
}

void ConcurrencyTester::VerifyRead(Reader *reader)
{
    CHECK(reader->req_.Error() == kvstore::KvError::NoError);
    const uint32_t key_begin = reader->begin_;
    const uint32_t key_end = reader->end_;
    const uint16_t seg_id = key_begin / seg_size_;
    const uint32_t partition_id = reader->partition_id_;
    const Partition &partition = partitions_[partition_id];
    const auto &entries = reader->req_.entries_;

    uint64_t sum_val = 0;
    for (auto &ent : entries)
    {
        uint32_t val = kvstore::DecodeFixed32(std::get<1>(ent).data());
        sum_val += val;
    }
    if (seg_sum_ != sum_val)
    {
        LOG(FATAL) << "sum of value mismatch " << sum_val << " != " << seg_sum_
                   << '\n'
                   << DebugSegment(partition_id, seg_id, &entries);
    }
    verify_sum_++;

    if (!partition.IsWriting() && partition.ticks_ == reader->start_tick_)
    {
        uint32_t key_ans = key_begin;
        for (auto &[k, v, _] : entries)
        {
            while (partition.kvs_[key_ans] == 0)
            {
                key_ans++;
            }

            uint32_t key_res = decode_key(k.data());
            uint32_t val_res = kvstore::DecodeFixed32(v.data());
            CHECK(key_res < key_end);
            if (key_ans != key_res || partition.kvs_[key_ans] != val_res)
            {
                LOG(FATAL) << "segment kvs mismatch " << '\n'
                           << DebugSegment(partition_id, seg_id, &entries);
            }

            key_ans++;
        }
        verify_kv_++;
    }

    partitions_[partition_id].verify_cnt_++;
}

// Tester: {100:5}{102:9}{103:2}
// Store:  {100:5}{102:9}{103:2}
std::string ConcurrencyTester::DebugSegment(
    uint32_t partition_id,
    uint16_t seg_id,
    const std::vector<kvstore::KvEntry> *resp) const
{
    const Partition &partition = partitions_[partition_id];
    const uint32_t begin = seg_id * seg_size_;
    const uint32_t end = begin + seg_size_;

    std::string kvs_str =
        "table " + tbl_name_ + " partition " + std::to_string(partition_id) +
        " segment " + std::to_string(seg_id) + " [" + std::to_string(begin) +
        ',' + std::to_string(end) + ')';

    kvs_str.append("\nTester: ");
    for (uint32_t k = begin; k < end; k++)
    {
        uint32_t v = partition.kvs_[k];
        if (v > 0)
        {
            kvs_str.push_back('{');
            kvs_str.append(std::to_string(k));
            kvs_str.push_back(':');
            kvs_str.append(std::to_string(v));
            kvs_str.push_back('}');
        }
    }

    kvs_str.append("\nStore:  ");
    if (resp != nullptr)
    {
        kvs_str.append(FormatEntries(*resp));
        return kvs_str;
    }

    auto ret = Scan(store_, {tbl_name_, partition_id}, begin, end);
    if (ret.second != kvstore::KvError::NoError)
    {
        kvs_str.append(kvstore::ErrorString(ret.second));
    }
    else
    {
        kvs_str.append(ret.first);
    }
    return kvs_str;
}

void ConcurrencyTester::ExecWrite(ConcurrencyTester::Partition &partition)
{
    assert(!partition.IsWriting());
    partition.ticks_++;
    uint64_t ts = CurrentTimestamp();
    std::vector<kvstore::WriteDataEntry> entries;
    uint32_t left = seg_sum_;
    for (uint32_t i = 0; i < partition.kvs_.size(); i++)
    {
        uint32_t new_val = 0;
        if ((i + 1) % seg_size_ == 0)
        {
            new_val = left;
            left = seg_sum_;
        }
        else if (rand() % 3 != 0)
        {
            new_val = rand() % (average_v * 3);
            new_val = std::min(new_val, left);
            left -= new_val;
        }

        if (new_val == 0)
        {
            if (partition.kvs_[i] != 0)
            {
                kvstore::WriteDataEntry &ent = entries.emplace_back();
                kvstore::PutFixed32(&ent.key_, kvstore::ToBigEndian(i));
                ent.timestamp_ = ts;
                ent.op_ = kvstore::WriteOp::Delete;
            }
        }
        else
        {
            kvstore::WriteDataEntry &ent = entries.emplace_back();
            kvstore::PutFixed32(&ent.key_, kvstore::ToBigEndian(i));
            kvstore::PutFixed32(&ent.val_, new_val);
            ent.timestamp_ = ts;
            ent.op_ = kvstore::WriteOp::Upsert;
        }
        partition.kvs_[i] = new_val;
    }

    partition.req_.SetArgs({tbl_name_, partition.id_}, std::move(entries));
    uint64_t user_data = (partition.id_ | (uint64_t(1) << 63));
    bool ok = store_->ExecAsyn(&partition.req_,
                               user_data,
                               [this](kvstore::KvRequest *req) { Wake(req); });
    CHECK(ok);
}

void ConcurrencyTester::Init()
{
    uint64_t ts = CurrentTimestamp();
    const uint32_t kvs_num = seg_size_ * seg_count_;
    for (Partition &partition : partitions_)
    {
        kvstore::TableIdent tbl_id(tbl_name_, partition.id_);

        // Try to load partition KVs from EloqStore
        kvstore::ScanRequest scan_req;
        scan_req.SetArgs(tbl_id, {}, {});
        store_->ExecSync(&scan_req);
        CHECK(scan_req.Error() == kvstore::KvError::NoError ||
              scan_req.Error() == kvstore::KvError::NotFound);
        if (!scan_req.entries_.empty())
        {
            partition.kvs_.resize(kvs_num, 0);
            CHECK(scan_req.entries_.size() <= partition.kvs_.size());
            for (auto &[k, v, _] : scan_req.entries_)
            {
                uint32_t key_res = decode_key(k.data());
                uint32_t val_res = kvstore::DecodeFixed32(v.data());
                CHECK(key_res < partition.kvs_.size());
                partition.kvs_[key_res] = val_res;
            }
            // verify partition KVs
            for (uint16_t seg = 0; seg < seg_count_; seg++)
            {
                uint64_t sum = 0;
                uint32_t idx = seg * seg_size_;
                for (uint8_t i = 0; i < seg_size_; i++)
                {
                    sum += partition.kvs_[idx++];
                }
                if (sum != seg_sum_)
                {
                    LOG(FATAL) << "segment sum is wrong " << '\n'
                               << DebugSegment(partition.id_, seg, nullptr);
                }
            }
            continue;
        }

        // Initialize partition KVs
        partition.kvs_.resize(kvs_num, average_v);
        std::vector<kvstore::WriteDataEntry> entries;
        for (uint32_t i = 0; i < kvs_num; i++)
        {
            kvstore::WriteDataEntry &ent = entries.emplace_back();
            kvstore::PutFixed32(&ent.key_, kvstore::ToBigEndian(i));
            kvstore::PutFixed32(&ent.val_, average_v);
            ent.timestamp_ = ts;
            ent.op_ = kvstore::WriteOp::Upsert;
        }
        partition.req_.SetArgs(tbl_id, std::move(entries));
        store_->ExecSync(&partition.req_);
        CHECK(partition.req_.Error() == kvstore::KvError::NoError);
    }
}

void ConcurrencyTester::Run(uint32_t rounds,
                            uint32_t interval,
                            uint16_t n_readers)
{
    uint16_t running_readers = 0;
    auto is_finished = [this, &running_readers, rounds]() -> bool
    {
        for (const Partition &partition : partitions_)
        {
            if (partition.FinishedRounds() < rounds)
            {
                return false;
            }
        }

        return running_readers == 0;
    };

    // Start readers
    if (n_readers < partitions_.size())
    {
        LOG(WARNING) << "n_readers is less than partitions, reset to "
                     << partitions_.size();
        n_readers = partitions_.size();
    }
    std::vector<Reader> readers(n_readers);
    for (Reader &reader : readers)
    {
        reader.id_ = running_readers++;
        ExecRead(&reader);
    }

    while (!is_finished())
    {
        uint64_t user_data;
        while (finished_reqs_.try_dequeue(user_data))
        {
            bool is_write = (user_data & (uint64_t(1) << 63));
            uint32_t id = (user_data & ((uint64_t(1) << 63) - 1));

            if (is_write)
            {
                Partition &partition = partitions_[id];
                partition.FinishWrite();
                continue;
            }

            Reader &reader = readers[id];
            Partition &partition = partitions_[reader.partition_id_];
            VerifyRead(&reader);
            if (partition.FinishedRounds() < rounds)
            {
                ExecRead(&reader);

                // Pause between each round of write
                if (partition.verify_cnt_ >= interval && !partition.IsWriting())
                {
                    ExecWrite(partition);
                }
            }
            else
            {
                running_readers--;
            }
        }
    }

    LOG(INFO) << "concurrency test statistic: verify kvs " << verify_kv_
              << ", verify sum " << verify_sum_;
}

void ConcurrencyTester::Clear()
{
    for (Partition &part : partitions_)
    {
        kvstore::TruncateRequest req;
        req.SetArgs({tbl_name_, part.id_}, {});
        store_->ExecSync(&req);
        CHECK(req.Error() == kvstore::KvError::NoError);
    }
}

uint64_t ConcurrencyTester::CurrentTimestamp()
{
    return utils::UnixTs<std::chrono::microseconds>();
}

ManifestVerifier::ManifestVerifier(kvstore::KvOptions opts)
    : options_(opts),
      io_mgr_(&options_),
      idx_mgr_(&io_mgr_),
      answer_(&idx_mgr_, &tbl_id_)
{
    if (!options_.data_append_mode)
    {
        answer_file_pages_ =
            static_cast<kvstore::PooledFilePages *>(answer_.FilePgAllocator());
    }
}

std::pair<kvstore::PageId, kvstore::FilePageId> ManifestVerifier::RandChoose()
{
    CHECK(!helper_.empty());
    auto it = std::next(helper_.begin(), rand() % helper_.size());
    return *it;
}

uint32_t ManifestVerifier::Size() const
{
    return helper_.size();
}

void ManifestVerifier::NewMapping()
{
    kvstore::PageId page_id = answer_.GetPage();
    kvstore::FilePageId file_page_id = answer_.FilePgAllocator()->Allocate();
    answer_.UpdateMapping(page_id, file_page_id);
    builder_.UpdateMapping(page_id, file_page_id);
    helper_[page_id] = file_page_id;
}

void ManifestVerifier::UpdateMapping()
{
    auto [page_id, old_fp_id] = RandChoose();
    root_id_ = page_id;

    kvstore::FilePageId new_fp_id = answer_.FilePgAllocator()->Allocate();
    answer_.UpdateMapping(page_id, new_fp_id);
    builder_.UpdateMapping(page_id, new_fp_id);
    if (answer_file_pages_)
    {
        answer_file_pages_->Free({old_fp_id});
    }
    helper_[page_id] = new_fp_id;
}

void ManifestVerifier::FreeMapping()
{
    auto [page_id, file_page_id] = RandChoose();
    helper_.erase(page_id);
    if (page_id == root_id_)
    {
        root_id_ = Size() == 0 ? kvstore::MaxPageId : RandChoose().first;
    }

    answer_.FreePage(page_id);
    builder_.DeleteMapping(page_id);
    if (answer_file_pages_)
    {
        answer_file_pages_->Free({file_page_id});
    }
}

void ManifestVerifier::Finish()
{
    if (!builder_.Empty())
    {
        if (file_.empty())
        {
            Snapshot();
        }
        else
        {
            std::string_view sv = builder_.Finalize(root_id_);
            file_.append(sv);
            builder_.Reset();
        }
    }
}

void ManifestVerifier::Snapshot()
{
    kvstore::FilePageId max_fp_id = answer_.FilePgAllocator()->MaxFilePageId();
    std::string_view sv =
        builder_.Snapshot(root_id_, answer_.GetMapping(), max_fp_id);
    file_ = sv;
    builder_.Reset();
}

void ManifestVerifier::Verify()
{
    kvstore::MemStoreMgr::Manifest file(file_);
    kvstore::Replayer replayer(&options_);
    kvstore::KvError err = replayer.Replay(&file);
    CHECK(err == kvstore::KvError::NoError);
    CHECK(replayer.root_ == root_id_);

    auto get_map_tbl = [](std::vector<uint64_t> tbl)
        -> std::unordered_map<kvstore::PageId, kvstore::FilePageId>
    {
        std::unordered_map<kvstore::PageId, kvstore::FilePageId> map_tbl;
        for (kvstore::PageId page_id; uint64_t val : tbl)
        {
            if (kvstore::MappingSnapshot::IsFilePageId(val))
            {
                map_tbl[page_id] = kvstore::MappingSnapshot::DecodeId(val);
            }
            page_id++;
        }
        return map_tbl;
    };
    auto mapper = replayer.GetMapper(&idx_mgr_, &tbl_id_);
    auto map_tbl_a = get_map_tbl(mapper->GetMapping()->mapping_tbl_);
    auto map_tbl_b = get_map_tbl(answer_.GetMapping()->mapping_tbl_);
    CHECK(map_tbl_a == map_tbl_b);
}
}  // namespace test_util