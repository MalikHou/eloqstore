#pragma once

#include <cstdint>
#include <string>
#include <string_view>

#include "async_io_manager.h"
#include "error.h"
#include "page_mapper.h"

namespace kvstore
{
class Replayer
{
public:
    Replayer(const KvOptions *opts);
    KvError Replay(ManifestFile *log);
    std::unique_ptr<PageMapper> GetMapper(IndexPageManager *idx_mgr,
                                          const TableIdent *tbl_ident);

    PageId root_;
    uint64_t file_size_;
    std::vector<uint64_t> mapping_tbl_;
    FilePageId max_fp_id_;

private:
    KvError NextRecord(ManifestFile *log);
    void DeserializeSnapshot(std::string_view snapshot);
    void ReplayLog(std::string_view log);

    const KvOptions *opts_;
    std::string log_buf_;
    std::string_view mapping_log_;
};
}  // namespace kvstore