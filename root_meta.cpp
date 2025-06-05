#include "root_meta.h"

#include <glog/logging.h>

#include <cassert>
#include <cstdint>
#include <string>
#include <string_view>

#include "coding.h"
#include "page_mapper.h"

namespace kvstore
{

ManifestBuilder::ManifestBuilder()
{
    buff_.resize(header_bytes);
}

void ManifestBuilder::UpdateMapping(PageId page_id, FilePageId file_page_id)
{
    PutVarint32(&buff_, page_id);
    PutVarint64(&buff_, MappingSnapshot::EncodeFilePageId(file_page_id));
}

void ManifestBuilder::DeleteMapping(PageId page_id)
{
    PutVarint32(&buff_, page_id);
    PutVarint64(&buff_, MappingSnapshot::InvalidValue);
}

std::string_view ManifestBuilder::Snapshot(uint32_t root_id,
                                           const MappingSnapshot *mapping,
                                           FilePageId max_fp_id)
{
    Reset();
    PutVarint64(&buff_, max_fp_id);
    mapping->Serialize(buff_);
    return Finalize(root_id);
}

void ManifestBuilder::Reset()
{
    buff_.resize(header_bytes);
}

bool ManifestBuilder::Empty() const
{
    return buff_.size() <= header_bytes;
}

uint32_t ManifestBuilder::CurrentSize() const
{
    return buff_.size();
}

std::string_view ManifestBuilder::Finalize(uint32_t new_root)
{
    uint32_t len = buff_.size() - header_bytes;
    EncodeFixed32(buff_.data() + offset_len, len);

    EncodeFixed32(buff_.data() + offset_root, new_root);

    uint64_t checksum = XXH3_64bits(buff_.data() + checksum_bytes,
                                    buff_.size() - checksum_bytes);
    EncodeFixed64(buff_.data(), checksum);
    return buff_;
}

std::string_view ManifestBuilder::BuffView() const
{
    return buff_;
}

void RootMeta::Pin()
{
    ref_cnt_++;
}

void RootMeta::Unpin()
{
    assert(ref_cnt_ > 0);
    ref_cnt_--;
}

}  // namespace kvstore