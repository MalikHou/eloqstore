#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <unordered_set>

#include "mem_index_page.h"
#include "page_mapper.h"

namespace kvstore
{
class RootMeta;

class ManifestBuilder
{
public:
    ManifestBuilder();
    void UpdateMapping(PageId page_id, FilePageId file_page_id);
    void DeleteMapping(PageId page_id);
    std::string_view Snapshot(uint32_t root_id,
                              const MappingSnapshot *mapping,
                              FilePageId max_fp_id);

    std::string_view Finalize(uint32_t new_root);
    std::string_view BuffView() const;
    void Reset();
    bool Empty() const;
    uint32_t CurrentSize() const;

    // checksum(8B), root_page_id(4B), log_size(4B)
    static constexpr uint16_t checksum_bytes = 8;
    static constexpr uint16_t header_bytes = checksum_bytes + 8;

    static constexpr uint16_t offset_root = checksum_bytes;
    static constexpr uint16_t offset_len = offset_root + 4;

private:
    std::string buff_;
};

struct CowRootMeta
{
    std::unique_ptr<PageMapper> mapper_;
    uint64_t manifest_size_;
    MemIndexPage *root_;
    std::shared_ptr<MappingSnapshot> old_mapping_;
};

struct RootMeta
{
    RootMeta() = default;
    RootMeta(const RootMeta &rhs) = delete;
    RootMeta(RootMeta &&rhs) = default;
    bool Evict();

    MemIndexPage *root_page_{nullptr};
    std::unique_ptr<PageMapper> mapper_{nullptr};
    std::unordered_set<MappingSnapshot *> mapping_snapshots_;
    uint32_t ref_cnt_{0};
    uint64_t manifest_size_{0};
};

}  // namespace kvstore