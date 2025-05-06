#pragma once

#include <cstdint>
#include <memory>
#include <vector>

#include "coding.h"
#include "xxhash.h"

namespace kvstore
{
constexpr uint8_t checksum_bytes = 8;
static uint16_t const page_type_offset = checksum_bytes;

enum struct PageType : uint8_t
{
    NonLeafIndex = 0,
    LeafIndex,
    Data,
    Overflow,
    Deleted = 255
};

inline static PageType TypeOfPage(const char *p)
{
    return static_cast<PageType>(p[page_type_offset]);
}

inline static void SetPageType(char *p, PageType t)
{
    p[page_type_offset] = static_cast<char>(t);
}

inline static uint64_t GetPageChecksum(const char *p)
{
    return DecodeFixed64(p);
}

inline static void SetPageChecksum(char *p, uint16_t pgsz)
{
    uint64_t checksum = XXH3_64bits(p + checksum_bytes, pgsz - checksum_bytes);
    EncodeFixed64(p, checksum);
}

inline static bool ValidatePageChecksum(char *p, uint16_t pgsz)
{
    uint64_t checksum = XXH3_64bits(p + checksum_bytes, pgsz - checksum_bytes);
    return checksum == GetPageChecksum(p);
}

inline static size_t page_align = sysconf(_SC_PAGESIZE);

using Page = std::unique_ptr<char, decltype(&std::free)>;

inline Page AllocPage(uint16_t page_size)
{
    char *p = (char *) std::aligned_alloc(page_align, page_size);
    assert(p);
    return Page(p, std::free);
}

class PagesPool
{
public:
    PagesPool(uint16_t page_size) : page_size_(page_size) {};
    Page Allocate()
    {
        if (pages_.empty())
        {
            return AllocPage(page_size_);
        }
        else
        {
            Page page = std::move(pages_.back());
            pages_.pop_back();
            return page;
        }
    }
    void Free(Page page)
    {
        pages_.emplace_back(std::move(page));
    }

private:
    const uint16_t page_size_;
    std::vector<Page> pages_;
};

}  // namespace kvstore