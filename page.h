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

class Page
{
public:
    Page(bool alloc);
    Page(char *ptr);
    Page(Page &&other) noexcept;
    Page &operator=(Page &&other) noexcept;
    Page(const Page &) = delete;
    Page &operator=(const Page &) = delete;
    ~Page();
    void Free();
    char *Ptr() const;

private:
    char *ptr_;
};

struct KvOptions;

class PagesPool
{
public:
    using UPtr = std::unique_ptr<char, decltype(&std::free)>;
    PagesPool(const KvOptions *options);
    char *Allocate();
    void Free(char *ptr);

private:
    void Extend(size_t pages);

    struct FreePage
    {
        FreePage *next_;
    };

    struct MemChunk
    {
        UPtr uptr_;
        size_t size_;
    };

    const KvOptions *options_;
    std::vector<MemChunk> chunks_;
    FreePage *free_head_;
    size_t free_cnt_;
};

}  // namespace kvstore