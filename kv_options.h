#pragma once

#include <cstdint>
#include <cstring>
#include <string>

#include "comparator.h"

namespace kvstore
{
constexpr uint8_t max_overflow_pointers = 128;
constexpr uint16_t max_read_pages_batch = max_overflow_pointers;
constexpr uint16_t max_write_pages_batch = 256;

constexpr uint16_t max_key_size = 2048;

struct KvOptions
{
    bool operator==(const KvOptions &other) const;

    /**
     * @brief Key-Value database storage path.
     * In-memory storage will be used if path is empty.
     */
    std::string db_path;
    /**
     * @brief Amount of threads.
     */
    uint16_t num_threads = 1;

    const Comparator *comparator_ = Comparator::DefaultComparator();
    uint16_t data_page_restart_interval = 16;
    uint16_t index_page_restart_interval = 16;
    uint16_t index_page_read_queue = 1024;
    uint32_t init_page_count = 1 << 15;

    /**
     * @brief Max amount of cached index pages per thread.
     */
    uint32_t index_buffer_pool_size = UINT32_MAX;
    /**
     * @brief Limit manifest file size.
     */
    uint64_t manifest_limit = 16 << 20;  // 16MB
    /**
     * @brief Max amount of opened files per thread.
     */
    uint32_t fd_limit = 1024;
    /**
     * @brief Size of io-uring submission queue per thread.
     */
    uint32_t io_queue_size = 4096;
    /**
     * @brief Max amount of inflight write IO per thread.
     */
    uint32_t max_inflight_write = 4096;
    /**
     * @brief The maximum number of pages per batch for the write task.
     */
    uint16_t max_write_batch_pages = 64;
    /**
     * @brief Size of io-uring selected buffer ring.
     * It must be a power-of 2, and can be up to 32768.
     */
    uint16_t buf_ring_size = 1 << 10;
    /**
     * @brief Size of coroutine stack.
     * According to the latest test results, at least 16KB is required.
     */
    uint32_t coroutine_stack_size = 16 * 1024;

    /**
     * @brief Limit number of retained archives.
     * Only take effect when data_append_mode is enabled.
     */
    uint16_t num_retained_archives = 0;
    /**
     * @brief Set the (minimum) archive time interval in seconds.
     * 0 means do not generate archives automatically.
     * Only take effect when data_append_mode is enabled and
     * num_retained_archives is not 0.
     */
    uint32_t archive_interval_secs = 86400;  // 1 day
    /**
     * @brief The maximum number of running archive tasks at the same time.
     */
    uint16_t max_archive_tasks = 256;
    /**
     * @brief Move pages in data file that space amplification factor
     * bigger than this value.
     * Only take effect when data_append_mode is enabled.
     */
    uint8_t file_amplify_factor = 2;
    /**
     * @brief Number of background file GC threads.
     * Only take effect when data_append_mode is enabled.
     */
    uint16_t num_gc_threads = 1;

    /* NOTE:
     * The following options will be persisted in storage, so after the first
     * setting, them cannot be changed anymore in the future.
     */

    /**
     * @brief Size of B+Tree index/data node (page).
     * Ensure that it is aligned to the system's page size.
     */
    uint16_t data_page_size = 1 << 12;  // 4KB

    size_t FilePageOffsetMask() const;
    /**
     * @brief Amount of pages per data file (1 << pages_per_file_shift).
     */
    uint8_t pages_per_file_shift = 11;  // 2048

    /**
     * @brief Amount of pointers stored in overflow page.
     * The maximum can be set to 128 (max_overflow_pointers).
     */
    uint8_t overflow_pointers = 16;
    /**
     * @brief Write data file pages in append only mode.
     */
    bool data_append_mode = false;
};

inline bool KvOptions::operator==(const KvOptions &other) const
{
    return std::memcmp(this, &other, sizeof(KvOptions)) == 0;
}

inline size_t KvOptions::FilePageOffsetMask() const
{
    return (1 << pages_per_file_shift) - 1;
}

}  // namespace kvstore