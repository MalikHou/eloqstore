#pragma once
#include <array>
#include <cstdint>
#include <limits>
#include <vector>

namespace eloqstore
{
inline uint64_t ReadTimeMicroseconds();

class TimeWheel
{
public:
    using Fn = void (*)(void *);

    struct TimerId
    {
        uint32_t idx = 0;
        uint32_t gen = 0;
        explicit operator bool() const noexcept
        {
            return idx != 0;
        }
    };

    struct HarvestResult
    {
        bool no_pending = false;
        uint32_t callbacks_run = 0;
        uint32_t cur_tick = 0;
    };

    static constexpr uint32_t kBits = 16;
    static constexpr uint32_t kSlots = 1u << kBits;  // 65536
    static constexpr uint32_t kMask = kSlots - 1u;

    // max delay time
    static constexpr uint32_t kMaxDelayMs = 60'000;

    explicit TimeWheel(uint32_t capacity = 4096, uint32_t start_tick = 0)
        : capacity_(capacity),
          nodes_(static_cast<size_t>(capacity_) + 1),
          cur_(start_tick)
    {
        // free-list: 1..capacity
        for (uint32_t i = 1; i <= capacity_; ++i)
        {
            nodes_[i].next_free = (i + 1u <= capacity_) ? (i + 1u) : 0u;
            nodes_[i].gen = 1u;
        }
        free_head_ = (capacity_ >= 1) ? 1u : 0u;
    }

    uint32_t CurrentTick() const noexcept;
    uint32_t Size() const noexcept;

    /**
     * @brief Add a timer after delay_ms
     * @param delay_ms delay time, delay_ms <= 60000, if delay_ms = 0, it will
     * be set to "next tick"
     * @param fn callback function
     * @param arg callback argument
     * @return TimerId, if add failed, return empty TimerId
     */
    TimerId AddAfterMs(uint32_t delay_ms, Fn fn, void *arg = nullptr) noexcept;

    /**
     * @brief Cancel a timer
     * @param TimerId
     * @return bool. if TimerId is valid, and timer is not in pending or
     * dispatching, return true, otherwise return false
     */
    bool Cancel(TimerId id) noexcept;

    /**
     * @brief Harvest expired timers
     * @param target_tick target tick
     * @param max_callbacks max callbacks to run
     * @return void
     */
    void HarvestTimers(
        uint32_t target_tick,
        HarvestResult &result,
        uint32_t max_callbacks = std::numeric_limits<uint32_t>::max()) noexcept;

private:
    /**
     * @brief Run pending timers. if tick is not changed, run pending timers
     * (budget cut-off scenario)
     * @param max_callbacks max callbacks to run
     * @return void
     */
    void RunPending(
        HarvestResult &result,
        uint32_t max_callbacks = std::numeric_limits<uint32_t>::max()) noexcept;

    /**
     * @brief roll-over safe compare two ticks
     * @param a first tick
     * @param b second tick
     * @return bool. if a < b, return true, otherwise return false
     */
    static inline bool IsTickLess(uint32_t a, uint32_t b) noexcept;

    /**
     * @brief roll-over safe compare two ticks
     * @param a first tick
     * @param b second tick
     * @return bool. if a >= b, return true, otherwise return false
     */
    static inline bool IsTickGreaterEqual(uint32_t a, uint32_t b) noexcept;

    enum : uint8_t
    {
        F_ACTIVE = 1u << 0,
        F_CANCELED = 1u << 1,
        F_DISPATCHING = 1u << 2,
    };

    struct Node
    {
        uint32_t next = 0u;  // wheel/pending reuse
        uint32_t *prev_link =
            nullptr;  // O(1) unlink, use for wheel and pending
        uint32_t next_free = 0u;

        uint32_t expire = 0u;  // expire tick (1ms)

        Fn fn = nullptr;
        void *arg = nullptr;

        uint32_t gen = 1u;  // ABA protection
        uint8_t flags = 0u;
        uint8_t _pad[3]{};
    };

    uint32_t capacity_ = 0;
    std::vector<Node> nodes_;
    std::array<uint32_t, kSlots> wheel_{};  // slot -> head idx

    uint32_t free_head_ = 0u;
    uint32_t active_count_ = 0u;

    uint32_t cur_ = 0u;  // current tick (logical time)
    bool cur_done_ =
        false;  // whether the current tick has been fully processed once

    uint32_t pending_head_ = 0u;

    /**
     * @brief Allocate a new node
     * @return uint32_t. the index of the new node
     */
    uint32_t AllocateNode() noexcept;

    /**
     * @brief Free a node
     * @param idx index of the node to free
     * @return void
     */
    void FreeNode(uint32_t idx) noexcept;

    /**
     * @brief Link a node to the front of the slot
     * @param slot slot index
     * @param idx node index
     * @return void
     */
    void LinkFront(uint32_t slot, uint32_t idx) noexcept;

    /**
     * @brief Unlink a node from the slot
     * @param idx node index
     * @return void
     */
    void Unlink(uint32_t idx) noexcept;

    /**
     * @brief Normalize expire time. if expire <= cur_, set expire to cur_+1
     * @param n node
     * @return void
     */
    void NormalizeExpire(Node &n) noexcept;

    /**
     * @brief Arm a node
     * @param idx node index
     * @return void
     */
    void Arm(uint32_t idx) noexcept;

    TimerId AddImpl(uint32_t expire, Fn fn, void *arg) noexcept;

    /**
     * @brief Drain current slot to pending, and set F_DISPATCHING flag
     * @return void
     */
    void DrainCurrentSlotToPending() noexcept;

    /**
     * @brief Process current tick
     * @param max_callbacks max callbacks to run
     * @return void
     */
    void ProcessCurrentTick(HarvestResult &result,
                            uint32_t max_callbacks) noexcept;
};
}  // namespace eloqstore
