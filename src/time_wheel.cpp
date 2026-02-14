#include "time_wheel.h"

#include <atomic>
#include <cassert>
#include <chrono>

#if defined(__x86_64__) || defined(_M_X64)
#include <x86intrin.h>
#endif

namespace eloqstore
{

std::atomic<uint64_t> tsc_cycles_per_microsecond_{0};

inline uint64_t ReadTimeMicroseconds()
{
#if defined(__x86_64__) || defined(_M_X64)
    uint64_t cycles_per_us =
        tsc_cycles_per_microsecond_.load(std::memory_order_relaxed);
    assert(cycles_per_us != 0);
    // Read TSC (Time Stamp Counter) - returns CPU cycles
    uint64_t cycles = __rdtsc();
    return cycles / cycles_per_us;
#elif defined(__aarch64__)
    // Ensure ARM timer frequency is initialized (thread-safe, only initializes
    // once)
    uint64_t cycles_per_us =
        tsc_cycles_per_microsecond_.load(std::memory_order_relaxed);
    assert(cycles_per_us != 0);
    // Read ARM virtual counter - returns timer ticks
    uint64_t ticks;
    __asm__ volatile("mrs %0, cntvct_el0" : "=r"(ticks));
    return ticks / cycles_per_us;
#else
    // Fallback to std::chrono (slower but portable and precise)
    using namespace std::chrono;
    auto now = steady_clock::now();
    return duration_cast<microseconds>(now.time_since_epoch()).count();
#endif
}

uint32_t TimeWheel::CurrentTick() const noexcept
{
    return cur_;
}

uint32_t TimeWheel::Size() const noexcept
{
    return active_count_;
}

TimeWheel::TimerId TimeWheel::AddAfterMs(uint32_t delay_ms,
                                         Fn fn,
                                         void *arg) noexcept
{
    if (!fn || delay_ms > kMaxDelayMs)
        return {};
    const uint32_t expire = static_cast<uint32_t>(cur_ + delay_ms);
    return AddImpl(expire, fn, arg);
}

bool TimeWheel::Cancel(TimeWheel::TimerId id) noexcept
{
    if (!id || id.idx > capacity_)
        return false;
    Node &n = nodes_[id.idx];
    if (!(n.flags & F_ACTIVE) || n.gen != id.gen)
        return false;

    n.flags |= F_CANCELED;

    // if timer is in pending or dispatching, don't unlink and free, just
    // set F_CANCELED
    if (n.flags & F_DISPATCHING)
        return true;

    // wheel: O(1) unlink and free
    Unlink(id.idx);
    FreeNode(id.idx);
    return true;
}

void TimeWheel::HarvestTimers(uint32_t target_tick,
                              HarvestResult &result,
                              uint32_t max_callbacks) noexcept
{
    // if there are pending timers, run them first
    if (pending_head_ != 0u)
    {
        RunPending(result, max_callbacks);
        if (pending_head_ != 0u)
            return;
    }

    // ensure "current tick" is processed at least once (otherwise
    // target==cur_ will also trigger)
    if (!cur_done_)
    {
        ProcessCurrentTick(result, max_callbacks);
        if (pending_head_ != 0u)
            return;
    }

    // if current tick is greater than or equal to target tick, return
    if (!IsTickLess(cur_, target_tick))
        return;

    // advance to target tick one by one
    while (IsTickLess(cur_, target_tick))
    {
        ++cur_;
        cur_done_ = false;

        if (active_count_ == 0u)
        {
            // if there are no timers, advance to target tick
            cur_ = target_tick;
            cur_done_ = true;
            return;
        }

        ProcessCurrentTick(result, max_callbacks);
        if (pending_head_ != 0u)
            return;
    }
    return;
}

void TimeWheel::RunPending(HarvestResult &result,
                           uint32_t max_callbacks) noexcept
{
    uint32_t fired = 0;
    while (pending_head_ != 0u && fired < max_callbacks)
    {
        const uint32_t idx = pending_head_;
        Node &n = nodes_[idx];
        pending_head_ = n.next;
        n.next = 0u;

        // must clear F_DISPATCHING, otherwise the subsequent cancel
        // semantic will be wrong
        n.flags &= (uint8_t) ~F_DISPATCHING;

        const bool canceled = (n.flags & F_CANCELED) != 0u;

        if (!canceled && IsTickGreaterEqual(cur_, n.expire))
        {
            if (n.fn)
                n.fn(n.arg);
        }

        // free the node after execution (whether canceled or not)
        FreeNode(idx);

        ++fired;
    }

    if (pending_head_ == 0u)
    {
        cur_done_ = true;
        result.no_pending = true;
    }
    result.callbacks_run = fired;
    result.cur_tick = cur_;
}

inline bool TimeWheel::IsTickLess(uint32_t a, uint32_t b) noexcept
{
    return (int32_t) (a - b) < 0;
}

inline bool TimeWheel::IsTickGreaterEqual(uint32_t a, uint32_t b) noexcept
{
    return (int32_t) (a - b) >= 0;
}

uint32_t TimeWheel::AllocateNode() noexcept
{
    const uint32_t idx = free_head_;
    if (idx == 0u)
        return 0u;
    Node &n = nodes_[idx];
    free_head_ = n.next_free;

    ++n.gen;  // ABA protection

    n.next = 0u;
    n.prev_link = nullptr;
    n.next_free = 0u;
    n.expire = 0u;
    n.fn = nullptr;
    n.arg = nullptr;
    n.flags = F_ACTIVE;

    ++active_count_;
    return idx;
}

void TimeWheel::FreeNode(uint32_t idx) noexcept
{
    Node &n = nodes_[idx];
    n.next = 0u;
    n.prev_link = nullptr;
    n.expire = 0u;
    n.fn = nullptr;
    n.arg = nullptr;
    n.flags = 0u;

    n.next_free = free_head_;
    free_head_ = idx;

    --active_count_;
}

void TimeWheel::LinkFront(uint32_t slot, uint32_t idx) noexcept
{
    uint32_t &head = wheel_[slot];
    Node &n = nodes_[idx];

    n.prev_link = &head;
    n.next = head;
    if (head != 0u)
    {
        nodes_[head].prev_link = &n.next;
    }
    head = idx;
}

void TimeWheel::Unlink(uint32_t idx) noexcept
{
    Node &n = nodes_[idx];
    uint32_t *pl = n.prev_link;
    if (!pl)
        return;

    const uint32_t nxt = n.next;
    *pl = nxt;
    if (nxt != 0u)
    {
        nodes_[nxt].prev_link = pl;
    }

    n.prev_link = nullptr;
    n.next = 0u;
}

void TimeWheel::NormalizeExpire(TimeWheel::Node &n) noexcept
{
    if (IsTickGreaterEqual(cur_, n.expire))
    {
        n.expire = static_cast<uint32_t>(cur_ + 1u);
    }
}

void TimeWheel::Arm(uint32_t idx) noexcept
{
    Node &n = nodes_[idx];

    NormalizeExpire(n);

    const uint32_t delta = (uint32_t) (n.expire - cur_);
    if (delta > kMaxDelayMs)
    {
        // if the delta is greater than kMaxDelayMs, free the node
        FreeNode(idx);
        return;
    }

    const uint32_t slot = n.expire & kMask;
    LinkFront(slot, idx);
}

TimeWheel::TimerId TimeWheel::AddImpl(uint32_t expire,
                                      Fn fn,
                                      void *arg) noexcept
{
    const uint32_t delta = (uint32_t) (expire - cur_);
    if (delta > kMaxDelayMs)
        return {};

    const uint32_t idx = AllocateNode();
    if (idx == 0u)
        return {};

    Node &n = nodes_[idx];
    n.fn = fn;
    n.arg = arg;
    n.expire = expire;

    Arm(idx);
    return TimerId{idx, n.gen};
}

void TimeWheel::DrainCurrentSlotToPending() noexcept
{
    const uint32_t slot = cur_ & kMask;
    uint32_t head = wheel_[slot];
    if (head == 0u)
        return;

    wheel_[slot] = 0u;

    uint32_t cur = head;
    while (cur != 0u)
    {
        Node &n = nodes_[cur];
        const uint32_t nxt = n.next;

        n.flags |= F_DISPATCHING;
        n.prev_link = nullptr;
        n.next = pending_head_;
        pending_head_ = cur;

        cur = nxt;
    }
}

void TimeWheel::ProcessCurrentTick(TimeWheel::HarvestResult &result,
                                   uint32_t max_callbacks) noexcept
{
    DrainCurrentSlotToPending();
    RunPending(result, max_callbacks);
    if (pending_head_ == 0u)
        cur_done_ = true;
}

}  // namespace eloqstore
