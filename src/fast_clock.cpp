#include "fast_clock.h"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>

#if defined(__x86_64__) || defined(_M_X64)
#include <x86intrin.h>  // For __rdtsc/__rdtscp.
#endif

namespace eloqstore::FastClock
{
namespace
{
using SteadyClock = std::chrono::steady_clock;

uint64_t SteadyNowMicroseconds()
{
    const auto now = SteadyClock::now();
    return std::chrono::duration_cast<std::chrono::microseconds>(
               now.time_since_epoch())
        .count();
}

#if defined(__x86_64__) || defined(_M_X64)
struct CoreClockEntry
{
    uint32_t core_aux{0};
    uint64_t cycles_per_microsecond{0};
    bool valid{false};
};

struct ThreadClockState
{
    static constexpr size_t kCoreSlots = 8;

    std::array<CoreClockEntry, kCoreSlots> core_clocks{};
    uint32_t last_core_aux{0};
    uint64_t last_cycles_per_microsecond{0};
    bool has_last{false};
    size_t next_insert_idx{0};
};

thread_local ThreadClockState g_thread_clock_state{};

uint64_t ReadCycles(uint32_t *core_aux)
{
    return __rdtscp(core_aux);
}

uint64_t CalibrateCyclesPerMicrosecondX86()
{
    constexpr auto kCalibrationWindow = std::chrono::milliseconds(2);
    constexpr int kCalibrationRounds = 3;
    constexpr uint64_t kFallbackCyclesPerMicrosecond = 2000;

    uint64_t estimate = 0;
    for (int i = 0; i < kCalibrationRounds; ++i)
    {
        const auto start_time = SteadyClock::now();
        const uint64_t start_cycles = __rdtsc();
        auto now = start_time;
        while (now - start_time < kCalibrationWindow)
        {
            _mm_pause();
            now = SteadyClock::now();
        }

        const uint64_t end_cycles = __rdtsc();
        const auto elapsed_us =
            std::chrono::duration_cast<std::chrono::microseconds>(now -
                                                                   start_time)
                .count();
        if (elapsed_us <= 0 || end_cycles <= start_cycles)
        {
            continue;
        }

        const uint64_t sample = (end_cycles - start_cycles) /
                                static_cast<uint64_t>(elapsed_us);
        if (sample == 0)
        {
            continue;
        }
        estimate = estimate == 0 ? sample : (estimate + sample) / 2;
    }

    return estimate == 0 ? kFallbackCyclesPerMicrosecond : estimate;
}

uint64_t LookupCyclesPerMicrosecond(uint32_t core_aux)
{
    auto &state = g_thread_clock_state;
    if (state.has_last && state.last_core_aux == core_aux)
    {
        return state.last_cycles_per_microsecond;
    }

    for (const auto &entry : state.core_clocks)
    {
        if (entry.valid && entry.core_aux == core_aux)
        {
            state.last_core_aux = core_aux;
            state.last_cycles_per_microsecond = entry.cycles_per_microsecond;
            state.has_last = true;
            return entry.cycles_per_microsecond;
        }
    }

    const uint64_t cycles_per_us = CalibrateCyclesPerMicrosecondX86();
    auto &slot = state.core_clocks[state.next_insert_idx];
    slot.core_aux = core_aux;
    slot.cycles_per_microsecond = cycles_per_us;
    slot.valid = true;
    state.next_insert_idx =
        (state.next_insert_idx + 1) % state.core_clocks.size();
    state.last_core_aux = core_aux;
    state.last_cycles_per_microsecond = cycles_per_us;
    state.has_last = true;
    return cycles_per_us;
}
#elif defined(__aarch64__)
struct ThreadClockState
{
    uint64_t cycles_per_microsecond{0};
    bool initialized{false};
};

thread_local ThreadClockState g_thread_clock_state{};

uint64_t ReadCounterFrequencyHz()
{
    uint64_t freq_hz = 0;
    __asm__ volatile("mrs %0, cntfrq_el0" : "=r"(freq_hz));
    return freq_hz;
}

uint64_t ReadCycles()
{
    uint64_t ticks = 0;
    __asm__ volatile("mrs %0, cntvct_el0" : "=r"(ticks));
    return ticks;
}

void EnsureInitialized()
{
    if (g_thread_clock_state.initialized)
    {
        return;
    }

    const uint64_t freq_hz = ReadCounterFrequencyHz();
    const uint64_t cycles_per_us = freq_hz / 1000000;
    g_thread_clock_state.cycles_per_microsecond =
        cycles_per_us == 0 ? 1 : cycles_per_us;
    g_thread_clock_state.initialized = true;
}
#endif
}  // namespace

void Initialize()
{
#if defined(__x86_64__) || defined(_M_X64)
    uint32_t core_aux = 0;
    (void) ReadCycles(&core_aux);
    (void) LookupCyclesPerMicrosecond(core_aux);
#elif defined(__aarch64__)
    EnsureInitialized();
#endif
}

uint64_t NowMicroseconds()
{
#if defined(__x86_64__) || defined(_M_X64)
    uint32_t core_aux = 0;
    const uint64_t cycles = ReadCycles(&core_aux);
    const uint64_t cycles_per_us = LookupCyclesPerMicrosecond(core_aux);
    return cycles / cycles_per_us;
#elif defined(__aarch64__)
    EnsureInitialized();
    return ReadCycles() / g_thread_clock_state.cycles_per_microsecond;
#else
    return SteadyNowMicroseconds();
#endif
}

uint64_t NowMilliseconds()
{
    const auto now = SteadyClock::now();
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               now.time_since_epoch())
        .count();
}

uint64_t ElapsedMicroseconds(uint64_t start_us)
{
    const uint64_t end_us = NowMicroseconds();
    if (end_us >= start_us)
    {
        return end_us - start_us;
    }
    return std::numeric_limits<uint64_t>::max();
}
}  // namespace eloqstore::FastClock
