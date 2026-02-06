#pragma once

#include <cstdint>

namespace eloqstore::FastClock
{
// Initializes the fast clock backend for the current thread.
// It is safe to call multiple times.
void Initialize();

// Returns a monotonic timestamp in microseconds.
uint64_t NowMicroseconds();

// Returns a monotonic timestamp in milliseconds.
uint64_t NowMilliseconds();

// Returns elapsed microseconds since start_us. Handles wraparound safely.
uint64_t ElapsedMicroseconds(uint64_t start_us);
}  // namespace eloqstore::FastClock
