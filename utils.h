#pragma once

#include <cassert>
#include <chrono>
#include <cstdint>

namespace utils
{
template <typename T>
inline T UnsetLowBits(T num, uint8_t n)
{
    assert(n < (sizeof(T) * 8));
    return num & (~((uint64_t(1) << n) - 1));
}

template <typename T>
uint64_t UnixTs()
{
    auto dur = std::chrono::system_clock::now().time_since_epoch();
    return std::chrono::duration_cast<T>(dur).count();
}
}  // namespace utils