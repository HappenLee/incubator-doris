#pragma once

#include <cstdint>
#include <cstdlib>

// Only the upstream dense, null-free visitor is instantiated by this harness.
namespace facebook::velox::bits {
inline int32_t countNonNulls(const uint64_t*, int32_t, int32_t) { std::abort(); }
inline bool isBitNull(const uint64_t*, int32_t) { std::abort(); }
}
