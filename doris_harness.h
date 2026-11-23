#pragma once

#include <climits>
#include <cstring>
#include <iostream>
#include <tuple>
#include <type_traits>
#include <utility>

#define DCHECK(...) \
    if (true) {     \
    } else          \
        std::cerr
#define DCHECK_EQ(...) DCHECK()
#define DCHECK_GE(...) DCHECK()
#define DCHECK_LE(...) DCHECK()
#define LIKELY(x) __builtin_expect(!!(x), true)
#define UNLIKELY(x) __builtin_expect(!!(x), false)
#define NO_SANITIZE_UNDEFINED __attribute__((__no_sanitize__("undefined")))
#define RETURN_IF_ERROR(...)               \
    do {                                   \
        const auto status = (__VA_ARGS__); \
        if (!status.ok()) return status;   \
    } while (false)

#include "util/bit_packing.inline.h"

namespace doris {
#include "doris_reader.inc"
#include "doris_reader_inline.inc"

struct Status {
    bool success;
    bool ok() const { return success; }
    static Status OK() { return {true}; }
    static Status IOError(const char*) { return {false}; }
};
} // namespace doris
