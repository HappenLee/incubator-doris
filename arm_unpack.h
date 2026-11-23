#pragma once

#if defined(__aarch64__) && defined(__ARM_NEON)
#include <arm_neon.h>

#include <cstdint>
#include <cstring>

namespace arm_bool {

inline void unpack_starrocks_style(const uint8_t* in, uint8_t* out) {
    for (int i = 0; i < 4; ++i) {
        const uint8_t packed = in[i];
        const uint8x8_t values = {
                static_cast<uint8_t>((packed >> 0) & 1), static_cast<uint8_t>((packed >> 1) & 1),
                static_cast<uint8_t>((packed >> 2) & 1), static_cast<uint8_t>((packed >> 3) & 1),
                static_cast<uint8_t>((packed >> 4) & 1), static_cast<uint8_t>((packed >> 5) & 1),
                static_cast<uint8_t>((packed >> 6) & 1), static_cast<uint8_t>((packed >> 7) & 1)};
        vst1_u8(out + i * 8, values);
    }
}

inline void unpack_neon(const uint8_t* in, uint8_t* out) {
    uint32_t packed;
    std::memcpy(&packed, in, sizeof(packed));
    const auto bytes = vreinterpretq_u8_u32(vdupq_n_u32(packed));
    const uint8x16_t first = {0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1};
    const uint8x16_t second = {2, 2, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3};
    const uint8x16_t masks = {1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128};
    const auto ones = vdupq_n_u8(1);
    const auto low = vandq_u8(vqtbl1q_u8(bytes, first), masks);
    const auto high = vandq_u8(vqtbl1q_u8(bytes, second), masks);
    vst1q_u8(out, vminq_u8(low, ones));
    vst1q_u8(out + 16, vminq_u8(high, ones));
}

} // namespace arm_bool
#endif
