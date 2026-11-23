#pragma once

#include <cstddef>
#include <cstdint>

void doris_native(const uint8_t* in, uint8_t* out, int rows, int block);
void doris_bulk(const uint8_t* in, uint8_t* out, int rows, int block);
void doris_native_sr(const uint8_t* in, uint8_t* out, int rows, int block);
void doris_bulk_sr(const uint8_t* in, uint8_t* out, int rows, int block);
void doris_native_neon(const uint8_t* in, uint8_t* out, int rows, int block);
void doris_bulk_neon(const uint8_t* in, uint8_t* out, int rows, int block);
void velox_dense(const uint8_t* in, uint8_t* out, int rows, int block);

void require(bool condition);
