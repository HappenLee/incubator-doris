#include "bench.h"

#include <algorithm>
#include "BooleanDecoder.h"

namespace {
#include "velox_visitor.inc"
}

void velox_dense(const uint8_t* in, uint8_t* out, int rows, int block) {
    const auto* start = reinterpret_cast<const char*>(in);
    facebook::velox::parquet::BooleanDecoder reader(start, start + (rows + 7) / 8);
    for (int decoded = 0; decoded < rows; decoded += block) {
        const int count = std::min(block, rows - decoded);
        reader.readWithVisitor<false>(nullptr, BenchmarkVisitor<uint8_t>(out + decoded, count));
    }
}
