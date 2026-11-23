#include "bench.h"

#include <algorithm>
#include <array>

#include "arm_unpack.h"

#if DORIS_ARM_VARIANT == 1
#define doris doris_sr_variant
#define doris_native doris_native_sr
#define doris_bulk doris_bulk_sr
#elif DORIS_ARM_VARIANT == 2
#define doris doris_neon_variant
#define doris_native doris_native_neon
#define doris_bulk doris_bulk_neon
#endif

#include "doris_harness.h"

namespace doris::format::parquet::native {

class ParquetFixedValueConsumer {
public:
    virtual Status consume(const uint8_t* values, size_t rows, size_t width) = 0;
    virtual ~ParquetFixedValueConsumer() = default;
};

class BoolPlainDecoder {
public:
    BoolPlainDecoder(const uint8_t* in, size_t bytes) {
        bool_values_.Reset(in, bytes);
    }

    Status decode_fixed_values(size_t num_values, ParquetFixedValueConsumer& consumer);

#include "doris_decoder.inc"
};

#include "doris_decoder_body.inc"

class ByteConsumer final : public ParquetFixedValueConsumer {
public:
    explicit ByteConsumer(uint8_t* out) : out_(out) {}

    Status consume(const uint8_t* values, size_t rows, size_t width) override {
        require(width == 1);
        std::memcpy(out_, values, rows);
        out_ += rows;
        return Status::OK();
    }

private:
    uint8_t* out_;
};

} // namespace doris::format::parquet::native

void doris_native(const uint8_t* in, uint8_t* out, int rows, int block) {
    using namespace doris::format::parquet::native;
    BoolPlainDecoder reader(in, (rows + 7) / 8);
    ByteConsumer consumer(out);
    for (int decoded = 0; decoded < rows; decoded += block) {
        const int count = std::min(block, rows - decoded);
        require(reader.decode_fixed_values(count, consumer).ok());
    }
}

void doris_bulk(const uint8_t* in, uint8_t* out, int rows, int block) {
    require(block % 8 == 0);
    doris::BatchedBitReader reader;
    reader.Reset(in, (rows + 7) / 8);
    for (int decoded = 0; decoded < rows; decoded += block) {
        const int count = std::min(block, rows - decoded);
        require(reader.UnpackBatch(1, count, out + decoded) == count);
    }
}
