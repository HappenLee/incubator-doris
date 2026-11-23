#include "bench.h"

#include <algorithm>
#include <array>
#include <climits>
#include <cstring>
#include <iostream>
#include <tuple>
#include <type_traits>
#include <utility>

#define DCHECK(...) if (true) {} else std::cerr
#define DCHECK_GE(...) DCHECK()
#define DCHECK_LE(...) DCHECK()
#define LIKELY(x) __builtin_expect(!!(x), true)
#define UNLIKELY(x) __builtin_expect(!!(x), false)
#define NO_SANITIZE_UNDEFINED __attribute__((__no_sanitize__("undefined")))
#define RETURN_IF_ERROR(...)                   \
    do {                                       \
        const auto status = (__VA_ARGS__);      \
        if (!status.ok()) return status;       \
    } while (false)

#include "util/bit_packing.inline.h"

namespace doris {
#include "doris_reader.inc"
#include "doris_reader_inline.inc"

namespace format::parquet::native {

struct Status {
    bool success;
    bool ok() const { return success; }
    static Status OK() { return {true}; }
    static Status IOError(const char*) { return {false}; }
};

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

} // namespace format::parquet::native
} // namespace doris

void verify_doris_boundary(const uint8_t* in, size_t bytes, uint8_t* out, int rows, int block) {
    using namespace doris::format::parquet::native;
    BoolPlainDecoder reader(in, bytes);
    ByteConsumer consumer(out);
    for (int decoded = 0; decoded < rows; decoded += block) {
        const int count = std::min(block, rows - decoded);
        require(reader.decode_fixed_values(count, consumer).ok());
    }
}

void doris_native(const uint8_t* in, uint8_t* out, int rows, int block) {
    verify_doris_boundary(in, (rows + 7) / 8, out, rows, block);
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
