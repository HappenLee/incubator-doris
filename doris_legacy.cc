#include <algorithm>
#include <cstdlib>
#include <limits>
#include <unordered_set>
#include <vector>

#include "bench.h"

#define doris doris_legacy_variant
#include "doris_harness.h"

namespace doris {

using UInt8 = uint8_t;
using DataTypePtr = void*;
using NullMap = std::vector<uint8_t>;
class FilterMap;

// Models the already-reserved UInt8 column. resize advances its logical size.
class ColumnUInt8 {
public:
    ColumnUInt8(uint8_t* data, size_t capacity) : data_(data), capacity_(capacity) {}
    ColumnUInt8& get_data() { return *this; }
    UInt8* data() { return data_; }
    size_t size() const { return size_; }
    void resize(size_t size) {
        if (size > capacity_) std::abort();
        size_ = size;
    }
    UInt8& operator[](size_t index) { return data_[index]; }

private:
    UInt8* data_;
    size_t capacity_;
    size_t size_ = 0;
};

struct MutableColumnPtr {
    ColumnUInt8* column;
    ColumnUInt8* get() const { return column; }
};

template <typename T, typename From>
T assert_cast(From* ptr) {
    return static_cast<T>(ptr);
}

template <typename T, typename From>
T cast_set(From value) {
    return static_cast<T>(value);
}

#include "doris_legacy_select.inc"

class BoolPlainDecoder {
public:
    BoolPlainDecoder(const uint8_t* in, size_t bytes) { bool_values_.Reset(in, bytes); }
    Status decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                         ColumnSelectVector& select_vector, bool is_dict_filter);

    template <bool has_filter>
    Status _decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                          ColumnSelectVector& select_vector, bool is_dict_filter);

#include "doris_legacy_decoder.inc"
};

#include "doris_legacy_body.inc"

} // namespace doris

namespace {

std::vector<uint16_t> full_runs;
std::vector<uint16_t> tail_runs;

std::vector<uint16_t> make_dense_runs(int rows) {
    constexpr int max_run = std::numeric_limits<uint16_t>::max();
    std::vector<uint16_t> runs;
    while (rows > max_run) {
        runs.push_back(max_run);
        runs.push_back(0);
        rows -= max_run;
    }
    if (rows > 0) runs.push_back(static_cast<uint16_t>(rows));
    return runs;
}

} // namespace

void prepare_doris_v1(int rows, int block) {
    full_runs = make_dense_runs(block);
    tail_runs = make_dense_runs(rows % block);
}

void doris_v1(const uint8_t* in, uint8_t* out, int rows, int block) {
    using namespace doris;
    BoolPlainDecoder reader(in, (rows + 7) / 8);
    ColumnUInt8 column(out, rows);
    MutableColumnPtr column_ptr {&column};
    DataTypePtr type = nullptr;
    for (int decoded = 0; decoded < rows; decoded += block) {
        const int count = std::min(block, rows - decoded);
        ColumnSelectVector selection(count == block ? full_runs : tail_runs, count);
        require(reader.decode_values(column_ptr, type, selection, false).ok());
    }
}
