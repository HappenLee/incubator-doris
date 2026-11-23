#include "bench.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <numeric>
#include <random>
#include <vector>

#if defined(__BMI2__) || defined(__AVX2__)
#error This benchmark must omit BMI2 and AVX2.
#endif

using Clock = std::chrono::steady_clock;
using Decode = void (*)(const uint8_t*, uint8_t*, int, int);

void require(bool condition) {
    if (!condition) {
        std::fputs("Decoder returned the wrong output size or values.\n", stderr);
        std::abort();
    }
}

struct Implementation {
    const char* name;
    Decode decode;
};

const std::array implementations = {
        Implementation {"Doris_native", doris_native},
        Implementation {"Velox_dense", velox_dense},
        Implementation {"Doris_bulk_primitive", doris_bulk},
};

struct Input {
    std::vector<uint8_t> packed;
    std::vector<uint8_t> expected;
    std::vector<uint8_t> output;
    uint64_t checksum;

    explicit Input(int rows, uint32_t seed)
            : packed((rows + 7) / 8), expected(rows), output(rows + 16, 0xA5) {
        std::mt19937 rng(seed);
        for (auto& byte : packed) byte = static_cast<uint8_t>(rng());
        for (int i = 0; i < rows; ++i) expected[i] = (packed[i / 8] >> (i % 8)) & 1;
        checksum = std::accumulate(expected.begin(), expected.end(), uint64_t {0});
    }

    void check() const {
        require(std::equal(expected.begin(), expected.end(), output.begin()));
        require(std::all_of(output.begin() + expected.size(), output.end(),
                            [](uint8_t byte) { return byte == 0xA5; }));
    }
};

void verify_decoders() {
    for (int rows : {1, 3, 7, 8, 9, 31, 32, 33, 127, 128, 129, 257, 4097}) {
        for (int block : {1, 3, 7, 8, 31, 32, 127, 128, 136, 4096}) {
            for (const auto& impl : implementations) {
                if (impl.decode == doris_bulk && block % 8 != 0) continue;
                Input data(rows, rows + block);
                impl.decode(data.packed.data(), data.output.data(), rows, block);
                data.check();
            }
        }
    }
}

double elapsed_ns(Decode decode, Input& data, int rows, int block, int iterations) {
    auto* out = data.output.data();
    const auto start = Clock::now();
    for (int i = 0; i < iterations; ++i) {
        decode(data.packed.data(), out, rows, block);
        asm volatile("" : : "r"(out) : "memory");
    }
    const auto end = Clock::now();
    data.check();
    return std::chrono::duration<double, std::nano>(end - start).count();
}

int calibrate(Decode decode, Input& data, int rows, int block) {
    int iterations = 1;
    while (elapsed_ns(decode, data, rows, block, iterations) < 10'000'000) {
        iterations *= 2;
    }
    return iterations;
}

void benchmark(int rows, int block) {
    constexpr int rounds = 11;
    Input data(rows, 731);
    std::array<std::vector<double>, implementations.size()> samples;
    std::array<int, implementations.size()> iterations;
    for (size_t i = 0; i < implementations.size(); ++i) {
        iterations[i] = calibrate(implementations[i].decode, data, rows, block);
    }
    std::array<size_t, implementations.size()> order {0, 1, 2};
    std::mt19937 schedule(rows + block);
    for (int repeat = 0; repeat < rounds; ++repeat) {
        std::shuffle(order.begin(), order.end(), schedule);
        for (size_t i : order) {
            const double ns = elapsed_ns(implementations[i].decode, data, rows, block, iterations[i]);
            const double result = ns / iterations[i];
            samples[i].push_back(result);
            std::printf("sample,%d,%d,%s,%d,%.3f\n", rows, block, implementations[i].name, repeat, result);
        }
    }
    for (size_t i = 0; i < implementations.size(); ++i) {
        auto& values = samples[i];
        std::sort(values.begin(), values.end());
        const auto median = values[rounds / 2];
        std::printf("result,%d,%d,%s,%.3f,%.3f,%.3f,%.4f,%llu\n", rows, block,
                    implementations[i].name, median, values[2], values[8], rows / median,
                    static_cast<unsigned long long>(data.checksum));
    }
}

int main() {
    verify_decoders();
    std::puts("Verified values, canaries, and split batches across byte and scratch boundaries.");
    std::puts("result,values,block,implementation,median_ns,p20_ns,p80_ns,Gvalues/s,checksum");
    benchmark(128, 128);
    benchmark(4096, 4096);
    benchmark(100'000, 100'000);
    benchmark(100'000, 4096);
    benchmark(1'048'576, 4096);
}
