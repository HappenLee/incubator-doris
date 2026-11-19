// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <benchmark/benchmark.h>

#include <algorithm>
#include <array>
#include <random>
#include <string>
#include <vector>

#include "api.h"

template <bool After>
void BM_Filter(benchmark::State& state) {
    const size_t size = state.range(0);
    const int percent = state.range(1);
    std::mt19937 random(70747);
    std::vector<uint8_t> bytes(size + 1);
    for (size_t i = 0; i < size; ++i) bytes[i + 1] = i * 100 / size < percent;
    std::shuffle(bytes.begin() + 1, bytes.end(), random);
    FilterView filter {bytes.data() + 1, size};
    for (auto _ : state) {
        benchmark::DoNotOptimize(filter.bytes);
        auto count = After ? after_count(filter) : before_count(filter);
        benchmark::DoNotOptimize(count);
    }
    state.SetBytesProcessed(state.iterations() * size);
}

template <bool After, int Symbols, bool Reverse>
void BM_Url(benchmark::State& state) {
    const size_t size = state.range(0);
    const int match = state.range(1);
    // 0: no match; 1: at the start; 2: at the end; 3: before a three-byte suffix.
    std::string data(size + 1, 'x');
    if (match == 1) data[1] = '.';
    if (match == 2) data[size] = '.';
    if (match == 3) data[size - 3] = '.';
    const char* input = data.data() + 1;
    for (auto _ : state) {
        benchmark::DoNotOptimize(input);
        auto position = After ? after_search(input, size, Symbols, Reverse)
                              : before_search(input, size, Symbols, Reverse);
        benchmark::DoNotOptimize(position);
    }
    state.SetItemsProcessed(state.iterations());
}

BENCHMARK_TEMPLATE(BM_Filter, false)
        ->Name("Filter/before")
        ->ArgsProduct({{64, 256, 4096, 65536, 1048576}, {0, 25, 50, 75, 100}});
BENCHMARK_TEMPLATE(BM_Filter, true)
        ->Name("Filter/after")
        ->ArgsProduct({{64, 256, 4096, 65536, 1048576}, {0, 25, 50, 75, 100}});

#define URL_BENCH(after, label, symbols, reverse, direction) \
    BENCHMARK_TEMPLATE(BM_Url, after, symbols, reverse)      \
            ->Name("URL/" label "/" direction "/" #symbols)  \
            ->ArgsProduct({{8, 15, 16, 17, 32, 64, 128, 256, 1024}, {0, 1, 2, 3}})
URL_BENCH(false, "before", 1, false, "forward");
URL_BENCH(true, "after", 1, false, "forward");
URL_BENCH(false, "before", 1, true, "reverse");
URL_BENCH(true, "after", 1, true, "reverse");
URL_BENCH(false, "before", 3, false, "forward");
URL_BENCH(true, "after", 3, false, "forward");
URL_BENCH(false, "before", 3, true, "reverse");
URL_BENCH(true, "after", 3, true, "reverse");
#undef URL_BENCH

BENCHMARK_MAIN();
