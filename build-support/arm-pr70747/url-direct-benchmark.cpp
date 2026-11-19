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

#include <string>

#include "after_url.h"
#include "before_url.h"

// Direct entry points remove the runtime symbol-count/direction dispatch used by
// the broad comparison harness, which can matter for a few-nanosecond early hit.
__attribute__((noinline)) const char* before_forward(const char* begin, const char* end) {
    return before_url::find_first_symbols<'.'>(begin, end);
}
__attribute__((noinline)) const char* after_forward(const char* begin, const char* end) {
    return after_url::find_first_symbols<'.'>(begin, end);
}
__attribute__((noinline)) const char* before_reverse(const char* begin, const char* end) {
    return before_url::find_last_symbols_or_null<'.'>(begin, end);
}
__attribute__((noinline)) const char* after_reverse(const char* begin, const char* end) {
    return after_url::find_last_symbols_or_null<'.'>(begin, end);
}

template <auto Search, bool Reverse>
void BM_Direct(benchmark::State& state) {
    const size_t size = state.range(0);
    const int match = state.range(1);
    std::string data(size + 1, 'x');
    const size_t position = match == 1 ? 0 : match == 2 ? size - 1 : size - 4;
    if (match != 0) data[position + 1] = '.';
    const char* begin = data.data() + 1;
    const char* end = begin + size;
    const char* expected = match == 0 ? (Reverse ? nullptr : end) : begin + position;
    if (Search(begin, end) != expected) {
        state.SkipWithError("Unexpected delimiter position");
        return;
    }
    for (auto _ : state) {
        benchmark::ClobberMemory();
        const char* result = Search(begin, end);
        benchmark::DoNotOptimize(result);
    }
    state.SetItemsProcessed(state.iterations());
}

BENCHMARK_TEMPLATE(BM_Direct, before_forward, false)
        ->Name("Direct/before/forward")
        ->ArgsProduct({{64, 256}, {0, 1, 2, 3}});
BENCHMARK_TEMPLATE(BM_Direct, after_forward, false)
        ->Name("Direct/after/forward")
        ->ArgsProduct({{64, 256}, {0, 1, 2, 3}});
BENCHMARK_TEMPLATE(BM_Direct, before_reverse, true)
        ->Name("Direct/before/reverse")
        ->ArgsProduct({{64, 256}, {0, 1, 2, 3}});
BENCHMARK_TEMPLATE(BM_Direct, after_reverse, true)
        ->Name("Direct/after/reverse")
        ->ArgsProduct({{64, 256}, {0, 1, 2, 3}});

BENCHMARK_MAIN();
