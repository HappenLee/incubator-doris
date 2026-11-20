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
#include "candidate_url.h"


#define DEFINE_VERSION(Name, scope)                                          \
    struct Name {                                                            \
        template <bool Reverse>                                              \
        static inline const char* find(const char* begin, const char* end) { \
            if constexpr (Reverse) {                                         \
                return scope::find_last_symbols_or_null<'.'>(begin, end);    \
            } else {                                                         \
                return scope::find_first_symbols<'.'>(begin, end);           \
            }                                                                \
        }                                                                    \
    }
DEFINE_VERSION(Before, before_url);
DEFINE_VERSION(After, after_url);
DEFINE_VERSION(Candidate, candidate_url);
#undef DEFINE_VERSION

template <class Version, bool Reverse>
__attribute__((noinline)) const char* direct_search(const char* begin, const char* end) {
    return Version::template find<Reverse>(begin, end);
}

template <class Version, bool Reverse, bool Inline>
void BM_Search(benchmark::State& state) {
    const size_t size = state.range(0);
    const int match = state.range(1);
    std::string data(size + 1, 'x');
    const size_t position = match == 1 ? 0 : match == 2 ? size - 1 : match == 4 ? 3 : match == 5 ? 4 : match == 6 ? size - 5 : size - 4;
    if (match != 0) data[position + 1] = '.';
    const char* begin = data.data() + 1;
    const char* end = begin + size;
    const char* expected = match == 0 ? (Reverse ? nullptr : end) : begin + position;
    if (Version::template find<Reverse>(begin, end) != expected ||
        direct_search<Version, Reverse>(begin, end) != expected) {
        state.SkipWithError("Unexpected delimiter position");
        return;
    }
    for (auto _ : state) {
        benchmark::ClobberMemory();
        const char* result;
        if constexpr (Inline) {
            result = Version::template find<Reverse>(begin, end);
        } else {
            result = direct_search<Version, Reverse>(begin, end);
        }
        benchmark::DoNotOptimize(result);
    }
    state.SetItemsProcessed(state.iterations());
}

#define REGISTER_SEARCH(version, label, reverse, direction, inline_call, call_label) \
    BENCHMARK_TEMPLATE(BM_Search, version, reverse, inline_call)                     \
            ->Name(call_label "/" label "/" direction)                               \
            ->ArgsProduct({{8, 16, 64, 256}, {0, 1, 2, 3, 4, 5, 6}})
#define REGISTER_VERSION(version, label)                                \
    REGISTER_SEARCH(version, label, false, "forward", false, "Direct"); \
    REGISTER_SEARCH(version, label, true, "reverse", false, "Direct");  \
    REGISTER_SEARCH(version, label, false, "forward", true, "Inline");  \
    REGISTER_SEARCH(version, label, true, "reverse", true, "Inline")
REGISTER_VERSION(Before, "before");
REGISTER_VERSION(After, "after");
REGISTER_VERSION(Candidate, "candidate");
#undef REGISTER_VERSION
#undef REGISTER_SEARCH

BENCHMARK_MAIN();
