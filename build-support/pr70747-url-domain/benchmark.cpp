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

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <random>
#include <string>
#include <vector>

#include "after_domain.h"
#include "before_domain.h"

constexpr size_t ROWS = 4096;

enum class Corpus { SHORT, BOUNDARY, LONG, MULTI_LABEL, MIXED };

struct UrlColumn {
    std::string bytes;
    std::vector<uint32_t> ends;
};

UrlColumn make_urls(Corpus corpus, uint32_t seed) {
    UrlColumn urls;
    std::mt19937 rng(seed);
    for (size_t i = 0; i < ROWS; ++i) {
        auto shape = corpus;
        if (shape == Corpus::MIXED) {
            unsigned pick = rng() % 100;
            shape = pick < 55   ? Corpus::SHORT
                    : pick < 75 ? Corpus::BOUNDARY
                    : pick < 85 ? Corpus::LONG
                                : Corpus::MULTI_LABEL;
        }
        const std::string number = std::to_string(100 + rng() % 900);
        const char c = 'a' + rng() % 26;
        std::string host;
        switch (shape) {
        case Corpus::SHORT:
            host = std::string(1 + rng() % 6, c) + number + ".com";
            break;
        case Corpus::BOUNDARY:
            host = std::string(8 + rng() % 3, c) + number + ".com";
            break;
        case Corpus::LONG:
            host = std::string(28 + rng() % 32, c) + number + ".com";
            break;
        case Corpus::MULTI_LABEL:
            host = "www.edge" + number + "." + std::string(4 + rng() % 23, c) +
                   (i % 2 ? ".co.uk" : ".net");
            break;
        case Corpus::MIXED:
            std::abort();
        }
        urls.bytes += i % 3 ? "https://" : "http://";
        urls.bytes += host;
        urls.bytes += i % 7 ? "/resource?id=" + number : "";
        urls.ends.push_back(urls.bytes.size());
    }
    urls.bytes.append(16, '\0');
    return urls;
}

struct Output {
    std::vector<char> bytes;
    std::vector<uint32_t> ends = std::vector<uint32_t>(ROWS);
};

template <class Extractor>
[[gnu::noinline]] size_t run_column(const UrlColumn& urls, Output& output) {
    size_t begin = 0;
    size_t output_end = 0;
    for (size_t i = 0; i < urls.ends.size(); ++i) {
        const char* result;
        size_t length;
        size_t end = urls.ends[i];
        Extractor::execute(urls.bytes.data() + begin, end - begin, result, length);
        std::memcpy(output.bytes.data() + output_end, result, length);
        output_end += length;
        output.ends[i] = output_end;
        begin = end;
    }
    return output_end;
}

template <class Extractor, class Reference, Corpus Shape>
void BM_Url(benchmark::State& state) {
    UrlColumn input = make_urls(Shape, 70747);
    Output output {std::vector<char>(input.bytes.size())};
    Output reference {std::vector<char>(input.bytes.size())};
    size_t got = run_column<Extractor>(input, output);
    size_t want = run_column<Reference>(input, reference);
    if (got != want || output.ends != reference.ends ||
        std::memcmp(output.bytes.data(), reference.bytes.data(), want) != 0) {
        state.SkipWithError("Extraction differs from baseline");
        return;
    }
    for (auto _ : state) {
        benchmark::ClobberMemory();
        auto written = run_column<Extractor>(input, output);
        benchmark::DoNotOptimize(written);
    }
    state.SetItemsProcessed(state.iterations() * ROWS);
}

#define REGISTER_PAIR(Function, Label, Shape, InputLabel)                        \
    BENCHMARK_TEMPLATE(BM_Url, before::Function, after::Function, Corpus::Shape) \
            ->Name(Label "/" InputLabel "/before");                              \
    BENCHMARK_TEMPLATE(BM_Url, after::Function, before::Function, Corpus::Shape) \
            ->Name(Label "/" InputLabel "/after")

#define REGISTER_CORPORA(Function, Label)                 \
    REGISTER_PAIR(Function, Label, SHORT, "short");       \
    REGISTER_PAIR(Function, Label, BOUNDARY, "boundary"); \
    REGISTER_PAIR(Function, Label, LONG, "long");         \
    REGISTER_PAIR(Function, Label, MULTI_LABEL, "multi"); \
    REGISTER_PAIR(Function, Label, MIXED, "mixed")

REGISTER_CORPORA(ExtractTopLevelDomain, "top_level_domain");
REGISTER_CORPORA(ExtractFirstSignificantSubdomain, "first_significant_subdomain");
REGISTER_CORPORA(CutToFirstSignificantSubdomain, "cut_to_first_significant_subdomain");

BENCHMARK_MAIN();
