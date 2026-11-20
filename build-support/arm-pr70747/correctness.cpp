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

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <random>
#include <string>
#include <vector>

#include "api.h"

TEST(FilterKernel, BoundariesAlignmentAndSignedValues) {
    constexpr std::array<uint8_t, 8> values = {0, 1, 2, 127, 128, 129, 254, 255};
    for (size_t size : {0, 1, 15, 16, 31, 32, 63, 64, 65, 127, 128, 129, 4096, 65536}) {
        for (size_t offset = 0; offset < 32; ++offset) {
            for (size_t pattern = 0; pattern <= values.size(); ++pattern) {
                // Exact allocation after the input allows ASAN to detect an over-read.
                std::vector<uint8_t> data(size + offset + (size == 0 && offset == 0));
                size_t expected = 0;
                for (size_t i = 0; i < size; ++i) {
                    auto value = values[pattern == values.size() ? (i * 7 + offset) % values.size()
                                                                 : pattern];
                    data[offset + i] = value;
                    expected += value > 0 && value < 128;
                }
                FilterView view {data.data() + offset, size};
                EXPECT_EQ(expected, before_count(view)) << size << "/" << offset;
                EXPECT_EQ(expected, candidate_count(view));
                EXPECT_EQ(expected, after_count(view)) << size << "/" << offset;
            }
        }
    }
}

ptrdiff_t reference_search(const char* data, size_t size, int symbols, int mode) {
    auto matches = [symbols, mode](char c) {
        std::string_view needles("./?:#", symbols);
        bool present = needles.find(c) != std::string_view::npos;
        return mode < 2 ? present : !present;
    };
    if (mode % 2 == 0) {
        for (size_t i = 0; i < size; ++i) {
            if (matches(data[i])) return i;
        }
    } else {
        for (size_t i = size; i > 0; --i) {
            if (matches(data[i - 1])) return i - 1;
        }
    }
    return -1;
}

TEST(UrlKernel, RandomBytesDirectionsAndSymbolSets) {
    std::mt19937 random(70747);
    for (size_t size : {0, 1, 15, 16, 17, 31, 32, 33, 63, 64, 65, 255, 256, 1024}) {
        for (size_t offset = 0; offset < 32; ++offset) {
            std::vector<char> data(size + offset + (size == 0 && offset == 0));
            for (size_t i = 0; i < size; ++i) data[offset + i] = random() & 255;
            for (int symbols : {0, 1, 3, 5}) {
                for (int mode = 0; mode < 4; ++mode) {
                    auto expected = reference_search(data.data() + offset, size, symbols, mode);
                    // The old x86 template cannot instantiate an empty symbol set.
#if defined(__aarch64__)
                    constexpr bool baseline_supports_empty = true;
#else
                    constexpr bool baseline_supports_empty = false;
#endif
                    if (symbols != 0 || baseline_supports_empty) {
                        EXPECT_EQ(expected,
                                  before_search(data.data() + offset, size, symbols, mode));
                    }
                    EXPECT_EQ(expected, after_search(data.data() + offset, size, symbols, mode));
                    EXPECT_EQ(expected, candidate_search(data.data() + offset, size, symbols, mode));
                }
            }
        }
    }
}

TEST(UrlKernel, EveryMatchPositionAndMiss) {
    for (size_t size : {0, 1, 15, 16, 17, 31, 32, 33, 63, 64, 65, 255, 256}) {
        for (size_t offset : {0, 1, 7, 15}) {
            std::vector<char> data(size + offset + (size == 0 && offset == 0), 'x');
            for (size_t position = 0; position <= size; ++position) {
                if (position != size) data[offset + position] = '.';
                for (int mode : {0, 1}) {
                    auto expected = position == size ? -1 : static_cast<ptrdiff_t>(position);
                    EXPECT_EQ(expected, before_search(data.data() + offset, size, 1, mode));
                    EXPECT_EQ(expected, after_search(data.data() + offset, size, 1, mode));
                    EXPECT_EQ(expected, candidate_search(data.data() + offset, size, 1, mode));
                }
                if (position != size) data[offset + position] = 'x';
            }
        }
    }
}
