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

#pragma once

#include <cstddef>
#include <cstdint>

// The measured loop only needs data() and size(); no Doris column is constructed here.
struct FilterView {
    const uint8_t* bytes;
    size_t length;
    const uint8_t* data() const { return bytes; }
    size_t size() const { return length; }
};

size_t before_count(const FilterView& filter);
size_t after_count(const FilterView& filter);
ptrdiff_t before_search(const char* data, size_t size, int symbols, int mode);
ptrdiff_t after_search(const char* data, size_t size, int symbols, int mode);
