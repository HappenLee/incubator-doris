#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Prepare isolated copies of the exact PR kernels; do not rewrite their loops."""
import hashlib
import json
from pathlib import Path
import re
import subprocess

BASE = "fda8d83426ae45bdf1bfef2f331846bbe9af7826"
HEAD = "a2a4709571873e475699d69787b0da8a486ae6a7"
out = Path("arm70747-generated")
out.mkdir(exist_ok=True)
manifest = {}
CANDIDATE = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
for variant, revision in (("before", BASE), ("after", HEAD), ("candidate", CANDIDATE)):
    def source(path):
        text = subprocess.check_output(["git", "show", f"{revision}:{path}"], text=True)
        manifest[f"{variant}:{path}"] = {
            "commit": revision, "sha256": hashlib.sha256(text.encode()).hexdigest()
        }
        return text

    column = source("be/src/core/column/columns_common.cpp")
    signature = "size_t count_bytes_in_filter(const IColumn::Filter& filt)"
    start = column.index(signature)
    opening = column.index("{", start)
    nesting = 1
    end = opening + 1
    while nesting:
        nesting += (column[end] == "{") - (column[end] == "}")
        end += 1
    body = column[opening:end]
    manifest[f"{variant}:filter_body_sha256"] = hashlib.sha256(body.encode()).hexdigest()
    # Only the function name and the two-accessor container interface are adapted.
    (out / f"{variant}_filter.cpp").write_text(
        '#include "api.h"\n#include "util/sse_util.hpp"\n'
        'using Int8 = int8_t;\nusing UInt64 = uint64_t;\n'
        f'size_t {variant}_count(const FilterView& filt) {body}\n')

    header = source("be/src/exprs/function/url/find_symbols.h")
    # Namespace isolation lets before and after coexist without inline ODR collisions.
    # Standard and SIMD includes stay outside that namespace.
    includes = re.findall(r'^#include .+$', header, re.MULTILINE)
    header = re.sub(r'^#include .+$', '', header, flags=re.MULTILINE)
    header = header.replace('#pragma once', '')
    header = header.replace('::detail::', f'::{variant}_url::detail::')
    (out / f"{variant}_url.h").write_text(
        '#pragma once\n#include <cstring>\n#include <stdexcept>\n'
        '#include <string_view>\n#include <type_traits>\n#include <utility>\n'
        '#include "util/sse_util.hpp"\n'
        # The original header's nmmintrin.h is x86-only; sse_util supplies the ISA header.
        + '\n'.join(x for x in includes if 'nmmintrin.h' not in x)
        + f'\nnamespace {variant}_url {{\n' + header + '\n}\n')

    empty_case = 'case 0: return search<>(data, size, mode);'
    if variant == "before":
        # The baseline's empty compile-time set fails to instantiate on x86.
        # Keep that unsupported case out of the wrapper; tests skip only that baseline case.
        empty_case = '#if defined(__aarch64__)\n' + empty_case + '\n#endif'
    (out / f"{variant}_url.cpp").write_text(f'''#include "api.h"
#include "{variant}_url.h"
template<char... Symbols>
ptrdiff_t search(const char* data, size_t size, int mode) {{
    const char* end = data + size;
    const char* found = nullptr;
    switch (mode) {{
    case 0: found = {variant}_url::find_first_symbols<Symbols...>(data, end); break;
    case 1: found = {variant}_url::find_last_symbols_or_null<Symbols...>(data, end); break;
    case 2: found = {variant}_url::find_first_not_symbols<Symbols...>(data, end); break;
    case 3: found = {variant}_url::find_last_not_symbols_or_null<Symbols...>(data, end); break;
    }}
    return found == nullptr || found == end ? -1 : found - data;
}}
ptrdiff_t {variant}_search(const char* data, size_t size, int symbols, int mode) {{
    switch (symbols) {{
    {empty_case}
    case 1: return search<'.'>(data, size, mode);
    case 3: return search<'.', '/', '?'>(data, size, mode);
    case 5: return search<'.', '/', '?', ':', '#'>(data, size, mode);
    default: __builtin_unreachable();
    }}
}}
'''.replace('template<char... Symbols>\nptrdiff_t search', 'template<char... Symbols>\nstatic ptrdiff_t search'))

(out / "manifest.json").write_text(json.dumps(manifest, indent=2) + '\n')
print(json.dumps(manifest, indent=2))
