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

import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys

base = "fda8d83426ae45bdf1bfef2f331846bbe9af7826"
head = "edc6f74cf8a1ca28890a390a115830fad997a521"
out = Path(sys.argv[1])
out.mkdir(parents=True, exist_ok=True)
manifest = {}

def read(revision, path):
    data = subprocess.check_output(["git", "show", f"{revision}:{path}"])
    manifest[f"{revision}:{path}"] = hashlib.sha256(data).hexdigest()
    return data.decode()

for label, revision in (("before", base), ("after", head)):
    root = "be/src/exprs/function/url/"
    find = read(revision, root + "find_symbols.h")
    domain = read(revision, root + "domain.h")
    domain = domain.replace("namespace doris {", f"namespace {label} {{")
    # The parser and lookup calls are unchanged. Replace the heavy ColumnString
    # include pulled in via protocol.h with the Pos alias and StringRef interface
    # these extractors use. Include the original ASCII helpers and TLD table.
    adapter = """
using Pos = const char*;
struct StringRef {
    const char* data = nullptr;
    size_t size = 0;
    StringRef() = default;
    StringRef(const char* p, size_t n) : data(p), size(n) {}
    std::string_view to_string_view() const { return {data, size}; }
};
"""
    domain = domain.replace(f"namespace {label} {{", f"namespace {label} {{" + adapter)
    domain = domain.replace('#include "exprs/function/url/protocol.h"', "")
    domain = domain.replace('#include "exprs/function/url/find_symbols.h"', f'#include "{label}_find.h"')
    # Keep the original standard/intrinsic include guards outside the namespace.
    includes = re.findall(r"^#include .+$", find, re.MULTILINE)
    find = re.sub(r"^#include .+$", "", find, flags=re.MULTILINE)
    find = find.replace("#pragma once", "").replace("::detail::", f"::{label}::detail::")
    imports = "\n".join(line for line in includes if "nmmintrin.h" not in line)
    (out/f"{label}_find.h").write_text(
        '#pragma once\n#include <cstring>\n#include <stdexcept>\n#include <string_view>\n'
        '#include <type_traits>\n#include <utility>\n#include "util/sse_util.hpp"\n'
        + imports + f"\nnamespace {label} {{\n" + find + "\n}\n")
    (out/f"{label}_domain.h").write_text(domain)
for path in ("be/src/exprs/function/url/tldLookup.h",
             "be/src/exprs/function/url/tldLookup.generated.cpp",
             "be/src/exec/common/string_utils/string_utils.h",
             "be/src/util/simd/bits.h", "be/src/util/sse_util.hpp"):
    assert read(head, path) == Path(path).read_text(), path
(out/"manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
