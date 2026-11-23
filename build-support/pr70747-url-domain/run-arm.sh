#!/usr/bin/env bash
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

set -euo pipefail
compiler=${1:?Pass compiler name}
test "$(uname -m)" = aarch64
result=url-domain-results
mkdir -p "$result/deps"
curl -fsSL --retry 3 https://github.com/DLTcollab/sse2neon/archive/refs/tags/v1.6.0.tar.gz -o "$result/deps/sse2neon.tar.gz"
echo "dce28eb6a78f45bf98740d5fad73febb  $result/deps/sse2neon.tar.gz" | md5sum -c -
tar -xzf "$result/deps/sse2neon.tar.gz" -C "$result/deps"

python3 build-support/pr70747-url-domain/prepare.py "$result/generated"
cp -r build-support/pr70747-url-domain "$result/harness"
lscpu > "$result/environment.txt"
"$compiler" --version >> "$result/environment.txt"
flags=(-std=c++23 -march=armv8-a+crc -Ibe/src -I"$result/generated" -I"$result/deps/sse2neon-1.6.0")
sources=(build-support/pr70747-url-domain/benchmark.cpp be/src/exprs/function/url/tldLookup.generated.cpp)

"$compiler" "${flags[@]}" -O2 -g -fsanitize=address,undefined -fno-omit-frame-pointer \
    "${sources[@]}" -lbenchmark -pthread -o domain-bench-sanitized
./domain-bench-sanitized --benchmark_min_time=0.001s --benchmark_repetitions=1 \
    --benchmark_out="$result/sanity.json" --benchmark_out_format=json > "$result/sanity.log" 2>&1

"$compiler" "${flags[@]}" -O3 -DNDEBUG "${sources[@]}" -lbenchmark -pthread -o domain-bench
objdump -d -C --no-show-raw-insn domain-bench > "$result/disassembly.txt"
for round in 1 2; do
  taskset -c 0 ./domain-bench --benchmark_min_time=0.04s --benchmark_repetitions=9 \
      --benchmark_enable_random_interleaving=true --benchmark_out="$result/trial-$round.json" --benchmark_out_format=json \
      > "$result/trial-$round.log" 2>&1
done

python3 - <<'PYCHECK'
import json
from pathlib import Path
for p in Path('url-domain-results').glob('*.json'):
    results = json.loads(p.read_text())['benchmarks']
    assert results and all(not b.get('error_occurred') for b in results), p
PYCHECK
