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
compiler=${1:?Pass the compiler name}
test "$(uname -m)" = aarch64
result_dir=arm70747-direct-results
mkdir -p "${result_dir}" arm70747-deps
curl -fsSL --retry 3 https://github.com/DLTcollab/sse2neon/archive/refs/tags/v1.6.0.tar.gz \
    -o arm70747-deps/sse2neon.tar.gz
echo 'dce28eb6a78f45bf98740d5fad73febb  arm70747-deps/sse2neon.tar.gz' | md5sum -c -
tar -xzf arm70747-deps/sse2neon.tar.gz -C arm70747-deps
python3 build-support/arm-pr70747/prepare.py > "${result_dir}/source-manifest.json"
python3 build-support/arm-pr70747/prepare-url-control.py
cp -r arm70747-generated build-support/arm-pr70747 "${result_dir}/"
cp .github/workflows/pr70747-arm-direct.yml "${result_dir}/workflow.yml"
uname -a > "${result_dir}/environment.txt"
lscpu >> "${result_dir}/environment.txt"
"${compiler}" --version >> "${result_dir}/environment.txt"
git rev-parse HEAD >> "${result_dir}/environment.txt"

includes=(-Ibe/src -Ibuild-support/arm-pr70747 -Iarm70747-generated
          -Iarm70747-deps/sse2neon-1.6.0)
common=(-std=c++23 -march=armv8-a+crc)
sources=(arm70747-generated/before_filter.cpp arm70747-generated/after_filter.cpp
         arm70747-generated/before_url.cpp arm70747-generated/after_url.cpp)
"${compiler}" "${common[@]}" -O2 -g -fsanitize=address,undefined -fno-omit-frame-pointer \
    "${includes[@]}" "${sources[@]}" build-support/arm-pr70747/correctness.cpp \
    be/test/exprs/function/find_symbols_test.cpp -lgtest_main -lgtest -pthread -o arm70747-ut
ASAN_OPTIONS=detect_leaks=1 ./arm70747-ut --gtest_output="xml:${result_dir}/unit.xml" \
    | tee "${result_dir}/unit.log"

# Verify all control/direct/inline inputs under sanitizers before timing release code.
"${compiler}" "${common[@]}" -O2 -g -fsanitize=address,undefined -fno-omit-frame-pointer \
    "${includes[@]}" build-support/arm-pr70747/url-arm-diagnostic.cpp -lbenchmark -pthread \
    -o arm70747-direct-asan
ASAN_OPTIONS=detect_leaks=1 ./arm70747-direct-asan --benchmark_min_time=0.001s \
    --benchmark_repetitions=1 --benchmark_out="${result_dir}/sanity.json" \
    --benchmark_out_format=json > "${result_dir}/sanity.log" 2>&1

"${compiler}" "${common[@]}" -O3 -DNDEBUG "${includes[@]}" \
    build-support/arm-pr70747/url-arm-diagnostic.cpp -lbenchmark -pthread -o arm70747-direct
"${compiler}" "${common[@]}" -O3 -DNDEBUG "${includes[@]}" -S \
    build-support/arm-pr70747/url-arm-diagnostic.cpp -o "${result_dir}/diagnostic.s"
objdump -d -C --no-show-raw-insn arm70747-direct > "${result_dir}/disassembly.txt"
for trial in 1 2; do
    taskset -c 0 ./arm70747-direct --benchmark_min_time=0.05s --benchmark_repetitions=9 \
        --benchmark_enable_random_interleaving=true \
        --benchmark_out="${result_dir}/trial-${trial}.json" --benchmark_out_format=json \
        > "${result_dir}/trial-${trial}.log" 2>&1
    echo "Trial ${trial} completed"
done

python3 - <<'PY'
import json
from pathlib import Path
for p in Path('arm70747-direct-results').glob('*.json'):
    if p.name == 'source-manifest.json':
        continue
    data = json.loads(p.read_text())
    assert not any(b.get('error_occurred') for b in data['benchmarks']), p
PY
