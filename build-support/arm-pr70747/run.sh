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
mkdir -p arm70747-results arm70747-deps

# Use the exact sse2neon version and checksum declared in this Doris revision.
curl -fsSL --retry 3 https://github.com/DLTcollab/sse2neon/archive/refs/tags/v1.6.0.tar.gz \
    -o arm70747-deps/sse2neon.tar.gz
echo 'dce28eb6a78f45bf98740d5fad73febb  arm70747-deps/sse2neon.tar.gz' | md5sum -c -
tar -xzf arm70747-deps/sse2neon.tar.gz -C arm70747-deps

python3 build-support/arm-pr70747/prepare.py > arm70747-results/source-manifest.json
uname -a > arm70747-results/environment.txt
lscpu >> arm70747-results/environment.txt
"${compiler}" --version >> arm70747-results/environment.txt
git rev-parse HEAD >> arm70747-results/environment.txt
"${compiler}" -march=armv8-a+crc -dM -E -x c++ /dev/null > arm70747-results/compiler-macros.txt
cp .github/workflows/pr70747-arm-benchmark.yml arm70747-results/workflow.yml
cp -r build-support/arm-pr70747 arm70747-generated arm70747-results/

includes=(-Ibe/src -Ibuild-support/arm-pr70747 -Iarm70747-generated
          -Iarm70747-deps/sse2neon-1.6.0)
sources=(arm70747-generated/before_filter.cpp arm70747-generated/after_filter.cpp
         arm70747-generated/before_url.cpp arm70747-generated/after_url.cpp)
common=(-std=c++23 -march=armv8-a+crc)

# These are isolated kernel tests, not a complete BE build.
"${compiler}" "${common[@]}" -O2 -g -fsanitize=address,undefined -fno-omit-frame-pointer \
    "${includes[@]}" "${sources[@]}" build-support/arm-pr70747/correctness.cpp \
    be/test/exprs/function/find_symbols_test.cpp -lgtest_main -lgtest -pthread -o arm70747-ut
ASAN_OPTIONS=detect_leaks=1 ./arm70747-ut --gtest_output=xml:arm70747-results/unit.xml \
    | tee arm70747-results/unit.log

"${compiler}" "${common[@]}" -O3 -DNDEBUG "${includes[@]}" "${sources[@]}" \
    build-support/arm-pr70747/benchmark.cpp -lbenchmark -pthread -o arm70747-bench
for source in "${sources[@]}"; do
    "${compiler}" "${common[@]}" -O3 -DNDEBUG "${includes[@]}" -S "${source}" \
        -o "arm70747-results/$(basename "${source}" .cpp).s"
done
taskset -c 0 ./arm70747-bench --benchmark_min_time=0.05s --benchmark_repetitions=7 \
    --benchmark_enable_random_interleaving=true \
    --benchmark_out=arm70747-results/benchmark.json --benchmark_out_format=json \
    > arm70747-results/benchmark.log 2>&1
