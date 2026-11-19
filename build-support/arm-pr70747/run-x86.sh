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
compiler=${1:?Pass the compiler path}
test_cpu=${2:?Pass a permitted CPU number}
test "$(uname -m)" = x86_64
result_dir=.agent_tasks/pr70747-x86/results
mkdir -p "${result_dir}"
python3 build-support/arm-pr70747/prepare.py > "${result_dir}/source-manifest.json"
uname -a > "${result_dir}/environment.txt"
lscpu >> "${result_dir}/environment.txt"
"${compiler}" --version >> "${result_dir}/environment.txt"
git rev-parse HEAD >> "${result_dir}/environment.txt"
echo "Pinned CPU: ${test_cpu}" >> "${result_dir}/environment.txt"
cat /proc/loadavg >> "${result_dir}/environment.txt"

includes=(-Ibe/src -Ibuild-support/arm-pr70747 -Iarm70747-generated
          -Ithirdparty/installed/include)
sources=(arm70747-generated/before_filter.cpp arm70747-generated/after_filter.cpp
         arm70747-generated/before_url.cpp arm70747-generated/after_url.cpp)

for mode in sse42 avx2; do
    mkdir -p "${result_dir}/${mode}"
    isa=(-msse4.2)
    if [ "${mode}" = avx2 ]; then isa+=(-mavx2); fi
    "${compiler}" -std=c++23 -O2 -g "${isa[@]}" -fsanitize=address,undefined \
        -fno-omit-frame-pointer "${includes[@]}" "${sources[@]}" \
        build-support/arm-pr70747/correctness.cpp be/test/exprs/function/find_symbols_test.cpp \
        -Lthirdparty/installed/lib64 -lgtest_main -lgtest -pthread \
        -o "${result_dir}/${mode}/unit"
    ASAN_OPTIONS=detect_leaks=1 "${result_dir}/${mode}/unit" \
        --gtest_output="xml:${result_dir}/${mode}/unit.xml" \
        > "${result_dir}/${mode}/unit.log" 2>&1
    "${compiler}" -std=c++23 -O3 -DNDEBUG "${isa[@]}" "${includes[@]}" "${sources[@]}" \
        build-support/arm-pr70747/benchmark.cpp -Lthirdparty/installed/lib64 \
        -lbenchmark -pthread -o "${result_dir}/${mode}/benchmark"
    for source in "${sources[@]}"; do
        "${compiler}" -std=c++23 -O3 -DNDEBUG "${isa[@]}" "${includes[@]}" -S "${source}" \
            -o "${result_dir}/${mode}/$(basename "${source}" .cpp).s"
    done
    taskset -c "${test_cpu}" "${result_dir}/${mode}/benchmark" \
        --benchmark_min_time=0.05s --benchmark_repetitions=7 \
        --benchmark_enable_random_interleaving=true \
        --benchmark_out="${result_dir}/${mode}/benchmark.json" --benchmark_out_format=json \
        > "${result_dir}/${mode}/benchmark.log" 2>&1
    echo "${mode}: correctness and benchmark finished"
done
cat /proc/loadavg >> "${result_dir}/environment.txt"

# Recheck the nanosecond-scale URL paths without runtime wrapper dispatch.
for mode in sse42 avx2; do
    isa=(-msse4.2)
    if [ "${mode}" = avx2 ]; then isa+=(-mavx2); fi
    "${compiler}" -std=c++23 -O3 -DNDEBUG "${isa[@]}" "${includes[@]}" \
        build-support/arm-pr70747/url-direct-benchmark.cpp \
        -Lthirdparty/installed/lib64 -lbenchmark -pthread -o "${result_dir}/${mode}/direct"
    objdump -d -C --no-show-raw-insn "${result_dir}/${mode}/direct" \
        > "${result_dir}/${mode}/direct-assembly.txt"
    taskset -c "${test_cpu}" "${result_dir}/${mode}/direct" \
        --benchmark_min_time=0.05s --benchmark_repetitions=7 \
        --benchmark_enable_random_interleaving=true \
        --benchmark_out="${result_dir}/${mode}/direct.json" --benchmark_out_format=json \
        > "${result_dir}/${mode}/direct.log" 2>&1
    echo "${mode}: direct URL benchmark finished"
done
