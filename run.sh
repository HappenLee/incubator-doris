#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

mkdir -p results
if [[ "${NATIVE_ARM_REQUIRED:-1}" == 1 ]]; then
    [[ "$(uname -m)" == aarch64 ]]
fi
cpu=$(python3 -c 'import os; print(min(os.sched_getaffinity(0)))')
{
    uname -m
    lscpu
    printf 'Pinned CPU: %s\n' "$cpu"
} | tee results/machine.txt
python3 prepare.py generated | tee results/sources.txt

for compiler in "$@"; do
    compiler_name=$(basename "$compiler")
    mkdir -p "results/$compiler_name"
    "$compiler" --version | tee "results/$compiler_name/compiler.txt"
    flags=(-O3 -DNDEBUG -std=c++20 -Wall -Wextra -Wno-unused-parameter -Igenerated -I.)
    if [[ "$(uname -m)" == aarch64 ]]; then
        flags+=(-march=armv8-a)
    fi
    if [[ -n "${BENCH_BOOST_HEADERS:-}" ]]; then
        flags+=(-isystem "$BENCH_BOOST_HEADERS")
    fi
    "$compiler" "${flags[@]}" -dM -E -x c++ /dev/null > "results/$compiler_name/macros.txt"
    ! grep -Eq '^#define (__BMI2__|__AVX2__) ' "results/$compiler_name/macros.txt"
    "$compiler" "${flags[@]}" doris_decoder.cc velox_decoder.cc benchmark.cc -o generated/benchmark
    taskset -c "$cpu" generated/benchmark | tee "results/$compiler_name/run.csv"
    awk '/^(result|Verified)/' "results/$compiler_name/run.csv"
done
