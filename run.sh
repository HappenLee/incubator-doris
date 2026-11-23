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
    for variant in 0 1 2; do
        "$compiler" "${flags[@]}" -DDORIS_ARM_VARIANT="$variant" \
            -c doris_decoder.cc -o "generated/doris_$variant.o"
    done
    "$compiler" "${flags[@]}" -DDORIS_ARM_VARIANT=0 generated/doris_[012].o \
        doris_legacy.cc velox_decoder.cc benchmark.cc \
        -o generated/benchmark
    if [[ "$(uname -m)" == aarch64 ]]; then
        objdump -d generated/doris_1.o > "results/$compiler_name/starrocks-style.s"
        objdump -d generated/doris_2.o > "results/$compiler_name/neon.s"
        grep -Eq '[[:space:]]tbl[[:space:]]' "results/$compiler_name/neon.s"
    fi
    taskset -c "$cpu" generated/benchmark | tee "results/$compiler_name/run.csv"
    awk '/^(result|Verified)/' "results/$compiler_name/run.csv"
done
