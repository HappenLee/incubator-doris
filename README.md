# Parquet PLAIN BOOLEAN on ARM64

The workflow runs the two upstream decoders on an ARM64 runner with identical
compiler flags, inputs, output format, and pinned core. The generated source
files come from the commits in `prepare.py`. The harness replaces the Doris
engine consumer with a byte-copying consumer and uses Velox's upstream
benchmark visitor, both writing one byte per value. Doris bulk unpack writes
straight to the same output as a separate primitive-level measurement.

This benchmark covers dense, null-free PLAIN values in memory. It times
resetting the decoder and writing decoded output without timing allocation,
I/O, decompression, or validation. It checks every output byte, leftover
bits across read calls, and output canaries. Cases use 11 interleaved rounds
with a calibrated number of iterations. The CSV files contain every round
and the summary medians.
