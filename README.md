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

Two candidate implementations replace only the 32-value kernel for
`uint8_t, bit width 1` on ARM64. The first uses scalar shift-and-mask with a
NEON store in 8-value groups, following the StarRocks approach. The second
uses NEON byte lookup, masking, and unsigned minimum to expand 32 values
from a 4-byte load. Each candidate is built alongside the baseline in its own
translation unit and namespace. It runs both inside the unchanged Doris
decoder and as a bulk primitive.

Native V1 uses the unchanged legacy PLAIN decoder and run iteration from the
last upstream commit before legacy scanner removal. Its output column models
already-reserved UInt8 storage. The no-null run metadata is prepared outside
timing.
