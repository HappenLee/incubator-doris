"""Fetch the pinned upstream implementations and isolate the decoder dependencies."""

from pathlib import Path
import hashlib
import sys
from urllib.request import urlopen

DORIS = "2ed7dc60c5bc327cd744999409cf9ea779ccba60"
DORIS_LEGACY = "25968c92bfeb5b659788cf01e9311d57f1220d98"
VELOX = "d43ce4dedbba86891e2e9e7a0fa957be71160c00"
ROOT = Path(sys.argv[1])
CACHE = ROOT / "upstream"


def fetch(project, commit, file):
    url = f"https://raw.githubusercontent.com/{project}/{commit}/{file}"
    path = CACHE / project / commit / file
    if not path.is_file():
        with urlopen(url, timeout=25) as response:
            value = response.read()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(value)
    value = path.read_bytes()
    print(f"{hashlib.sha256(value).hexdigest()}  {url}")
    return value.decode()


def put(file, content):
    path = ROOT / file
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)


def between(text, first, last):
    start = text.index(first)
    return text[start:text.index(last, start)]


def doris(file, commit=DORIS):
    return fetch("apache/doris", commit, f"be/src/{file}")


def velox(file):
    return fetch("facebookincubator/velox", VELOX, file)


put("util/bit_packing.h", doris("util/bit_packing.h"))
unpack_point = "    // Call UnpackValue for 0 <= i < 32.\n"
packing_inline = doris("util/bit_packing.inline.h")
assert packing_inline.count(unpack_point) == 1
put("util/bit_packing.inline.h", packing_inline.replace(unpack_point, """\
#if DORIS_ARM_VARIANT && defined(__aarch64__) && defined(__ARM_NEON)
    if constexpr (BIT_WIDTH == 1 && std::is_same_v<OutType, uint8_t>) {
#if DORIS_ARM_VARIANT == 1
        arm_bool::unpack_starrocks_style(in, out);
#elif DORIS_ARM_VARIANT == 2
        arm_bool::unpack_neon(in, out);
#else
#error Unsupported benchmark variant
#endif
        return in + BYTES_TO_READ;
    }
#endif

""" + unpack_point))
bit_util = doris("util/bit_util.h")
bit_methods = [
    between(bit_util, "    static inline int64_t round_up(", "\n    }") + "\n    }\n",
    *(
        next(line for line in bit_util.splitlines() if needle in line)
        for needle in ("uint32_t round_up_numi32(", "bool IsPowerOf2(", "uint32_t RoundUpNumBytes(")
    ),
    between(bit_util, "    constexpr static inline int64_t Ceil(", "\n    }") + "\n    }\n",
]
put("util/bit_util.h", "#pragma once\n#include <cstdint>\n\nnamespace doris {\n"
    "struct BitUtil {\n" + "\n".join(bit_methods) + "\n};\n}\n")

reader = doris("util/bit_stream_utils.h")
put("doris_reader.inc", between(reader, "class BatchedBitReader {", "\n} // namespace doris"))
reader_inline = doris("util/bit_stream_utils.inline.h")
put("doris_reader_inline.inc", between(
    reader_inline, "template <typename T>\nint BatchedBitReader::UnpackBatch(",
    "\ninline bool BatchedBitReader::SkipBatch"))

decoder = "format_v2/parquet/reader/native/bool_plain_decoder"
doris_header = doris(decoder + ".h")
put("doris_decoder.inc", between(doris_header, "protected:\n", "    std::vector<uint8_t> selected_values_"))
doris_cpp = doris(decoder + ".cpp")
put("doris_decoder_body.inc", between(
    doris_cpp, "Status BoolPlainDecoder::decode_fixed_values(",
    "\nStatus BoolPlainDecoder::decode_selected_fixed_values("))

legacy = "format/parquet/bool_plain_decoder"
legacy_header = doris(legacy + ".h", DORIS_LEGACY)
put("doris_legacy_decoder.inc", between(legacy_header, "protected:\n", "\n};"))
legacy_cpp = doris(legacy + ".cpp", DORIS_LEGACY)
put("doris_legacy_body.inc", between(
    legacy_cpp, "Status BoolPlainDecoder::decode_values(", "\n} // namespace doris"))
parquet_common = doris("format/parquet/parquet_common.h", DORIS_LEGACY)
select_vector = between(parquet_common, "class ColumnSelectVector {", "\nenum class ColumnOrderName")
put("doris_legacy_select.inc", select_vector.replace(
    "    ColumnSelectVector() = default;", """\
    ColumnSelectVector(const std::vector<uint16_t>& runs, size_t rows)
            : _run_length_null_map(&runs), _has_filter(false), _num_values(rows), _num_nulls(0),
              _num_filtered(0), _read_index(0) {}"""))

put("BooleanDecoder.h", velox("velox/dwio/parquet/reader/BooleanDecoder.h"))
visitor = velox("velox/dwio/parquet/tests/reader/BooleanDecoderBenchmark.cpp")
put("velox_visitor.inc", between(visitor, "template <typename T>\nclass BenchmarkVisitor", "\nconstexpr int"))
