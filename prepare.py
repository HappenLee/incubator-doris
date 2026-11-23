"""Fetch the pinned upstream implementations and isolate the decoder dependencies."""

from pathlib import Path
import hashlib
import sys
from urllib.request import urlopen

DORIS = "2ed7dc60c5bc327cd744999409cf9ea779ccba60"
VELOX = "d43ce4dedbba86891e2e9e7a0fa957be71160c00"
ROOT = Path(sys.argv[1])
CACHE = ROOT / "upstream"


def fetch(project, commit, file):
    url = f"https://raw.githubusercontent.com/{project}/{commit}/{file}"
    path = CACHE / project / file
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


def doris(file):
    return fetch("apache/doris", DORIS, f"be/src/{file}")


def velox(file):
    return fetch("facebookincubator/velox", VELOX, file)


for path in ("util/bit_packing.h", "util/bit_packing.inline.h"):
    put(path, doris(path))
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

put("BooleanDecoder.h", velox("velox/dwio/parquet/reader/BooleanDecoder.h"))
visitor = velox("velox/dwio/parquet/tests/reader/BooleanDecoderBenchmark.cpp")
put("velox_visitor.inc", between(visitor, "template <typename T>\nclass BenchmarkVisitor", "\nconstexpr int"))
