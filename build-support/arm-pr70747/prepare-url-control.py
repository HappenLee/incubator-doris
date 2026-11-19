#!/usr/bin/env python3
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

"""Diagnostic control: disable only the two new ARM SIMD search loops."""
import hashlib
import json
from pathlib import Path

out = Path("arm70747-generated")
original = (out / "after_url.h").read_text()
control = original.replace("after_url", "control_url")
for loop in ("for (; end - pos >= 16; pos += 16) {",
             "for (; pos - begin >= 16; pos -= 16) {"):
    old = "#if defined(__SSE2__) || defined(__aarch64__)\n    " + loop
    assert control.count(old) == 1, loop
    control = control.replace(old, "#if defined(__SSE2__)\n    " + loop)
(out / "control_url.h").write_text(control)
(out / "control-manifest.json").write_text(json.dumps({
    "purpose": "Diagnostic control only; not a proposed production patch",
    "change": "Disable the two compile-time ARM vector loops; keep the PR scalar tails",
    "original_sha256": hashlib.sha256(original.encode()).hexdigest(),
    "control_sha256": hashlib.sha256(control.encode()).hexdigest(),
}, indent=2) + "\n")
