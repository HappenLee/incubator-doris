// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef DORIS_EXEC_PARTITIONED_HASH_JOIN_NODE_INLINE_H
#define DORIS_EXEC_PARTITIONED_HASH_JOIN_NODE_INLINE_H

#include "exec/partitioned_hash_join_node.h"
#include "runtime/buffered_tuple_stream2.inline.h"

namespace doris {

inline void PartitionedHashJoinNode::ResetForProbe() {
  _current_left_child_row = NULL;
  _left_batch_pos = 0;
  matched_probe_ = true;
  hash_tbl_iterator_.SetAtEnd();
  ht_ctx_->expr_values_cache()->Reset();
}

inline bool PartitionedHashJoinNode::AppendRow(BufferedTupleStream2* stream,
    TupleRow* row, Status* status) {
  if (LIKELY(stream->add_row(row, status))) return true;
  return AppendRowStreamFull(stream, row, status);
}

}

#endif
