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

#include "exec/partitioned_hash_join_node.inline.h"
#include "exec/partitioned_hash_table.inline.h"
//#include "runtime/raw_value.inline.h"
//#include "runtime/runtime_filter.h"
//#include "util/bloom_filter.h"

#include "common/names.h"

namespace doris {

// Wrapper around ExecNode's eval conjuncts with a different function name.
// This lets us distinguish between the join conjuncts vs. non-join conjuncts
// for codegen.
// Note: don't declare this static.  LLVM will pick the fastcc calling convention and
// we will not be able to replace the functions with codegen'd versions.
// TODO: explicitly set the calling convention?
// TODO: investigate using fastcc for all codegen internal functions?
bool IR_NO_INLINE EvalOtherJoinConjuncts(
    ExprContext* const* ctxs, int num_ctxs, TupleRow* row) {
  return ExecNode::eval_conjuncts(ctxs, num_ctxs, row);
}

bool IR_ALWAYS_INLINE PartitionedHashJoinNode::ProcessProbeRowInnerJoin(
    ExprContext* const* other_join_conjunct_ctxs, int num_other_join_conjuncts,
    ExprContext* const* conjunct_ctxs, int num_conjuncts,
    RowBatch::Iterator* out_batch_iterator, int* remaining_capacity) {
  DCHECK(_current_left_child_row != NULL);
  TupleRow* out_row = out_batch_iterator->get();
  for (; !hash_tbl_iterator_.AtEnd(); hash_tbl_iterator_.NextDuplicate()) {
    TupleRow* matched_build_row = hash_tbl_iterator_.GetRow();
    DCHECK(matched_build_row != NULL);

    // Create an output row with all probe/build tuples and evaluate the
    // non-equi-join conjuncts.
    create_output_row(out_row, _current_left_child_row, matched_build_row);
    if (!EvalOtherJoinConjuncts(other_join_conjunct_ctxs, num_other_join_conjuncts,
        out_row)) {
      continue;
    }
    if (ExecNode::eval_conjuncts(conjunct_ctxs, num_conjuncts, out_row)) {
      --(*remaining_capacity);
      if (*remaining_capacity == 0) {
        hash_tbl_iterator_.NextDuplicate();
        return false;
      }
      out_row = out_batch_iterator->next();
    }
  }
  return true;
}

template<int const JoinOp>
bool IR_ALWAYS_INLINE PartitionedHashJoinNode::ProcessProbeRowRightSemiJoins(
    ExprContext* const* other_join_conjunct_ctxs, int num_other_join_conjuncts,
    ExprContext* const* conjunct_ctxs, int num_conjuncts,
    RowBatch::Iterator* out_batch_iterator, int* remaining_capacity) {
  DCHECK(_current_left_child_row != NULL);
  DCHECK(JoinOp == TJoinOp::RIGHT_SEMI_JOIN || JoinOp == TJoinOp::RIGHT_ANTI_JOIN);
  TupleRow* out_row = out_batch_iterator->get();
  for (; !hash_tbl_iterator_.AtEnd(); hash_tbl_iterator_.NextDuplicate()) {
    TupleRow* matched_build_row = hash_tbl_iterator_.GetRow();
    DCHECK(matched_build_row != NULL);

    if (hash_tbl_iterator_.IsMatched()) continue;
    // Evaluate the non-equi-join conjuncts against a temp row assembled from all
    // build and probe tuples.
    if (num_other_join_conjuncts > 0) {
      create_output_row(semi_join_staging_row_, _current_left_child_row, matched_build_row);
      if (!EvalOtherJoinConjuncts(other_join_conjunct_ctxs,
          num_other_join_conjuncts, semi_join_staging_row_)) {
        continue;
      }
    }
    // Create output row assembled from build tuples.
    out_batch_iterator->parent()->copy_row(matched_build_row, out_row);
    // Update the hash table to indicate that this entry has been matched.
    hash_tbl_iterator_.SetMatched();
    if (JoinOp == TJoinOp::RIGHT_SEMI_JOIN &&
        ExecNode::eval_conjuncts(conjunct_ctxs, num_conjuncts, out_row)) {
      --(*remaining_capacity);
      if (*remaining_capacity == 0) {
        hash_tbl_iterator_.NextDuplicate();
        return false;
      }
      out_row = out_batch_iterator->next();
    }
  }
  return true;
}

template<int const JoinOp>
bool IR_ALWAYS_INLINE PartitionedHashJoinNode::ProcessProbeRowLeftSemiJoins(
    ExprContext* const* other_join_conjunct_ctxs, int num_other_join_conjuncts,
    ExprContext* const* conjunct_ctxs, int num_conjuncts,
    RowBatch::Iterator* out_batch_iterator, int* remaining_capacity, Status* status) {
  DCHECK(_current_left_child_row != NULL);
  DCHECK(JoinOp == TJoinOp::LEFT_ANTI_JOIN || JoinOp == TJoinOp::LEFT_SEMI_JOIN ||
      JoinOp == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN);
  TupleRow* out_row = out_batch_iterator->get();
  for (; !hash_tbl_iterator_.AtEnd(); hash_tbl_iterator_.NextDuplicate()) {
    TupleRow* matched_build_row = hash_tbl_iterator_.GetRow();
    DCHECK(matched_build_row != NULL);
    // Evaluate the non-equi-join conjuncts against a temp row assembled from all
    // build and probe tuples.
    if (num_other_join_conjuncts > 0) {
      create_output_row(semi_join_staging_row_, _current_left_child_row, matched_build_row);
      if (!EvalOtherJoinConjuncts(other_join_conjunct_ctxs,
          num_other_join_conjuncts, semi_join_staging_row_)) {
        continue;
      }
    }
    // Create output row assembled from probe tuples.
    out_batch_iterator->parent()->copy_row(_current_left_child_row, out_row);
    // A match is found in the hash table. The search is over for this probe row.
    matched_probe_ = true;
    hash_tbl_iterator_.SetAtEnd();
    // Append to output batch for left semi joins if the conjuncts are satisfied.
    if (JoinOp == TJoinOp::LEFT_SEMI_JOIN &&
        ExecNode::eval_conjuncts(conjunct_ctxs, num_conjuncts, out_row)) {
      --(*remaining_capacity);
      if (*remaining_capacity == 0) return false;
      out_row = out_batch_iterator->next();
    }
    // Done with this probe row.
    return true;
  }

  if (JoinOp != TJoinOp::LEFT_SEMI_JOIN && !matched_probe_) {
    if (JoinOp == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
      // Null aware behavior. The probe row did not match in the hash table so we
      // should interpret the hash table probe as "unknown" if there are nulls on the
      // build side. For those rows, we need to process the remaining join
      // predicates later.
      if (null_aware_partition_->build_rows()->num_rows() != 0) {
        if (num_other_join_conjuncts > 0 &&
            UNLIKELY(!AppendRow(null_aware_partition_->probe_rows(),
                         _current_left_child_row, status))) {
          DCHECK(!status->ok());
          return false;
        }
        return true;
      }
    }
    // No match for this _current_left_child_row, we need to output it. No need to
    // evaluate the conjunct_ctxs since anti joins cannot have any.
    out_batch_iterator->parent()->copy_row(_current_left_child_row, out_row);
    matched_probe_ = true;
    --(*remaining_capacity);
    if (*remaining_capacity == 0) return false;
    out_row = out_batch_iterator->next();
  }
  return true;
}

template<int const JoinOp>
bool IR_ALWAYS_INLINE PartitionedHashJoinNode::ProcessProbeRowOuterJoins(
    ExprContext* const* other_join_conjunct_ctxs, int num_other_join_conjuncts,
    ExprContext* const* conjunct_ctxs, int num_conjuncts,
    RowBatch::Iterator* out_batch_iterator, int* remaining_capacity) {
  DCHECK(JoinOp == TJoinOp::LEFT_OUTER_JOIN || JoinOp == TJoinOp::RIGHT_OUTER_JOIN ||
      JoinOp == TJoinOp::FULL_OUTER_JOIN);
  DCHECK(_current_left_child_row != NULL);
  TupleRow* out_row = out_batch_iterator->get();
  for (; !hash_tbl_iterator_.AtEnd(); hash_tbl_iterator_.NextDuplicate()) {
    TupleRow* matched_build_row = hash_tbl_iterator_.GetRow();
    DCHECK(matched_build_row != NULL);
    // Create an output row with all probe/build tuples and evaluate the
    // non-equi-join conjuncts.
    create_output_row(out_row, _current_left_child_row, matched_build_row);
    if (!EvalOtherJoinConjuncts(other_join_conjunct_ctxs, num_other_join_conjuncts,
        out_row)) {
      continue;
    }
    // At this point the probe is considered matched.
    matched_probe_ = true;
    if (JoinOp == TJoinOp::RIGHT_OUTER_JOIN || JoinOp == TJoinOp::FULL_OUTER_JOIN) {
      // There is a match for this build row. Mark the Bucket or the DuplicateNode
      // as matched for right/full outer joins.
      hash_tbl_iterator_.SetMatched();
    }
    if (ExecNode::eval_conjuncts(conjunct_ctxs, num_conjuncts, out_row)) {
      --(*remaining_capacity);
      if (*remaining_capacity == 0) {
        hash_tbl_iterator_.NextDuplicate();
        return false;
      }
      out_row = out_batch_iterator->next();
    }
  }

  if (JoinOp != TJoinOp::RIGHT_OUTER_JOIN && !matched_probe_) {
    // No match for this row, we need to output it if it's a left/full outer join.
    create_output_row(out_row, _current_left_child_row, NULL);
    if (ExecNode::eval_conjuncts(conjunct_ctxs, num_conjuncts, out_row)) {
      matched_probe_ = true;
      --(*remaining_capacity);
      if (*remaining_capacity == 0) return false;
      out_row = out_batch_iterator->next();
    }
  }
  return true;
}

template<int const JoinOp>
bool IR_ALWAYS_INLINE PartitionedHashJoinNode::ProcessProbeRow(
    ExprContext* const* other_join_conjunct_ctxs, int num_other_join_conjuncts,
    ExprContext* const* conjunct_ctxs, int num_conjuncts,
    RowBatch::Iterator* out_batch_iterator, int* remaining_capacity, Status* status) {
  if (JoinOp == TJoinOp::INNER_JOIN) {
    return ProcessProbeRowInnerJoin(other_join_conjunct_ctxs, num_other_join_conjuncts,
        conjunct_ctxs, num_conjuncts, out_batch_iterator, remaining_capacity);
  } else if (JoinOp == TJoinOp::RIGHT_SEMI_JOIN ||
             JoinOp == TJoinOp::RIGHT_ANTI_JOIN) {
    return ProcessProbeRowRightSemiJoins<JoinOp>(other_join_conjunct_ctxs,
        num_other_join_conjuncts, conjunct_ctxs, num_conjuncts, out_batch_iterator,
        remaining_capacity);
  } else if (JoinOp == TJoinOp::LEFT_SEMI_JOIN ||
             JoinOp == TJoinOp::LEFT_ANTI_JOIN ||
             JoinOp == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
    return ProcessProbeRowLeftSemiJoins<JoinOp>(other_join_conjunct_ctxs,
        num_other_join_conjuncts, conjunct_ctxs, num_conjuncts, out_batch_iterator,
        remaining_capacity, status);
  } else {
    DCHECK(JoinOp == TJoinOp::RIGHT_OUTER_JOIN ||
           JoinOp == TJoinOp::LEFT_OUTER_JOIN ||
           JoinOp == TJoinOp::FULL_OUTER_JOIN);
    return ProcessProbeRowOuterJoins<JoinOp>(other_join_conjunct_ctxs,
        num_other_join_conjuncts, conjunct_ctxs, num_conjuncts, out_batch_iterator,
        remaining_capacity);
  }
}

template<int const JoinOp>
bool IR_ALWAYS_INLINE PartitionedHashJoinNode::NextProbeRow(
    PartitionedHashTableCtx* ht_ctx, RowBatch::Iterator* probe_batch_iterator,
    int* remaining_capacity, Status* status) {
  PartitionedHashTableCtx::ExprValuesCache* expr_vals_cache = ht_ctx->expr_values_cache();
  while (!expr_vals_cache->AtEnd()) {
    // Establish _current_left_child_row and find its corresponding partition.
    DCHECK(!probe_batch_iterator->at_end());
    _current_left_child_row = probe_batch_iterator->get();
    matched_probe_ = false;

    // True if the current row should be skipped for probing.
    bool skip_row = false;

    // The hash of the expressions results for the current probe row.
    uint32_t hash = expr_vals_cache->CurExprValuesHash();
    // Hoist the followings out of the else statement below to speed up non-null case.
    const uint32_t partition_idx = hash >> (32 - NUM_PARTITIONING_BITS);
    PartitionedHashTable* hash_tbl = hash_tbls_[partition_idx];

    // Fetch the hash and expr values' nullness for this row.
    if (expr_vals_cache->IsRowNull()) {
      if (JoinOp == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN && non_empty_build_) {
        const int num_other_join_conjuncts = other_join_conjunct_ctxs_.size();
        // For NAAJ, we need to treat NULLs on the probe carefully. The logic is:
        // 1. No build rows -> Return this row. The check for 'non_empty_build_'
        //    is for this case.
        // 2. Has build rows & no other join predicates, skip row.
        // 3. Has build rows & other join predicates, we need to evaluate against all
        // build rows. First evaluate it against this partition, and if there is not
        // a match, save it to evaluate against other partitions later. If there
        // is a match, the row is skipped.
        if (num_other_join_conjuncts == 0) {
          // Condition 2 above.
          skip_row = true;
        } else if (LIKELY(AppendRow(null_probe_rows_, _current_left_child_row, status))) {
          // Condition 3 above.
          matched_null_probe_.push_back(false);
          skip_row = true;
        } else {
          // Condition 3 above but failed to append to 'null_probe_rows_'. Bail out.
          DCHECK(!status->ok());
          return false;
        }
      }
    } else {
      // The build partition is in memory. Return this row for probing.
      if (LIKELY(hash_tbl != NULL)) {
        hash_tbl_iterator_ = hash_tbl->FindProbeRow(ht_ctx);
      } else {
        // The build partition is either empty or spilled.
        Partition* partition = hash_partitions_[partition_idx];
        // This partition is closed, meaning the build side for this partition was empty.
        if (UNLIKELY(partition->is_closed())) {
          DCHECK(state_ == PROCESSING_PROBE || state_ == REPARTITIONING);
        } else {
          // This partition is not in memory, spill the probe row and move to the next row.
          DCHECK(partition->is_spilled());
          DCHECK(partition->probe_rows() != NULL);
          // Skip the current row if we manage to append to the spilled partition's BTS.
          // Otherwise, we need to bail out and report the failure.
          if (UNLIKELY(!AppendRow(partition->probe_rows(), _current_left_child_row, status))) {
            DCHECK(!status->ok());
            return false;
          }
          skip_row = true;
        }
      }
    }
    // Move to the next probe row and hash table context's cached value.
    probe_batch_iterator->next();
    expr_vals_cache->NextRow();
    if (skip_row) continue;
    DCHECK(status->ok());
    return true;
  }
  if (probe_batch_iterator->at_end()) {
    // No more probe row.
    _current_left_child_row = NULL;
  }
  return false;
}

void IR_ALWAYS_INLINE PartitionedHashJoinNode::EvalAndHashProbePrefetchGroup(
    TPrefetchMode::type prefetch_mode, PartitionedHashTableCtx* ht_ctx) {
  RowBatch* probe_batch = _left_batch.get();
  PartitionedHashTableCtx::ExprValuesCache* expr_vals_cache = ht_ctx->expr_values_cache();
  const int prefetch_size = expr_vals_cache->capacity();
  DCHECK(expr_vals_cache->AtEnd());

  expr_vals_cache->Reset();
  FOREACH_ROW_LIMIT(probe_batch, _left_batch_pos, prefetch_size, batch_iter) {
    TupleRow* row = batch_iter.get();
    if (ht_ctx->EvalAndHashProbe(row)) {
      if (prefetch_mode != TPrefetchMode::NONE) {
        uint32_t hash = expr_vals_cache->CurExprValuesHash();
        const uint32_t partition_idx = hash >> (32 - NUM_PARTITIONING_BITS);
        PartitionedHashTable* hash_tbl = hash_tbls_[partition_idx];
        if (LIKELY(hash_tbl != NULL)) hash_tbl->PrefetchBucket<true>(hash);
      }
    } else {
      expr_vals_cache->SetRowNull();
    }
    expr_vals_cache->NextRow();
  }
  expr_vals_cache->ResetForRead();
}

// create_output_row, EvalOtherJoinConjuncts, and eval_conjuncts are replaced by codegen.
template<int const JoinOp>
int PartitionedHashJoinNode::ProcessProbeBatch(TPrefetchMode::type prefetch_mode,
    RowBatch* out_batch, PartitionedHashTableCtx* __restrict__ ht_ctx, Status* __restrict__ status) {
  ExprContext* const* other_join_conjunct_ctxs = &other_join_conjunct_ctxs_[0];
  const int num_other_join_conjuncts = other_join_conjunct_ctxs_.size();
  ExprContext* const* conjunct_ctxs = &_conjunct_ctxs[0];
  const int num_conjuncts = _conjunct_ctxs.size();

  DCHECK(!out_batch->at_capacity());
  DCHECK_GE(_left_batch_pos, 0);
  RowBatch::Iterator out_batch_iterator(out_batch, out_batch->add_row());
  const long max_rows = out_batch->capacity() - out_batch->num_rows();
  // Note that '_left_batch_pos' is the row no. of the row after '_current_left_child_row'.
  RowBatch::Iterator probe_batch_iterator(_left_batch.get(), _left_batch_pos);
  int target_capacity = _limit == -1 ? max_rows : std::min(max_rows, _limit - _num_rows_returned);
  int remaining_capacity = target_capacity;

  bool has_probe_rows = _current_left_child_row != NULL || !probe_batch_iterator.at_end();
  PartitionedHashTableCtx::ExprValuesCache* expr_vals_cache = ht_ctx->expr_values_cache();

  // Keep processing more probe rows if there are more to process and the output batch
  // has room and we haven't hit any error yet.
  while (has_probe_rows && remaining_capacity > 0 && status->ok()) {
    // Prefetch for the current hash_tbl_iterator_.
    if (prefetch_mode != TPrefetchMode::NONE) {
      hash_tbl_iterator_.PrefetchBucket<true>();
    }
    // Evaluate and hash more rows if prefetch group is empty. A prefetch group is a cache
    // of probe expressions results, nullness of the expression values and hash values
    // against some consecutive number of rows in the probe batch. Prefetching, if
    // enabled, is interleaved with the rows' evaluation and hashing. If the prefetch
    // group is partially full (e.g. we returned before the current prefetch group was
    // exhausted in the previous iteration), we will proceed with the remaining items in
    // the values cache.
    if (expr_vals_cache->AtEnd()) {
      EvalAndHashProbePrefetchGroup(prefetch_mode, ht_ctx);
    }
    // Process the prefetch group.
    do {
      // '_current_left_child_row' can be NULL on the first iteration through this loop.
      if (_current_left_child_row != NULL) {
        if (!ProcessProbeRow<JoinOp>(other_join_conjunct_ctxs, num_other_join_conjuncts,
            conjunct_ctxs, num_conjuncts, &out_batch_iterator, &remaining_capacity,
            status)) {
          if (status->ok()) DCHECK_EQ(remaining_capacity, 0);
          break;
        }
      }
      // Must have reached the end of the hash table iterator for the current row before
      // moving to the next row.
      DCHECK(hash_tbl_iterator_.AtEnd());
      DCHECK(status->ok());
    } while (NextProbeRow<JoinOp>(ht_ctx, &probe_batch_iterator, &remaining_capacity,
        status));
    // Update whether there are more probe rows to process in the current batch.
    has_probe_rows = _current_left_child_row != NULL;
    if (!has_probe_rows) DCHECK(probe_batch_iterator.at_end());
    // Update where we are in the probe batch.
    _left_batch_pos = (probe_batch_iterator.get() - _left_batch->get_row(0)) /
        _left_batch->num_tuples_per_row();
  }

  int num_rows_added;
  if (LIKELY(status->ok())) {
    num_rows_added = target_capacity - remaining_capacity;
  } else {
    num_rows_added = -1;
  }
  DCHECK_GE(_left_batch_pos, 0);
  DCHECK_LE(_left_batch_pos, _left_batch->capacity());
  DCHECK_LE(num_rows_added, max_rows);
  return num_rows_added;
}

Status PartitionedHashJoinNode::ProcessBuildBatch(RowBatch* build_batch,
    bool build_filters) {
  PartitionedHashTableCtx::ExprValuesCache* expr_vals_cache = ht_ctx_->expr_values_cache();
  expr_vals_cache->Reset();
  FOREACH_ROW(build_batch, 0, build_batch_iter) {
    DCHECK(build_status_.ok());
    TupleRow* build_row = build_batch_iter.get();
    if (!ht_ctx_->EvalAndHashBuild(build_row)) {
      if (null_aware_partition_ != NULL) {
        // TODO: remove with codegen/template
        // If we are NULL aware and this build row has NULL in the eq join slot,
        // append it to the null_aware partition. We will need it later.
        if (UNLIKELY(!AppendRow(null_aware_partition_->build_rows(),
                build_row, &build_status_))) {
          return build_status_;
        }
      }
      continue;
    }
//    if (build_filters) {
//      DCHECK_EQ(ht_ctx_->level(), 0)
//          << "Runtime filters should not be built during repartitioning.";
//      for (const FilterContext& ctx: filters_) {
//        // TODO: codegen expr evaluation and hashing
//        if (ctx.local_bloom_filter == NULL) continue;
//        void* e = ctx.expr->GetValue(build_row);
//        uint32_t filter_hash = RawValue::GetHashValue(e, ctx.expr->root()->type(),
//            RuntimeFilterBank::DefaultHashSeed());
//        ctx.local_bloom_filter->Insert(filter_hash);
//      }
//    }
    const uint32_t hash = expr_vals_cache->CurExprValuesHash();
    const uint32_t partition_idx = hash >> (32 - NUM_PARTITIONING_BITS);
    Partition* partition = hash_partitions_[partition_idx];
    const bool result = AppendRow(partition->build_rows(), build_row, &build_status_);
    if (UNLIKELY(!result)) return build_status_;
  }
  return Status::OK();
}

bool PartitionedHashJoinNode::Partition::InsertBatch(
    TPrefetchMode::type prefetch_mode, PartitionedHashTableCtx* ht_ctx, RowBatch* batch,
    const vector<BufferedTupleStream2::RowIdx>& indices) {
  // Compute the hash values and prefetch the hash table buckets.
  const int num_rows = batch->num_rows();
  PartitionedHashTableCtx::ExprValuesCache* expr_vals_cache = ht_ctx->expr_values_cache();
  const int prefetch_size = expr_vals_cache->capacity();
  const BufferedTupleStream2::RowIdx* row_indices = indices.data();
  for (int prefetch_group_row = 0; prefetch_group_row < num_rows;
       prefetch_group_row += prefetch_size) {
    int cur_row = prefetch_group_row;
    expr_vals_cache->Reset();
    FOREACH_ROW_LIMIT(batch, cur_row, prefetch_size, batch_iter) {
      if (ht_ctx->EvalAndHashBuild(batch_iter.get())) {
        if (prefetch_mode != TPrefetchMode::NONE) {
          hash_tbl_->PrefetchBucket<false>(expr_vals_cache->CurExprValuesHash());
        }
      } else {
        expr_vals_cache->SetRowNull();
      }
      expr_vals_cache->NextRow();
    }
    // Do the insertion.
    expr_vals_cache->ResetForRead();
    FOREACH_ROW_LIMIT(batch, cur_row, prefetch_size, batch_iter) {
      TupleRow* row = batch_iter.get();
      BufferedTupleStream2::RowIdx row_idx = row_indices[cur_row];
      Status status;
      if (!expr_vals_cache->IsRowNull() &&
          UNLIKELY(!hash_tbl_->Insert(ht_ctx, row_idx, row, &status))) {
        return false;
      }
      expr_vals_cache->NextRow();
      ++cur_row;
    }
  }
  return true;
}

template int PartitionedHashJoinNode::ProcessProbeBatch<TJoinOp::INNER_JOIN>(
    TPrefetchMode::type prefetch_mode, RowBatch* out_batch, PartitionedHashTableCtx* ht_ctx,
    Status* status);
template int PartitionedHashJoinNode::ProcessProbeBatch<TJoinOp::LEFT_OUTER_JOIN>(
    TPrefetchMode::type prefetch_mode, RowBatch* out_batch, PartitionedHashTableCtx* ht_ctx,
    Status* status);
template int PartitionedHashJoinNode::ProcessProbeBatch<TJoinOp::LEFT_SEMI_JOIN>(
    TPrefetchMode::type prefetch_mode, RowBatch* out_batch, PartitionedHashTableCtx* ht_ctx,
    Status* status);
template int PartitionedHashJoinNode::ProcessProbeBatch<TJoinOp::LEFT_ANTI_JOIN>(
    TPrefetchMode::type prefetch_mode, RowBatch* out_batch, PartitionedHashTableCtx* ht_ctx,
    Status* status);
template int PartitionedHashJoinNode::ProcessProbeBatch<TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN>(
    TPrefetchMode::type prefetch_mode, RowBatch* out_batch, PartitionedHashTableCtx* ht_ctx,
    Status* status);
template int PartitionedHashJoinNode::ProcessProbeBatch<TJoinOp::RIGHT_OUTER_JOIN>(
    TPrefetchMode::type prefetch_mode, RowBatch* out_batch, PartitionedHashTableCtx* ht_ctx,
    Status* status);
template int PartitionedHashJoinNode::ProcessProbeBatch<TJoinOp::RIGHT_SEMI_JOIN>(
    TPrefetchMode::type prefetch_mode, RowBatch* out_batch, PartitionedHashTableCtx* ht_ctx,
    Status* status);
template int PartitionedHashJoinNode::ProcessProbeBatch<TJoinOp::RIGHT_ANTI_JOIN>(
    TPrefetchMode::type prefetch_mode, RowBatch* out_batch, PartitionedHashTableCtx* ht_ctx,
    Status* status);
template int PartitionedHashJoinNode::ProcessProbeBatch<TJoinOp::FULL_OUTER_JOIN>(
    TPrefetchMode::type prefetch_mode, RowBatch* out_batch, PartitionedHashTableCtx* ht_ctx,
    Status* status);
}
