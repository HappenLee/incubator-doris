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

#include <functional>
#include <numeric>
#include <sstream>
#include <gutil/strings/substitute.h>

//#include "codegen/llvm_codegen.h"
#include "exec/partitioned_hash_table.inline.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "exprs/slot_ref.h"
#include "runtime/buffered_block_mgr2.h"
#include "runtime/buffered_tuple_stream2.inline.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "runtime/row_batch.h"
//#include "runtime/runtime_filter.h"
//#include "runtime/runtime_filter_bank.h"
#include "runtime/runtime_state.h"
//#include "util/bloom_filter.h"
#include "util/debug_util.h"
#include "util/runtime_profile.h"

#include "gen_cpp/PlanNodes_types.h"

#include "common/names.h"

//DEFINE_bool(enable_phj_probe_side_filtering, true, "Deprecated.");

const string PREPARE_FOR_READ_FAILED_ERROR_MSG = "Failed to acquire initial read buffer "
    "for stream in hash join node $0. Reducing query concurrency or increasing the "
    "memory limit may help this query to complete successfully.";

using namespace doris;
using namespace strings;

PartitionedHashJoinNode::PartitionedHashJoinNode(
    ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
  : BlockingJoinNode("PartitionedHashJoinNode", tnode.hash_join_node.join_op,
        pool, tnode, descs),
    is_not_distinct_from_(),
    block_mgr_client_(NULL),
    partition_build_timer_(NULL),
    null_aware_eval_timer_(NULL),
    state_(PARTITIONING_BUILD),
    partition_pool_(new ObjectPool()),
    input_partition_(NULL),
    null_aware_partition_(NULL),
    non_empty_build_(false),
    null_probe_rows_(NULL),
    null_probe_output_idx_(-1),
    process_build_batch_fn_(NULL),
    process_build_batch_fn_level0_(NULL),
    insert_batch_fn_(NULL),
    insert_batch_fn_level0_(NULL) {
  memset(hash_tbls_, 0, sizeof(PartitionedHashTable*) * PARTITION_FANOUT);
}

PartitionedHashJoinNode::~PartitionedHashJoinNode() {
  // Check that we didn't leak any memory.
  DCHECK(null_probe_rows_ == NULL);
}

Status PartitionedHashJoinNode::init(const TPlanNode& tnode, RuntimeState* state) {
  RETURN_IF_ERROR(BlockingJoinNode::init(tnode, state));
  DCHECK(tnode.__isset.hash_join_node);
  const vector<TEqJoinCondition>& eq_join_conjuncts =
      tnode.hash_join_node.eq_join_conjuncts;
  for (int i = 0; i < eq_join_conjuncts.size(); ++i) {
    ExprContext* ctx;
    RETURN_IF_ERROR(Expr::create_expr_tree(_pool, eq_join_conjuncts[i].left, &ctx));
    probe_expr_ctxs_.push_back(ctx);
    RETURN_IF_ERROR(Expr::create_expr_tree(_pool, eq_join_conjuncts[i].right, &ctx));
    build_expr_ctxs_.push_back(ctx);
    if (eq_join_conjuncts[i].__isset.opcode
        && eq_join_conjuncts[i].opcode == TExprOpcode::EQ_FOR_NULL) {
        is_not_distinct_from_.push_back(true);
    } else {
        is_not_distinct_from_.push_back(false);
    }
  }
  RETURN_IF_ERROR(
      Expr::create_expr_trees(_pool, tnode.hash_join_node.other_join_conjuncts,
                            &other_join_conjunct_ctxs_));

//  for (const TRuntimeFilterDesc& filter: tnode.runtime_filters) {
//    // If filter propagation not enabled, only consider building broadcast joins (that may
//    // be consumed by this fragment).
//    if (state->query_options().runtime_filter_mode != TRuntimeFilterMode::GLOBAL &&
//        !filter.is_broadcast_join) {
//      continue;
//    }
//    if (state->query_options().disable_row_runtime_filtering
//        && !filter.applied_on_partition_columns) {
//      continue;
//    }
//    FilterContext filter_ctx;
//    filter_ctx.filter = state->filter_bank()->RegisterFilter(filter, true);
//    RETURN_IF_ERROR(Expr::create_expr_tree(_pool, filter.src_expr, &filter_ctx.expr));
//    filters_.push_back(filter_ctx);
//  }

  DCHECK(_join_op != TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN ||
      eq_join_conjuncts.size() == 1);
  return Status::OK();
}

Status PartitionedHashJoinNode::prepare(RuntimeState* state) {
  SCOPED_TIMER(_runtime_profile->total_time_counter());

  // Create the codegen object before preparing _conjunct_ctxs and children_, so that any
  // ScalarFnCalls will use codegen.
  // TODO: this is brittle and hard to reason about, revisit
//  if (state->codegen_enabled()) {
//    LlvmCodeGen* codegen;
//    RETURN_IF_ERROR(state->GetCodegen(&codegen));
//  }
  RETURN_IF_ERROR(BlockingJoinNode::prepare(state));
  runtime_state_ = state;

  // build and probe exprs are evaluated in the context of the rows produced by our
  // right and left children, respectively
  RETURN_IF_ERROR(
      Expr::prepare(build_expr_ctxs_, state, child(1)->row_desc(), expr_mem_tracker()));
  RETURN_IF_ERROR(
      Expr::prepare(probe_expr_ctxs_, state, child(0)->row_desc(), expr_mem_tracker()));
//  for (const FilterContext& ctx: filters_) {
//    RETURN_IF_ERROR(ctx.expr->prepare(state, child(1)->row_desc(), expr_mem_tracker()));
//    AddExprCtxToFree(ctx.expr);
//  }

  // Although ProcessBuildInput() may be run in a separate thread, it is safe to free
  // local allocations in QueryMaintenance() since the build thread is not run
  // concurrently with other expr evaluation in this join node.
  // Probe side expr is not included in QueryMaintenance(). We cache the probe expression
  // values in ExprValuesCache. Local allocations need to survive until the cache is reset
  // so we need to manually free probe expr local allocations.
  // add_expr_ctxs_to_free(build_expr_ctxs_);

  // other_join_conjunct_ctxs_ are evaluated in the context of rows assembled from all
  // build and probe tuples; full_row_desc is not necessarily the same as the output row
  // desc, e.g., because semi joins only return the build xor probe tuples
  RowDescriptor full_row_desc(child(0)->row_desc(), child(1)->row_desc());
  RETURN_IF_ERROR(
      Expr::prepare(other_join_conjunct_ctxs_, state, full_row_desc, expr_mem_tracker()));
  //add_expr_ctxs_to_free(other_join_conjunct_ctxs_);

  RETURN_IF_ERROR(state->block_mgr2()->register_client(
      MinRequiredBuffers(), mem_tracker(), state, &block_mgr_client_));

  const bool should_store_nulls = _join_op == TJoinOp::RIGHT_OUTER_JOIN ||
      _join_op == TJoinOp::RIGHT_ANTI_JOIN || _join_op == TJoinOp::FULL_OUTER_JOIN ||
      std::accumulate(is_not_distinct_from_.begin(), is_not_distinct_from_.end(), false,
                      std::logical_or<bool>());

  build_exprs_.resize(build_expr_ctxs_.size());
  probe_exprs_.resize(probe_expr_ctxs_.size());
  std::transform(build_expr_ctxs_.begin(), build_expr_ctxs_.end(), build_exprs_.begin(), [](ExprContext* ec){
      return ec->root();
  });
  std::transform(probe_expr_ctxs_.begin(), probe_expr_ctxs_.end(), probe_exprs_.begin(), [](ExprContext* ec){
      return ec->root();
  });

  RETURN_IF_ERROR(PartitionedHashTableCtx::Create(_pool, state, build_exprs_, probe_exprs_,
      should_store_nulls, is_not_distinct_from_, state->fragment_hash_seed(),
      MAX_PARTITION_DEPTH, child(1)->row_desc().tuple_descriptors().size(), expr_mem_pool(),
      expr_mem_pool(), expr_mem_tracker(), child(1)->row_desc(), child(0)->row_desc(),
      &ht_ctx_));

  if (_join_op == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
    null_aware_eval_timer_ = ADD_TIMER(runtime_profile(), "NullAwareAntiJoinEvalTime");
  }

  partition_build_timer_ = ADD_TIMER(runtime_profile(), "BuildPartitionTime");
  num_hash_buckets_ =
      ADD_COUNTER(runtime_profile(), "HashBuckets", TUnit::UNIT);
  partitions_created_ =
      ADD_COUNTER(runtime_profile(), "PartitionsCreated", TUnit::UNIT);
  max_partition_level_ = runtime_profile()->AddHighWaterMarkCounter(
      "MaxPartitionLevel", TUnit::UNIT);
  num_build_rows_partitioned_ =
      ADD_COUNTER(runtime_profile(), "BuildRowsPartitioned", TUnit::UNIT);
  num_probe_rows_partitioned_ =
      ADD_COUNTER(runtime_profile(), "ProbeRowsPartitioned", TUnit::UNIT);
  num_repartitions_ =
      ADD_COUNTER(runtime_profile(), "NumRepartitions", TUnit::UNIT);
  num_spilled_partitions_ =
      ADD_COUNTER(runtime_profile(), "SpilledPartitions", TUnit::UNIT);
  largest_partition_percent_ = runtime_profile()->AddHighWaterMarkCounter(
      "LargestPartitionPercent", TUnit::UNIT);
  num_hash_collisions_ =
      ADD_COUNTER(runtime_profile(), "HashCollisions", TUnit::UNIT);

//  bool build_codegen_enabled = false;
//  bool probe_codegen_enabled = false;
//  bool ht_construction_codegen_enabled = false;
//  Status codegen_status;
//  Status build_codegen_status;
//  Status probe_codegen_status;
//  Status insert_codegen_status;
//  if (state->codegen_enabled()) {
//    // Codegen for hashing rows
//    Function* hash_fn;
//    codegen_status = ht_ctx_->CodegenHashCurrentRow(state, false, &hash_fn);
//    Function* murmur_hash_fn;
//    codegen_status.MergeStatus(
//        ht_ctx_->CodegenHashCurrentRow(state, true, &murmur_hash_fn));
//
//    // Codegen for evaluating build rows
//    Function* eval_build_row_fn;
//    codegen_status.MergeStatus(ht_ctx_->CodegenEvalRow(state, true, &eval_build_row_fn));
//
//    if (codegen_status.ok()) {
//      // Codegen for build path
//      build_codegen_status =
//          CodegenProcessBuildBatch(state, hash_fn, murmur_hash_fn, eval_build_row_fn);
//      if (build_codegen_status.ok()) build_codegen_enabled = true;
//      // Codegen for probe path
//      probe_codegen_status = CodegenProcessProbeBatch(state, hash_fn, murmur_hash_fn);
//      if (probe_codegen_status.ok()) probe_codegen_enabled = true;
//      // Codegen for InsertBatch()
//      insert_codegen_status = CodegenInsertBatch(state, hash_fn, murmur_hash_fn,
//          eval_build_row_fn);
//      if (insert_codegen_status.ok()) ht_construction_codegen_enabled = true;
//    } else {
//      build_codegen_status = codegen_status;
//      probe_codegen_status = codegen_status;
//      insert_codegen_status = codegen_status;
//    }
//  }
//  AddCodegenExecOption(build_codegen_enabled, codegen_status, "Build Side");
//  AddCodegenExecOption(probe_codegen_enabled, codegen_status, "Probe Side");
//  AddCodegenExecOption(ht_construction_codegen_enabled, codegen_status,
//      "Hash Table Construction");
  return Status::OK();
}

Status PartitionedHashJoinNode::open(RuntimeState* state) {
  SCOPED_TIMER(_runtime_profile->total_time_counter());
//  RETURN_IF_ERROR(BlockingJoinNode::open(state));
  RETURN_IF_ERROR(Expr::open(build_expr_ctxs_, state));
  RETURN_IF_ERROR(Expr::open(probe_expr_ctxs_, state));
  RETURN_IF_ERROR(Expr::open(other_join_conjunct_ctxs_, state));
  if (ht_ctx_.get() != nullptr) RETURN_IF_ERROR(ht_ctx_->Open(state));

//  for (const FilterContext& filter: filters_) RETURN_IF_ERROR(filter.expr->Open(state));
//  AllocateRuntimeFilters(state);

  if (_join_op == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
    null_aware_partition_ = partition_pool_->add(new Partition(state, this, 0));
    RETURN_IF_ERROR(
        null_aware_partition_->build_rows()->init(id(), runtime_profile(), false));
    RETURN_IF_ERROR(
        null_aware_partition_->probe_rows()->init(id(), runtime_profile(), false));
    null_probe_rows_ = new BufferedTupleStream2(
        state, child(0)->row_desc(), state->block_mgr2(), block_mgr_client_,
        true /* use_initial_small_buffers */, false /* read_write */ );
    RETURN_IF_ERROR(null_probe_rows_->init(id(), runtime_profile(), false));
  }

  // Claim reservation after the child has been opened to reduce the peak reservation
  // requirement.
  if (!_buffer_pool_client.is_registered()) {
      DCHECK_GE(_resource_profile.min_reservation, 0);
      RETURN_IF_ERROR(claim_buffer_reservation(state));

      ht_allocator_.reset(new Suballocator(state->exec_env()->buffer_pool(),
                                           &_buffer_pool_client, _resource_profile.spillable_buffer_size));
  }

  // Check for errors and free local allocations before opening children.
  RETURN_IF_CANCELLED(state);
//  RETURN_IF_ERROR(QueryMaintenance(state));
  // The prepare functions of probe expressions may have done local allocations implicitly
  // (e.g. calling UdfBuiltins::Lower()). The probe expressions' local allocations need to
  // be freed now as they don't get freed again till probing. Other exprs' local allocations
  // are freed in ExecNode::free_local_allocations() in ProcessBuildInput().
  ExprContext::free_local_allocations(probe_expr_ctxs_);

  RETURN_IF_ERROR(BlockingJoinNode::open(state));
  ResetForProbe();
  DCHECK(null_aware_partition_ == NULL || _join_op == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN);
  return Status::OK();
}

Status PartitionedHashJoinNode::reset(RuntimeState* state) {
  if (_join_op == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
    non_empty_build_ = false;
    null_probe_output_idx_ = -1;
    matched_null_probe_.clear();
    nulls_build_batch_.reset();
  }
  state_ = PARTITIONING_BUILD;
  ht_ctx_->set_level(0);
  ClosePartitions();
  memset(hash_tbls_, 0, sizeof(PartitionedHashTable*) * PARTITION_FANOUT);
  return ExecNode::reset(state);
}

void PartitionedHashJoinNode::ClosePartitions() {
  for (int i = 0; i < hash_partitions_.size(); ++i) {
    hash_partitions_[i]->Close(NULL);
  }
  hash_partitions_.clear();
  for (list<Partition*>::iterator it = spilled_partitions_.begin();
      it != spilled_partitions_.end(); ++it) {
    (*it)->Close(NULL);
  }
  spilled_partitions_.clear();
  for (list<Partition*>::iterator it = output_build_partitions_.begin();
      it != output_build_partitions_.end(); ++it) {
    (*it)->Close(NULL);
  }
  output_build_partitions_.clear();
  if (input_partition_ != NULL) {
    input_partition_->Close(NULL);
    input_partition_ = NULL;
  }
  if (null_aware_partition_ != NULL) {
    null_aware_partition_->Close(NULL);
    null_aware_partition_ = NULL;
  }
  if (null_probe_rows_ != NULL) {
    null_probe_rows_->close();
    delete null_probe_rows_;
    null_probe_rows_ = NULL;
  }
  partition_pool_->clear();
}

Status PartitionedHashJoinNode::close(RuntimeState* state) {
  if (is_closed()) return Status::OK();
  if (ht_ctx_.get() != NULL) ht_ctx_->Close(state);

  nulls_build_batch_.reset();

  ClosePartitions();

  if (block_mgr_client_ != NULL) {
    state->block_mgr2()->clear_reservations(block_mgr_client_);
  }
  Expr::close(build_expr_ctxs_, state);
  Expr::close(probe_expr_ctxs_, state);
  Expr::close(other_join_conjunct_ctxs_, state);
//  for (const FilterContext& ctx: filters_) {
//    ctx.expr->close(state);
//  }
  return BlockingJoinNode::close(state);
}

PartitionedHashJoinNode::Partition::Partition(RuntimeState* state,
    PartitionedHashJoinNode* parent, int level)
  : parent_(parent),
    is_closed_(false),
    is_spilled_(false),
    level_(level) {
  build_rows_ = new BufferedTupleStream2(state, parent_->child(1)->row_desc(),
      state->block_mgr2(), parent_->block_mgr_client_,
      level == 0 /* use_initial_small_buffers */, false /* read_write */);
  DCHECK(build_rows_ != NULL);
  probe_rows_ = new BufferedTupleStream2(state, parent_->child(0)->row_desc(),
      state->block_mgr2(), parent_->block_mgr_client_,
      level == 0 /* use_initial_small_buffers */, false /* read_write */ );
  DCHECK(probe_rows_ != NULL);
}

PartitionedHashJoinNode::Partition::~Partition() {
  DCHECK(is_closed());
}

int64_t PartitionedHashJoinNode::Partition::EstimatedInMemSize() const {
  return build_rows_->byte_size() + PartitionedHashTable::EstimateSize(build_rows_->num_rows());
}

void PartitionedHashJoinNode::Partition::Close(RowBatch* batch) {
  if (is_closed()) return;
  is_closed_ = true;

  if (hash_tbl_.get() != NULL) {
    COUNTER_UPDATE(parent_->num_hash_collisions_, hash_tbl_->NumHashCollisions());
    hash_tbl_->Close();
  }

  // Transfer ownership of build_rows_/probe_rows_ to batch if batch is not NULL.
  // Otherwise, close the stream here.
  if (build_rows_ != NULL) {
    if (batch == NULL) {
      build_rows_->close();
      delete build_rows_;
    } else {
      batch->add_tuple_stream(build_rows_);
    }
    build_rows_ = NULL;
  }
  if (probe_rows_ != NULL) {
    if (batch == NULL) {
      probe_rows_->close();
      delete probe_rows_;
    } else {
      batch->add_tuple_stream(probe_rows_);
    }
    probe_rows_ = NULL;
  }
}

Status PartitionedHashJoinNode::Partition::Spill(bool unpin_all_build) {
  DCHECK(!is_closed_);
  // Spilling should occur before we start processing probe rows.
  DCHECK(parent_->state_ != PROCESSING_PROBE &&
         parent_->state_ != PROBING_SPILLED_PARTITION) << parent_->state_;
  DCHECK((is_spilled_ && parent_->state_ == REPARTITIONING) ||
         probe_rows_->num_rows() == 0);
  // Close the hash table as soon as possible to release memory.
  if (hash_tbl() != NULL) {
    hash_tbl_->Close();
    hash_tbl_.reset();
  }

  bool got_buffer = true;
  if (build_rows_->using_small_buffers()) {
    RETURN_IF_ERROR(build_rows_->switch_to_io_buffers(&got_buffer));
  }
  // Unpin the stream as soon as possible to increase the chances that the
  // switch_to_io_buffers() call below will succeed.
  RETURN_IF_ERROR(build_rows_->unpin_stream(unpin_all_build));

  if (got_buffer && probe_rows_->using_small_buffers()) {
    RETURN_IF_ERROR(probe_rows_->switch_to_io_buffers(&got_buffer));
  }
  if (!got_buffer) {
    // We'll try again to get the buffers when the stream fills up the small buffers.
    VLOG_QUERY << "Not enough memory to switch to IO-sized buffer for partition "
               << this << " of join=" << parent_->_id << " build small buffers="
               << build_rows_->using_small_buffers() << " probe small buffers="
               << probe_rows_->using_small_buffers();
  }

  if (!is_spilled_) {
    COUNTER_UPDATE(parent_->num_spilled_partitions_, 1);
    if (parent_->num_spilled_partitions_->value() == 1) {
      parent_->add_runtime_exec_option("Spilled");
    }
  }

  is_spilled_ = true;
  return Status::OK();
}

Status PartitionedHashJoinNode::Partition::BuildHashTable(RuntimeState* state,
    bool* built) {
  DCHECK(build_rows_ != NULL);
  *built = false;

  // TODO: estimate the entire size of the hash table and reserve all of it from
  // the block mgr.

  // We got the buffers we think we will need, try to build the hash table.
  RETURN_IF_ERROR(build_rows_->pin_stream(false, built));
  if (!*built) return Status::OK();
  bool got_read_buffer;
  RETURN_IF_ERROR(build_rows_->prepare_for_read(false, &got_read_buffer));
  DCHECK(got_read_buffer) << "Stream was already pinned.";

  RowBatch batch(parent_->child(1)->row_desc(), state->batch_size(),
      parent_->mem_tracker());
  PartitionedHashTableCtx* ctx = parent_->ht_ctx_.get();
  // TODO: move the batch and indices as members to avoid reallocating.
  vector<BufferedTupleStream2::RowIdx> indices;
  bool eos = false;

  // Allocate the partition-local hash table. Initialize the number of buckets based on
  // the number of build rows (the number of rows is known at this point). This assumes
  // there are no duplicates which can be wrong. However, the upside in the common case
  // (few/no duplicates) is large and the downside when there are is low (a bit more
  // memory; the bucket memory is small compared to the memory needed for all the build
  // side allocations).
  // One corner case is if the stream contains tuples with zero footprint (no materialized
  // slots). If the tuples occupy no space, this implies all rows will be duplicates, so
  // create a small hash table, IMPALA-2256.
  // We always start with small pages in the hash table.
  int64_t estimated_num_buckets = build_rows()->row_consumes_memory() ?
      PartitionedHashTable::EstimateNumBuckets(build_rows()->num_rows()) : state->batch_size() * 2;
  hash_tbl_.reset(PartitionedHashTable::Create(state, parent_->block_mgr_client_, parent_->ht_allocator_.get(),
      true /* store_duplicates */,
      parent_->child(1)->row_desc().tuple_descriptors().size(), build_rows(),
      1 << (32 - NUM_PARTITIONING_BITS), estimated_num_buckets));
  bool got_memory;
  hash_tbl_->Init(&got_memory);
  if (!got_memory) goto not_built;

  do {
    RETURN_IF_ERROR(build_rows_->get_next(&batch, &eos, &indices));
    DCHECK_EQ(batch.num_rows(), indices.size());
    DCHECK_LE(batch.num_rows(), hash_tbl_->EmptyBuckets())
        << build_rows()->row_consumes_memory();
    TPrefetchMode::type prefetch_mode = config::enable_prefetch ? TPrefetchMode::HT_BUCKET :
            TPrefetchMode::NONE;
    SCOPED_TIMER(parent_->_build_timer);
    if (parent_->insert_batch_fn_ != NULL) {
      InsertBatchFn insert_batch_fn;
      if (ctx->level() == 0) {
        insert_batch_fn = parent_->insert_batch_fn_level0_;
      } else {
        insert_batch_fn = parent_->insert_batch_fn_;
      }
      DCHECK(insert_batch_fn != NULL);
      if (UNLIKELY(!insert_batch_fn(this, prefetch_mode, ctx, &batch, indices))) {
        goto not_built;
      }
    } else {
      if (UNLIKELY(!InsertBatch(prefetch_mode, ctx, &batch, indices))) {
        goto not_built;
      }
    }
    
//    RETURN_IF_ERROR(state->GetQueryStatus());
//    parent_->free_local_allocations();
    batch.reset();
  } while (!eos);

  // The hash table fits in memory and is built.
  DCHECK(*built);
  DCHECK(hash_tbl_.get() != NULL);
  is_spilled_ = false;
  COUNTER_UPDATE(parent_->num_hash_buckets_, hash_tbl_->num_buckets());
  return Status::OK();

not_built:
  *built = false;
  if (hash_tbl_.get() != NULL) {
    hash_tbl_->Close();
    hash_tbl_.reset();
  }
  return Status::OK();
}

//void PartitionedHashJoinNode::AllocateRuntimeFilters(RuntimeState* state) {
//  DCHECK(_join_op != TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN || filters_.size() == 0)
//      << "Runtime filters not supported with NULL_AWARE_LEFT_ANTI_JOIN";
//  DCHECK(ht_ctx_.get() != NULL);
//  for (int i = 0; i < filters_.size(); ++i) {
//    filters_[i].local_bloom_filter =
//        state->filter_bank()->AllocateScratchBloomFilter(filters_[i].filter->id());
//  }
//}

//void PartitionedHashJoinNode::PublishRuntimeFilters(RuntimeState* state,
//    int64_t total_build_rows) {
//  int32_t num_enabled_filters = 0;
//  // Use total_build_rows to estimate FP-rate of each Bloom filter, and publish
//  // 'always-true' filters if it's too high. Doing so saves CPU at the coordinator,
//  // serialisation time, and reduces the cost of applying the filter at the scan - most
//  // significantly for per-row filters. However, the number of build rows could be a very
//  // poor estimate of the NDV - particularly if the filter expression is a function of
//  // several columns.
//  // TODO: Better heuristic.
//  for (const FilterContext& ctx: filters_) {
//    // TODO: Consider checking actual number of bits set in filter to compute FP rate.
//    // TODO: Consider checking this every few batches or so.
//    bool fp_rate_too_high =
//        state->filter_bank()->FpRateTooHigh(ctx.filter->filter_size(), total_build_rows);
//    state->filter_bank()->UpdateFilterFromLocal(ctx.filter->id(),
//        fp_rate_too_high ? BloomFilter::ALWAYS_TRUE_FILTER : ctx.local_bloom_filter);
//
//    num_enabled_filters += !fp_rate_too_high;
//  }
//
//  if (filters_.size() > 0) {
//    if (num_enabled_filters == filters_.size()) {
//      add_runtime_exec_option(Substitute("$0 of $0 Runtime Filter$1 Published",
//          filters_.size(), filters_.size() == 1 ? "" : "s"));
//    } else {
//      string exec_option = Substitute("$0 of $1 Runtime Filter$2 Published, $3 Disabled",
//          num_enabled_filters, filters_.size(), filters_.size() == 1 ? "" : "s",
//          filters_.size() - num_enabled_filters);
//      add_runtime_exec_option(exec_option);
//    }
//  }
//}

bool PartitionedHashJoinNode::AppendRowStreamFull(BufferedTupleStream2* stream,
    TupleRow* row, Status* status) {
  while (status->ok()) {
    // Check if the stream is still using small buffers and try to switch to IO-buffers.
    if (stream->using_small_buffers()) {
      bool got_buffer;
      *status = stream->switch_to_io_buffers(&got_buffer);
      if (!status->ok()) return false;
      if (got_buffer) {
        if (LIKELY(stream->add_row(row, status))) return true;
        if (!status->ok()) return false;
      }
    }
    // We ran out of memory. Pick a partition to spill.
    Partition* spilled_partition;
    *status = SpillPartition(&spilled_partition);
    if (!status->ok()) return false;
    if (stream->add_row(row, status)) return true;
    // Spilling one partition does not guarantee we can append a row. Keep
    // spilling until we can append this row.
  }
  return false;
}

// TODO: Can we do better with a different spilling heuristic?
Status PartitionedHashJoinNode::SpillPartition(Partition** spilled_partition) {
  int64_t max_freed_mem = 0;
  int partition_idx = -1;
  *spilled_partition = NULL;

  // Iterate over the partitions and pick the largest partition to spill.
  for (int i = 0; i < hash_partitions_.size(); ++i) {
    Partition* candidate = hash_partitions_[i];
    if (candidate->is_closed()) continue;
    if (candidate->is_spilled()) continue;
    int64_t mem = candidate->build_rows()->bytes_in_mem(false);
    // TODO: What should we do here if probe_rows()->num_rows() > 0 ? We should be able
    // to spill INNER JOINS, but not many of the other joins.
    if (candidate->hash_tbl() != NULL) {
      // IMPALA-1488: Do not spill partitions that already had matches, because we
      // are going to lose information and return wrong results.
      if (UNLIKELY(candidate->hash_tbl()->HasMatches())) continue;
      mem += candidate->hash_tbl()->ByteSize();
    }
    if (mem > max_freed_mem) {
      max_freed_mem = mem;
      partition_idx = i;
    }
  }

  if (partition_idx == -1) {
    // Could not find a partition to spill. This means the mem limit was just too low.
    return runtime_state_->block_mgr2()->mem_limit_too_low_error(block_mgr_client_, id());
  }

  VLOG(2) << "Spilling partition: " << partition_idx << endl << NodeDebugString();
  RETURN_IF_ERROR(hash_partitions_[partition_idx]->Spill(false));
  DCHECK(hash_partitions_[partition_idx]->probe_rows()->has_write_block());
  hash_tbls_[partition_idx] = NULL;
  *spilled_partition = hash_partitions_[partition_idx];
  return Status::OK();
}

void PartitionedHashJoinNode::init_get_next(TupleRow* first_left_row) {}

Status PartitionedHashJoinNode::construct_build_side(RuntimeState* state) {
  // Do a full scan of child(1) and partition the rows.
  {
//    SCOPED_STOP_WATCH(&built_probe_overlap_stop_watch_);
    RETURN_IF_ERROR(child(1)->open(state));
  }
  RETURN_IF_ERROR(ProcessBuildInput(state, 0));

  UpdateState(PROCESSING_PROBE);
  return Status::OK();
}

Status PartitionedHashJoinNode::ProcessBuildInput(RuntimeState* state, int level) {
  if (UNLIKELY(level >= MAX_PARTITION_DEPTH)) {
      std::stringstream error_msg;
      error_msg << "Cannot perform aggregation at hash join node with id "
                << _id << '.'
                << " The input data was partitioned the maximum number of "
                << MAX_PARTITION_DEPTH << " times."
                << " This could mean there is significant skew in the data or the memory limit is"
                << " set too low.";
      return Status::MemoryLimitExceeded(error_msg.str().c_str());
  }

  DCHECK(hash_partitions_.empty());
  if (input_partition_ != NULL) {
    DCHECK(input_partition_->build_rows() != NULL);
    DCHECK_EQ(input_partition_->build_rows()->blocks_pinned(), 0) << NodeDebugString();
    bool got_read_buffer;
    RETURN_IF_ERROR(
        input_partition_->build_rows()->prepare_for_read(true, &got_read_buffer));
    if (!got_read_buffer) {
      std::string msg = Substitute(PREPARE_FOR_READ_FAILED_ERROR_MSG, _id);
      Status status = Status::MemoryLimitExceeded(msg.c_str());
      return status;
    }
  }

  for (int i = 0; i < PARTITION_FANOUT; ++i) {
    Partition* new_partition = new Partition(state, this, level);
    DCHECK(new_partition != NULL);
    hash_partitions_.push_back(partition_pool_->add(new_partition));
    RETURN_IF_ERROR(new_partition->build_rows()->init(id(), runtime_profile(), true));
    // Initialize a buffer for the probe here to make sure why have it if we need it.
    // While this is not strictly necessary (there are some cases where we won't need this
    // buffer), the benefit is low. Not grabbing this buffer means there is an additional
    // buffer that could be used for the build side. However since this is only one
    // buffer, there is only a small range of build input sizes where this is beneficial
    // (an IO buffer size). It makes the logic much more complex to enable this
    // optimization.
    RETURN_IF_ERROR(new_partition->probe_rows()->init(id(), runtime_profile(), false));
  }
  COUNTER_UPDATE(partitions_created_, PARTITION_FANOUT);
  COUNTER_SET(max_partition_level_, level);

  DCHECK_EQ(build_batch_->num_rows(), 0);
  bool eos = false;
  int64_t total_build_rows = 0;
  while (!eos) {
    RETURN_IF_CANCELLED(state);
//    RETURN_IF_ERROR(QueryMaintenance(state));
    // 'probe_expr_ctxs_' should have made no local allocations in this function.
//    DCHECK(!ExprContext::HasLocalAllocations(probe_expr_ctxs_))
//        << Expr::debug_string(probe_expr_ctxs_);
    if (input_partition_ == NULL) {
      // If we are still consuming batches from the build side.
      {
        // SCOPED_STOP_WATCH(&built_probe_overlap_stop_watch_);
        RETURN_IF_ERROR(child(1)->get_next(state, build_batch_.get(), &eos));
      }
      COUNTER_UPDATE(_build_row_counter, build_batch_->num_rows());
    } else {
      // If we are consuming batches that have already been partitioned.
      RETURN_IF_ERROR(input_partition_->build_rows()->get_next(build_batch_.get(), &eos));
    }
    total_build_rows += build_batch_->num_rows();

    SCOPED_TIMER(partition_build_timer_);
    if (process_build_batch_fn_ == NULL) {
      bool build_filters = ht_ctx_->level() == 0;
      RETURN_IF_ERROR(ProcessBuildBatch(build_batch_.get(), build_filters));
    } else {
      DCHECK(process_build_batch_fn_level0_ != NULL);
      if (ht_ctx_->level() == 0) {
        RETURN_IF_ERROR(
            process_build_batch_fn_level0_(this, build_batch_.get(), true));
      } else {
        RETURN_IF_ERROR(process_build_batch_fn_(this, build_batch_.get(), false));
      }
    }
    build_batch_->reset();
    DCHECK(!build_batch_->at_capacity());
  }

//  if (ht_ctx_->level() == 0) PublishRuntimeFilters(state, total_build_rows);

  if (input_partition_ != NULL) {
    // Done repartitioning build input, close it now.
    input_partition_->build_rows_->close();
    input_partition_->build_rows_ = NULL;
  }

  stringstream ss;
  ss << Substitute("PHJ(node_id=$0) partitioned(level=$1) $2 rows into:", id(),
            hash_partitions_[0]->level_, total_build_rows);
  for (int i = 0; i < hash_partitions_.size(); ++i) {
    Partition* partition = hash_partitions_[i];
    double percent =
        partition->build_rows()->num_rows() * 100 / static_cast<double>(total_build_rows);
    ss << "  " << i << " "  << (partition->is_spilled() ? "spilled" : "not spilled")
       << " (fraction=" << fixed << setprecision(2) << percent << "%)" << endl
       << "    #rows:" << partition->build_rows()->num_rows() << endl;
    COUNTER_SET(largest_partition_percent_, static_cast<int64_t>(percent));
  }
  VLOG(2) << ss.str();

  COUNTER_UPDATE(num_build_rows_partitioned_, total_build_rows);
  non_empty_build_ |= (total_build_rows > 0);
  RETURN_IF_ERROR(BuildHashTables(state));
  return Status::OK();
}

Status PartitionedHashJoinNode::NextProbeRowBatch(
    RuntimeState* state, RowBatch* out_batch) {
  DCHECK(_left_batch_pos == _left_batch->num_rows() || _left_batch_pos == -1);
  do {
    // Loop until we find a non-empty row batch.
    _left_batch->transfer_resource_ownership(out_batch);
    if (out_batch->at_capacity()) {
      // This out batch is full. Need to return it before getting the next batch.
      _left_batch_pos = -1;
      return Status::OK();
    }
    if (_left_side_eos) {
      _current_left_child_row = NULL;
      _left_batch_pos = -1;
      return Status::OK();
    }
    RETURN_IF_ERROR(child(0)->get_next(state, _left_batch.get(), &_left_side_eos));
    COUNTER_UPDATE(_left_child_row_counter, _left_batch->num_rows());
  } while (_left_batch->num_rows() == 0);

  ResetForProbe();
  return Status::OK();
}

Status PartitionedHashJoinNode::NextSpilledProbeRowBatch(
    RuntimeState* state, RowBatch* out_batch) {
  DCHECK(input_partition_ != NULL);
  _left_batch->transfer_resource_ownership(out_batch);
  if (out_batch->at_capacity()) {
    // The out_batch has resources associated with it that will be recycled on the
    // next call to get_next() on the probe stream. Return this batch now.
    _left_batch_pos = -1;
    return Status::OK();
  }
  BufferedTupleStream2* probe_rows = input_partition_->probe_rows();
  if (LIKELY(probe_rows->rows_returned() < probe_rows->num_rows())) {
    // Continue from the current probe stream.
    bool eos = false;
    RETURN_IF_ERROR(input_partition_->probe_rows()->get_next(_left_batch.get(), &eos));
    DCHECK_GT(_left_batch->num_rows(), 0);
    ResetForProbe();
  } else {
    // Done with this partition.
    if (!input_partition_->is_spilled() &&
        (_join_op == TJoinOp::RIGHT_OUTER_JOIN || _join_op == TJoinOp::RIGHT_ANTI_JOIN ||
         _join_op == TJoinOp::FULL_OUTER_JOIN)) {
      // In case of right-outer, right-anti and full-outer joins, we move this partition
      // to the list of partitions that we need to output their unmatched build rows.
      DCHECK(output_build_partitions_.empty());
      DCHECK(input_partition_->hash_tbl_.get() != NULL) << " id: " << _id
           << " Build: " << input_partition_->build_rows()->num_rows()
           << " Probe: " << probe_rows->num_rows() << endl;
      hash_tbl_iterator_ =
          input_partition_->hash_tbl_->FirstUnmatched(ht_ctx_.get());
      output_build_partitions_.push_back(input_partition_);
    } else {
      // In any other case, just close the input partition.
      input_partition_->Close(out_batch);
      input_partition_ = NULL;
    }
    _current_left_child_row = NULL;
    _left_batch_pos = -1;
  }
  return Status::OK();
}

Status PartitionedHashJoinNode::PrepareNextPartition(RuntimeState* state) {
  DCHECK(input_partition_ == NULL);
  if (spilled_partitions_.empty()) return Status::OK();

  input_partition_ = spilled_partitions_.front();
  spilled_partitions_.pop_front();
  DCHECK(input_partition_->is_spilled());

  // Reserve one buffer to read the probe side.
  bool got_read_buffer;
  RETURN_IF_ERROR(input_partition_->probe_rows()->prepare_for_read(true, &got_read_buffer));
  if (!got_read_buffer) {
    auto msg = Substitute(PREPARE_FOR_READ_FAILED_ERROR_MSG, _id);
    Status status = Status::MemoryLimitExceeded(msg.c_str());
    return status;
  }
  ht_ctx_->set_level(input_partition_->level_);

  int64_t mem_limit = mem_tracker()->spare_capacity() +
          state->block_mgr2()->available_buffers(block_mgr_client_) * state->block_mgr2()->max_block_size();
  // Try to build a hash table on top the spilled build rows.
  bool built = false;
  int64_t estimated_memory = input_partition_->EstimatedInMemSize();
  if (estimated_memory < mem_limit) {
    ht_ctx_->set_level(input_partition_->level_);
    RETURN_IF_ERROR(input_partition_->BuildHashTable(state, &built));
  } else {
    LOG(INFO) << "In hash join id=" << _id << " the estimated needed memory ("
        << estimated_memory << ") for partition " << input_partition_ << " with "
        << input_partition_->build_rows()->num_rows() << " build rows is larger "
        << " than the mem_limit (" << mem_limit << ").";
  }

  if (!built) {
    // This build partition still does not fit in memory, repartition.
    UpdateState(REPARTITIONING);
    DCHECK(input_partition_->is_spilled());
    input_partition_->Spill(false);
    ht_ctx_->set_level(input_partition_->level_ + 1);
    int64_t num_input_rows = input_partition_->build_rows()->num_rows();
    RETURN_IF_ERROR(ProcessBuildInput(state, input_partition_->level_ + 1));

    // Check if there was any reduction in the size of partitions after repartitioning.
    int64_t largest_partition = LargestSpilledPartition();
    DCHECK_GE(num_input_rows, largest_partition) << "Cannot have a partition with "
        "more rows than the input";
    if (UNLIKELY(num_input_rows == largest_partition)) {
        std::stringstream error_msg;
        error_msg << "Cannot perform aggregation at hash join node with id "
                  << _id << '.'
                  << " The input data was partitioned the maximum number of "
                  << MAX_PARTITION_DEPTH << " times."
                  << " This could mean there is significant skew in the data or the memory limit is"
                  << " set too low.";
        return Status::MemoryLimitExceeded(error_msg.str().c_str());
    }
  } else {
    DCHECK(hash_partitions_.empty());
    DCHECK(!input_partition_->is_spilled());
    DCHECK(input_partition_->hash_tbl() != NULL);
    // In this case, we did not have to partition the build again, we just built
    // a hash table. This means the probe does not have to be partitioned either.
    for (int i = 0; i < PARTITION_FANOUT; ++i) {
      hash_tbls_[i] = input_partition_->hash_tbl();
    }
    UpdateState(PROBING_SPILLED_PARTITION);
  }

  COUNTER_UPDATE(num_repartitions_, 1);
  COUNTER_UPDATE(num_probe_rows_partitioned_, input_partition_->probe_rows()->num_rows());
  return Status::OK();
}

int64_t PartitionedHashJoinNode::LargestSpilledPartition() const {
  int64_t max_rows = 0;
  for (int i = 0; i < hash_partitions_.size(); ++i) {
    Partition* partition = hash_partitions_[i];
    DCHECK(partition != NULL) << i << " " << hash_partitions_.size();
    if (partition->is_closed() || !partition->is_spilled()) continue;
    int64_t rows = partition->build_rows()->num_rows();
    rows += partition->probe_rows()->num_rows();
    if (rows > max_rows) max_rows = rows;
  }
  return max_rows;
}

int PartitionedHashJoinNode::ProcessProbeBatch(
    const TJoinOp::type join_op, TPrefetchMode::type prefetch_mode,
    RowBatch* out_batch, PartitionedHashTableCtx* ht_ctx, Status* status) {
  switch (join_op) {
    case TJoinOp::INNER_JOIN:
      return ProcessProbeBatch<TJoinOp::INNER_JOIN>(prefetch_mode, out_batch,
          ht_ctx, status);
    case TJoinOp::LEFT_OUTER_JOIN:
      return ProcessProbeBatch<TJoinOp::LEFT_OUTER_JOIN>(prefetch_mode, out_batch,
          ht_ctx, status);
    case TJoinOp::LEFT_SEMI_JOIN:
      return ProcessProbeBatch<TJoinOp::LEFT_SEMI_JOIN>(prefetch_mode, out_batch,
          ht_ctx, status);
    case TJoinOp::LEFT_ANTI_JOIN:
      return ProcessProbeBatch<TJoinOp::LEFT_ANTI_JOIN>(prefetch_mode, out_batch,
          ht_ctx, status);
    case TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN:
      return ProcessProbeBatch<TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN>(prefetch_mode,
          out_batch, ht_ctx, status);
    case TJoinOp::RIGHT_OUTER_JOIN:
      return ProcessProbeBatch<TJoinOp::RIGHT_OUTER_JOIN>(prefetch_mode, out_batch,
          ht_ctx, status);
    case TJoinOp::RIGHT_SEMI_JOIN:
      return ProcessProbeBatch<TJoinOp::RIGHT_SEMI_JOIN>(prefetch_mode, out_batch,
          ht_ctx, status);
    case TJoinOp::RIGHT_ANTI_JOIN:
      return ProcessProbeBatch<TJoinOp::RIGHT_ANTI_JOIN>(prefetch_mode, out_batch,
          ht_ctx, status);
    case TJoinOp::FULL_OUTER_JOIN:
      return ProcessProbeBatch<TJoinOp::FULL_OUTER_JOIN>(prefetch_mode, out_batch,
          ht_ctx, status);
    default:
      DCHECK(false) << "Unknown join type";
      return -1;
  }
}

Status PartitionedHashJoinNode::get_next(RuntimeState* state, RowBatch* out_batch,
    bool* eos) {
  SCOPED_TIMER(_runtime_profile->total_time_counter());
//  RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::GETNEXT, state));
  RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::GETNEXT));
  DCHECK(!out_batch->at_capacity());

  if (reached_limit()) {
    *eos = true;
    return Status::OK();
  } else {
    *eos = false;
  }

  Status status = Status::OK();
  while (true) {
    DCHECK(status.ok());
    DCHECK_NE(state_, PARTITIONING_BUILD) << "Should not be in get_next()";
    RETURN_IF_CANCELLED(state);
    // RETURN_IF_ERROR(QueryMaintenance(state));

    if ((_join_op == TJoinOp::RIGHT_OUTER_JOIN || _join_op == TJoinOp::RIGHT_ANTI_JOIN ||
         _join_op == TJoinOp::FULL_OUTER_JOIN) &&
        !output_build_partitions_.empty())  {
      // In case of right-outer, right-anti and full-outer joins, flush the remaining
      // unmatched build rows of any partition we are done processing, before processing
      // the next batch.
      OutputUnmatchedBuild(out_batch);
      if (!output_build_partitions_.empty()) break;

      // Finished to output unmatched build rows, move to next partition.
      DCHECK(hash_partitions_.empty());
      RETURN_IF_ERROR(PrepareNextPartition(state));
      if (input_partition_ == NULL) {
        *eos = true;
        break;
      }
      if (out_batch->at_capacity()) break;
    }

    if (_join_op == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
      // In this case, we want to output rows from the null aware partition.
      if (null_aware_partition_ == NULL) {
        *eos = true;
        break;
      }

      if (null_probe_output_idx_ >= 0) {
        RETURN_IF_ERROR(OutputNullAwareNullProbe(state, out_batch));
        if (out_batch->at_capacity()) break;
        continue;
      }

      if (nulls_build_batch_.get() != NULL) {
        RETURN_IF_ERROR(OutputNullAwareProbeRows(state, out_batch));
        if (out_batch->at_capacity()) break;
        continue;
      }
    }

    // Finish up the current batch.
    if (_left_batch_pos != -1) {
      // Putting SCOPED_TIMER in ProcessProbeBatch() causes weird exception handling IR
      // in the xcompiled function, so call it here instead.
      int rows_added = 0;
      TPrefetchMode::type prefetch_mode = config::enable_prefetch ? TPrefetchMode::HT_BUCKET :
              TPrefetchMode::NONE;
      SCOPED_TIMER(_left_child_timer);
//      if (process_build_batch_fn_ == NULL) {
        rows_added = ProcessProbeBatch(_join_op, prefetch_mode, out_batch, ht_ctx_.get(),
            &status);
//      } else {
//        DCHECK(process_build_batch_fn_level0_ != NULL);
//        if (ht_ctx_->level() == 0) {
//          rows_added = process_build_batch_fn_level0_(this, prefetch_mode, out_batch,
//              ht_ctx_.get(), &status);
//        } else {
//          rows_added = process_build_batch_fn_(this, prefetch_mode, out_batch,
//              ht_ctx_.get(), &status);
//        }
//      }
      if (UNLIKELY(rows_added < 0)) {
        DCHECK(!status.ok());
        return status;
      }
      DCHECK(status.ok());
      out_batch->commit_rows(rows_added);
      _num_rows_returned += rows_added;
      if (out_batch->at_capacity() || reached_limit()) break;

      DCHECK(_current_left_child_row == NULL);
      COUNTER_SET(_rows_returned_counter, _num_rows_returned);
    }

    // Try to continue from the current probe side input.
    if (input_partition_ == NULL) {
      RETURN_IF_ERROR(NextProbeRowBatch(state, out_batch));
    } else {
      RETURN_IF_ERROR(NextSpilledProbeRowBatch(state, out_batch));
    }
    // Free local allocations of the probe side expressions only after ExprValuesCache
    // has been reset.
    DCHECK(ht_ctx_->expr_values_cache()->AtEnd());
    ExprContext::free_local_allocations(probe_expr_ctxs_);

    // We want to return as soon as we have attached a tuple stream to the out_batch
    // (before preparing a new partition). The attached tuple stream will be recycled
    // by the caller, freeing up more memory when we prepare the next partition.
    if (out_batch->at_capacity()) break;

    // Got a batch, just keep going.
    if (_left_batch_pos == 0) continue;
    DCHECK_EQ(_left_batch_pos, -1);

    // Finished up all probe rows for hash_partitions_.
    RETURN_IF_ERROR(CleanUpHashPartitions(out_batch));
    if (out_batch->at_capacity()) break;

    if ((_join_op == TJoinOp::RIGHT_OUTER_JOIN || _join_op == TJoinOp::RIGHT_ANTI_JOIN ||
         _join_op == TJoinOp::FULL_OUTER_JOIN) &&
        !output_build_partitions_.empty()) {
      // There are some partitions that need to flush their unmatched build rows.
      continue;
    }
    // Move onto the next partition.
    RETURN_IF_ERROR(PrepareNextPartition(state));

    if (input_partition_ == NULL) {
      if (_join_op == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
        RETURN_IF_ERROR(PrepareNullAwarePartition());
      }
      if (null_aware_partition_ == NULL) {
        *eos = true;
        break;
      } else {
        *eos = false;
      }
    }
  }

  if (reached_limit()) *eos = true;
  return Status::OK();
}

void PartitionedHashJoinNode::OutputUnmatchedBuild(RowBatch* out_batch) {
  SCOPED_TIMER(_left_child_timer);
  DCHECK(_join_op == TJoinOp::RIGHT_OUTER_JOIN || _join_op == TJoinOp::RIGHT_ANTI_JOIN ||
         _join_op == TJoinOp::FULL_OUTER_JOIN);
  DCHECK(!output_build_partitions_.empty());
  ExprContext* const* conjunct_ctxs = &_conjunct_ctxs[0];
  const int num_conjuncts = _conjunct_ctxs.size();
  RowBatch::Iterator out_batch_iterator(out_batch, out_batch->num_rows());
  const int start_num_rows = out_batch->num_rows();

  while (!out_batch->at_capacity() && !hash_tbl_iterator_.AtEnd()) {
    // Output remaining unmatched build rows.
    if (!hash_tbl_iterator_.IsMatched()) {
      TupleRow* build_row = hash_tbl_iterator_.GetRow();
      DCHECK(build_row != NULL);
      create_output_row(out_batch_iterator.get(), NULL, build_row);
      if (ExecNode::eval_conjuncts(conjunct_ctxs, num_conjuncts,
          out_batch_iterator.get())) {
        out_batch->commit_last_row();
        out_batch_iterator.next();
      }
      hash_tbl_iterator_.SetMatched();
    }
    // Move to the next unmatched entry.
    hash_tbl_iterator_.NextUnmatched();
  }

  // If we reached the end of the hash table, then there are no other unmatched build
  // rows for this partition. In that case we need to close the partition, and move to
  // the next. If we have not reached the end of the hash table, it means that we reached
  // out_batch capacity and we need to continue to output unmatched build rows, without
  // closing the partition.
  if (hash_tbl_iterator_.AtEnd()) {
    output_build_partitions_.front()->Close(out_batch);
    output_build_partitions_.pop_front();
    // Move to the next partition to output unmatched rows.
    if (!output_build_partitions_.empty()) {
      hash_tbl_iterator_ =
          output_build_partitions_.front()->hash_tbl()->FirstUnmatched(ht_ctx_.get());
    }
  }

  _num_rows_returned += out_batch->num_rows() - start_num_rows;
  COUNTER_UPDATE(_rows_returned_counter, _num_rows_returned);
}

Status PartitionedHashJoinNode::PrepareNullAwareNullProbe() {
  DCHECK_EQ(null_probe_output_idx_, -1);
  bool got_read_buffer;
  RETURN_IF_ERROR(null_probe_rows_->prepare_for_read(true, &got_read_buffer));
  if (!got_read_buffer) {
    auto msg = Substitute(PREPARE_FOR_READ_FAILED_ERROR_MSG, _id);
    Status status = Status::MemoryLimitExceeded(msg.c_str());
    return status;
  }
  DCHECK_EQ(_left_batch->num_rows(), 0);
  _left_batch_pos = 0;
  null_probe_output_idx_ = 0;
  return Status::OK();
}

Status PartitionedHashJoinNode::OutputNullAwareNullProbe(RuntimeState* state,
    RowBatch* out_batch) {
  DCHECK(null_aware_partition_ != NULL);
  DCHECK(nulls_build_batch_.get() == NULL);
  DCHECK_NE(_left_batch_pos, -1);

  if (_left_batch_pos == _left_batch->num_rows()) {
    _left_batch_pos = 0;
    _left_batch->transfer_resource_ownership(out_batch);
    if (out_batch->at_capacity()) return Status::OK();
    bool eos;
    RETURN_IF_ERROR(null_probe_rows_->get_next(_left_batch.get(), &eos));
    if (_left_batch->num_rows() == 0) {
      // All done.
      null_aware_partition_->Close(out_batch);
      null_aware_partition_ = NULL;
      out_batch->add_tuple_stream(null_probe_rows_);
      null_probe_rows_ = NULL;
      return Status::OK();
    }
  }

  for (; _left_batch_pos < _left_batch->num_rows();
      ++_left_batch_pos, ++null_probe_output_idx_) {
    if (out_batch->at_capacity()) break;
    if (matched_null_probe_[null_probe_output_idx_]) continue;
    TupleRow* out_row = out_batch->get_row(out_batch->add_row());
    out_batch->copy_row(_left_batch->get_row(_left_batch_pos), out_row);
    out_batch->commit_last_row();
  }

  return Status::OK();
}

// In this case we had a lot of NULLs on either the build/probe side. While this is
// possible to process by re-reading the spilled streams for each row with minimal code
// effort, this would behave very slowly (we'd need to do IO for each row). This seems
// like a reasonable limitation for now.
// TODO: revisit.
static Status NullAwareAntiJoinError(bool build) {
  auto msg = Substitute("Unable to perform Null-Aware Anti-Join. There are too"
                       " many NULLs on the $0 side to perform this join.", build ? "build" : "probe");
  return Status::InternalError(msg.c_str());
}

Status PartitionedHashJoinNode::PrepareNullAwarePartition() {
  DCHECK(null_aware_partition_ != NULL);
  DCHECK(nulls_build_batch_.get() == NULL);
  DCHECK_EQ(_left_batch_pos, -1);
  DCHECK_EQ(_left_batch->num_rows(), 0);

  BufferedTupleStream2* build_stream = null_aware_partition_->build_rows();
  BufferedTupleStream2* probe_stream = null_aware_partition_->probe_rows();

  if (build_stream->num_rows() == 0) {
    // There were no build rows. Nothing to do. Just prepare to output the null
    // probe rows.
    DCHECK_EQ(probe_stream->num_rows(), 0);
    nulls_build_batch_.reset();
    RETURN_IF_ERROR(PrepareNullAwareNullProbe());
    return Status::OK();
  }

  // Bring the entire spilled build stream into memory and read into a single batch.
  bool got_rows;
  RETURN_IF_ERROR(build_stream->get_rows(&nulls_build_batch_, &got_rows));
  if (!got_rows) return NullAwareAntiJoinError(true);

  // Initialize the streams for read.
  bool got_read_buffer;
  RETURN_IF_ERROR(probe_stream->prepare_for_read(true, &got_read_buffer));
  if (!got_read_buffer) {
    Status status = Status::MemoryLimitExceeded(Substitute(PREPARE_FOR_READ_FAILED_ERROR_MSG, _id).c_str());
    return status;
  }
  _left_batch_pos = 0;
  return Status::OK();
}

Status PartitionedHashJoinNode::OutputNullAwareProbeRows(RuntimeState* state,
    RowBatch* out_batch) {
  DCHECK(null_aware_partition_ != NULL);
  DCHECK(nulls_build_batch_.get() != NULL);

  ExprContext* const* join_conjunct_ctxs = &other_join_conjunct_ctxs_[0];
  int num_join_conjuncts = other_join_conjunct_ctxs_.size();
  DCHECK(_left_batch.get() != NULL);

  BufferedTupleStream2* probe_stream = null_aware_partition_->probe_rows();
  if (_left_batch_pos == _left_batch->num_rows()) {
    _left_batch_pos = 0;
    _left_batch->transfer_resource_ownership(out_batch);
    if (out_batch->at_capacity()) return Status::OK();

    // Get the next probe batch.
    bool eos;
    RETURN_IF_ERROR(probe_stream->get_next(_left_batch.get(), &eos));

    if (_left_batch->num_rows() == 0) {
      RETURN_IF_ERROR(EvaluateNullProbe(null_aware_partition_->build_rows()));
      nulls_build_batch_.reset();
      RETURN_IF_ERROR(PrepareNullAwareNullProbe());
      return Status::OK();
    }
  }

  // For each probe row, iterate over all the build rows and check for rows
  // that did not have any matches.
  for (; _left_batch_pos < _left_batch->num_rows(); ++_left_batch_pos) {
    if (out_batch->at_capacity()) break;
    TupleRow* probe_row = _left_batch->get_row(_left_batch_pos);

    bool matched = false;
    for (int i = 0; i < nulls_build_batch_->num_rows(); ++i) {
      create_output_row(semi_join_staging_row_, probe_row, nulls_build_batch_->get_row(i));
      if (ExecNode::eval_conjuncts(
          join_conjunct_ctxs, num_join_conjuncts, semi_join_staging_row_)) {
        matched = true;
        break;
      }
    }

    if (!matched) {
      TupleRow* out_row = out_batch->get_row(out_batch->add_row());
      out_batch->copy_row(probe_row, out_row);
      out_batch->commit_last_row();
    }
  }
  return Status::OK();
}

// When this function is called, we've finished processing the current build input
// (either from child(1) or from repartitioning a spilled partition). The build rows
// have only been partitioned, we still need to build hash tables over them. Some
// of the partitions could have already been spilled and attempting to build hash
// tables over the non-spilled ones can cause them to spill.
//
// At the end of the function we'd like all partitions to either have a hash table
// (and therefore not spilled) or be spilled. Partitions that have a hash table don't
// need to spill on the probe side.
//
// This maps perfectly to a 0-1 knapsack where the weight is the memory to keep the
// build rows and hash table and the value is the expected IO savings.
// For now, we go with a greedy solution.
//
// TODO: implement the knapsack solution.
Status PartitionedHashJoinNode::BuildHashTables(RuntimeState* state) {
  DCHECK_EQ(hash_partitions_.size(), PARTITION_FANOUT);

  // First loop over the partitions and build hash tables for the partitions that did
  // not already spill.
  for (Partition* partition: hash_partitions_) {
    if (partition->build_rows()->num_rows() == 0) {
      // This partition is empty, no need to do anything else.
      partition->Close(NULL);
      continue;
    }

    if (!partition->is_spilled()) {
      bool built = false;
      DCHECK(partition->build_rows()->is_pinned());
      RETURN_IF_ERROR(partition->BuildHashTable(state, &built));
      // If we did not have enough memory to build this hash table, we need to spill this
      // partition (clean up the hash table, unpin build).
      if (!built) RETURN_IF_ERROR(partition->Spill(true));
    }
  }

  // Collect all the spilled partitions that don't have an IO buffer. We need to reserve
  // an IO buffer for those partitions. Reserving an IO buffer can cause more partitions
  // to spill so this process is recursive.
  list<Partition*> spilled_partitions;
  for (Partition* partition: hash_partitions_) {
    if (partition->is_closed()) continue;
    if (partition->is_spilled() && partition->probe_rows()->using_small_buffers()) {
      spilled_partitions.push_back(partition);
    }
  }
  while (!spilled_partitions.empty()) {
    Partition* partition = spilled_partitions.front();
    spilled_partitions.pop_front();

    while (true) {
      bool got_buffer;
      RETURN_IF_ERROR(partition->probe_rows()->switch_to_io_buffers(&got_buffer));
      if (got_buffer) break;
      Partition* spilled_partition;
      RETURN_IF_ERROR(SpillPartition(&spilled_partition));
      DCHECK(spilled_partition->is_spilled());
      if (spilled_partition->probe_rows()->using_small_buffers()) {
        spilled_partitions.push_back(spilled_partition);
      }
    }

    DCHECK(partition->probe_rows()->has_write_block());
    DCHECK(!partition->probe_rows()->using_small_buffers());
  }

  // At this point, the partition is in one of these states:
  // 1. closed. All done, no buffers in either the build or probe stream.
  // 2. in_mem. The build side is pinned and has a hash table built.
  // 3. spilled. The build side is fully unpinned and the probe side has an io
  //    sized buffer.
  for (Partition* partition: hash_partitions_) {
    if (partition->hash_tbl() != NULL) partition->probe_rows()->close();
  }

  // TODO: at this point we could have freed enough memory to pin and build some
  // spilled partitions. This can happen, for example is there is a lot of skew.
  // Partition 1: 10GB (pinned initially).
  // Partition 2,3,4: 1GB (spilled during partitioning the build).
  // In the previous step, we could have unpinned 10GB (because there was not enough
  // memory to build a hash table over it) which can now free enough memory to
  // build hash tables over the remaining 3 partitions.
  // We start by spilling the largest partition though so the build input would have
  // to be pretty pathological.
  // Investigate if this is worthwhile.

  // Initialize the hash_tbl_ caching array.
  for (int i = 0; i < PARTITION_FANOUT; ++i) {
    hash_tbls_[i] = hash_partitions_[i]->hash_tbl();
  }
  return Status::OK();
}

Status PartitionedHashJoinNode::EvaluateNullProbe(BufferedTupleStream2* build) {
  if (null_probe_rows_ == NULL || null_probe_rows_->num_rows() == 0) {
    return Status::OK();
  }
  DCHECK_EQ(null_probe_rows_->num_rows(), matched_null_probe_.size());

  // Bring both the build and probe side into memory and do a pairwise evaluation.
  bool got_rows;
  boost::scoped_ptr<RowBatch> build_rows;
  RETURN_IF_ERROR(build->get_rows(&build_rows, &got_rows));
  if (!got_rows) return NullAwareAntiJoinError(true);
  boost::scoped_ptr<RowBatch> probe_rows;
  RETURN_IF_ERROR(null_probe_rows_->get_rows(&probe_rows, &got_rows));
  if (!got_rows) return NullAwareAntiJoinError(false);

  ExprContext* const* join_conjunct_ctxs = &other_join_conjunct_ctxs_[0];
  int num_join_conjuncts = other_join_conjunct_ctxs_.size();

  DCHECK_LE(probe_rows->num_rows(), matched_null_probe_.size());
  // For each row, iterate over all rows in the build table.
  SCOPED_TIMER(null_aware_eval_timer_);
  for (int i = 0; i < probe_rows->num_rows(); ++i) {
    if (matched_null_probe_[i]) continue;
    for (int j = 0; j < build_rows->num_rows(); ++j) {
      create_output_row(semi_join_staging_row_, probe_rows->get_row(i),
          build_rows->get_row(j));
      if (ExecNode::eval_conjuncts(
            join_conjunct_ctxs, num_join_conjuncts, semi_join_staging_row_)) {
        matched_null_probe_[i] = true;
        break;
      }
    }
  }

  return Status::OK();
}

Status PartitionedHashJoinNode::CleanUpHashPartitions(RowBatch* batch) {
  DCHECK_EQ(_left_batch_pos, -1);
  // At this point all the rows have been read from the probe side for all partitions in
  // hash_partitions_.

  // Walk the partitions that had hash tables built for the probe phase and close them.
  // In the case of right outer and full outer joins, instead of closing those partitions,
  // add them to the list of partitions that need to output any unmatched build rows.
  // This partition will be closed by the function that actually outputs unmatched build
  // rows.
  for (int i = 0; i < hash_partitions_.size(); ++i) {
    Partition* partition = hash_partitions_[i];
    if (partition->is_closed()) continue;
    if (partition->is_spilled()) {
      // Unpin the build and probe stream to free up more memory. We need to free all
      // memory so we can recurse the algorithm and create new hash partitions from
      // spilled partitions.
      RETURN_IF_ERROR(partition->build_rows()->unpin_stream(true));
      RETURN_IF_ERROR(partition->probe_rows()->unpin_stream(true));

      // Push new created partitions at the front. This means a depth first walk
      // (more finely partitioned partitions are processed first). This allows us
      // to delete blocks earlier and bottom out the recursion earlier.
      spilled_partitions_.push_front(partition);
    } else {
      DCHECK_EQ(partition->probe_rows()->num_rows(), 0)
        << "No probe rows should have been spilled for this partition.";
      if (_join_op == TJoinOp::RIGHT_OUTER_JOIN || _join_op == TJoinOp::RIGHT_ANTI_JOIN ||
          _join_op == TJoinOp::FULL_OUTER_JOIN) {
        if (output_build_partitions_.empty()) {
          hash_tbl_iterator_ = partition->hash_tbl_->FirstUnmatched(ht_ctx_.get());
        }
        output_build_partitions_.push_back(partition);
      } else if (_join_op == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
        // For NAAJ, we need to try to match all the NULL probe rows with this partition
        // before closing it. The NULL probe rows could have come from any partition
        // so we collect them all and match them at the end.
        RETURN_IF_ERROR(EvaluateNullProbe(partition->build_rows()));
        partition->Close(batch);
      } else {
        partition->Close(batch);
      }
    }
  }

  // Just finished evaluating the null probe rows with all the non-spilled build
  // partitions. Unpin this now to free this memory for repartitioning.
  if (null_probe_rows_ != NULL) RETURN_IF_ERROR(null_probe_rows_->unpin_stream());

  hash_partitions_.clear();
  input_partition_ = NULL;
  return Status::OK();
}

void PartitionedHashJoinNode::AddToDebugString(int indent, stringstream* out) const {
  *out << " hash_tbl=";
  *out << string(indent * 2, ' ');
  *out << "HashTbl("
       << " build_exprs=" << Expr::debug_string(build_expr_ctxs_)
       << " probe_exprs=" << Expr::debug_string(probe_expr_ctxs_);
  *out << ")";
}

void PartitionedHashJoinNode::UpdateState(HashJoinState s) {
  state_ = s;
  VLOG(2) << "Transitioned State:" << endl << NodeDebugString();
}

string PartitionedHashJoinNode::PrintState() const {
  switch (state_) {
    case PARTITIONING_BUILD: return "PartitioningBuild";
    case PROCESSING_PROBE: return "ProcessingProbe";
    case PROBING_SPILLED_PARTITION: return "ProbingSpilledPartitions";
    case REPARTITIONING: return "Repartitioning";
    default: DCHECK(false);
  }
  return "";
}

string PartitionedHashJoinNode::NodeDebugString() const {
  stringstream ss;
  ss << "PartitionedHashJoinNode (id=" << id() << " op=" << _join_op
     << " state=" << PrintState()
     << " #partitions=" << hash_partitions_.size()
     << " #spilled_partitions=" << spilled_partitions_.size()
     << ")" << endl;

  for (int i = 0; i < hash_partitions_.size(); ++i) {
    Partition* partition = hash_partitions_[i];
    ss << i << ": ptr=" << partition;
    DCHECK(partition != NULL);
    if (partition->is_closed()) {
      ss << " Closed" << endl;
      continue;
    }
    if (partition->is_spilled()) {
      ss << " Spilled" << endl;
    }
    DCHECK(partition->build_rows() != NULL);
    DCHECK(partition->probe_rows() != NULL);
    ss << endl
       << "   Build Rows: " << partition->build_rows()->num_rows()
       << " (Blocks pinned: " << partition->build_rows()->blocks_pinned() << ")"
       << endl;
    ss << "   Probe Rows: " << partition->probe_rows()->num_rows()
       << " (Blocks pinned: " << partition->probe_rows()->blocks_pinned() << ")"
       << endl;
    if (partition->hash_tbl() != NULL) {
      ss << "   Hash Table Rows: " << partition->hash_tbl()->size() << endl;
    }
  }

  if (!spilled_partitions_.empty()) {
    ss << "SpilledPartitions" << endl;
    for (list<Partition*>::const_iterator it = spilled_partitions_.begin();
        it != spilled_partitions_.end(); ++it) {
      DCHECK((*it)->is_spilled());
      DCHECK((*it)->hash_tbl() == NULL);
      DCHECK((*it)->build_rows() != NULL);
      DCHECK((*it)->probe_rows() != NULL);
      ss << "  Partition=" << *it << endl
         << "   Spilled Build Rows: "
         << (*it)->build_rows()->num_rows() << endl
         << "   Spilled Probe Rows: "
         << (*it)->probe_rows()->num_rows() << endl;
    }
  }
  if (input_partition_ != NULL) {
    DCHECK(input_partition_->build_rows() != NULL);
    DCHECK(input_partition_->probe_rows() != NULL);
    ss << "InputPartition: " << input_partition_ << endl
       << "   Spilled Build Rows: "
       << input_partition_->build_rows()->num_rows() << endl
       << "   Spilled Probe Rows: "
       << input_partition_->probe_rows()->num_rows() << endl;
  } else {
    ss << "InputPartition: NULL" << endl;
  }
  return ss.str();
}

// For a left outer join, the IR looks like:
// define void @create_output_row(%"class.impala::BlockingJoinNode"* %this_ptr,
//                              %"class.impala::TupleRow"* %out_arg,
//                              %"class.impala::TupleRow"* %probe_arg,
//                              %"class.impala::TupleRow"* %build_arg) #20 {
// entry:
//   %out = bitcast %"class.impala::TupleRow"* %out_arg to i8**
//   %probe = bitcast %"class.impala::TupleRow"* %probe_arg to i8**
//   %build = bitcast %"class.impala::TupleRow"* %build_arg to i8**
//   %0 = bitcast i8** %out to i8*
//   %1 = bitcast i8** %probe to i8*
//   call void @llvm.memcpy.p0i8.p0i8.i32(i8* %0, i8* %1, i32 8, i32 0, i1 false)
//   %build_dst_ptr = getelementptr i8** %out, i32 1
//   %is_build_null = icmp eq i8** %build, null
//   br i1 %is_build_null, label %build_null, label %build_not_null
//
// build_not_null:                                   ; preds = %entry
//   %2 = bitcast i8** %build_dst_ptr to i8*
//   %3 = bitcast i8** %build to i8*
//   call void @llvm.memcpy.p0i8.p0i8.i32(i8* %2, i8* %3, i32 8, i32 0, i1 false)
//   ret void
//
// build_null:                                       ; preds = %entry
//   %dst_tuple_ptr = getelementptr i8** %out, i32 1
//   store i8* null, i8** %dst_tuple_ptr
//   ret void
// }
//Status PartitionedHashJoinNode::Codegencreate_output_row(LlvmCodeGen* codegen,
//    Function** fn) {
//  Type* tuple_row_type = codegen->GetType(TupleRow::LLVM_CLASS_NAME);
//  DCHECK(tuple_row_type != NULL);
//  PointerType* tuple_row_ptr_type = PointerType::get(tuple_row_type, 0);
//
//  Type* this_type = codegen->GetType(BlockingJoinNode::LLVM_CLASS_NAME);
//  DCHECK(this_type != NULL);
//  PointerType* this_ptr_type = PointerType::get(this_type, 0);
//
//  // TupleRows are really just an array of pointers.  Easier to work with them
//  // this way.
//  PointerType* tuple_row_working_type = PointerType::get(codegen->ptr_type(), 0);
//
//  // Construct function signature to match create_output_row()
//  LlvmCodeGen::FnPrototype prototype(codegen, "create_output_row", codegen->vo_idtype());
//  prototype.AddArgument(LlvmCodeGen::NamedVariable("this_ptr", this_ptr_type));
//  prototype.AddArgument(LlvmCodeGen::NamedVariable("out_arg", tuple_row_ptr_type));
//  prototype.AddArgument(LlvmCodeGen::NamedVariable("probe_arg", tuple_row_ptr_type));
//  prototype.AddArgument(LlvmCodeGen::NamedVariable("build_arg", tuple_row_ptr_type));
//
//  LLVMContext& context = codegen->context();
//  LlvmCodeGen::LlvmBuilder builder(context);
//  Value* args[4];
//  *fn = prototype.GeneratePrototype(&builder, args);
//  Value* out_row_arg = builder.CreateBitCast(args[1], tuple_row_working_type, "out");
//  Value* probe_row_arg = builder.CreateBitCast(args[2], tuple_row_working_type, "probe");
//  Value* build_row_arg = builder.CreateBitCast(args[3], tuple_row_working_type, "build");
//
//  int num_probe_tuples = child(0)->row_desc().tuple_descriptors().size();
//  int num_build_tuples = child(1)->row_desc().tuple_descriptors().size();
//
//  // Copy probe row
//  codegen->CodegenMemcpy(&builder, out_row_arg, probe_row_arg, probe_tuple_row_size_);
//  Value* build_row_idx[] = { codegen->GetIntConstant(TYPE_INT, num_probe_tuples) };
//  Value* build_row_dst = builder.CreateGEP(out_row_arg, build_row_idx, "build_dst_ptr");
//
//  // Copy build row.
//  BasicBlock* build_not_null_block = BasicBlock::Create(context, "build_not_null", *fn);
//  BasicBlock* build_null_block = NULL;
//
//  if (_join_op == TJoinOp::LEFT_ANTI_JOIN || _join_op == TJoinOp::LEFT_OUTER_JOIN ||
//      _join_op == TJoinOp::FULL_OUTER_JOIN ||
//      _join_op == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
//    // build tuple can be null
//    build_null_block = BasicBlock::Create(context, "build_null", *fn);
//    Value* is_build_null = builder.CreateIsNull(build_row_arg, "is_build_null");
//    builder.CreateCondBr(is_build_null, build_null_block, build_not_null_block);
//
//    // Set tuple build ptrs to NULL
//    // TODO: this should be replaced with memset() but I can't get the llvm intrinsic
//    // to work.
//    builder.SetInsertPoint(build_null_block);
//    for (int i = 0; i < num_build_tuples; ++i) {
//      Value* array_idx[] =
//          { codegen->GetIntConstant(TYPE_INT, i + num_probe_tuples) };
//      Value* dst = builder.CreateGEP(out_row_arg, array_idx, "dst_tuple_ptr");
//      builder.CreateStore(codegen->null_ptr_value(), dst);
//    }
//    builder.CreateRetVoid();
//  } else {
//    // build row can't be NULL
//    builder.CreateBr(build_not_null_block);
//  }
//
//  // Copy build tuple ptrs
//  builder.SetInsertPoint(build_not_null_block);
//  codegen->CodegenMemcpy(&builder, build_row_dst, build_row_arg, build_tuple_row_size_);
//  builder.CreateRetVoid();
//
//  *fn = codegen->FinalizeFunction(*fn);
//  if (*fn == NULL) {
//    return Status("PartitionedHashJoinNode::Codegencreate_output_row(): codegen'd "
//        "create_output_row() function failed verification, see log");
//  }
//  return Status::OK();
//}

//Status PartitionedHashJoinNode::CodegenProcessBuildBatch(RuntimeState* state,
//    Function* hash_fn, Function* murmur_hash_fn, Function* eval_row_fn) {
//  LlvmCodeGen* codegen;
//  RETURN_IF_ERROR(state->GetCodegen(&codegen));
//
//  Function* process_build_batch_fn =
//      codegen->GetFunction(IRFunction::PHJ_PROCESS_BUILD_BATCH, true);
//  DCHECK(process_build_batch_fn != NULL);
//
//  // Replace call sites
//  int replaced = codegen->ReplaceCallSites(process_build_batch_fn, eval_row_fn,
//      "EvalBuildRow");
//  DCHECK_EQ(replaced, 1);
//
//  // Replace some hash table parameters with constants.
//  PartitionedHashTableCtx::PartitionedHashTableReplacedConstants replaced_constants;
//  const bool stores_duplicates = true;
//  const int num_build_tuples = child(1)->row_desc().tuple_descriptors().size();
//  RETURN_IF_ERROR(ht_ctx_->ReplacePartitionedHashTableConstants(state, stores_duplicates,
//      num_build_tuples, process_build_batch_fn, &replaced_constants));
//  DCHECK_GE(replaced_constants.stores_nulls, 1);
//  DCHECK_EQ(replaced_constants.finds_some_nulls, 0);
//  DCHECK_EQ(replaced_constants.stores_duplicates, 0);
//  DCHECK_EQ(replaced_constants.stores_tuples, 0);
//  DCHECK_EQ(replaced_constants.quadratic_probing, 0);
//
//  Function* process_build_batch_fn_level0 =
//      codegen->CloneFunction(process_build_batch_fn);
//
//  // Always build runtime filters at level0 (if there are any).
//  // Note that the first argument of this function is the return value.
//  Value* build_filters_l0_arg = codegen->GetArgument(process_build_batch_fn_level0, 3);
//  build_filters_l0_arg->replaceAllUsesWith(
//      ConstantInt::get(Type::getInt1Ty(codegen->context()), filters_.size() > 0));
//
//  // process_build_batch_fn_level0 uses CRC hash if available,
//  replaced = codegen->ReplaceCallSites(process_build_batch_fn_level0, hash_fn,
//      "HashCurrentRow");
//  DCHECK_EQ(replaced, 1);
//
//  // process_build_batch_fn uses murmur
//  replaced = codegen->ReplaceCallSites(process_build_batch_fn, murmur_hash_fn,
//      "HashCurrentRow");
//  DCHECK_EQ(replaced, 1);
//
//  // Never build filters after repartitioning, as all rows have already been added to the
//  // filters during the level0 build. Note that the first argument of this function is the
//  // return value.
//  Value* build_filters_arg = codegen->GetArgument(process_build_batch_fn, 3);
//  build_filters_arg->replaceAllUsesWith(
//      ConstantInt::get(Type::getInt1Ty(codegen->context()), false));
//
//  // Finalize ProcessBuildBatch functions
//  process_build_batch_fn = codegen->FinalizeFunction(process_build_batch_fn);
//  if (process_build_batch_fn == NULL) {
//    return Status("Codegen'd PartitionedHashJoinNode::ProcessBuildBatch() function "
//        "failed verification, see log");
//  }
//  process_build_batch_fn_level0 =
//      codegen->FinalizeFunction(process_build_batch_fn_level0);
//  if (process_build_batch_fn == NULL) {
//    return Status("Codegen'd level-zero PartitionedHashJoinNode::ProcessBuildBatch() "
//        "function failed verification, see log");
//  }
//
//  // Register native function pointers
//  codegen->AddFunctionToJit(process_build_batch_fn,
//                            reinterpret_cast<void**>(&process_build_batch_fn_));
//  codegen->AddFunctionToJit(process_build_batch_fn_level0,
//                            reinterpret_cast<void**>(&process_build_batch_fn_level0_));
//  return Status::OK();
//}

//Status PartitionedHashJoinNode::CodegenProcessProbeBatch(
//    RuntimeState* state, Function* hash_fn, Function* murmur_hash_fn) {
//  LlvmCodeGen* codegen;
//  RETURN_IF_ERROR(state->GetCodegen(&codegen));
//
//  // Get cross compiled function
//  IRFunction::Type ir_fn = IRFunction::FN_END;
//  switch (_join_op) {
//    case TJoinOp::INNER_JOIN:
//      ir_fn = IRFunction::PHJ_PROCESS_PROBE_BATCH_INNER_JOIN;
//      break;
//    case TJoinOp::LEFT_OUTER_JOIN:
//      ir_fn = IRFunction::PHJ_PROCESS_PROBE_BATCH_LEFT_OUTER_JOIN;
//      break;
//    case TJoinOp::LEFT_SEMI_JOIN:
//      ir_fn = IRFunction::PHJ_PROCESS_PROBE_BATCH_LEFT_SEMI_JOIN;
//      break;
//    case TJoinOp::LEFT_ANTI_JOIN:
//      ir_fn = IRFunction::PHJ_PROCESS_PROBE_BATCH_LEFT_ANTI_JOIN;
//      break;
//    case TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN:
//      ir_fn = IRFunction::PHJ_PROCESS_PROBE_BATCH_NULL_AWARE_LEFT_ANTI_JOIN;
//      break;
//    case TJoinOp::RIGHT_OUTER_JOIN:
//      ir_fn = IRFunction::PHJ_PROCESS_PROBE_BATCH_RIGHT_OUTER_JOIN;
//      break;
//    case TJoinOp::RIGHT_SEMI_JOIN:
//      ir_fn = IRFunction::PHJ_PROCESS_PROBE_BATCH_RIGHT_SEMI_JOIN;
//      break;
//    case TJoinOp::RIGHT_ANTI_JOIN:
//      ir_fn = IRFunction::PHJ_PROCESS_PROBE_BATCH_RIGHT_ANTI_JOIN;
//      break;
//    case TJoinOp::FULL_OUTER_JOIN:
//      ir_fn = IRFunction::PHJ_PROCESS_PROBE_BATCH_FULL_OUTER_JOIN;
//      break;
//    default:
//      DCHECK(false);
//  }
//  Function* process__left_batchfn = codegen->GetFunction(ir_fn, true);
//  DCHECK(process__left_batchfn != NULL);
//  process__left_batchfn->setName("ProcessProbeBatch");
//
//  // Verifies that ProcessProbeBatch() has weak_odr linkage so it's not discarded even
//  // if it's not referenced. See http://llvm.org/docs/LangRef.html#linkage-types
//  DCHECK(process__left_batchfn->getLinkage() == GlobalValue::WeakODRLinkage)
//      << LlvmCodeGen::Print(process__left_batchfn);
//
//  // Bake in %this pointer argument to process__left_batchfn.
//  Value* this_arg = codegen->GetArgument(process__left_batchfn, 0);
//  Value* this_loc = codegen->CastPtrToLlvmPtr(this_arg->getType(), this);
//  this_arg->replaceAllUsesWith(this_loc);
//
//  // Replace the parameter 'prefetch_mode' with constant.
//  Value* prefetch_mode_arg = codegen->GetArgument(process__left_batchfn, 1);
//  TPrefetchMode::type prefetch_mode = state->query_options().prefetch_mode;
//  DCHECK_GE(prefetch_mode, TPrefetchMode::NONE);
//  DCHECK_LE(prefetch_mode, TPrefetchMode::HT_BUCKET);
//  prefetch_mode_arg->replaceAllUsesWith(
//      ConstantInt::get(Type::getInt32Ty(codegen->context()), prefetch_mode));
//
//  // Bake in %ht_ctx pointer argument to process__left_batchfn
//  Value* ht_ctx_arg = codegen->GetArgument(process__left_batchfn, 3);
//  Value* ht_ctx_loc = codegen->CastPtrToLlvmPtr(ht_ctx_arg->getType(), ht_ctx_.get());
//  ht_ctx_arg->replaceAllUsesWith(ht_ctx_loc);
//
//  // Codegen PartitionedHashTable::Equals
//  Function* probe_equals_fn;
//  RETURN_IF_ERROR(ht_ctx_->CodegenEquals(state, false, &probe_equals_fn));
//
//  // Codegen for evaluating probe rows
//  Function* eval_row_fn;
//  RETURN_IF_ERROR(ht_ctx_->CodegenEvalRow(state, false, &eval_row_fn));
//
//  // Codegen create_output_row
//  Function* create_output_row_fn;
//  RETURN_IF_ERROR(Codegencreate_output_row(codegen, &create_output_row_fn));
//
//  // Codegen evaluating other join conjuncts
//  Function* eval_other_conjuncts_fn;
//  RETURN_IF_ERROR(ExecNode::Codegeneval_conjuncts(state, other_join_conjunct_ctxs_,
//      &eval_other_conjuncts_fn, "EvalOtherConjuncts"));
//
//  // Codegen evaluating conjuncts
//  Function* eval_conjuncts_fn;
//  RETURN_IF_ERROR(ExecNode::Codegeneval_conjuncts(state, _conjunct_ctxs,
//      &eval_conjuncts_fn));
//
//  // Replace all call sites with codegen version
//  int replaced = codegen->ReplaceCallSites(process__left_batchfn, eval_row_fn,
//      "EvalProbeRow");
//  DCHECK_EQ(replaced, 1);
//
//  replaced = codegen->ReplaceCallSites(process__left_batchfn, create_output_row_fn,
//      "create_output_row");
//  // Depends on _join_op
//  // TODO: switch statement
//  DCHECK(replaced == 1 || replaced == 2) << replaced;
//
//  replaced = codegen->ReplaceCallSites(process__left_batchfn, eval_conjuncts_fn,
//      "eval_conjuncts");
//  switch (_join_op) {
//    case TJoinOp::INNER_JOIN:
//    case TJoinOp::LEFT_SEMI_JOIN:
//    case TJoinOp::RIGHT_OUTER_JOIN:
//    case TJoinOp::RIGHT_SEMI_JOIN:
//      DCHECK_EQ(replaced, 1);
//      break;
//    case TJoinOp::LEFT_OUTER_JOIN:
//    case TJoinOp::FULL_OUTER_JOIN:
//      DCHECK_EQ(replaced, 2);
//      break;
//    case TJoinOp::LEFT_ANTI_JOIN:
//    case TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN:
//    case TJoinOp::RIGHT_ANTI_JOIN:
//      DCHECK_EQ(replaced, 0);
//      break;
//    default:
//      DCHECK(false);
//  }
//
//  replaced = codegen->ReplaceCallSites(process__left_batchfn, eval_other_conjuncts_fn,
//      "EvalOtherJoinConjuncts");
//  DCHECK_EQ(replaced, 1);
//
//  replaced = codegen->ReplaceCallSites(process__left_batchfn, probe_equals_fn, "Equals");
//  // Depends on _join_op
//  // TODO: switch statement
//  DCHECK(replaced == 1 || replaced == 2 || replaced == 3 || replaced == 4) << replaced;
//
//  // Replace hash-table parameters with constants.
//  PartitionedHashTableCtx::PartitionedHashTableReplacedConstants replaced_constants;
//  const bool stores_duplicates = true;
//  const int num_build_tuples = child(1)->row_desc().tuple_descriptors().size();
//  RETURN_IF_ERROR(ht_ctx_->ReplacePartitionedHashTableConstants(state, stores_duplicates,
//      num_build_tuples, process__left_batchfn, &replaced_constants));
//  DCHECK_GE(replaced_constants.stores_nulls, 1);
//  DCHECK_GE(replaced_constants.finds_some_nulls, 1);
//  DCHECK_GE(replaced_constants.stores_duplicates, 1);
//  DCHECK_GE(replaced_constants.stores_tuples, 1);
//  DCHECK_GE(replaced_constants.quadratic_probing, 1);
//
//  Function* process_build_batch_fn_level0 =
//      codegen->CloneFunction(process__left_batchfn);
//
//  // process_build_batch_fn_level0 uses CRC hash if available,
//  // process__left_batchfn uses murmur
//  replaced = codegen->ReplaceCallSites(process_build_batch_fn_level0, hash_fn,
//      "HashCurrentRow");
//  DCHECK_EQ(replaced, 1);
//
//  replaced = codegen->ReplaceCallSites(process__left_batchfn, murmur_hash_fn,
//      "HashCurrentRow");
//  DCHECK_EQ(replaced, 1);
//
//  // Finalize ProcessProbeBatch functions
//  process__left_batchfn = codegen->FinalizeFunction(process__left_batchfn);
//  if (process__left_batchfn == NULL) {
//    return Status("PartitionedHashJoinNode::CodegenProcessProbeBatch(): codegen'd "
//        "ProcessProbeBatch() function failed verification, see log");
//  }
//  process_build_batch_fn_level0 =
//      codegen->FinalizeFunction(process_build_batch_fn_level0);
//  if (process_build_batch_fn_level0 == NULL) {
//    return Status("PartitionedHashJoinNode::CodegenProcessProbeBatch(): codegen'd "
//        "level-zero ProcessProbeBatch() function failed verification, see log");
//  }
//
//  // Register native function pointers
//  codegen->AddFunctionToJit(process__left_batchfn,
//                            reinterpret_cast<void**>(&process_build_batch_fn_));
//  codegen->AddFunctionToJit(process_build_batch_fn_level0,
//                            reinterpret_cast<void**>(&process_build_batch_fn_level0_));
//  return Status::OK();
//}

//Status PartitionedHashJoinNode::CodegenInsertBatch(RuntimeState* state,
//    Function* hash_fn, Function* murmur_hash_fn, Function* eval_row_fn) {
//  LlvmCodeGen* codegen;
//  RETURN_IF_ERROR(state->GetCodegen(&codegen));
//
//  Function* insert_batch_fn = codegen->GetFunction(IRFunction::PHJ_INSERT_BATCH, true);
//  Function* build_equals_fn;
//  RETURN_IF_ERROR(ht_ctx_->CodegenEquals(state, true, &build_equals_fn));
//
//  // Replace the parameter 'prefetch_mode' with constant.
//  Value* prefetch_mode_arg = codegen->GetArgument(insert_batch_fn, 1);
//  TPrefetchMode::type prefetch_mode = state->query_options().prefetch_mode;
//  DCHECK_GE(prefetch_mode, TPrefetchMode::NONE);
//  DCHECK_LE(prefetch_mode, TPrefetchMode::HT_BUCKET);
//  prefetch_mode_arg->replaceAllUsesWith(
//      ConstantInt::get(Type::getInt32Ty(codegen->context()), prefetch_mode));
//
//  // Use codegen'd EvalBuildRow() function
//  int replaced = codegen->ReplaceCallSites(insert_batch_fn, eval_row_fn, "EvalBuildRow");
//  DCHECK_EQ(replaced, 1);
//
//  // Use codegen'd Equals() function
//  replaced = codegen->ReplaceCallSites(insert_batch_fn, build_equals_fn, "Equals");
//  DCHECK_EQ(replaced, 1);
//
//  // Replace hash-table parameters with constants.
//  PartitionedHashTableCtx::PartitionedHashTableReplacedConstants replaced_constants;
//  const bool stores_duplicates = true;
//  const int num_build_tuples = child(1)->row_desc().tuple_descriptors().size();
//  RETURN_IF_ERROR(ht_ctx_->ReplacePartitionedHashTableConstants(state, stores_duplicates,
//      num_build_tuples, insert_batch_fn, &replaced_constants));
//  DCHECK_GE(replaced_constants.stores_nulls, 1);
//  DCHECK_EQ(replaced_constants.finds_some_nulls, 0);
//  DCHECK_GE(replaced_constants.stores_duplicates, 1);
//  DCHECK_GE(replaced_constants.stores_tuples, 1);
//  DCHECK_GE(replaced_constants.quadratic_probing, 1);
//
//  Function* insert_batch_fn_level0 = codegen->CloneFunction(insert_batch_fn);
//
//  // Use codegen'd hash functions
//  replaced = codegen->ReplaceCallSites(insert_batch_fn_level0, hash_fn, "HashCurrentRow");
//  DCHECK_EQ(replaced, 1);
//  replaced = codegen->ReplaceCallSites(insert_batch_fn, murmur_hash_fn, "HashCurrentRow");
//  DCHECK_EQ(replaced, 1);
//
//  insert_batch_fn = codegen->FinalizeFunction(insert_batch_fn);
//  if (insert_batch_fn == NULL) {
//    return Status("PartitionedHashJoinNode::CodegenInsertBatch(): codegen'd "
//        "InsertBatch() function failed verification, see log");
//  }
//  insert_batch_fn_level0 = codegen->FinalizeFunction(insert_batch_fn_level0);
//  if (insert_batch_fn_level0 == NULL) {
//    return Status("PartitionedHashJoinNode::CodegenInsertBatch(): codegen'd zero-level "
//        "InsertBatch() function failed verification, see log");
//  }
//
//  codegen->AddFunctionToJit(insert_batch_fn, reinterpret_cast<void**>(&insert_batch_fn_));
//  codegen->AddFunctionToJit(insert_batch_fn_level0,
//      reinterpret_cast<void**>(&insert_batch_fn_level0_));
//  return Status::OK();s
//}
