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

#include "olap/memtable.h"

#include "common/logging.h"
#include "olap/row.h"
#include "olap/row_cursor.h"
#include "olap/rowset/column_data_writer.h"
#include "olap/rowset/rowset_writer.h"
#include "olap/schema.h"
#include "runtime/tuple.h"
#include "util/debug_util.h"
#include "util/doris_metrics.h"

namespace doris {

MemTable::MemTable(int64_t tablet_id, Schema* schema, const TabletSchema* tablet_schema,
                   const std::vector<SlotDescriptor*>* slot_descs, TupleDescriptor* tuple_desc,
                   KeysType keys_type, RowsetWriter* rowset_writer,
                   const std::shared_ptr<MemTracker>& parent_tracker)
        : _tablet_id(tablet_id),
          _schema(schema),
          _tablet_schema(tablet_schema),
          _tuple_desc(tuple_desc),
          _slot_descs(slot_descs),
          _keys_type(keys_type),
          _row_comparator(_schema, *_slot_descs),
          _mem_tracker(MemTracker::CreateTracker(-1, "MemTable", parent_tracker)),
          _table_mem_pool(new MemPool(_mem_tracker.get())),
          _schema_size(_schema->schema_size()),
          _skip_list(new Table(_row_comparator, _table_mem_pool.get(),
                               _keys_type == KeysType::DUP_KEYS)),
          _rowset_writer(rowset_writer) {
    // init the _need_init_slot_descs
    if (_slot_descs != nullptr && !_slot_descs->empty()) {
        for (int i = 0; i < _slot_descs->size(); ++i) {
            auto slot_type = (*_slot_descs)[i]->type().type;
            if (slot_type == TYPE_DECIMALV2 ||
                slot_type == TYPE_DECIMAL ||
                slot_type == TYPE_DATETIME ||
                slot_type == TYPE_DATE ||
                slot_type == TYPE_HLL ||
                slot_type == TYPE_OBJECT)
                _need_init_slot_descs.emplace_back(i, (*_slot_descs)[i]);
        }
    }
}

MemTable::~MemTable() {
    delete _skip_list;
}

MemTable::TupleComparator::TupleComparator(const Schema* schema, const std::vector<SlotDescriptor*>& slot_descs) :
    _schema(schema), _slot_descs(slot_descs) {}

int MemTable::TupleComparator::operator()(const char* left, const char* right) const {
    auto* left_tuple = (Tuple*) left;
    auto* right_tuple = (Tuple*) right;

    for (int i = 0; i < _schema->num_key_columns(); ++i) {
        auto res = _schema->column(i)->compare_cell(left_tuple, _slot_descs[i], right_tuple, _slot_descs[i]);
        if (res != 0) return res;
    }
    return 0;
}

void MemTable::insert(const Tuple* tuple) {
    bool overwritten = false;
    if (_keys_type == KeysType::DUP_KEYS) {
//        // Will insert directly, so use memory from _table_mem_pool
        auto* new_tuple = _init_copy_tuple_in_load(tuple);
        _skip_list->Insert((TableKey)new_tuple, &overwritten);
        DCHECK(!overwritten) << "Duplicate key model meet overwrite in SkipList";
        return;
    }

    // For non-DUP models, for the data rows passed from the upper layer, when copying the data,
    // we first allocate from _buffer_mem_pool, and then check whether it already exists in
    // _skiplist.  If it exists, we aggregate the new row into the row in skiplist.
    // otherwise, we need to copy it into _table_mem_pool before we can insert it.

    // if there is no _need_init_solt which means we can use tuple directly. otherwise,
    // we must init the tuple data before search it in the skip list
    tuple = _need_init_slot_descs.empty() ? tuple : _init_copy_tuple_in_load(tuple);
    bool is_exist = _skip_list->Find((TableKey)tuple, &_hint);
    if (is_exist) {
        _aggregate_two_tuple(tuple, (Tuple*)_hint.curr->key);
    } else {
        // if there is _need_init_slot_descs means tuple had init and copy, no need copy again
        auto* new_tuple = _need_init_slot_descs.empty() ? _init_copy_tuple_in_load(tuple) : tuple;
        _skip_list->InsertWithHint((TableKey)new_tuple, false, &_hint);
    }
}

void MemTable::_aggregate_two_tuple(const Tuple* tuple, Tuple* tuple_in_skiplist) {
    if (_tablet_schema->has_sequence_col()) {
        auto sequence_idx = _tablet_schema->sequence_col_idx();
        TupleRowCursorCell src_cell(const_cast<Tuple*>(tuple), (*_slot_descs)[sequence_idx]);
        TupleRowCursorCell dst_cell(tuple_in_skiplist, (*_slot_descs)[sequence_idx]);
        auto res = _schema->column(sequence_idx)->compare_cell(dst_cell, src_cell);
        // dst sequence column larger than src, don't need to update
        if (res > 0) {
            return;
        }
    }
    agg_update_tuple(_schema, tuple_in_skiplist, *_slot_descs, tuple, *_slot_descs, _table_mem_pool.get());
}

Tuple* MemTable::_init_copy_tuple_in_load(const Tuple *tuple) {
    auto* new_tuple = const_cast<Tuple*>(tuple)->deep_copy(*_tuple_desc, _table_mem_pool.get());
    init_tuple_in_load(new_tuple, _need_init_slot_descs, _schema,
            _table_mem_pool.get(), &_agg_object_pool);
    return new_tuple;
}

OLAPStatus MemTable::flush() {
    int64_t duration_ns = 0;
    {
        SCOPED_RAW_TIMER(&duration_ns);
        OLAPStatus st = _rowset_writer->flush_single_memtable(this, &_flush_size);
        if (st == OLAP_ERR_FUNC_NOT_IMPLEMENTED) {
            // For alpha rowset, we do not implement "flush_single_memtable".
            // Flush the memtable like the old way.
            Table::Iterator it(_skip_list);
            for (it.SeekToFirst(); it.Valid(); it.Next()) {
                const auto *tuple = (const Tuple *) it.key();
                agg_finalize_tuple(_schema, const_cast<Tuple *>(tuple), *_slot_descs, _table_mem_pool.get());
                RETURN_NOT_OK(_rowset_writer->add_row(tuple, *_slot_descs));
            }
            RETURN_NOT_OK(_rowset_writer->flush());
        } else {
            RETURN_NOT_OK(st);
        }
    }
    DorisMetrics::instance()->memtable_flush_total->increment(1);
    DorisMetrics::instance()->memtable_flush_duration_us->increment(duration_ns / 1000);
    return OLAP_SUCCESS;
}

OLAPStatus MemTable::close() {
    return flush();
}

MemTable::Iterator::Iterator(MemTable* memtable):
    _mem_table(memtable),
    _it(memtable->_skip_list) {
}

void MemTable::Iterator::seek_to_first() {
    _it.SeekToFirst();
}

bool MemTable::Iterator::valid() {
    return _it.Valid();
}

void MemTable::Iterator::next() {
    _it.Next();
}

Tuple* MemTable::Iterator::get_current_tuple() {
    auto tuple = (Tuple *) _it.key();
    agg_finalize_tuple(_mem_table->_schema, tuple, *_mem_table->_slot_descs,
            _mem_table->_table_mem_pool.get());
    return tuple;
}

} // namespace doris
