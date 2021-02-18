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

#pragma once

#include "runtime/tuple.h"
#include "runtime/descriptors.h"

namespace doris {

struct RowCursorCell {
    RowCursorCell(void* ptr) : _ptr(ptr) {}
    RowCursorCell(const void* ptr) : _ptr((void*)ptr) {}
    virtual bool is_null() const { return *reinterpret_cast<bool*>(_ptr); }
    virtual void set_is_null(bool is_null) const { *reinterpret_cast<bool*>(_ptr) = is_null; }
    virtual void set_null() const { *reinterpret_cast<bool*>(_ptr) = true; }
    virtual void set_not_null() const { *reinterpret_cast<bool*>(_ptr) = false; }
    virtual const void* cell_ptr() const { return (char*)_ptr + 1; }
    virtual void* mutable_cell_ptr() const { return (char*)_ptr + 1; }

private:
    void* _ptr;
};

// A wrapper class for Tuple to simulate Tuple as RowCursorCell
struct TupleRowCursorCell : public RowCursorCell {
public:
    TupleRowCursorCell(const Tuple* tuple, SlotDescriptor* slot_desc) :
        TupleRowCursorCell(const_cast<Tuple*>(tuple), slot_desc) {}
    TupleRowCursorCell(Tuple* tuple, SlotDescriptor* slot_desc) : RowCursorCell(this), _tuple(tuple), _slot_desc(slot_desc) {}

    bool is_null() const override { return _tuple->is_null(_slot_desc->null_indicator_offset()); }
    void set_is_null(bool is_null) const override {
        is_null ? _tuple->set_null(_slot_desc->null_indicator_offset()) :
            _tuple->set_not_null(_slot_desc->null_indicator_offset());
    }
    void set_null() const override { set_is_null(true); }
    void set_not_null() const override { set_is_null(false); }
    const void* cell_ptr() const override { return _tuple->get_slot(_slot_desc->tuple_offset());}
    void* mutable_cell_ptr() const override { return _tuple->get_slot(_slot_desc->tuple_offset());}
private:
    Tuple* _tuple;
    SlotDescriptor* _slot_desc;
};

} // namespace doris
