// Copyright 2010 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#ifndef SUPERSONIC_CURSOR_INFRASTRUCTURE_TABLE_H_
#define SUPERSONIC_CURSOR_INFRASTRUCTURE_TABLE_H_

#include "supersonic/utils/integral_types.h"
#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/macros.h"
#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/utils/exception/failureor.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/bit_pointers.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/base/infrastructure/types_infrastructure.h"
#include "supersonic/base/infrastructure/view_copier.h"
#include "supersonic/cursor/infrastructure/basic_operation.h"
#include "supersonic/cursor/infrastructure/row.h"
#include "supersonic/cursor/infrastructure/row_copier.h"
#include "supersonic/cursor/infrastructure/writer.h"
#include "supersonic/proto/supersonic.pb.h"
#include "supersonic/utils/strings/stringpiece.h"

namespace supersonic {

class BufferAllocator;
class Cursor;

// In-memory data materialization. Contains a block of data, exposes APIs that
// allow appending content to it, and provides the Operation interface on top
// of it (i.e. it supports creating cursors that iterate over the data).
class Table : public BasicOperation {
 public:
  // Creates an empty table with the specified schema.
  // Buffer allocator is used to (re)allocate internal block.
  Table(const TupleSchema& schema, BufferAllocator* buffer_allocator);

  // Creates a table encapsulating the specified, existing block. Takes
  // ownership of the block.
  explicit Table(Block* block);

  virtual ~Table();

  // Removes all data from this table. Does not decrease the capacity.
  void Clear();

  // Reallocates the internal block to the specified row capacity. The new
  // capacity must be equal to or larger than my_view().row_count(). Returns
  // true on success; false if reallocation fails with OOM. Upon failure, the
  // table is left in an consistent state, but if the requested capacity is
  // lower than the current capacity, the capacity might get decreased (because
  // realloc might have partially succeeded). Invalidates all cursors created
  // by CreateCursor().
  bool SetRowCapacity(rowcount_t row_capacity);

  // Grows the table, if necessary, to have capacity for at least
  // 'needed_capacity' rows. Analogous to vector::reserve(). Returns true
  // on success, i.e. if the reallocation succeeded or the table already had
  // sufficient capacity; false on OOM.
  // To avert quadratic complexity of successive 'reserve's, the array is
  // enlarged at least 2x when it needs reallocation.
  bool ReserveRowCapacity(rowcount_t needed_capacity);

  // Sets the capacity of this table to its current size.
  // Equivalent to SetRowCapacity(row_count()).
  bool Compact() { return Reallocate(row_count()); }

  // Makes this table a copy of the specified table. The specified table must
  // have the same schema as this table. Returns true if the entire specified
  // table has been successfully copied; false otherwise.
  bool CopyFrom(const Table& other) {
    Clear();
    return AppendView(other.view()) == other.view().row_count();
  }

  // Creates a cursor over this table's data.
  virtual FailureOrOwned<Cursor> CreateCursor() const;

  // Appends the content of the specified view, performing a deep copy of
  // variable-length columns. The view must have a compatible schema (or
  // it will crash). Invalidates all opened cursors. Returns the number
  // of rows successfully copied, which can be less than view.row_count() iff
  // OOM occurs.
  rowcount_t AppendView(const View& view);

  // Appends a new row at the end of the table. On success, returns the index
  // of the newly added row. On failure (due to OOM), returns -1. If failed,
  // the table is not mutated. Regardless of the successfulness, invalidates
  // all opened cursors. The content of the added row is undefined. It must be
  // initialized by calling SetValue and SetNull on all the columns.
  rowid_t AddRow();

  // Sets datum at (col_index, row_index) to the specified value. Overwrites
  // the previous value (or NULL). May fail if type is variable-length, and
  // it fails to allocate storage to copy the underlying value. Returns true
  // on success; false on failure. If failed, the previous value is preserved.
  //
  // For variable-length types, the memory occupied by the previous value is
  // NOT immediately freed (it remains in the column's arena until the table
  // is destroyed); hence, repetitive calls of this function for the same
  // (col_index, row_index) may cause memory leak.
  template <DataType type>
  bool Set(int col_index,
           rowid_t row_index,
           const typename TypeTraits<type>::cpp_type& value);

  // Sets datum at (col_index, row_index) to NULL. The column must be nullable.
  // If column type is variable-length, the storage occupied by previous value,
  // if any, is NOT immediately freed (it remains in the column's arena until
  // the table is destroyed).
  void SetNull(int col_index, rowid_t row_index);

  // Returns the current row capacity. Always greater than or equal to the
  // current row count.
  rowcount_t row_capacity() const { return block_->row_capacity(); }

  // Returns the view representing the full content of this table.
  const View& view() const { return view_; }

  // Returns the schema of this table. Equivalent to view().schema().
  const TupleSchema& schema() const { return view().schema(); }

  // Returns the number of rows in this table. Equivalent to
  // view().row_count().
  const rowcount_t row_count() const { return view().row_count(); }

  // Returns (and removes) the internal content of this table. Clears the table.
  // NOTE: the number of rows in the block will be equal to row_capacity()
  // (not row_count()), so you may want to Compact() the table before calling
  // this method.
  Block* extract_block() {
    scoped_ptr<Block> swapped(new Block(block_->schema(),
                                        block_->allocator()));
    swapped.swap(block_);
    Clear();
    return swapped.release();
  }

 private:
  template<typename RowReader> friend class TableRowAppender;

  bool Reallocate(rowcount_t new_capacity) {
    bool result = block_->Reallocate(new_capacity);
    view_.ResetFromSubRange(block_->view(), 0, row_count());
    return result;
  }

  // For use by TableRowAppender.
  Block* block() { return block_.get(); }

  scoped_ptr<Block> block_;
  View view_;
  ViewCopier view_copier_;
  DISALLOW_COPY_AND_ASSIGN(Table);
};

template<typename RowReader>
class TableRowAppender {
 public:
  // Creates a new appender for the specified table. Normally, deep_copy
  // should be true, unless it's guaranteed that all variable-length attributes
  // (strings, binary) added to the table via this appender will remain valid
  // for the lifetime of the table.
  TableRowAppender(
      Table* table,
      bool deep_copy,
      const RowReader& reader = RowReader::Default())
      : table_(table),
        reader_(reader),
        copier_(table->schema(), deep_copy) {}
  bool AppendRow(const typename RowReader::ValueType& row) {
    int64 row_id = table_->AddRow();
    if (row_id < 0) return false;
    DirectRowSourceWriter<RowSinkAdapter> writer;
    RowSinkAdapter sink(table_->block(), row_id);
    return copier_.Copy(reader_, row, writer, &sink);
  }
 private:
  Table* table_;
  const RowReader& reader_;
  RowCopier<RowReader, DirectRowSourceWriter<RowSinkAdapter> > copier_;
};

// A convenience class to fill a table programatically, row-by-row,
// element-by-element. The usage pattern is:
//
// Table table(...);
// TableRowWriter(&table)
//     .AddRow().Int32(1).String("a")
//     .AddRow().Int32(3).String("b")
//     .AddRow().AllFurtherNull()
//     .AddRow().Int32(3).Null()
//     .AddRow().Null().Null()
//     .CheckSuccess();
class TableRowWriter {
 public:
  // Creates a row builder that can append rows to the specified table.
  explicit TableRowWriter(Table* table)
      : table_(table),
        col_index_(table_->schema().attribute_count()),
        row_index_(-1),
        failed_(false) {}

  // Adds a new row to the table, reallocating it as necessary. If reallocation
  // fails, sets the success() status to false, causing all future calls on
  // this row builder to be ignored.
  // The newly added row needs to be filled by repeatedly calling Set*
  // functions initializing consecutive columns. All columns must be set
  // before calling another AddRow.
  TableRowWriter& AddRow() {
    if (success()) {
      DCHECK_EQ(table_->schema().attribute_count(), col_index_)
          << "Can't add row until the previously added row is completely built";
      row_index_ = table_->AddRow();
      failed_ = (row_index_ < 0);
      col_index_ = 0;
    }
    return *this;
  }

  TableRowWriter& Int32(int32 value)    { return Set<INT32>(value); }
  TableRowWriter& Int64(int64 value)    { return Set<INT64>(value); }
  TableRowWriter& Uint32(uint32 value)  { return Set<UINT32>(value); }
  TableRowWriter& Uint64(uint64 value)  { return Set<UINT64>(value); }
  TableRowWriter& Float(float value)    { return Set<FLOAT>(value); }
  TableRowWriter& Double(double value)  { return Set<DOUBLE>(value); }
  TableRowWriter& Bool(bool value)      { return Set<BOOL>(value); }
  TableRowWriter& Date(int32 value)     { return Set<DATE>(value); }
  TableRowWriter& Datetime(int64 value) { return Set<DATETIME>(value); }

  TableRowWriter& String(const StringPiece& value) {
    return Set<STRING>(value);
  }
  TableRowWriter& Binary(const StringPiece& value) {
    return Set<BINARY>(value);
  }

  // Sets a subsequent column to NULL. The column must be nullable.
  TableRowWriter& Null() {
    if (success()) table_->SetNull(col_index_++, row_index_);
    return *this;
  }

  // Sets all remaining columns to NULL. All of them must be nullable.
  TableRowWriter& AllFurtherNull() {
    if (success()) {
      while (col_index_ < table_->schema().attribute_count()) {
        table_->SetNull(col_index_++, row_index_);
      }
    }
    return *this;
  }

  // Sets a subsequent column to the specified value. The column must be of
  // the specified type. If OOM occurs, sets the success() status to false,
  // causing all subsquent calls on this row builder to be ignored.
  template <DataType type>
  TableRowWriter& Set(const typename TypeTraits<type>::cpp_type& value) {
    if (success()) {
      failed_ = !table_->Set<type>(col_index_++, row_index_, value);
    }
    return *this;
  }

  // Returns true if all rows have been successfully written; false otherwise.
  bool success() const { return !failed_; }

  // Crashes if unsuccessful so far.
  void CheckSuccess() const {
    CHECK(success())
        <<  "Writer failed at row " << row_index_ << ", column " << col_index_;
  }

  const TupleSchema& schema() const { return table_->schema(); }

 private:
  Table* table_;
  int col_index_;
  rowid_t row_index_;
  bool failed_;
};

class TableSink : public Sink {
 public:
  // Does NOT take ownership of the table.
  explicit TableSink(Table* table) : table_(table) {}
  virtual FailureOr<rowcount_t> Write(const View& data) {
    return Success(table_->AppendView(data));
  }
  virtual FailureOrVoid Finalize() { return Success(); }
 private:
  Table* table_;
};

// Convenience function to write a cursor into a table.
FailureOrOwned<Table> MaterializeTable(BufferAllocator* allocator,
                                       Cursor* cursor);

// Inline and template functions.

template <DataType type>
bool Table::Set(int col_index,
                rowid_t row_index,
                const typename TypeTraits<type>::cpp_type& value) {
  DCHECK_LT(row_index, row_count());
  DCHECK_GE(row_index, 0);
  DCHECK_LT(col_index, schema().attribute_count());
  DCHECK_EQ(schema().attribute(col_index).type(), type);
  DatumCopy<type, true> copy;
  OwnedColumn* column = block_->mutable_column(col_index);
  if (!copy(value,
            &column->mutable_typed_data<type>()[row_index],
            column->arena())) {
    return false;
  }
  bool_ptr is_null = block_->mutable_column(col_index)->
      mutable_is_null_plus_offset(row_index);
  if (is_null != NULL) { *is_null = false; }
  return true;
}

}  // namespace supersonic

#endif  // SUPERSONIC_CURSOR_INFRASTRUCTURE_TABLE_H_
