// Copyright 2010 Google Inc.  All Rights Reserved
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
//
// Basic data units.
// - Column      - points to the data.
// - OwnedColumn - owns the data.
// - View        - collection of columns.
// - Block       - collection of owned columns.

#ifndef SUPERSONIC_BASE_INFRASTRUCTURE_BLOCK_H_
#define SUPERSONIC_BASE_INFRASTRUCTURE_BLOCK_H_

#include <stddef.h>

#include <string>
using std::string;

#include "supersonic/utils/integral_types.h"
#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/macros.h"
#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/base/infrastructure/bit_pointers.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/base/infrastructure/variant_pointer.h"
#include "supersonic/base/memory/arena.h"
#include "supersonic/base/memory/memory.h"
#include "supersonic/proto/supersonic.pb.h"

class StringPiece;

namespace supersonic {

const size_t kMaxArenaBufferSize = 16 * 1024 * 1024;

// Immutable column content.
// Knows its type. Currently, does not know its length. (May be revisited).
// Exposes read-only pointers to the underlying data and nullability arrays,
// via data() and is_null() methods. Allows these pointers to be reset,
// via Reset() and ResetFrom() methods. Provides templatized methods for
// type-safe access, should the type be known at compile time.
class Column {
 public:
  // Returns the associated schema attribute.
  const Attribute& attribute() const {
    CheckInitialized();
    return *attribute_;
  }

  // Returns the (cached) type_info.
  const TypeInfo& type_info() const {
    CheckInitialized();
    return *type_info_;
  }

  // Returns an untyped pointer to the data.
  VariantConstPointer data() const { return data_; }

  // Returns an untyped pointer to the data, plus the specified offset.
  VariantConstPointer data_plus_offset(rowid_t offset) const {
    return data().offset(offset, type_info());
  }

  // Returns an untyped pointer to the data, plus the the statically known
  // offset. The complier changes the multiplication into a combination of
  // add&shift (avoiding slow imul). Therefore, this method should be favored
  // over data_plus_offset if the offset is known at compile time.
  template<int64 offset>
  VariantConstPointer data_plus_static_offset() const {
    return data().static_offset<offset>(type_info());
  }

  // Returns a pointer to typed data. (If you need to add an offset, you
  // can do it explicitly, since the result is statically typed).
  // Type-checks in debug mode.
  template<DataType type>
  const typename TypeTraits<type>::cpp_type* typed_data() const {
    DCHECK_EQ(type_info().type(), type)
        << "Type mismatch; trying to read " << type_info().name() << " as "
        << GetTypeInfo(type).name();
    return data().as<type>();
  }

  // Similar to typed_data(), but tolerates type mismatch as long as
  // sizeof(type) match. Can be used, for example, to cast signed to unsigned,
  // DATETIME to int64, etc.
  template<DataType type>
  const typename TypeTraits<type>::cpp_type* weakly_typed_data() const {
    DCHECK_EQ(type_info().size(), GetTypeInfo(type).size())
        << "Type size mismatch; trying to read " << type_info().name()
        << " of size " << type_info().size() << " as "
        << GetTypeInfo(type).name() << " of size " << GetTypeInfo(type).size();
    return data().as<type>();
  }

  // Returns a typed pointer to variable-length data (STRING or BINARY).
  const StringPiece* variable_length_data() const {
    DCHECK(type_info().is_variable_length())
        << "Type mismatch; trying to read " << type_info().name() << " as "
        << "variable length";
    return data().as_variable_length();
  }

  // Returns the is_null vector.
  // Returns NULL for columns that are not nullable. May return NULL for
  // a column that is nullable according to the schema, but happens to have
  // no NULLs in this particular view.
  bool_const_ptr is_null() const { return is_null_; }

  // A convenience method that returns the is_null vector shifted by the
  // specified offset, or NULL if the is_null vector == NULL.
  bool_const_ptr is_null_plus_offset(rowcount_t offset) const {
    return is_null() == NULL ? bool_const_ptr(NULL) : is_null() + offset;
  }

  // Updates the column to point to a new place.
  // Ownership of data and is_null stays with the callee.
  void Reset(VariantConstPointer data, bool_const_ptr is_null) {
    CheckInitialized();
    DCHECK(is_null == NULL || attribute().is_nullable())
        << "Attempt to use is_null vector for a non-nullable attribute "
        << "'" << attribute().name() << "'";
    data_ = data;
    is_null_ = is_null;
  }

  // Updates the column to point to a new place, the same as pointed to by the
  // specified column.
  void ResetFrom(const Column& other) {
    CheckInitialized();
    DCHECK_EQ(type_info().type(), other.type_info().type())
        << "Type mismatch; trying to reset " << type_info().name() << " from "
        << other.type_info().name();
    Reset(other.data(), other.is_null());
  }

  // Updates the column to point to a new place, as pointed to by the specified
  // column, plus the specified offset.
  void ResetFromPlusOffset(const Column& other, const rowcount_t offset) {
    CheckInitialized();
    DCHECK_EQ(type_info().type(), other.type_info().type())
        << "Type mismatch; trying to reset " << type_info().name() << " from "
        << other.type_info().name();
    Reset(other.data_plus_offset(offset), other.is_null_plus_offset(offset));
  }

  // Resets only the is_null vector. If the column is not_nullable, does
  // nothing, if it is nullable, resets is_null to the bit_ptr passed.
  // Used to insert the incoming selection vector as the is_null vector of the
  // resultant view in expressions.
  void ResetIsNull(bool_const_ptr is_null) {
    CheckInitialized();
    if (attribute().is_nullable()) is_null_ = is_null;
  }

 private:
  // Only the view to create an uninitialized Column.
  friend class View;
  Column() : attribute_(NULL), type_info_(NULL), data_(NULL), is_null_(NULL) {}

  // Must be called before use, if the no-arg constructor was used to create.
  // Ownership of the attribute remains with the caller.
  void Init(const Attribute* attribute) {
    CHECK(type_info_ == NULL) << "Column already initialized";
    attribute_ = attribute;
    type_info_ = &GetTypeInfo(attribute_->type());
  }

  void CheckInitialized() const {
    DCHECK(type_info_ != NULL) << "Column not initialized";
  }

  const Attribute* attribute_;
  const TypeInfo* type_info_;

  VariantConstPointer data_;
  bool_const_ptr is_null_;
  DISALLOW_COPY_AND_ASSIGN(Column);
};

// Container for column data (data, is_null and arena) used by Block (which owns
// its columns). Provides read/write access.
class OwnedColumn {
 public:
  // Called from the Block's reallocate.
  bool Reallocate(rowcount_t row_capacity, BufferAllocator* allocator);

  // Returns the read-only view on the data.
  const Column& content() const { return *column_; }

  // Returns an untyped pointer to the mutable owned data.
  void* mutable_data() { return data_buffer_->data(); }

  void* mutable_data_plus_offset(rowcount_t offset) {
    return static_cast<char*>(data_buffer_->data()) +
        (offset << column_->type_info().log2_size());
  }

  // Returns a typed pointer to the mutable owned data. Type-checks in
  // debug mode.
  template<DataType type>
  typename TypeTraits<type>::cpp_type* mutable_typed_data() {
    DCHECK_EQ(content().type_info().type(), type)
        << "Type mismatch; trying to write " << GetTypeInfo(type).name()
        << " as " << content().type_info().name();
    return static_cast<typename TypeTraits<type>::cpp_type*>(mutable_data());
  }

  // Like owned_typed_data(), but tolerates type mismatch as long as
  // sizeof(type) match. Can be used, for example, to cast signed to unsigned,
  // DATETIME to int64, etc.
  template<DataType type>
  typename TypeTraits<type>::cpp_type* mutable_weakly_typed_data() {
    DCHECK_EQ(content().type_info().size(), GetTypeInfo(type).size())
        << "Type size mismatch; trying to write "
        << GetTypeInfo(type).name() << " of size " << GetTypeInfo(type).size()
        << " as " << content().type_info().name()
        << " of size " << content().type_info().size();
    return static_cast<typename TypeTraits<type>::cpp_type*>(mutable_data());
  }

  // Returns a typed pointer to the mutable, owned variable-length data
  // (STRING or BINARY).
  StringPiece* mutable_variable_length_data() {
    DCHECK(content().type_info().is_variable_length())
        << "Type mismatch; trying to write variable-length data as "
        << content().type_info().name();
    return static_cast<StringPiece*>(mutable_data());
  }

  // Returns a pointer to the mutable owned is_null vector.
  // Returns NULL for columns that are not nullable according to the schema;
  // non-NULL otherwise.
  bool_ptr mutable_is_null() {
    return (is_nullable()) ? is_null_array_.mutable_data() : bool_ptr(NULL);
  }

  // Returns a pointer to the mutable owned is_null vector, shifted by the
  // specified offset.
  // Returns NULL for columns that are not nullable according to the schema;
  // non-NULL otherwise.
  bool_ptr mutable_is_null_plus_offset(rowcount_t offset) {
    return (is_nullable()) ? mutable_is_null() + offset : bool_ptr(NULL);
  }

  // Returns the arena used by this column, if it is variable-length.
  Arena* arena() { return arena_.get(); }

 private:
  // Only the block is to create an uninitialized owned column.
  friend class Block;
  OwnedColumn() : column_(NULL), data_buffer_(), is_null_array_(), arena_() {}

  // Called by the Block's constructor.
  void Init(BufferAllocator* allocator, Column* column);

  bool is_nullable() const { return content().attribute().is_nullable(); }

  // The immutable view. (We don't own it; the block - who owns us - does so).
  // The member field is set in Init() (invoked by the block), and then remains
  // constant.
  Column* column_;
  // Holds data.
  scoped_ptr<Buffer> data_buffer_;
  // Holds info about NULLs in column. Can be NULL for non-null columns.
  bool_array is_null_array_;
  // Holds variable length data. Null for other columns.
  scoped_ptr<Arena> arena_;

  DISALLOW_COPY_AND_ASSIGN(OwnedColumn);
};

// Read-only view of a multi-column 'block' of data. The data is usually owned
// by one or more Blocks (below).
class View {
 public:
  // Creates an empty view that can hold data of the specified schema.
  explicit View(const TupleSchema& schema)
      : schema_(schema),
        columns_(new Column[schema.attribute_count()]),
        row_count_(0) {
    Init();
  }

  // A convenience copy constructor.
  View(const View& other)
      : schema_(other.schema()),
        columns_(new Column[other.schema().attribute_count()]),
        row_count_(0) {
    Init();
    ResetFrom(other);
  }

  // A convenience 'subrange' copy constructor.
  explicit View(const View& other, rowcount_t offset, rowcount_t row_count)
      : schema_(other.schema()),
        columns_(new Column[other.schema().attribute_count()]),
        row_count_(0) {
    Init();
    ResetFromSubRange(other, offset, row_count);
  }

  // A convenience constructor, wrapping a single column into a view.
  View(const Column& column, rowcount_t row_count)
      : schema_(TupleSchema::Singleton(column.attribute().name(),
                                       column.attribute().type(),
                                       column.attribute().nullability())),
        columns_(new Column[1]),
        row_count_(row_count) {
    Init();
    mutable_column(0)->ResetFrom(column);
  }

  // Returns the view's schema.
  const TupleSchema& schema() const { return schema_; }

  // Equivalent to schema().attribute_count().
  int column_count() const { return schema().attribute_count(); }

  // Returns the number of rows in the view.
  rowcount_t row_count() const { return row_count_; }

  // Sets the row count. Unchecked.
  void set_row_count(const rowcount_t row_count) { row_count_ = row_count; }

  // Returns an immutable reference to the specified column.
  const Column& column(int column_index) const {
    DCHECK_GE(column_index, 0);
    DCHECK_LT(column_index, column_count());
    return columns_[column_index];
  }

  // Returns a pointer to the mutable (resettable) column.
  Column* mutable_column(int column_index) {
    DCHECK_GE(column_index, 0);
    DCHECK_LT(column_index, column_count());
    return &columns_[column_index];
  }

  // Resets View's columns from another View. Sets the row_count as well.
  void ResetFrom(const View& other) {
    for (int i = 0; i < schema_.attribute_count(); ++i) {
      mutable_column(i)->ResetFrom(other.column(i));
    }
    set_row_count(other.row_count());
  }

  // Resets View's columns from a sub-range in an another View. Sets the
  // row_count as well.
  void ResetFromSubRange(const View& other,
                         rowcount_t offset,
                         rowcount_t row_count) {
    DCHECK_LE(offset, other.row_count());
    DCHECK_LE(offset + row_count, other.row_count());
    for (int i = 0; i < column_count(); ++i) {
      mutable_column(i)->ResetFromPlusOffset(other.column(i), offset);
    }
    set_row_count(row_count);
  }

  // Advances the view by the specified offset. Reduces view size accordingly.
  // In debug mode, guards against moving past the range.
  // Note: views can only more forward; the offset is unsigned.
  void Advance(rowcount_t offset) {
    DCHECK_LE(offset, row_count());
    for (int i = 0; i < column_count(); i++) {
      Column* column = mutable_column(i);
      column->ResetFromPlusOffset(*column, offset);
    }
    row_count_ -= (row_count_ < offset ? row_count_ : offset);
  }

 private:
  void Init() {
    for (int i = 0; i < schema_.attribute_count(); i++) {
      columns_[i].Init(&schema_.attribute(i));
    }
  }

  // Intentionally left undefined, to guard against accidental passing of
  // views by value. (Views should be passed via const reference). If you need
  // to copy a view from another view, use ResetFrom(...).
  View& operator=(const View& other);

  const TupleSchema schema_;
  scoped_array<Column> columns_;
  rowcount_t row_count_;
  // Views are copyable.
};

// Represents a collection of column stripes materialized in memory.
// Owns columns. Maintains a reference to a memory allocator, so that it can
// be reallocated as needed.
// Block is a 'dumb' memory buffer, somewhat like a piece of memory allocated
// with malloc. It has 'row capacity' but no 'row count'; it doesn't know
// which rows have been initialized. If you need a higher-level abstraction,
// consider the Table - a dynamically appendable container analogous to the STL
// vector.
class Block {
 public:
  // Creates a new block with zero capacity, that can hold data that conforms to
  // the specified schema.
  Block(const TupleSchema& schema, BufferAllocator* allocator)
      : allocator_(allocator),
        columns_(new OwnedColumn[schema.attribute_count()]),
        view_(schema) {
    for (int i = 0; i < schema.attribute_count(); i++) {
      columns_[i].Init(allocator, view_.mutable_column(i));
    }
  }

  // Reallocates row_capacity rows for all columns, possibly including
  // per-column is_null vectors. Arenas for variable-sized columns aren't
  // reallocated or truncated. Returns true iff all allocations are successful.
  // Can be called multiple times.
  // If failed, some of the buffers might have been reallocated. The block's
  // row_capacity is not increased. It might be decreased, if the new capacity
  // is lower than the previous capacity. The block remains in a consistent
  // state, i.e. it is safe to read from it up to its row_capacity, or to
  // attempt to Reallocate again.
  bool Reallocate(rowcount_t new_row_capacity);

  // Erases arenas of all variable length columns. Invalidates the data stored
  // in these columns, if any.
  void ResetArenas() {
    for (int i = 0; i < schema().attribute_count(); i++) {
      if (mutable_column(i)->arena() != NULL) {
        mutable_column(i)->arena()->Reset();
      }
    }
  }

  // Exposes a column for mutation.
  OwnedColumn* mutable_column(int column_index) {
    return &columns_[column_index];
  }

  // Returns the read-only view on the entire content of this block. Enables
  // use of blocks in APIs that accept views.
  const View& view() const { return view_; }

  // Returns the allocator used to allocate storage for this block's data.
  BufferAllocator* allocator() { return allocator_; }

  // Convenience accessors, delegating to the methods defined above.

  // Returns the block's schema. Equivalent to view().schema().
  const TupleSchema& schema() const { return view().schema(); }

  // Equivalent to schema().attribute_count().
  const int column_count() const { return schema().attribute_count(); }

  // Returns the row capacity of the block. Equivalent to view().row_count().
  const rowcount_t row_capacity() const { return view().row_count(); }

  // Returns an immutable reference to the specified view's column.
  const Column& column(size_t column_index) const {
    return view().column(column_index);
  }

  // Returns an immutable pointer to the is_null vector, if present.
  // Equivalent to column(column_index).is_null().
  bool_const_ptr is_null(size_t column_index) const {
    return column(column_index).is_null();
  }

 private:
  void set_row_capacity(rowcount_t row_capacity) {
    view_.set_row_count(row_capacity);
  }

  BufferAllocator* const allocator_;
  scoped_array<OwnedColumn> columns_;
  View view_;  // Full view on the entire block.
  DISALLOW_COPY_AND_ASSIGN(Block);
};

}  // namespace supersonic

#endif  // SUPERSONIC_BASE_INFRASTRUCTURE_BLOCK_H_
