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
//
// Adapters for row-oriented algorithms and data formats.
//
// TODO(user): update the documentation once the contract is finished.
//
// We define the template contracts 'RowSource' and 'RowSink', as follows:
//
// (1) RowSource:
//
// const TupleSchema& schema() const;
// const TypeInfo& type_info(const size_t column_index) const;
// bool is_null(const size_t column_index) const;
// VariantConstPointer data(const size_t column_index) const;
//
// template<DataType type>
// const typename TypeTraits<type>::cpp_type* typed_data(
//     const size_t column_index) const;
//
// template<DataType type>
// const typename TypeTraits<type>::cpp_type& typed_notnull_data(
//     const size_t column_index) const;
//
//
// (1) RowSink:
//
// const TupleSchema& schema() const;
// const TypeInfo& type_info(const size_t column_index) const;
//
// void set_null(const size_t column_index);
// template<DataType type, bool nullable>
// void set_fixed_length_value(
//     const size_t column_index,
//     const typename TypeTraits<type>::cpp_type& value) const;
//
// template<DataType type, bool nullable, bool deep_copy>
// bool set_variable_length_value(
//     const size_t column_index,
//     const StringPiece& value) const;
//
// The adapters below implement this interface.

#ifndef SUPERSONIC_CURSOR_INFRASTRUCTURE_ROW_H_
#define SUPERSONIC_CURSOR_INFRASTRUCTURE_ROW_H_

#include <stddef.h>

#include "supersonic/utils/integral_types.h"
#include "supersonic/base/infrastructure/bit_pointers.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/base/infrastructure/variant_pointer.h"
#include "supersonic/proto/supersonic.pb.h"

namespace supersonic {

class TupleSchema;

// Default, stateless reader, conforming to the RowReader contract. Its
// parameter must conform to the RowSource contract.
template<typename Value>
struct DirectRowSourceReader {
  typedef Value ValueType;

  // Returns the singleton instance that can be used as a default reader.
  static const DirectRowSourceReader<Value>& Default() {
    static DirectRowSourceReader<Value> singleton;
    return singleton;
  }

  const TupleSchema& schema(const ValueType& value) const {
    return value.schema();
  }

  bool is_null(const ValueType& value,
               const size_t column_index) const {
    return value.is_null(column_index);
  }

  template<DataType type> typename TypeTraits<type>::cpp_type fetch(
      const ValueType& input,
      const size_t column_index) const {
    return input.template typed_notnull_data<type>(column_index);
  }

  template<DataType type, bool deep_copy, typename Allocator>
  bool fetch_variable_length_using_allocator(
      const ValueType& input,
      const size_t column_index,
      StringPiece* output,
      Allocator allocator) const {
    const StringPiece& data =
        input.template typed_notnull_data<type>(column_index);
    if (deep_copy) {
      void* buffer = allocator.Allocate(data.size());
      if (buffer == NULL) return false;
      memcpy(buffer, data.data(), data.size());
      output->set(buffer, data.size());
    } else {
      output->set(data.data(), data.size());
    }
    return true;
  }
};

// An adapter for reading a single row from a view. Conforms to the 'RowSource'
// template contract (see above).
struct RowSourceAdapter {
  // Creates an adapter representing offset-th row in the specified view.
  // The view is kept by reference; it must outlive the adapter.
  RowSourceAdapter(const View& view, const int64 offset)
      : view(view),
        offset(offset) {}

  const TupleSchema& schema() const { return view.schema(); }

  const TypeInfo& type_info(const size_t column_index) const {
    return view.column(column_index).type_info();
  }

  bool is_null(const size_t column_index) const {
    bool_const_ptr is_null = view.column(column_index).is_null();
    return (is_null != NULL) && is_null[offset];
  }

  VariantConstPointer data(const size_t column_index) const {
    return view.column(column_index).data_plus_offset(offset);
  }

  template<DataType type>
  const typename TypeTraits<type>::cpp_type* typed_data(
      const size_t column_index) const {
    return view.column(column_index).typed_data<type>() + offset;
  }

  template<DataType type>
  const typename TypeTraits<type>::cpp_type& typed_notnull_data(
      const size_t column_index) const {
    return view.column(column_index).typed_data<type>()[offset];
  }

  const View& view;
  const int64 offset;
};

// Default, stateless writer, conforming to the RowWriter contract. Its
// parameter must conform to the RowSink contract.
// Used, in particular, to write rows to Blocks, and append rows to Tables.
template<typename Value>
struct DirectRowSourceWriter {
  typedef Value ValueType;
  typedef typename ValueType::Allocator Allocator;

  static const DirectRowSourceWriter<Value>& Default() {
    static DirectRowSourceWriter<Value> singleton;
    return singleton;
  }

  const TupleSchema& schema(const ValueType* value) const {
    return value->schema();
  }

  void set_null(const size_t column_index, ValueType* output) const {
    output->set_null(column_index);
  }

  template<DataType type, bool nullable>
  void set_value(
      const size_t column_index,
      const typename TypeTraits<type>::cpp_type& value,
      ValueType* output) const {
    output->template set_value<type, nullable>(
        column_index, value);
  }

  // Returns the allocator for the variable-length content.
  Allocator allocator(const size_t column_index, ValueType* output) const {
    return output->allocator(column_index);
  }

  template<DataType type, bool nullable, bool deep_copy>
  bool set_variable_length_value(
      const size_t column_index,
      const StringPiece& value,
      ValueType* output) const {
    return output->template set_variable_length_value<type, nullable,
                                                      deep_copy>(
        column_index, value);
  }
};

// An adapter for writing a single row into a block. Conforms to the 'RowSink'
// template contract (see above).
struct RowSinkAdapter {
  RowSinkAdapter(Block* block, const int64 offset)
      : block(block),
        offset(offset) {}

  struct Allocator {
    explicit Allocator(Arena* arena) : arena(arena) {
      DCHECK(arena != NULL);
    }
    void* Allocate(size_t requested_size) const {
      return arena->AllocateBytes(requested_size);
    }
    Arena* const arena;
  };

  const TupleSchema& schema() const { return block->schema(); }

  const TypeInfo& type_info(const size_t column_index) const {
    return block->column(column_index).type_info();
  }

  void set_null(const size_t column_index) {
    block->mutable_column(column_index)->mutable_is_null()[offset] = true;
  }

  template<DataType type, bool nullable>
  void set_value(const size_t column_index,
                 const typename TypeTraits<type>::cpp_type& value) const {
    OwnedColumn* column = block->mutable_column(column_index);
    if (nullable) {
      column->mutable_is_null()[offset] = false;
    }
    column->mutable_typed_data<type>()[offset] = value;
  }

  Allocator allocator(size_t column_index) const {
    OwnedColumn* column = block->mutable_column(column_index);
    return Allocator(column->arena());
  }

  Block* block;
  const int64 offset;
};

}  // namespace supersonic

#endif  // SUPERSONIC_CURSOR_INFRASTRUCTURE_ROW_H_
