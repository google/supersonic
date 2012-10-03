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
// Adapters for row-oriented algorithms. Ideally, long-term they won't be used.
// That said, there's a number of algorithms that are very natural in the
// row-oriented form: Union All, Merge Join, Hashed Group By.
//
//
// We define the template contract 'ConstRow', to be used by implementations
// of transformation algorithms. To conform to this contract, a class must
// provide the following:
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

// An adapter for reading a single row from a view. Conforms to the 'ConstRow'
// template contract (see above).
struct RowAdapter {
  // Creates an adapter representing offset-th row in the specified view.
  // The view is kept by reference; it must outlive the adapter.
  RowAdapter(const View& view, const int64 offset)
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

}  // namespace supersonic

#endif  // SUPERSONIC_CURSOR_INFRASTRUCTURE_ROW_H_
