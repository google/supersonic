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
// Family of CopyColumn functions.

#include "supersonic/base/infrastructure/copy_column.h"

#include <string.h>

#include <string>
using std::string;

#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/base/infrastructure/bit_pointers.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/base/infrastructure/types_infrastructure.h"
#include "supersonic/base/infrastructure/variant_pointer.h"

namespace supersonic {

// The naming convention for template functions in this file is that names of
// all bound dimensions are listed after the word 'Static' in function name.

class Arena;

namespace {

// Used to specialize column copiers for the is_null column.
enum IsNullCopierType {
  BOOL_NONE,                 // Both input and output are not nullable.
  BOOL_NONE_DCHECK,          // Nullable-to-not-nullable, with selector.
  BOOL_MEMSET,               // Input is not nullable; no selection vector.
  BOOL_MEMCPY,               // Copy w/o a selection vector.
  BOOL_LOOP_FILL_SELECTION,  // Input is not nullable; selector vector present.
  BOOL_LOOP_COPY_SELECTION   // Both nullable, and selection vector present.
};

enum DataCopierType {
  DATA_MEMCPY,                          // Simple types or shallow copy.
  DATA_LOOP_COPY,                       // Deep copy of variable length.
  DATA_LOOP_COPY_SKIP_VECTOR,           // As above; nullable output.
  DATA_LOOP_COPY_SELECTION,             // Selector vector present.
  DATA_LOOP_COPY_SELECTION_SKIP_VECTOR  // As above; nullable output.
};

// Used to specialize column copiers for the data column.
template<IsNullCopierType copier_type, bool deep_copy>
struct IsNullCopier;

template<bool deep_copy>
struct IsNullCopier<BOOL_NONE, deep_copy> {
  void operator()(const rowcount_t row_count,
                  const bool* source, bool* destination,
                  const rowid_t* selection) {}
};

template<bool deep_copy>
struct IsNullCopier<BOOL_NONE_DCHECK, deep_copy> {
  void operator()(const rowcount_t row_count,
                  const bool* source, bool* destination,
                  const rowid_t* selection) {
#ifndef NDEBUG
    if (selection != NULL) {
      for (int i = 0; i < row_count; ++i) { DCHECK(!source[selection[i]]); }
    } else {
      for (int i = 0; i < row_count; ++i) { DCHECK(!source[i]); }
    }
#endif
  }
};

template<bool deep_copy>
struct IsNullCopier<BOOL_MEMCPY, deep_copy> {
  void operator()(const rowcount_t row_count,
                  const bool* source, bool* destination,
                  const rowid_t* selection) {
    if (source != NULL) {
      memcpy(destination, source, row_count * sizeof(*destination));
    } else {
      // The contract allows NULL vector to be missing even if the schema is
      // NULLABLE. In that case, we do BOOL_MEMSET.
      memset(destination, '\0', row_count * sizeof(*destination));
    }
  }
};

template<bool deep_copy>
struct IsNullCopier<BOOL_MEMSET, deep_copy> {
  void operator()(const rowcount_t row_count,
                  const bool* source, bool* destination,
                  const rowid_t* selection) {
    memset(destination, '\0', row_count * sizeof(*destination));
  }
};

template<bool deep_copy>
struct IsNullCopier<BOOL_LOOP_COPY_SELECTION, deep_copy> {
  void operator()(const rowcount_t row_count,
                  const bool* source, bool* destination,
                  const rowid_t* selection) {
    if (source != NULL) {
      for (rowid_t i = 0; i < row_count; ++i) {
        destination[i] = (selection[i] < 0 || source[selection[i]]);
      }
    } else {
      // The contract allows NULL vector to be missing even if the schema is
      // NULLABLE. In that case, we do BOOL_LOOP_FILL_SELECTION.
      for (rowid_t i = 0; i < row_count; ++i) {
        destination[i] = (selection[i] < 0);
      }
    }
  }
};

template<bool deep_copy>
struct IsNullCopier<BOOL_LOOP_FILL_SELECTION, deep_copy> {
  void operator()(const rowcount_t row_count,
                  const bool* source, bool* destination,
                  const rowid_t* selection) {
    for (rowid_t i = 0; i < row_count; ++i) {
      destination[i] = (selection[i] < 0);
    }
  }
};

template<DataCopierType, DataType type, bool deep_copy>
struct DataCopier;

template<DataType type, bool deep_copy>
struct DataCopier<DATA_MEMCPY, type, deep_copy> {
  rowcount_t operator()(const rowcount_t row_count,
                        const typename TypeTraits<type>::cpp_type* source,
                        typename TypeTraits<type>::cpp_type* destination,
                        const rowid_t* selection,
                        const bool* skip_vector,
                        Arena* arena) {
    DCHECK(selection == NULL);
    DCHECK(!deep_copy || !TypeTraits<type>::is_variable_length);
    memcpy(destination, source, row_count * TypeTraits<type>::size);
    return row_count;
  }
};

template<DataType type, bool deep_copy>
struct DataCopier<DATA_LOOP_COPY, type, deep_copy> {
  rowcount_t operator()(
      const rowcount_t row_count,
      const typename TypeTraits<type>::cpp_type* source,
      typename TypeTraits<type>::cpp_type* destination,
      const rowid_t* selection,
      const bool* skip_vector,
      Arena* arena) {
    DCHECK(selection == NULL);
    DatumCopy<type, deep_copy> copier;
    for (int i = 0; i < row_count; ++i) {
      if (!copier(*source++, destination++, arena)) return i;
    }
    return row_count;
  }
};

template<DataType type, bool deep_copy>
struct DataCopier<DATA_LOOP_COPY_SKIP_VECTOR, type, deep_copy> {
  rowcount_t operator()(
      const rowcount_t row_count,
      const typename TypeTraits<type>::cpp_type* source,
      typename TypeTraits<type>::cpp_type* destination,
      const rowid_t* selection,
      const bool* skip_vector,
      Arena* arena) {
    DCHECK(selection == NULL);
    DatumCopy<type, deep_copy> copier;
    for (int i = 0; i < row_count; ++i) {
      if (!*skip_vector++) {
        if (!copier(*source, destination, arena)) return i;
      }
      ++source;
      ++destination;
    }
    return row_count;
  }
};

template<DataType type, bool deep_copy>
struct DataCopier<DATA_LOOP_COPY_SELECTION, type, deep_copy> {
  rowcount_t operator()(
      const rowcount_t row_count,
      const typename TypeTraits<type>::cpp_type* source,
      typename TypeTraits<type>::cpp_type* destination,
      const rowid_t* selection,
      const bool* skip_vector,
      Arena* arena) {
    DCHECK(selection != NULL);
    DatumCopy<type, deep_copy> copier;
    for (int i = 0; i < row_count; ++i) {
      if (selection[i] >= 0) {
        if (!copier(source[selection[i]], destination, arena)) return i;
      }
      ++destination;
    }
    return row_count;
  }
};

template<DataType type, bool deep_copy>
struct DataCopier<DATA_LOOP_COPY_SELECTION_SKIP_VECTOR, type, deep_copy> {
  rowcount_t operator()(
      const rowcount_t row_count,
      const typename TypeTraits<type>::cpp_type* source,
      typename TypeTraits<type>::cpp_type* destination,
      const rowid_t* selection,
      const bool* skip_vector,
      Arena* arena) {
    DCHECK(selection != NULL);
    DatumCopy<type, deep_copy> copier;
    for (int i = 0; i < row_count; ++i) {
      if (!*skip_vector++ && selection[i] >= 0) {
        if (!copier(source[selection[i]], destination, arena)) return i;
      }
      ++destination;
    }
    return row_count;
  }
};

// Helper; see below.
template<IsNullCopierType>
bool* GetDestinationIsNull(OwnedColumn* destination,
                           rowcount_t destination_offset) {
  return destination->mutable_is_null() + destination_offset;
}

// Specialication for non-nullable columns that avoids any arithmetics and
// avoids returning a toxic pointer.
template<>
bool* GetDestinationIsNull<BOOL_NONE>(OwnedColumn* destination,
                                      rowcount_t destination_offset) {
  return NULL;
}

// Puts together the copiers for data and is_null columns. Conforms to the
// ColumnCopier contract declared in copy_column.h.
// NOTE(user): when adding structure, we will probably want to separate these
// two into independent two functions, called via separate pointers.
template<DataType type,
         IsNullCopierType is_null_copier_type, DataCopierType data_copier_type,
         bool deep_copy>
rowcount_t ColumnCopierFn(const rowcount_t row_count,
                          const Column& source,
                          const rowid_t* selection,
                          const rowcount_t destination_offset,
                          OwnedColumn* const destination) {
  DCHECK_EQ(type, source.type_info().type());
  DCHECK(source.typed_data<type>() != NULL)
      << "Source data " << source.attribute().name()
      << " is NULL; can't copy";
  DCHECK(destination->mutable_typed_data<type>() != NULL)
      << "Output buffer for " << destination->content().attribute().name()
      << " is NULL; can't copy";

  IsNullCopier<is_null_copier_type, deep_copy> is_null_copier;
  DataCopier<data_copier_type, type, deep_copy> data_copier;

  bool* destination_is_null = GetDestinationIsNull<is_null_copier_type>(
      destination, destination_offset);
  is_null_copier(row_count, source.is_null(), destination_is_null, selection);
  return data_copier(
      row_count, source.data().as<type>(),
      destination->mutable_typed_data<type>() + destination_offset,
      selection, destination_is_null, destination->arena());
}

// Helper to resolve run-time options into template specializations.
struct CopyColumnResolver {
  CopyColumnResolver(Nullability input_nullability,
                     Nullability output_nullability,
                     RowSelectorType row_selector_type,
                     bool deep_copy)
      : input_nullability(input_nullability),
        output_nullability(output_nullability),
        row_selector_type(row_selector_type),
        deep_copy(deep_copy) {}

  template<DataType type>
  ColumnCopier operator()() const {
    switch (is_null_copier_type()) {
      case BOOL_NONE:
        return Resolve1<type, BOOL_NONE>();
      case BOOL_NONE_DCHECK:
        return Resolve1<type, BOOL_NONE_DCHECK>();
      case BOOL_MEMSET:
        return Resolve1<type, BOOL_MEMSET>();
      case BOOL_MEMCPY:
        return Resolve1<type, BOOL_MEMCPY>();
      case BOOL_LOOP_FILL_SELECTION:
        return Resolve1<type, BOOL_LOOP_FILL_SELECTION>();
      case BOOL_LOOP_COPY_SELECTION:
        return Resolve1<type, BOOL_LOOP_COPY_SELECTION>();
    }
    LOG(FATAL) << "Unhandled switch case";
  }

  template<DataType type, IsNullCopierType is_null_copier_type_p>
  ColumnCopier Resolve1() const {
    switch (data_copier_type<type>(deep_copy)) {
      case DATA_MEMCPY:
        return Resolve2<type, is_null_copier_type_p,
                        DATA_MEMCPY>();
      case DATA_LOOP_COPY:
        return Resolve2<type, is_null_copier_type_p,
                        DATA_LOOP_COPY>();
      case DATA_LOOP_COPY_SKIP_VECTOR:
        return Resolve2<type, is_null_copier_type_p,
                        DATA_LOOP_COPY_SKIP_VECTOR>();
      case DATA_LOOP_COPY_SELECTION:
        return Resolve2<type, is_null_copier_type_p,
                        DATA_LOOP_COPY_SELECTION>();
      case DATA_LOOP_COPY_SELECTION_SKIP_VECTOR:
        return Resolve2<type, is_null_copier_type_p,
                        DATA_LOOP_COPY_SELECTION_SKIP_VECTOR>();
    }
    LOG(FATAL) << "Unhandled case";
  }

  template<DataType type, IsNullCopierType is_null_copier_type_p,
           DataCopierType data_copier_type_p>
  ColumnCopier Resolve2() const {
    if (deep_copy) {
      return &ColumnCopierFn<type, is_null_copier_type_p, data_copier_type_p,
                             true>;
    } else {
      return &ColumnCopierFn<type, is_null_copier_type_p, data_copier_type_p,
                             false>;
    }
  }

  IsNullCopierType is_null_copier_type() const {
    // Determine the type of is_null copier.
    if (output_nullability == NOT_NULLABLE) {
      if (input_nullability == NULLABLE) {
        return BOOL_NONE_DCHECK;
      }
      return BOOL_NONE;
    } else {
      // output_nullability == NULLABLE.
      if (input_nullability == NOT_NULLABLE) {
        switch (row_selector_type) {
          case NO_SELECTOR: return BOOL_MEMSET;
          case INPUT_SELECTOR: return BOOL_LOOP_FILL_SELECTION;
        }
      } else {
        // output_nullability == NULLABLE, input_nullability == NULLABLE.
        switch (row_selector_type) {
          case NO_SELECTOR: return BOOL_MEMCPY;
          case INPUT_SELECTOR: return BOOL_LOOP_COPY_SELECTION;
        }
      }
    }
    LOG(FATAL) << "Unhandled case";
  }

  template<DataType type>
  DataCopierType data_copier_type(bool deep_copy) const {
    // Determine the type of data copier.
    switch (row_selector_type) {
      case NO_SELECTOR:
        if (!TypeTraits<type>::is_variable_length || !deep_copy) {
          // Ignore the skip vector if memcpy is otherwise feasible.
          return DATA_MEMCPY;
        } else {
          return output_nullability == NULLABLE
              ? DATA_LOOP_COPY_SKIP_VECTOR
              : DATA_LOOP_COPY;
        }
      case INPUT_SELECTOR:
        return output_nullability == NULLABLE
            ? DATA_LOOP_COPY_SELECTION_SKIP_VECTOR
            : DATA_LOOP_COPY_SELECTION;
    }
    LOG(FATAL) << "Unhandled case";
  }

  Nullability input_nullability;
  Nullability output_nullability;
  const RowSelectorType row_selector_type;
  bool deep_copy;
};

}  // namespace

ColumnCopier ResolveCopyColumnFunction(
    const DataType type,
    const Nullability input_nullability,
    const Nullability output_nullability,
    const RowSelectorType row_selector_type,
    bool deep_copy) {
  CopyColumnResolver resolver(input_nullability, output_nullability,
                              row_selector_type, deep_copy);
  return TypeSpecialization<ColumnCopier, CopyColumnResolver>(type, resolver);
}

}  // namespace supersonic
