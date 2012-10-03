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

// Helper functions to resolve copying effectively depending on the nullability
// and selectivity of input and output.

// This simply copies all the data (with appropriate selectivity). Used when
// we disregard the null columns when copying the data:
template <DataType type,
          RowSelectorType row_selector_type, bool deep_copy>
inline rowcount_t CopyDataInLoop(
    const rowcount_t row_count,
    const typename TypeTraits<type>::cpp_type* input_data,
    typename TypeTraits<type>::cpp_type* output_data,
    const rowid_t* selector_row_ids,
    Arena* arena) {
  DatumCopy<type, deep_copy> copier;
  for (rowcount_t n = 0; n < row_count; ++n) {
    const rowid_t nth_input_row_id =
        (row_selector_type & INPUT_SELECTOR_BIT) ? selector_row_ids[n] : n;
    const rowid_t nth_output_row_id =
        (row_selector_type & OUTPUT_SELECTOR_BIT) ? selector_row_ids[n] : n;
    if (!copier(input_data[nth_input_row_id],
                &output_data[nth_output_row_id],
                arena))
      return n;
  }
  return row_count;
}

// This simply copies all the data (with appropriate selectivity). Used when
// we disregard the null columns when copying the data:
template <DataType type,
          RowSelectorType row_selector_type, bool deep_copy>
inline rowcount_t CopyDataAndSetNullToFalseInLoop(
    const rowcount_t row_count,
    const typename TypeTraits<type>::cpp_type* input_data,
    typename TypeTraits<type>::cpp_type* output_data,
    const rowid_t* selector_row_ids,
    Arena* arena,
    bool_ptr output_is_null) {
  DatumCopy<type, deep_copy> copier;
  for (rowcount_t n = 0; n < row_count; ++n) {
    const rowid_t nth_input_row_id =
        (row_selector_type & INPUT_SELECTOR_BIT) ? selector_row_ids[n] : n;
    const rowid_t nth_output_row_id =
        (row_selector_type & OUTPUT_SELECTOR_BIT) ? selector_row_ids[n] : n;
    // We use &= instead of = because it's faster for bit_ptrs, and in this
    // case equivalent.
    output_is_null[nth_output_row_id] &= false;
    if (!copier(input_data[nth_input_row_id],
                &output_data[nth_output_row_id],
                arena))
      return n;
  }
  return row_count;
}

// Helper, to copy data with statically resolved both input and output
// nullability. Its purpose is to factor input nullability checks out of
// the loop from CopyColumnWithForLoopStaticTypeNullability.
template <DataType type,
          Nullability input_nullability,
          Nullability output_nullability,
          RowSelectorType row_selector_type, bool deep_copy>
inline rowcount_t CopyColumnInternal(const rowcount_t row_count,
                                     const Column& input,
                                     const rowid_t* selector_row_ids,
                                     const rowcount_t offset,
                                     OwnedColumn* const output) {
  typedef typename TypeTraits<type>::cpp_type cpp_type;
  DCHECK(input.typed_data<type>() != NULL)
      << "Input data " << input.attribute().name() << " is NULL; can't copy";
  DCHECK(output->mutable_typed_data<type>() != NULL)
      << "Output buffer for " << output->content().attribute().name()
      << " is NULL; can't copy";

  const cpp_type* const input_data = input.typed_data<type>() +
      (row_selector_type == OUTPUT_SELECTOR ? offset : 0);
  cpp_type* const output_data = output->mutable_typed_data<type>() +
      (row_selector_type != OUTPUT_SELECTOR ? offset : 0);
  bool_const_ptr input_is_null = (row_selector_type == OUTPUT_SELECTOR)
      ? input.is_null_plus_offset(offset)
      : input.is_null();
  // output_is_null is only used in the specialization with
  // output_nullability == NULLABLE.
  bool_ptr output_is_null = (row_selector_type != OUTPUT_SELECTOR)
      ? output->mutable_is_null_plus_offset(offset)
      : output->mutable_is_null();

  // Both the input and output are not nullable, we just copy the data.
  // Note - the option when we could memcpy the data (no selectors, shallow
  // copy) was already explored, and has been dealt with separately. Here it
  // does not apply, so we simply copy in a loop.
  if (input_nullability == NOT_NULLABLE && output_nullability == NOT_NULLABLE) {
    return CopyDataInLoop<type, row_selector_type, deep_copy>(
        row_count, input_data, output_data, selector_row_ids, output->arena());
  }
  // The output is nullable, but the input is null, so we have to fill the
  // output_is_null column with false. If there is no output selector, we can do
  // this with a single FillWithFalse call, and then copy the data.
  // We can do this only if the input_selector_bit is not set - if it is set,
  // it's possible that the caller wants to set additional nulls by setting
  // minus ones in the selection vector, and then we do not want to copy the
  // data.
  if (input_nullability == NOT_NULLABLE && output_nullability == NULLABLE &&
      (row_selector_type & INPUT_SELECTOR_BIT) == 0 &&
      (row_selector_type & OUTPUT_SELECTOR_BIT) == 0) {
    bit_pointer::FillWithFalse(output_is_null, row_count);
    return CopyDataInLoop<type, row_selector_type, deep_copy>(
        row_count, input_data, output_data, selector_row_ids, output->arena());
  }
  // The same situation, but the output is equipped with a selector. In this
  // case we have to fill the bit values one by one.
  // In this case we can't use minus ones to pass extra nulls (as they would
  // be interpreted as row_ids for the output), so we do not have to avoid the
  // case of the INPUT_SELECTOR_BIT being set.
  if (input_nullability == NOT_NULLABLE && output_nullability == NULLABLE &&
      (row_selector_type & OUTPUT_SELECTOR_BIT)) {
    return CopyDataAndSetNullToFalseInLoop<type, row_selector_type, deep_copy>(
        row_count, input_data, output_data, selector_row_ids, output->arena(),
        output_is_null);
  }

  // The cases left are that either the input is NULLABLE or we have input
  // NOT_NULLABLE, output NULLABLE, input_selection TRUE and output_selection
  // FALSE.
  DCHECK(input_nullability == NULLABLE ||
         (input_nullability == NOT_NULLABLE && output_nullability == NULLABLE &&
          row_selector_type == INPUT_SELECTOR));
  for (rowcount_t n = 0; n < row_count; ++n) {
    const rowid_t nth_input_row_id =
        (row_selector_type & INPUT_SELECTOR_BIT) ? selector_row_ids[n] : n;
    const rowid_t nth_output_row_id =
        (row_selector_type & OUTPUT_SELECTOR_BIT) ? selector_row_ids[n] : n;
    // Copy is_null information.
    if (output_nullability == NULLABLE) {
      // Be careful not to use a negative index for input_is_null[]. Negative
      // nth_input_row_id means that we should put NULL in the corresponding
      // output column cell.
      const bool is_null =
          ((row_selector_type & INPUT_SELECTOR_BIT) && nth_input_row_id < 0) ||
          (input_nullability == NULLABLE && input_is_null != NULL &&
           input_is_null[nth_input_row_id]);
      output_is_null[nth_output_row_id] = is_null;
      if (is_null) continue;
    }
    // Copy data.
    DatumCopy<type, deep_copy> copier;
    if (!copier(input_data[nth_input_row_id],
                &output_data[nth_output_row_id],
                output->arena()))
      return n;
  }
  return row_count;
}

}  // namespace.

// The generic for loop-based variant of the core copying function.
// Returns the number of rows copied successfully.
template <DataType type, Nullability output_nullability,
          RowSelectorType row_selector_type, bool deep_copy>
rowcount_t CopyColumnWithForLoopStaticTypeNullabilityRowSelector(
    const rowcount_t row_count,
    const Column& input,
    const rowid_t* selector_row_ids,
    const rowcount_t offset,
    OwnedColumn* const output) {
  DCHECK_EQ(selector_row_ids != NULL, row_selector_type != NO_SELECTOR);
  DCHECK(!(offset && row_selector_type == INPUT_OUTPUT_SELECTOR));

  // NOTE: the contract allows the input is_null vector to be missing, even
  // if the input schema says the column is nullable.
  return (input.is_null() != NULL)
      ? CopyColumnInternal<type, NULLABLE, output_nullability,
                           row_selector_type, deep_copy>(
            row_count, input, selector_row_ids, offset, output)
      : CopyColumnInternal<type, NOT_NULLABLE, output_nullability,
                           row_selector_type, deep_copy>(
            row_count, input, selector_row_ids, offset, output);
}

// The memcpy-based variant of the core copying function. Only used in special
// cases, if selection vector is not given and either type is fixed-length or
// only a shallow copy is made. Returns row_count.
template <DataType type, Nullability output_nullability>
rowcount_t CopyColumnWithMemCpyStaticTypeNullability(
    const rowcount_t row_count,
    const Column& input,
    const rowid_t* selector_row_ids_unused,
    const rowcount_t offset,
    OwnedColumn* const output) {
  DCHECK(output != NULL);
  DCHECK(selector_row_ids_unused == NULL);
  // Copy data.
  DCHECK(!input.data().is_null())
      << "Input data " << input.attribute().name() << " is NULL; can't copy";
  DCHECK(output->mutable_typed_data<type>() != NULL)
      << "Output buffer for " << output->content().attribute().name()
      << " is NULL; can't copy";
  memcpy(output->mutable_typed_data<type>() + offset, input.data().raw(),
         TypeTraits<type>::size * row_count);
  // Copy is_null information.
  // NOTE: the contract allows the input is_null vector to be missing, even
  // if the input schema says the column is nullable.
  if (output_nullability == NULLABLE && input.is_null() != NULL) {
    // Already asserted that output_is_nullable.
    bit_pointer::FillFrom(output->mutable_is_null() + offset,
                          input.is_null(), row_count);
  } else if (output_nullability == NULLABLE) {
    bit_pointer::FillWithFalse(output->mutable_is_null() + offset, row_count);
  } else {
    DCHECK(input.is_null() == NULL)
        << "Nullable input while copying to not nullable output";
  }
  return row_count;
}

// Incrementally binds run-time arguments to static template arguments,
// one dimension at a time.
struct CopyColumnResolver {
  CopyColumnResolver(Nullability output_nullability,
                     RowSelectorType row_selector_type,
                     bool deep_copy)
      : output_nullability(output_nullability),
        row_selector_type(row_selector_type),
        deep_copy(deep_copy) {}

  template<DataType type>
  ColumnCopier operator()() const {
    return output_nullability == NULLABLE
        ? Resolve1<type, NULLABLE>()
        : Resolve1<type, NOT_NULLABLE>();
  }

  template<DataType type, Nullability nullability_p>
  ColumnCopier Resolve1() const {
    if ((!TypeTraits<type>::is_variable_length || !deep_copy)
        && row_selector_type == NO_SELECTOR) {
      // Prefer the memcpy variant when possible.
      return CopyColumnWithMemCpyStaticTypeNullability<type, nullability_p>;
    }
    switch (row_selector_type) {
      case NO_SELECTOR:
        return Resolve2<type, nullability_p, NO_SELECTOR>();
      case INPUT_SELECTOR:
        return Resolve2<type, nullability_p, INPUT_SELECTOR>();
      case OUTPUT_SELECTOR:
        return Resolve2<type, nullability_p, OUTPUT_SELECTOR>();
      case INPUT_OUTPUT_SELECTOR:
        return Resolve2<type, nullability_p, INPUT_OUTPUT_SELECTOR>();
    }
    LOG(FATAL) << "Unable to resolve; unexpected input data";
  }

  template<DataType type, Nullability nullability_p,
           RowSelectorType row_selector_type_p>
  ColumnCopier Resolve2() const {
    return deep_copy
        ? Resolve3<type, nullability_p, row_selector_type_p, true>()
        : Resolve3<type, nullability_p, row_selector_type_p, false>();
  }

  template<DataType type, Nullability nullability_p,
           RowSelectorType row_selector_type_p, bool deep_copy_p>
  ColumnCopier Resolve3() const {
    return CopyColumnWithForLoopStaticTypeNullabilityRowSelector<
        type, nullability_p, row_selector_type_p, deep_copy_p>;
  }

  Nullability output_nullability;
  const RowSelectorType row_selector_type;
  bool deep_copy;
};

ColumnCopier ResolveCopyColumnFunction(
    const DataType type,
    const Nullability output_nullability,
    const RowSelectorType row_selector_type,
    bool deep_copy) {
  CopyColumnResolver resolver(output_nullability, row_selector_type, deep_copy);
  return TypeSpecialization<ColumnCopier, CopyColumnResolver>(type, resolver);
}

}  // namespace supersonic
