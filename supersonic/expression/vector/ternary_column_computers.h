// Copyright 2011 Google Inc. All Rights Reserved.
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
// Author: onufry@google.com (Onufry Wojtaszczyk)
//
// A Ternary Column Computer is a templated class that can perform the
// evaluation of a ternary expression on three input columns.
//
// The implementation performs failure checking, null setting and the
// calculation itself.

#ifndef SUPERSONIC_EXPRESSION_VECTOR_TERNARY_COLUMN_COMPUTERS_H_
#define SUPERSONIC_EXPRESSION_VECTOR_TERNARY_COLUMN_COMPUTERS_H_

#include <stddef.h>

#include <string>
namespace supersonic {using std::string; }

#include "supersonic/utils/stringprintf.h"
#include "supersonic/utils/exception/failureor.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/bit_pointers.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/expression/infrastructure/expression_utils.h"
#include "supersonic/expression/proto/operators.pb.h"
#include "supersonic/expression/vector/expression_traits.h"
#include "supersonic/proto/supersonic.pb.h"

namespace supersonic {
class Arena;
}  // namespace supersonic

namespace supersonic {

namespace ternary_column_computers {

// Returns an appropriately described evaluation error.
template<OperatorId op>
static FailureOrVoid ReportError(const ReturnCode code,
                                 const string& message,
                                 const Column& left,
                                 const Column& middle,
                                 const Column& right) {
  THROW(new Exception(
      code, message + " in " +  TernaryExpressionTraits<op>::FormatDescription(
          left.attribute().name(), middle.attribute().name(),
          right.attribute().name())));
}

// This is a templated version of a runtime if, which assures we will never
// try to call the TernaryExpressionTraits<op>::CheckFailure operator for a
// operator which doesn't have one.
template<OperatorId op, bool failing, typename left_type, typename middle_type,
         typename right_type>
struct TernaryFailureChecker {};

// The version which does something (call the
// TernaryExpressionTraits<op>::CheckFailure operator).
template<OperatorId op, typename left_type, typename middle_type,
         typename right_type>
struct TernaryFailureChecker<op, true, left_type, middle_type, right_type> {
  // Returns the number of rows containing input that causes the evaluation
  // to fail.
  int operator()(const left_type* left_input,
                 bool_const_ptr left_is_null,
                 const middle_type* middle_input,
                 bool_const_ptr middle_is_null,
                 const right_type* right_input,
                 bool_const_ptr right_is_null,
                 size_t row_count) const {
    typename TernaryExpressionTraits<op>::CheckFailure failer;
    return failer(left_input, left_is_null, middle_input, middle_is_null,
                  right_input, right_is_null, row_count);
  }
};

// The trivial version which does nothing, for non-failing operations.
template<OperatorId op, typename left_type, typename middle_type,
         typename right_type>
struct TernaryFailureChecker<op, false, left_type, middle_type, right_type> {
  int operator()(const left_type* left_input,
                 bool_const_ptr left_is_null,
                 const middle_type* middle_input,
                 bool_const_ptr middle_is_null,
                 const right_type* right_input,
                 bool_const_ptr right_is_null,
                 size_t row_count) const {
    return 0;
  }
};

// A column-wise null-filler. It takes the input columns and checks which of
// the outputs should be NULL. The rationale for templating is the same as in
// the case of the FailureCheckers.
template<OperatorId op, bool nulling, typename left_type, typename middle_type,
         typename right_type>
struct TernaryNuller {};

// The version for nulling operators, calls the FillNulls operator drawn from
// the TernaryExpressionTraits<op> struct.
template<OperatorId op, typename left_type, typename middle_type,
         typename right_type>
struct TernaryNuller<op, true, left_type, middle_type, right_type> {
  void operator()(const left_type* left_input,
                  const middle_type* middle_input,
                  const right_type* right_input,
                  bool_ptr skip_vector,
                  size_t row_count) const {
    typename TernaryExpressionTraits<op>::FillNulls nuller;
    // Nullers, by contract, only introduce new NULLs, but do not reset old
    // ones (OR semantics).
    nuller(left_input, middle_input, right_input, skip_vector, row_count);
  }
};

// The version for non-nulling operators.
template<OperatorId op, typename left_type, typename middle_type,
         typename right_type>
struct TernaryNuller<op, false, left_type, middle_type, right_type> {
  void operator()(const left_type* left_input,
                  const middle_type* middle_input,
                  const right_type* right_input,
                  bool_ptr output_is_null,
                  size_t row_count) const {
    return;
  }
};

// A convenience function, checks the validity of the input data, fills out the
// output_is_null column.
template<OperatorId op, DataType left_type, DataType middle_type,
         DataType right_type, DataType result_type>
FailureOrVoid CheckAndNull(const Column& left_column,
                           const Column& middle_column,
                           const Column& right_column,
                           bool_ptr skip_vector,
                           size_t row_count) {
  typedef typename TypeTraits<left_type>::cpp_type left_cpp_type;
  typedef typename TypeTraits<middle_type>::cpp_type middle_cpp_type;
  typedef typename TypeTraits<right_type>::cpp_type right_cpp_type;
  const left_cpp_type* left_input = left_column.typed_data<left_type>();
  const middle_cpp_type* middle_input = middle_column.typed_data<middle_type>();
  const right_cpp_type* right_input = right_column.typed_data<right_type>();

  // Failure checking. Returns a failure if there is a positive number of
  // failing rows.
  TernaryFailureChecker<op, TernaryExpressionTraits<op>::can_fail,
                        left_cpp_type, middle_cpp_type, right_cpp_type> failer;
  int failures = failer(left_input, skip_vector, middle_input, skip_vector,
                        right_input, skip_vector, row_count);
  if (failures > 0) {
    string message =
        StringPrintf("Evaluation error (invalid data in %d rows)", failures);
    return ReportError<op>(ERROR_EVALUATION_ERROR, message, left_column,
                           middle_column, right_column);
  }

  // Propagate nulls resulting from invalid inputs (if applicable).
  TernaryNuller<op, TernaryExpressionTraits<op>::can_return_null, left_cpp_type,
                middle_cpp_type, right_cpp_type> nuller;
  nuller(left_input, middle_input, right_input, skip_vector, row_count);

  return Success();
}

}  // namespace ternary_column_computers

// A generic class for ternary column computers.
// See top of file for description.
template<OperatorId op, DataType left_type, DataType middle_type,
         DataType right_type, DataType result_type, bool allocating>
struct ColumnTernaryComputer {};

// The non-allocating version of the column ternary computer.
template<OperatorId op, DataType left_type, DataType middle_type,
         DataType right_type, DataType result_type>
struct ColumnTernaryComputer<op, left_type, middle_type, right_type,
                             result_type, false> {
  typedef typename TypeTraits<result_type>::cpp_type ResultCppType;
  typedef typename TypeTraits<left_type>::cpp_type LeftCppType;
  typedef typename TypeTraits<middle_type>::cpp_type MiddleCppType;
  typedef typename TypeTraits<right_type>::cpp_type RightCppType;

  FailureOrVoid operator()(const Column& left,
                           const Column& middle,
                           const Column& right,
                           size_t const row_count,
                           OwnedColumn* const output,
                           bool_ptr skip_vector) const {
    // Convenience shorthands.
    const LeftCppType* left_data = left.typed_data<left_type>();
    const MiddleCppType* middle_data = middle.typed_data<middle_type>();
    const RightCppType* right_data = right.typed_data<right_type>();
    ResultCppType* result_data = output->mutable_typed_data<result_type>();

    // Failing and nulling checks through CheckAndNull.
    PROPAGATE_ON_FAILURE((ternary_column_computers::CheckAndNull<
        op, left_type, middle_type, right_type, result_type>(
            left, middle, right, skip_vector, row_count)));

    // Actual evaluation.
    typename TernaryExpressionTraits<op>::basic_operator my_operator;
    if (!SelectivityIsGreaterThan(
            skip_vector, row_count,
            TernaryExpressionTraits<op>::selectivity_threshold)
        && !TypeTraits<left_type>::is_variable_length
        && !TypeTraits<middle_type>::is_variable_length
        && !TypeTraits<right_type>::is_variable_length) {
      for (int i = 0; i < row_count; ++i) {
        *result_data++ = my_operator(*left_data++, *middle_data++,
                                     *right_data++);
      }
    } else {
      for (int i = 0; i < row_count; ++i) {
        if (!*skip_vector) {
          *result_data = my_operator(*left_data, *middle_data, *right_data);
        }
        ++result_data;
        ++skip_vector;
        ++left_data;
        ++middle_data;
        ++right_data;
      }
    }
    return Success();
  }
};

// The allocating version of the ternary column computer. This one passes the
// arena to the operator.
template<OperatorId op, DataType left_type, DataType middle_type,
         DataType right_type, DataType result_type>
struct ColumnTernaryComputer<op, left_type, middle_type, right_type,
                             result_type, true> {
  typedef typename TypeTraits<result_type>::cpp_type ResultCppType;
  typedef typename TypeTraits<left_type>::cpp_type LeftCppType;
  typedef typename TypeTraits<middle_type>::cpp_type MiddleCppType;
  typedef typename TypeTraits<right_type>::cpp_type RightCppType;

  FailureOrVoid operator()(const Column& left,
                           const Column& middle,
                           const Column& right,
                           size_t const row_count,
                           OwnedColumn* const output,
                           bool_ptr skip_vector) const {
    // Convenience shorthands.
    const LeftCppType* left_data = left.typed_data<left_type>();
    const MiddleCppType* middle_data = middle.typed_data<middle_type>();
    const RightCppType* right_data = right.typed_data<right_type>();
    Arena* arena = output->arena();
    ResultCppType* result_data = output->mutable_typed_data<result_type>();

    // Failing and nulling checks through CheckAndNull.
    PROPAGATE_ON_FAILURE((ternary_column_computers::CheckAndNull<op,
        left_type, middle_type, right_type, result_type>(
            left, middle, right, skip_vector, row_count)));

    // Actual evaluation.
    typename TernaryExpressionTraits<op>::basic_operator my_operator;
    if (!SelectivityIsGreaterThan(
        skip_vector, row_count,
        TernaryExpressionTraits<op>::selectivity_threshold)
        && !TypeTraits<left_type>::is_variable_length
        && !TypeTraits<middle_type>::is_variable_length
        && !TypeTraits<right_type>::is_variable_length) {
      for (int i = 0; i < row_count; ++i) {
        *result_data++ = my_operator(*left_data++, *middle_data++,
                                     *right_data++, arena);
      }
    } else {
      for (int i = 0; i < row_count; ++i) {
        if (!*skip_vector) {
          *result_data = my_operator(*left_data, *middle_data, *right_data,
                                     arena);
        }
        ++result_data;
        ++skip_vector;
        ++left_data;
        ++middle_data;
        ++right_data;
      }
    }
    return Success();
  }
};

}  // namespace supersonic

#endif  // SUPERSONIC_EXPRESSION_VECTOR_TERNARY_COLUMN_COMPUTERS_H_
