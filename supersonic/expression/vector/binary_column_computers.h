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
// Author: onufry@google.com (Onufry Wojtaszczyk)
//
// A Binary Column Computer is a templated class that can perform the
// evaluation of a binary expression on two input columns.
//
// This implementation concerns itself with failure checking and null
// setting, while the calculation itself is delegated to VectorBinaryPrimitives.

#ifndef SUPERSONIC_EXPRESSION_VECTOR_BINARY_COLUMN_COMPUTERS_H_
#define SUPERSONIC_EXPRESSION_VECTOR_BINARY_COLUMN_COMPUTERS_H_

#include <stddef.h>
#include <string>
namespace supersonic {using std::string; }

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
#include "supersonic/expression/vector/vector_primitives.h"
#include "supersonic/proto/supersonic.pb.h"
#include "supersonic/utils/strings/join.h"

namespace supersonic {
class Arena;
}  // namespace supersonic

namespace supersonic {

namespace binary_column_computers {

// Returns an appropriately described evaluation error.
template<OperatorId op>
static FailureOrVoid ReportError(const ReturnCode code,
                                 const string& message,
                                 const Column& left,
                                 const Column& right) {
  THROW(new Exception(
      code, message + " in " +
      BinaryExpressionTraits<op>::FormatDescription(left.attribute().name(),
                                                    right.attribute().name())));
}

// A column-wise checker whether the data is failing. The check is done in
// a template fashion to avoid the need to define the failers in every
// expression trait, even the ones that can't fail (which would have to be
// done if we used a runtime if).
template<OperatorId op, bool failing, typename left_type, typename right_type>
struct BinaryFailureChecker {};

// The version which does indeed check something (calls
// BinaryExpressionTraits<op>::CheckFailure).
template<OperatorId op, typename left_type, typename right_type>
struct BinaryFailureChecker<op, true, left_type, right_type> {
  int operator()(const left_type* left_input,
                 bool_const_ptr left_is_null,
                 const right_type* right_input,
                 bool_const_ptr right_is_null,
                 size_t row_count) const {
    typename BinaryExpressionTraits<op>::CheckFailure failer;
    return failer(left_input, left_is_null, right_input, right_is_null,
                  row_count);
  }
};

// The trivial version for non-failing operations.
template<OperatorId op, typename left_type, typename right_type>
struct BinaryFailureChecker<op, false, left_type, right_type> {
  int operator()(const left_type* left_input,
                 bool_const_ptr left_is_null,
                 const right_type* right_input,
                 bool_const_ptr right_is_null,
                 size_t row_count) const {
    return 0;
  }
};

// A column-wise null-filler. It takes takes the input columns and checks
// which of the ouputs should be NULL. There is a templated if similar to the
// case of the BinaryFailureChecker.
template<OperatorId op, bool nulling, typename left_type, typename right_type>
struct BinaryNuller {};

// The version which calls the BinaryExpressionTraits<op>::FillNulls operator,
// for nulling operators.
template<OperatorId op, typename left_type, typename right_type>
struct BinaryNuller<op, true, left_type, right_type> {
  void operator()(const left_type* left_input,
                  const right_type* right_input,
                  bool_ptr skip_vector,
                  size_t row_count) const {
    typename BinaryExpressionTraits<op>::FillNulls nuller;
    // nullers, by contract, never unset any previously set is_null fields
    // (an OR semantic).
    nuller(left_input, right_input, skip_vector, row_count);
  }
};

// The version which does nothing, except possibly initialize the is_null field
// to false.
template<OperatorId op, typename left_type, typename right_type>
struct BinaryNuller<op, false, left_type, right_type> {
  void operator()(const left_type* left_input,
                  const right_type* right_input,
                  bool_ptr skip_vector,
                  size_t row_count) const {
    // We do nothing.
    return;
  }
};

// Check the validity of the input data, fills out the output_is_null column.
// Assumes that if any of the inputs is a NULL, the output will also be a NULL.
template<OperatorId op, DataType left_type, DataType right_type,
         DataType result_type>
struct CheckAndNull {
  FailureOrVoid operator()(const Column& left_column,
                        const Column& right_column,
                        bool_ptr skip_vector,
                        size_t row_count) const {
    // Local definitions.
    typedef typename TypeTraits<left_type>::cpp_type left_cpp_type;
    typedef typename TypeTraits<right_type>::cpp_type right_cpp_type;
    const left_cpp_type* left_input = left_column.typed_data<left_type>();
    const right_cpp_type* right_input = right_column.typed_data<right_type>();

    // Failure checking - returns a Failure if there is a positive number of
    // failing rows.
    BinaryFailureChecker<op, BinaryExpressionTraits<op>::can_fail,
                         left_cpp_type, right_cpp_type> failer;
    int failures = failer(left_input, skip_vector,
                          right_input, skip_vector, row_count);
    if (failures > 0) {
      string message = StrCat("Evaluation error (invalid data in ",
                              failures, " rows)");
      return ReportError<op>(ERROR_EVALUATION_ERROR, message,
                             left_column, right_column);
    }

    // Propagate nulls resulting from invalid inputs.
    BinaryNuller<op, BinaryExpressionTraits<op>::can_return_null, left_cpp_type,
        right_cpp_type> nuller;
    nuller(left_input, right_input, skip_vector, row_count);

    return Success();
  }
};

}  // namespace binary_column_computers

// BinaryColumnComputer definition. See top of file for description.
template<OperatorId op, DataType left_type, DataType right_type,
         DataType result_type>
struct ColumnBinaryComputer {
  typedef typename TypeTraits<result_type>::cpp_type ResultCppType;
  typedef typename TypeTraits<left_type>::cpp_type LeftCppType;
  typedef typename TypeTraits<right_type>::cpp_type RightCppType;

  FailureOrVoid operator()(const Column& left,
                           const Column& right,
                           size_t row_count,
                           OwnedColumn* const output,
                           bool_ptr skip_vector) const {
    // Convenience shorthands.
    const LeftCppType* left_data = left.typed_data<left_type>();
    const RightCppType* right_data = right.typed_data<right_type>();
    ResultCppType* result_data = output->mutable_typed_data<result_type>();
    Arena* arena = output->arena();

    // Run CheckAndNull - check for failures, fill out the result_is_null field.
    binary_column_computers::CheckAndNull<op, left_type, right_type,
        result_type> check_and_null;
    PROPAGATE_ON_FAILURE(check_and_null(left, right, skip_vector, row_count));

    // We use the same VectorBinaryPrimitive in both branches of the if - the
    // one appropriate for our operation - but in the second case we pass
    // one more argument to the operator() of the primitive - the
    // skip vector boolean array.
    VectorBinaryPrimitive<op, DirectIndexResolver, DirectIndexResolver,
        result_type, left_type, right_type,
        BinaryExpressionTraits<op>::needs_allocator> column_operator;

    // This branch calculates all the values, the Null column nonewithstanding.
    // The assumption is it's quicker to calculate all, than to check for
    // NULLs at each step.
    if (!SelectivityIsGreaterThan(
            skip_vector, row_count,
            BinaryExpressionTraits<op>::selectivity_threshold)
        && !TypeTraits<left_type>::is_variable_length
        && !TypeTraits<right_type>::is_variable_length) {
      bool is_success = column_operator(left_data, right_data, NULL, NULL,
                                        static_cast<const index_t>(row_count),
                                        result_data, arena);
      if (!is_success) {
        string message = "Expression evaluation failed inside "
                         "VectorBinaryPrimitive (maybe an OOM in the arena?).";
        return binary_column_computers::ReportError<op>(ERROR_EVALUATION_ERROR,
                                                        message, left, right);
      }
    } else {  // We calculate by hand, avoiding NULLed rows.
      bool is_success = column_operator(left_data, right_data, NULL, NULL,
                                        static_cast<const index_t>(row_count),
                                        skip_vector, result_data, arena);
      if (!is_success) {
        string message = "Expression evaluation failed inside "
                         "VectorBinaryPrimitive with a skip-list "
                         "(maybe an OOM in the arena?).";
        return binary_column_computers::ReportError<op>(ERROR_EVALUATION_ERROR,
                                                        message, left, right);
      }
    }
    return Success();
  }
};

}  // namespace supersonic

#endif  // SUPERSONIC_EXPRESSION_VECTOR_BINARY_COLUMN_COMPUTERS_H_
