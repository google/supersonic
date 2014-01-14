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
// A Unary Column Computer is a templated class that can perform the
// evaluation of a unary expression on a single input column.
//
// The Computer performs input validity checks (using a CheckFailure struct
// drawn from the appropriate UnaryExpressionTraits), sets the is_null field
// of the output (based on the is_null fields of the inputs and on the
// FillNulls struct from the traits) and performs the evaluation.

#ifndef SUPERSONIC_EXPRESSION_VECTOR_UNARY_COLUMN_COMPUTERS_H_
#define SUPERSONIC_EXPRESSION_VECTOR_UNARY_COLUMN_COMPUTERS_H_

#include <stddef.h>
#include <string>
namespace supersonic {using std::string; }

#include "supersonic/utils/integral_types.h"
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
#include "supersonic/expression/vector/expression_evaluators.h"
#include "supersonic/expression/vector/expression_traits.h"
#include "supersonic/proto/supersonic.pb.h"
#include "supersonic/utils/strings/join.h"

class StringPiece;
namespace supersonic {
class Arena;
}  // namespace supersonic

namespace supersonic {

namespace unary_column_computers {

inline FailureOrVoid CheckNoFailures(const size_t failures,
                                     const string& expression_name) {
  if (failures > 0) {
    string message =
        StrCat("Evaluation error (invalid data in ", failures,
               " rows) in expression", expression_name);
    THROW(new Exception(ERROR_EVALUATION_ERROR, message));
  }
  return Success();
}

// This is a templated version of a runtime if, which assures we will never
// try to call the UnaryExpressionTraits<op>::CheckFailure operator for a
// operator which doesn't have one.
template<OperatorId op, bool failing, typename input_type>
struct UnaryFailureChecker {};

// The version which counts the failures (using the CheckFailure operator drawn
// from UnaryExpressionTraits<op>).
template<OperatorId op, typename input_type>
struct UnaryFailureChecker<op, true, input_type> {
  // Returns the number of rows containing input that causes the evaluation
  // to fail.
  int operator()(const input_type* input,
                 bool_const_ptr input_is_null,
                 size_t row_count) const {
    typename UnaryExpressionTraits<op>::CheckFailure failer;
    return failer(input, input_is_null, row_count);
  }
};

// The version which does nothing (for non-failing operators).
template<OperatorId op, typename input_type>
struct UnaryFailureChecker<op, false, input_type> {
  inline int operator()(const input_type* input,
                        bool_const_ptr input_is_null,
                        size_t row_count) const {
    return 0;
  }
};

// A column-wise null-filler. It takes the input columns and checks which of
// the outputs should be NULL. There is a templated if similar as in the case
// of FailureCheckers.
template<OperatorId op, bool nulling, typename input_type>
struct UnaryNuller {};

// The version which performs a FillNulls operation.
template<OperatorId op, typename input_type>
struct UnaryNuller<op, true, input_type> {
  void operator()(const input_type* input,
                  bool_ptr skip_vector,
                  size_t row_count) const {
    typename UnaryExpressionTraits<op>::FillNulls nuller;
    // Nullers, by contract, only introduce new NULLs, but do not reset old
    // ones (OR semantics).
    nuller(input, skip_vector, row_count);
  }
};

// The version which does nothing.
template<OperatorId op, typename input_type>
struct UnaryNuller<op, false, input_type> {
  void operator()(const input_type* input,
                  bool_ptr skip_vector,
                  size_t row_count) const {
    return;
  }
};

// A convenience function, checks the validity of the input data, fills out the
// output_is_null column.
template<OperatorId op, DataType input_type, DataType result_type>
FailureOrVoid CheckAndNull(const Column& input_column,
                           bool_ptr skip_vector,
                           size_t row_count) {
  typedef typename TypeTraits<input_type>::cpp_type input_cpp_type;
  const input_cpp_type* input = input_column.typed_data<input_type>();

  UnaryFailureChecker<op, UnaryExpressionTraits<op>::can_fail,
                      input_cpp_type> failer;
  PROPAGATE_ON_FAILURE(CheckNoFailures(
      failer(input, skip_vector, row_count),
      UnaryExpressionTraits<op>::FormatDescription(
          input_column.attribute().name())));

  UnaryNuller<op, UnaryExpressionTraits<op>::can_return_null, input_cpp_type>
      nuller;
  nuller(input, skip_vector, row_count);

  return Success();
}

}  // namespace unary_column_computers

// A generic class for unary column computers.
template<OperatorId op, DataType input_type, DataType result_type,
         bool allocating>
struct ColumnUnaryComputer {};

// The non-allocating version of the column unary computer.
template<OperatorId op, DataType input_type, DataType result_type>
struct ColumnUnaryComputer<op, input_type, result_type, false> {
  typedef typename TypeTraits<result_type>::cpp_type ResultCppType;
  typedef typename TypeTraits<input_type>::cpp_type InputCppType;

  FailureOrVoid operator()(const Column& input,
                           size_t const row_count,
                           OwnedColumn* const output,
                           bool_ptr skip_vector) const {
    // Convenience shorthands.
    const InputCppType* input_data = input.typed_data<input_type>();
    ResultCppType* result_data = output->mutable_typed_data<result_type>();

    // Failing and nulling checks through CheckAndNull.
    PROPAGATE_ON_FAILURE((unary_column_computers::CheckAndNull<
        op, input_type, result_type>(input, skip_vector, row_count)));

    // Actual evaluation.
    typename UnaryExpressionTraits<op>::basic_operator my_operator;
    // Check whether we should evaluate all rows, or just the ones given by the
    // skip vector, based on the contents of the skip vector and the expression
    // selection level.
    if (!SelectivityIsGreaterThan(
            skip_vector, row_count,
            UnaryExpressionTraits<op>::selectivity_threshold)
        && !TypeTraits<input_type>::is_variable_length) {
      for (int i = 0; i < row_count; ++i) {
        *result_data++ = my_operator(*input_data++);
      }
    } else {
      for (int i = 0; i < row_count; ++i) {
        if (!*skip_vector) {
          *result_data = my_operator(*input_data);
        }
        ++result_data;
        ++skip_vector;
        ++input_data;
      }
    }
    return Success();
  }
};

// The allocating version of the unary column computer. This one passes the
// arena to the operator.
template<OperatorId op, DataType input_type, DataType result_type>
struct ColumnUnaryComputer<op, input_type, result_type, true> {
  typedef typename TypeTraits<result_type>::cpp_type ResultCppType;
  typedef typename TypeTraits<input_type>::cpp_type InputCppType;

  FailureOrVoid operator()(const Column& input,
                           size_t const row_count,
                           OwnedColumn* const output,
                           bool_ptr skip_vector) const {
    // Convenient shorthands.
    const InputCppType* input_data = input.typed_data<input_type>();
    Arena* arena = output->arena();
    ResultCppType* result_data = output->mutable_typed_data<result_type>();

    // Failing and nulling.
    PROPAGATE_ON_FAILURE((unary_column_computers::CheckAndNull<
        op, input_type, result_type>(input, skip_vector, row_count)));

    // Actual calculation.
    typename UnaryExpressionTraits<op>::basic_operator my_operator;
    // Remark - using the arena is almost never safe (and thus almost always
    // will have a selection threshold of 100), but keeping this line
    // of code is not very costly.
    if (!SelectivityIsGreaterThan(
            skip_vector, row_count,
            UnaryExpressionTraits<op>::selectivity_threshold)
        && !TypeTraits<input_type>::is_variable_length) {
      for (int i = 0; i < row_count; ++i) {
        *result_data++ = my_operator(*input_data++, arena);
      }
    } else {
      for (int i = 0; i < row_count; ++i) {
        if (!*skip_vector) {
          *result_data = my_operator(*input_data, arena);
        }
        ++result_data;
        ++skip_vector;
        ++input_data;
      }
    }
    return Success();
  }
};

// This case is separated from the typical, because here the operator to be
// applied is templated (by input_type), which does not fit into the general
// scheme.
template<DataType input_type>
struct ColumnUnaryComputer<OPERATOR_TOSTRING, input_type, STRING, true> {
  typedef typename TypeTraits<input_type>::cpp_type InputCppType;

  FailureOrVoid operator()(const Column& input,
                           size_t const row_count,
                           OwnedColumn* const output,
                           bool_ptr skip_vector) const {
    const InputCppType* input_data = input.typed_data<input_type>();
    StringPiece* result_data = output->mutable_typed_data<STRING>();
    Arena* arena = output->arena();

    PROPAGATE_ON_FAILURE((unary_column_computers::CheckAndNull<
        OPERATOR_TOSTRING, input_type, STRING>(input, skip_vector,
                                               row_count)));

    typename operators::TypedToString<input_type> my_operator;
    if (!SelectivityIsGreaterThan(
            skip_vector, row_count,
            UnaryExpressionTraits<OPERATOR_TOSTRING>::selectivity_threshold)) {
      for (int i = 0; i < row_count; ++i) {
        *result_data++ = my_operator(*input_data++, arena);
      }
    } else {
      for (int i = 0; i < row_count; ++i) {
        if (!*skip_vector) {
          *result_data = my_operator(*input_data, arena);
        }
        ++result_data;
        ++skip_vector;
        ++input_data;
      }
    }
    return Success();
  }
};

// This case is separated from the typical, because here the operator to be
// applied is templated (by output_type, this time), which does not fit into
// the general scheme.
template<DataType output_type>
struct ColumnUnaryComputer<OPERATOR_PARSE_STRING_QUIET, STRING,
                           output_type, false> {
  typedef typename TypeTraits<output_type>::cpp_type OutputCppType;

  FailureOrVoid operator()(const Column& input,
                           size_t const row_count,
                           OwnedColumn* const output,
                           bool_ptr skip_vector) const {
    const StringPiece* input_data = input.typed_data<STRING>();
    OutputCppType* result_data = output->mutable_typed_data<output_type>();

    typename operators::TypedParseString<output_type> parse_operator(
        output->content().attribute());
    // Failure is a dummy variable, ignored, as this is a quiet operator.
    uint32 failure;
    if (!SelectivityIsGreaterThan(
            skip_vector, row_count,
            UnaryExpressionTraits<
                OPERATOR_PARSE_STRING_QUIET>::selectivity_threshold)) {
      for (int i = 0; i < row_count; ++i) {
        parse_operator(*input_data++, result_data++, bool_ptr(&failure));
      }
    } else {
      for (int i = 0; i < row_count; ++i) {
        if (!*skip_vector) {
          parse_operator(*input_data, result_data, bool_ptr(&failure));
        }
        ++result_data;
        ++skip_vector;
        ++input_data;
      }
    }
    return Success();
  }
};

// This case is separated from the typical, because here the operator to be
// applied is templated (by output_type, this time), which does not fit into
// the general scheme. Also, the Nulling is performed differently than in
// the standard procedure - instead of validating the data a priori, we
// try to perform the parsing, and null the output field if the parsing was
// not a success.
template<DataType output_type>
struct ColumnUnaryComputer<OPERATOR_PARSE_STRING_NULLING, STRING,
                           output_type, false> {
  typedef typename TypeTraits<output_type>::cpp_type OutputCppType;

  FailureOrVoid operator()(const Column& input,
                           size_t const row_count,
                           OwnedColumn* const output,
                           bool_ptr skip_vector) const {
    const StringPiece* input_data = input.typed_data<STRING>();
    OutputCppType* result_data = output->mutable_typed_data<output_type>();

    typename operators::TypedParseString<output_type> parse_operator(
        output->content().attribute());

    // We go row by row, because we have to fill in selection data.
    for (int i = 0; i < row_count; ++i) {
      if (!*skip_vector) {
        parse_operator(*input_data, result_data, skip_vector);
      }
      ++result_data;
      ++skip_vector;
      ++input_data;
    }
    return Success();
  }
};

}  // namespace supersonic

#endif  // SUPERSONIC_EXPRESSION_VECTOR_UNARY_COLUMN_COMPUTERS_H_
