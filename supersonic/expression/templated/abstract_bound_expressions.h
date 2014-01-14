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
// Author: onufry@google.com (Jakub Onufry Wojtaszczyk)
//
// Provides bound expression templates that do automatic evaluation for
// standard operations.

#ifndef SUPERSONIC_EXPRESSION_TEMPLATED_ABSTRACT_BOUND_EXPRESSIONS_H_
#define SUPERSONIC_EXPRESSION_TEMPLATED_ABSTRACT_BOUND_EXPRESSIONS_H_

#include <algorithm>
#include "supersonic/utils/std_namespace.h"
#include <memory>
#include <string>
namespace supersonic {using std::string; }
#include <vector>
using std::vector;

#include "supersonic/utils/scoped_ptr.h"

#include "supersonic/base/infrastructure/bit_pointers.h"
#include "supersonic/expression/infrastructure/basic_bound_expression.h"
#include "supersonic/expression/vector/binary_column_computers.h"
#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/expression/vector/expression_traits.h"
#include "supersonic/expression/infrastructure/expression_utils.h"
#include "supersonic/expression/proto/operators.pb.h"
#include "supersonic/expression/vector/ternary_column_computers.h"
#include "supersonic/expression/vector/unary_column_computers.h"

namespace supersonic {

// ----------- Abstract Unary Expressions Without Assignment -----------

// Implements a basic abstract class for unary operators.
// For usage instructions see abstract_expressions.h
template<OperatorId op, DataType input_type, DataType output_type>
class AbstractBoundUnaryExpression : public BoundUnaryExpression {
 public:
  AbstractBoundUnaryExpression(const string& output_name,
                               BufferAllocator* const allocator,
                               BoundExpression* arg)
      : BoundUnaryExpression(
          CreateSchema(output_name, output_type, arg,
                       UnaryExpressionTraits<op>::can_return_null
                           ? NULLABLE
                           : NOT_NULLABLE),
          allocator, arg, input_type) {
  }

  virtual EvaluationResult DoEvaluate(const View& input,
                                      const BoolView& skip_vectors) {
    CHECK_EQ(1, skip_vectors.column_count());
    if (TypeTraits<output_type>::is_variable_length) my_block()->ResetArenas();
    // Evaluate the argument.
    EvaluationResult argument_result(
        argument()->DoEvaluate(input, skip_vectors));
    PROPAGATE_ON_FAILURE(argument_result);
    PROPAGATE_ON_FAILURE(column_operator_(argument_result.get().column(0),
                                          input.row_count(),
                                          my_block()->mutable_column(0),
                                          skip_vectors.column(0)));
    my_view()->set_row_count(input.row_count());
    my_view()->mutable_column(0)->ResetIsNull(skip_vectors.column(0));
    return Success(*my_view());
  }

 private:
  typedef typename TypeTraits<input_type>::cpp_type CppFrom;
  typedef typename TypeTraits<output_type>::cpp_type CppTo;

  // TODO(user): Make this a local variable.
  ColumnUnaryComputer<op, input_type, output_type,
      UnaryExpressionTraits<op>::needs_allocator> column_operator_;

  DISALLOW_COPY_AND_ASSIGN(AbstractBoundUnaryExpression);
};

// A generic template for creating unary bound expressions for an arbitrary
// operator.
template<OperatorId op, DataType input_type, DataType output_type>
FailureOrOwned<BoundExpression> AbstractBoundUnary(
    BoundExpression* argument_ptr,
    BufferAllocator* allocator,
    rowcount_t max_row_count) {
  std::unique_ptr<BoundExpression> argument(argument_ptr);
  PROPAGATE_ON_FAILURE(
      CheckAttributeCount(UnaryExpressionTraits<op>::name(),
                          argument->result_schema(), 1));
  PROPAGATE_ON_FAILURE(CheckExpressionType(input_type, argument.get()));
  string description = UnaryExpressionTraits<op>::FormatBoundDescription(
      argument->result_schema().attribute(0).name(), input_type, output_type);
  return InitBasicExpression(
      max_row_count,
      new AbstractBoundUnaryExpression<op, input_type, output_type>(
          description, allocator, argument.release()),
      allocator);
}

// -------------- Abstract Binary Expressions (without assignment) -------

// Abstract class for bound binary expressions, implements standard operations
// through column computers.
// For usage instructions see abstract_expressions.h
template<OperatorId op,
         DataType output_type, DataType left_type, DataType right_type>
class AbstractBoundBinaryExpression : public BoundBinaryExpression {
 public:
  // Takes ownership of expressions.
  AbstractBoundBinaryExpression(const TupleSchema& result_schema,
                                BufferAllocator* const allocator,
                                BoundExpression* left,
                                BoundExpression* right)
      : BoundBinaryExpression(result_schema, allocator, left, left_type,
                              right, right_type) {}

  virtual EvaluationResult DoEvaluate(const View& input,
                                      const BoolView& skip_vectors) {
    CHECK_EQ(1, skip_vectors.column_count());
    my_block()->ResetArenas();
    if (TypeTraits<output_type>::is_variable_length) my_block()->ResetArenas();
    EvaluationResult left_result = left()->DoEvaluate(input, skip_vectors);
    PROPAGATE_ON_FAILURE(left_result);
    const View& left = left_result.get();
    EvaluationResult right_result = right()->DoEvaluate(input, skip_vectors);
    PROPAGATE_ON_FAILURE(right_result);
    const View& right = right_result.get();
    PROPAGATE_ON_FAILURE(column_operator_(left.column(0), right.column(0),
                                          input.row_count(),
                                          my_block()->mutable_column(0),
                                          skip_vectors.column(0)));
    my_view()->set_row_count(input.row_count());
    my_view()->mutable_column(0)->ResetIsNull(skip_vectors.column(0));
    return Success(*my_view());
  }

 private:
  ColumnBinaryComputer<
      op, left_type, right_type, output_type> column_operator_;
};

// Standard creation template for Abstract bound binary expressions.
template<OperatorId op, DataType left_type, DataType right_type,
    DataType output_type>
FailureOrOwned<BoundExpression> CreateAbstractBoundBinaryExpression(
    BoundExpression* left_ptr,
    BoundExpression* right_ptr,
    BufferAllocator* allocator,
    rowcount_t max_row_count) {
  std::unique_ptr<BoundExpression> left(left_ptr);
  std::unique_ptr<BoundExpression> right(right_ptr);
  const string op_name = BinaryExpressionTraits<op>::name();
  PROPAGATE_ON_FAILURE(CheckAttributeCount(op_name, left->result_schema(), 1));
  PROPAGATE_ON_FAILURE(CheckAttributeCount(op_name, right->result_schema(), 1));
  PROPAGATE_ON_FAILURE(CheckExpressionType(left_type, left.get()));
  PROPAGATE_ON_FAILURE(CheckExpressionType(right_type, right.get()));
  string expression_name = BinaryExpressionTraits<op>::FormatBoundDescription(
      left->result_schema().attribute(0).name(),
      left->result_schema().attribute(0).type(),
      right->result_schema().attribute(0).name(),
      right->result_schema().attribute(0).type(),
      output_type);
  AbstractBoundBinaryExpression<op, output_type,
                                left_type, right_type>* result =
      new AbstractBoundBinaryExpression<op, output_type,
                                        left_type, right_type> (
          CreateSchema(expression_name, output_type, left.get(), right.get(),
                       BinaryExpressionTraits<op>::can_return_null
                           ? NULLABLE
                           : NOT_NULLABLE),
          allocator, left.release(), right.release());
  return InitBasicExpression(max_row_count, result, allocator);
}

// -------------- Abstract Ternary Expressions (without assignment) ---------

// Abstract expression template. Performs evaluation, null-checking and
// validity checking using the functions given in expression traits.
// For usage instructions see abstract_expressions.h.
template<OperatorId op, DataType left_type, DataType middle_type,
         DataType right_type, DataType output_type>
class AbstractBoundTernaryExpression : public BoundTernaryExpression {
 public:
  AbstractBoundTernaryExpression(const string& output_name,
                                 BufferAllocator* const allocator,
                                 BoundExpression* left,
                                 BoundExpression* middle,
                                 BoundExpression* right)
      : BoundTernaryExpression(
          CreateSchema(output_name, output_type, left, middle, right,
                       TernaryExpressionTraits<op>::can_return_null
                           ? NULLABLE
                           : NOT_NULLABLE),
           allocator, left, left_type, middle, middle_type, right, right_type) {
  }

  virtual EvaluationResult DoEvaluate(const View& input,
                                      const BoolView& skip_vectors) {
    CHECK_EQ(1, skip_vectors.column_count());
    if (TypeTraits<output_type>::is_variable_length) my_block()->ResetArenas();
    // Evaluate the arguments.
    EvaluationResult left_result = left_->DoEvaluate(input, skip_vectors);
    PROPAGATE_ON_FAILURE(left_result);
    EvaluationResult middle_result = middle_->DoEvaluate(input, skip_vectors);
    PROPAGATE_ON_FAILURE(middle_result);
    EvaluationResult right_result = right_->DoEvaluate(input, skip_vectors);
    PROPAGATE_ON_FAILURE(right_result);

    PROPAGATE_ON_FAILURE(column_operator_(left_result.get().column(0),
                                          middle_result.get().column(0),
                                          right_result.get().column(0),
                                          input.row_count(),
                                          my_block()->mutable_column(0),
                                          skip_vectors.column(0)));
    my_view()->set_row_count(input.row_count());
    my_view()->mutable_column(0)->ResetIsNull(skip_vectors.column(0));
    return Success(*my_view());
  }

 private:
  ColumnTernaryComputer<op, left_type, middle_type, right_type, output_type,
      TernaryExpressionTraits<op>::needs_allocator> column_operator_;

  DISALLOW_COPY_AND_ASSIGN(AbstractBoundTernaryExpression);
};

// A generic template for creating ternary bound expressions for an arbitrary
// operator. It requires that the operator does not need to allocate extra
// space (e.g. for strings or blobs).
template<OperatorId op, DataType left_type, DataType middle_type,
         DataType right_type, DataType output_type>
FailureOrOwned<BoundExpression> CreateAbstractBoundTernaryExpression(
    BoundExpression* left, BoundExpression* middle, BoundExpression* right,
    BufferAllocator* allocator, rowcount_t max_row_count) {
  PROPAGATE_ON_FAILURE(CheckAttributeCount(
      TernaryExpressionTraits<op>::name(), left->result_schema(), 1));
  PROPAGATE_ON_FAILURE(CheckAttributeCount(
      TernaryExpressionTraits<op>::name(), middle->result_schema(), 1));
  PROPAGATE_ON_FAILURE(CheckAttributeCount(
      TernaryExpressionTraits<op>::name(), right->result_schema(), 1));

  PROPAGATE_ON_FAILURE(CheckExpressionType(left_type, left));
  PROPAGATE_ON_FAILURE(CheckExpressionType(middle_type, middle));
  PROPAGATE_ON_FAILURE(CheckExpressionType(right_type, right));

  return InitBasicExpression(max_row_count,
      new AbstractBoundTernaryExpression<op, left_type, middle_type,
                                         right_type, output_type> (
          TernaryExpressionTraits<op>::FormatBoundDescription(
              left->result_schema().attribute(0).name(),
              left->result_schema().attribute(0).type(),
              middle->result_schema().attribute(0).name(),
              middle->result_schema().attribute(0).type(),
              right->result_schema().attribute(0).name(),
              right->result_schema().attribute(0).type(), output_type),
          allocator, left, middle, right), allocator);
}

}  // namespace supersonic

#endif  // SUPERSONIC_EXPRESSION_TEMPLATED_ABSTRACT_BOUND_EXPRESSIONS_H_
