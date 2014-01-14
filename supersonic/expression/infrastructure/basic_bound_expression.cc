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

#include "supersonic/expression/infrastructure/basic_bound_expression.h"

#include <memory>
#include <string>
namespace supersonic {using std::string; }

#include "supersonic/utils/casts.h"
#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/utils/stringprintf.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/bit_pointers.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/base/infrastructure/types_infrastructure.h"
#include "supersonic/base/infrastructure/variant_pointer.h"
#include "supersonic/expression/base/expression.h"
#include "supersonic/expression/infrastructure/elementary_const_expressions.h"
#include "supersonic/expression/infrastructure/expression_utils.h"
#include "supersonic/expression/infrastructure/terminal_expressions.h"
#include "supersonic/utils/strings/join.h"

namespace supersonic {

class BufferAllocator;

FailureOrVoid BasicBoundExpression::Init(rowcount_t row_capacity,
                                         BufferAllocator* allocator) {
  if (!block_.Reallocate(row_capacity)) {
    THROW(new Exception(
        ERROR_MEMORY_EXCEEDED,
        StrCat("Failed to allocate a block of capacity ",
               row_capacity,
               " with schema ",
               result_schema().GetHumanReadableSpecification())));
  }
  my_view()->ResetFrom(block_.view());
  return PostInit();
}

FailureOrOwned<BoundExpression> InitBasicExpression(
    rowcount_t row_capacity,
    BasicBoundExpression* expression,
    BufferAllocator* allocator) {
  std::unique_ptr<BasicBoundExpression> expression_ptr(expression);
  if (expression_ptr->can_be_resolved()) {
    // This expression has only constant children, meaning we can precalculate
    // it now, and replace it with the appropriate constant. Thus we will
    // need to evaluate it only once (for a single row, to get the appropriate
    // constant value) - thus we initialize it for one row.
    PROPAGATE_ON_FAILURE(expression_ptr->Init(1, allocator));
    FailureOrOwned<const Expression> resolved = ResolveToConstant(
        expression_ptr.release());
    PROPAGATE_ON_FAILURE(resolved);
    // We usually should pass the input TupleSchema at binding. We know,
    // however, that the expression we are to bind is actually constant, and
    // so does not read from the input at all - thus being oblivious to the
    // input schema. Thus, as we do not have access to the input schema here,
    // we'll just pass an empty schema. As an added bonus, we will obtain an
    // error if something goes wrong, instead of having some random expression
    // being bound to the schema.
    TupleSchema empty_schema;
    // Now expression_ptr we want to point expression_ptr to the expression we
    // want to work with later on, namely the bound constant.
    FailureOrOwned<BoundExpression> bound_constant = resolved->DoBind(
        empty_schema, allocator, row_capacity);
    PROPAGATE_ON_FAILURE(bound_constant);
    // We know a priori that bound_constant is a BasicBoundExpression, namely
    // a ConstExpression, so we can downcast safely.
    expression_ptr.reset(down_cast<BasicBoundExpression*>(
        bound_constant.release()));
    return Success(expression_ptr.release());
  }
  PROPAGATE_ON_FAILURE(expression_ptr->Init(row_capacity, allocator));
  return Success(expression_ptr.release());
}

BoundUnaryExpression::BoundUnaryExpression(const TupleSchema& schema,
                                           BufferAllocator* allocator,
                                           BoundExpression* arg,
                                           const DataType expected_arg_type)
    : BasicBoundExpression(schema, allocator),
      arg_(CHECK_NOTNULL(arg)) {
  CHECK_EQ(1, arg->result_schema().attribute_count());
  CHECK_EQ(expected_arg_type, GetExpressionType(arg));
}

BoundBinaryExpression::BoundBinaryExpression(const TupleSchema& schema,
                                             BufferAllocator* allocator,
                                             BoundExpression* left,
                                             const DataType expected_left_type,
                                             BoundExpression* right,
                                             const DataType expected_right_type)
    : BasicBoundExpression(schema, allocator),
      left_(CHECK_NOTNULL(left)),
      right_(CHECK_NOTNULL(right)) {
  CHECK_EQ(1, left->result_schema().attribute_count());
  CHECK_EQ(1, right->result_schema().attribute_count());
  CHECK_EQ(expected_left_type, GetExpressionType(left));
  CHECK_EQ(expected_right_type, GetExpressionType(right));
}

BoundTernaryExpression::BoundTernaryExpression(const TupleSchema& schema,
                                               BufferAllocator* allocator,
                                               BoundExpression* left,
                                               const DataType left_type,
                                               BoundExpression* middle,
                                               const DataType mid_type,
                                               BoundExpression* right,
                                               const DataType right_type)
    : BasicBoundExpression(schema, allocator),
      left_(CHECK_NOTNULL(left)),
      middle_(CHECK_NOTNULL(middle)),
      right_(CHECK_NOTNULL(right)) {
  CHECK_EQ(1, left->result_schema().attribute_count());
  CHECK_EQ(1, middle->result_schema().attribute_count());
  CHECK_EQ(1, right->result_schema().attribute_count());
  CHECK_EQ(left_type, GetExpressionType(left));
  CHECK_EQ(mid_type, GetExpressionType(middle));
  CHECK_EQ(right_type, GetExpressionType(right));
}

template<DataType data_type>
FailureOr<typename TypeTraits<data_type>::hold_type>
    GetConstantBoundExpressionValue(
    BoundExpression* expression, bool* is_null) {
  CHECK(expression->is_constant());
  CHECK(CheckAttributeCount("Bound expression",
                            expression->result_schema(), 1).is_success());
  TupleSchema empty_schema;
  View empty_view(empty_schema);
  empty_view.set_row_count(1);
  BoolBlock skip_vector_block(1, HeapBufferAllocator::Get());
  skip_vector_block.TryReallocate(1);
  bit_pointer::FillWithFalse(skip_vector_block.view().column(0), 1);
  EvaluationResult evaluation_result =
      expression->DoEvaluate(empty_view, skip_vector_block.view());
  PROPAGATE_ON_FAILURE(evaluation_result);
  if (*(skip_vector_block.view().column(0))) {
    *is_null = true;
  } else {
    *is_null = false;
  }
  return Success(
      *(evaluation_result.get().column(0).typed_data<data_type>()));
}

template<DataType data_type>
FailureOr<typename TypeTraits<data_type>::hold_type>
    GetVariableLengthConstantExpressionValue(
    BoundExpression* expression, bool* is_null) {
  CHECK(expression->is_constant());
  CHECK(CheckAttributeCount("Bound expression",
                            expression->result_schema(), 1).is_success());
  TupleSchema empty_schema;
  View empty_view(empty_schema);
  empty_view.set_row_count(1);
  BoolBlock skip_vector_block(1, HeapBufferAllocator::Get());
  skip_vector_block.TryReallocate(1);
  bit_pointer::FillWithFalse(skip_vector_block.view().column(0), 1);
  EvaluationResult evaluation_result =
      expression->DoEvaluate(empty_view, skip_vector_block.view());
  PROPAGATE_ON_FAILURE(evaluation_result);
  if (*(skip_vector_block.view().column(0))) {
    *is_null = true;
  } else {
    *is_null = false;
  }
  return Success(
      evaluation_result.get().column(0).typed_data<data_type>()->as_string());
}

// For STRING and BINARY, we have to specialize them
template<>
FailureOr<TypeTraits<STRING>::hold_type>
    GetConstantBoundExpressionValue<STRING>(
    BoundExpression* expression, bool* is_null) {
  return GetVariableLengthConstantExpressionValue<STRING>(expression, is_null);
}

template<>
FailureOr<TypeTraits<BINARY>::hold_type>
    GetConstantBoundExpressionValue<BINARY>(
    BoundExpression* expression, bool* is_null) {
  return GetVariableLengthConstantExpressionValue<BINARY>(expression, is_null);
}

template<DataType data_type>
FailureOr<typename TypeTraits<data_type>::hold_type> GetConstantExpressionValue(
    const Expression& expression, bool* is_null) {
  FailureOrOwned<BoundExpression> bound_expression(
      expression.DoBind(TupleSchema(), HeapBufferAllocator::Get(), 1));
  PROPAGATE_ON_FAILURE(bound_expression);
  return GetConstantBoundExpressionValue<data_type>(bound_expression.get(),
                                                    is_null);
}

// Force instantiating.
#define INSTANTIATE_EVALUATE_CONSTANT_EXPRESSION(data_type)                    \
template FailureOr<TypeTraits<data_type>::hold_type>                           \
    GetConstantBoundExpressionValue<data_type>(                                \
    BoundExpression* input_expression, bool* is_null);                         \
                                                                               \
template FailureOr<TypeTraits<data_type>::hold_type>                           \
    GetConstantExpressionValue<data_type>(                                     \
    const Expression& expression, bool* is_null)                               \

INSTANTIATE_EVALUATE_CONSTANT_EXPRESSION(INT32);
INSTANTIATE_EVALUATE_CONSTANT_EXPRESSION(INT64);
INSTANTIATE_EVALUATE_CONSTANT_EXPRESSION(UINT32);
INSTANTIATE_EVALUATE_CONSTANT_EXPRESSION(UINT64);
INSTANTIATE_EVALUATE_CONSTANT_EXPRESSION(FLOAT);
INSTANTIATE_EVALUATE_CONSTANT_EXPRESSION(DOUBLE);
INSTANTIATE_EVALUATE_CONSTANT_EXPRESSION(BOOL);
INSTANTIATE_EVALUATE_CONSTANT_EXPRESSION(DATE);
INSTANTIATE_EVALUATE_CONSTANT_EXPRESSION(DATETIME);
INSTANTIATE_EVALUATE_CONSTANT_EXPRESSION(ENUM);
INSTANTIATE_EVALUATE_CONSTANT_EXPRESSION(STRING);
INSTANTIATE_EVALUATE_CONSTANT_EXPRESSION(BINARY);
INSTANTIATE_EVALUATE_CONSTANT_EXPRESSION(DATA_TYPE);
#undef INSTANTIATE_EVALUATE_CONSTANT_EXPRESSION


namespace {

// A struct to be used with the TypedSpecialization function from
// types_infrastructure.h. See comment below.
class CreateConstantFunctor {
 public:
  explicit CreateConstantFunctor(VariantConstPointer data) : data_(data) {}
  template<DataType type> const Expression* operator()() const {
    return new ConstExpression<type>(*data_.as<type>());
  }
 private:
  const VariantConstPointer data_;
};


// A convenience function for a templatized call to new ConstExpression. This
// is preferable to a switch, as it will not require adding new code if the list
// of available DataTypes is modified.
const Expression* CreateConstantExpression(DataType type,
                                           VariantConstPointer value) {
  CreateConstantFunctor functor(value);
  return TypeSpecialization<const Expression*, CreateConstantFunctor>(
      type, functor);
}

// A helper function for ResolveToConstant, deals with an evaluated column,
// and returns a constant equal to the first value of the column.
FailureOrOwned<const Expression> ResolveColumnToConstant(
    const Column& column) {
  // If we got a null, we return a null of the appropriate type.
  if (column.is_null() != NULL && column.is_null()[0]) {
    return Success(Null(column.attribute().type()));
  } else {
    // The call to CreateConstantExpression hides types_infrastructure
    // factory magic of resolving a constant to the appropriate type.
    const Expression* constant = CreateConstantExpression(
        column.attribute().type(), column.data());
    return Success(constant);
  }
}

}  // namespace

// We evaluate the input expression, extract the value it evaluated to, and
// create a constant expression of the appropriate type and value.
// Takes ownership of the bound expression.
FailureOrOwned<const Expression> ResolveToConstant(
    BoundExpression* expression_ptr) {
  small_bool_array skip_array;
  *(skip_array.mutable_data()) = false;
  std::unique_ptr<BoundExpression> expression(expression_ptr);
  // We evaluate the expression with an empty schema. This way we will get an
  // error (instead of random results) if the expression demands any input.
  TupleSchema schema;
  View view(schema);  // A view with no columns.
  view.set_row_count(1);  // We want a one-row output, we give a one-row input.
  BoolView skip_pointers(skip_array.mutable_data());
  skip_pointers.set_row_count(1);
  EvaluationResult result =
      expression->DoEvaluate(view, skip_pointers);

  PROPAGATE_ON_FAILURE_WITH_CONTEXT(
      result,
      StringPrintf(
          "Exception while pre-evaluating a constant subtree [%s]",
          expression->result_schema().GetHumanReadableSpecification().c_str()),
      "");
  // Can't be greater than one, we set the row_count. If it is smaller
  // (i.e. 0), it means that some constant expression evaluates in a rather
  // unexpected way.
  if (result.get().row_count() != 1) {
    // This could possibly happen for some weird expressions, and probably is
    // a bug.
    string message = StrCat(
        "Row evaluation while precalculating the constant expression ",
        expression->result_schema().GetHumanReadableSpecification(),
        ", got ", result.get().row_count(), " rows while expecting one. ",
        "Probably a bug, contact the Supersonic team.");
    THROW(new Exception(ERROR_EVALUATION_ERROR, message));
  }
  PROPAGATE_ON_FAILURE(
      CheckAttributeCount(
          expression->result_schema().GetHumanReadableSpecification(),
          expression->result_schema(), 1));
  return ResolveColumnToConstant(result.get().column(0));
}

}  // namespace supersonic
