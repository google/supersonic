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

#include "supersonic/expression/core/arithmetic_bound_expressions.h"

#include "supersonic/expression/base/expression.h"
#include "supersonic/expression/proto/operators.pb.h"
#include "supersonic/expression/templated/bound_expression_factory.h"
#include "supersonic/proto/supersonic.pb.h"

namespace supersonic {

class BufferAllocator;

FailureOrOwned<BoundExpression> BoundNegate(BoundExpression* arg,
                                            BufferAllocator* allocator,
                                            rowcount_t max_row_count) {
  return CreateUnarySignedNumericExpression<OPERATOR_NEGATE>(allocator,
      max_row_count, arg);
}

#define DEFINE_BINARY_NUMERIC_EXPRESSION(expression_name, operator_name)      \
FailureOrOwned<BoundExpression> expression_name(BoundExpression* left,        \
                                                BoundExpression* right,       \
                                                BufferAllocator* allocator,   \
                                                rowcount_t max_row_count) {   \
  return CreateBinaryNumericExpression<operator_name>(                        \
    allocator, max_row_count, left, right);                                   \
}

DEFINE_BINARY_NUMERIC_EXPRESSION(BoundPlus, OPERATOR_ADD);
DEFINE_BINARY_NUMERIC_EXPRESSION(BoundMultiply, OPERATOR_MULTIPLY);
DEFINE_BINARY_NUMERIC_EXPRESSION(BoundMinus, OPERATOR_SUBTRACT);

FailureOrOwned<BoundExpression> BoundDivideSignaling(
    BoundExpression* left,
    BoundExpression* right,
    BufferAllocator* allocator,
    rowcount_t max_row_count) {
  return CreateTypedBoundBinaryExpression<OPERATOR_DIVIDE_SIGNALING,
      DOUBLE, DOUBLE, DOUBLE>(allocator, max_row_count, left, right);
}

FailureOrOwned<BoundExpression> BoundDivideNulling(
    BoundExpression* left,
    BoundExpression* right,
    BufferAllocator* allocator,
    rowcount_t max_row_count) {
  return CreateTypedBoundBinaryExpression<OPERATOR_DIVIDE_NULLING,
      DOUBLE, DOUBLE, DOUBLE>(allocator, max_row_count, left, right);
}

FailureOrOwned<BoundExpression> BoundDivideQuiet(
    BoundExpression* left,
    BoundExpression* right,
    BufferAllocator* allocator,
    rowcount_t max_row_count) {
  return CreateTypedBoundBinaryExpression<OPERATOR_DIVIDE_QUIET,
      DOUBLE, DOUBLE, DOUBLE>(allocator, max_row_count, left, right);
}

DEFINE_BINARY_NUMERIC_EXPRESSION(BoundCppDivideSignaling,
                                 OPERATOR_CPP_DIVIDE_SIGNALING);
DEFINE_BINARY_NUMERIC_EXPRESSION(BoundCppDivideNulling,
                                 OPERATOR_CPP_DIVIDE_NULLING);

FailureOrOwned<BoundExpression> BoundModulusSignaling(
    BoundExpression* left,
    BoundExpression* right,
    BufferAllocator* allocator,
    rowcount_t max_row_count) {
  return CreateBinaryIntegerExpression<OPERATOR_MODULUS_SIGNALING>(
      allocator, max_row_count, left, right);
}

FailureOrOwned<BoundExpression> BoundModulusNulling(
    BoundExpression* left,
    BoundExpression* right,
    BufferAllocator* allocator,
    rowcount_t max_row_count) {
  return CreateBinaryIntegerExpression<OPERATOR_MODULUS_NULLING>(
      allocator, max_row_count, left, right);
}

#undef DEFINE_BINARY_NUMERIC_EXPRESSION

}  // namespace supersonic
