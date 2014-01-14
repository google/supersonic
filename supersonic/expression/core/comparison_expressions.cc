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
// Author:  onufry@google.com (Onufry Wojtaszczyk)

#include "supersonic/expression/core/comparison_expressions.h"

#include <string>
namespace supersonic {using std::string; }
#include <vector>
using std::vector;

#include "supersonic/utils/macros.h"
#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/utils/stringprintf.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/base/infrastructure/types_infrastructure.h"
#include "supersonic/expression/base/expression.h"
#include "supersonic/expression/core/comparison_bound_expressions.h"
#include "supersonic/expression/infrastructure/basic_expressions.h"
#include "supersonic/expression/infrastructure/expression_utils.h"
#include "supersonic/expression/templated/cast_bound_expression.h"
#include "supersonic/proto/supersonic.pb.h"

namespace supersonic {

class BufferAllocator;

namespace {
// The in expression set expression, extending expression. Similar to Concat,
// this doesn't fit into the general scheme of abstract_expressions.h as it has
// an arbitrary number of arguments.
class InExpressionSetExpression : public Expression {
 public:
  InExpressionSetExpression(const Expression* const needle_expression,
                            const ExpressionList* const haystack_arguments)
      : needle_expression_(needle_expression),
        haystack_arguments_(haystack_arguments) {}

 private:
  virtual FailureOrOwned<BoundExpression> DoBind(
      const TupleSchema& input_schema,
      BufferAllocator* allocator,
      rowcount_t max_row_count) const {
    FailureOrOwned<BoundExpression> bound_needle =
        needle_expression_->DoBind(input_schema, allocator, max_row_count);
    PROPAGATE_ON_FAILURE(bound_needle);
    FailureOrOwned<BoundExpressionList> bound_haystack =
        haystack_arguments_->DoBind(input_schema, allocator, max_row_count);
    PROPAGATE_ON_FAILURE(bound_haystack);
    return BoundInSet(bound_needle.release(),
                      bound_haystack.release(),
                      allocator,
                      max_row_count);
  }

  virtual string ToString(bool verbose) const {
    return StrCat(needle_expression_->ToString(verbose),
                  " IN (",
                  haystack_arguments_->ToString(verbose),
                  ")");
  }

  const scoped_ptr<const Expression> needle_expression_;
  const scoped_ptr<const ExpressionList> haystack_arguments_;
};
}  // namespace

const Expression* In(const Expression* const needle_expression,
                     const ExpressionList* haystack_arguments) {
  return new InExpressionSetExpression(needle_expression, haystack_arguments);
}

const Expression* IsOdd(const Expression* const arg) {
  return CreateExpressionForExistingBoundFactory(
      arg, &BoundIsOdd, "IS_ODD($0)");
}

const Expression* IsEven(const Expression* const arg) {
  return CreateExpressionForExistingBoundFactory(
      arg, &BoundIsEven, "IS_EVEN($0)");
}

const Expression* NotEqual(const Expression* const left,
                           const Expression* const right) {
  return CreateExpressionForExistingBoundFactory(
      left, right, &BoundNotEqual, "$0 <> $1");
}

const Expression* Equal(const Expression* const left,
                        const Expression* const right) {
  return CreateExpressionForExistingBoundFactory(
      left, right, &BoundEqual, "$0 == $1");
}

const Expression* Greater(const Expression* const left,
                          const Expression* const right) {
  return CreateExpressionForExistingBoundFactory(
      left, right, &BoundGreater, "$0 > $1");
}

const Expression* GreaterOrEqual(const Expression* const left,
                                 const Expression* const right) {
  return CreateExpressionForExistingBoundFactory(
      left, right, &BoundGreaterOrEqual, "$0 >= $1");
}

const Expression* Less(const Expression* const left,
                       const Expression* const right) {
  return CreateExpressionForExistingBoundFactory(
      left, right, &BoundLess, "$0 < $1");
}

const Expression* LessOrEqual(const Expression* const left,
                              const Expression* const right) {
  return CreateExpressionForExistingBoundFactory(
      left, right, &BoundLessOrEqual, "$0 <= $1");
}
}  // namespace supersonic
