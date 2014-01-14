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

#include "supersonic/expression/core/elementary_expressions.h"

#include <stddef.h>
#include <string>
namespace supersonic {using std::string; }

#include "supersonic/utils/macros.h"
#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/utils/stringprintf.h"
#include "supersonic/utils/exception/failureor.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/expression/base/expression.h"
#include "supersonic/expression/core/elementary_bound_expressions.h"
#include "supersonic/expression/infrastructure/basic_expressions.h"
#include "supersonic/expression/infrastructure/expression_utils.h"
#include "supersonic/expression/proto/operators.pb.h"
#include "supersonic/expression/templated/abstract_expressions.h"
#include "supersonic/expression/templated/bound_expression_factory.h"
#include "supersonic/expression/templated/cast_expression.h"
#include "supersonic/expression/vector/expression_traits.h"

namespace supersonic {

class BufferAllocator;
class TupleSchema;

namespace {

// ----------------------------------------------------------------------------
// Unary expressions.
//

// A separate class for expressions that only transform the input from one type
// into another. They are bound in a different than usual way - if the
// from_type and to_type are equal, we simply pass the source expression along.
// This, along with the need to store the to_type somewhere, warrants a
// separate class for the Cast Expression (note that there is no separate
// class for the BoundChangeTypeExpression).
template<OperatorId op>
class ChangeTypeExpression : public UnaryExpression {
 public:
  ChangeTypeExpression(DataType type, const Expression* const source)
      : UnaryExpression(source),
        to_type_(type) {}
  virtual ~ChangeTypeExpression() {}

  virtual string ToString(bool verbose) const {
    return UnaryExpressionTraits<op>::FormatDescription(
        child_expression_->ToString(verbose), to_type_);
  }

 private:
  virtual FailureOrOwned<BoundExpression> CreateBoundUnaryExpression(
      const TupleSchema& input_schema,
      BufferAllocator* const allocator,
      rowcount_t row_capacity,
      BoundExpression* child) const;

  DataType to_type_;
  DISALLOW_COPY_AND_ASSIGN(ChangeTypeExpression);
};

template<>
FailureOrOwned<BoundExpression>
ChangeTypeExpression<OPERATOR_CAST_QUIET>::CreateBoundUnaryExpression(
    const TupleSchema& input_schema,
    BufferAllocator* const allocator,
    rowcount_t row_capacity,
    BoundExpression* child) const {
  DataType from_type = GetExpressionType(child);
  if (from_type == to_type_) return Success(child);
  return CreateUnaryNumericExpression<OPERATOR_CAST_QUIET>(allocator,
      row_capacity, child, to_type_);
}

template<>
FailureOrOwned<BoundExpression>
ChangeTypeExpression<OPERATOR_PARSE_STRING_QUIET>::CreateBoundUnaryExpression(
    const TupleSchema& input_schema,
    BufferAllocator* const allocator,
    rowcount_t row_capacity,
    BoundExpression* child) const {
  return BoundParseStringQuiet(to_type_, child, allocator, row_capacity);
}

template<>
FailureOrOwned<BoundExpression>
ChangeTypeExpression<OPERATOR_PARSE_STRING_NULLING>::CreateBoundUnaryExpression(
    const TupleSchema& input_schema,
    BufferAllocator* const allocator,
    rowcount_t row_capacity,
    BoundExpression* child) const {
  return BoundParseStringNulling(to_type_, child, allocator, row_capacity);
}

// ----------------------------------------------------------------------------
// Multi-parameter expressions. The reason this exists is that the logic to
// check the argument number is even needs to live somewhere, and it's better to
// fail at expression creation time (as all other expressions fail) instead of
// failing at binding time.
class CaseExpression : public Expression {
 public:
  explicit CaseExpression(const ExpressionList* const arguments)
      : arguments_(arguments) {}

  virtual FailureOrOwned<BoundExpression> DoBind(
      const TupleSchema& input_schema,
      BufferAllocator* allocator,
      rowcount_t max_row_count) const {
    // Bind all arguments.
    FailureOrOwned<BoundExpressionList> bound_arguments = arguments_->DoBind(
        input_schema, allocator, max_row_count);
    PROPAGATE_ON_FAILURE(bound_arguments);
    size_t num_arguments = bound_arguments->size();

    if (num_arguments < 4) {
      THROW(new Exception(
          ERROR_ATTRIBUTE_COUNT_MISMATCH,
          StringPrintf("Bind failed: "
                       "CASE needs at least 4 arguments (%zd provided).",
                       num_arguments)));
    }

    if (num_arguments % 2 != 0) {
      THROW(new Exception(
          ERROR_ATTRIBUTE_COUNT_MISMATCH,
          StringPrintf("Bind failed: "
                       "CASE needs even number of arguments (%zd provided). "
                       "Maybe you forgot the ELSE argument?",
                       num_arguments)));
    }

    return BoundCase(bound_arguments.release(), allocator, max_row_count);
  }

  virtual string ToString(bool verbose) const {
    return StringPrintf("CASE(%s)", arguments_->ToString(verbose).c_str());
  }

 private:
  scoped_ptr<const ExpressionList> const arguments_;
  DISALLOW_COPY_AND_ASSIGN(CaseExpression);
};

}  // namespace

// ----------------------------------------------------------------------------
// Expressions instantiation:
const Expression* CastTo(DataType to_type, const Expression* const child) {
  return InternalCast(to_type, child, false);
}

const Expression* ParseStringQuiet(DataType to_type,
                                   const Expression* const child) {
  return new ChangeTypeExpression<OPERATOR_PARSE_STRING_QUIET>(to_type, child);
}

const Expression* ParseStringNulling(DataType to_type,
                                     const Expression* const child) {
  return new ChangeTypeExpression<OPERATOR_PARSE_STRING_NULLING>(to_type,
                                                                 child);
}

const Expression* Case(const ExpressionList* const arguments) {
  return new CaseExpression(arguments);
}

const Expression* Not(const Expression* const argument) {
  return CreateExpressionForExistingBoundFactory(
      argument, &BoundNot, "(NOT $0)");
}

const Expression* And(const Expression* const left,
                      const Expression* const right) {
  return CreateExpressionForExistingBoundFactory(
      left, right, &BoundAnd, "($0 AND $1)");
}

const Expression* Xor(const Expression* const left,
                      const Expression* const right) {
  return CreateExpressionForExistingBoundFactory(
      left, right, &BoundXor, "($0 XOR $1)");
}

const Expression* Or(const Expression* const left,
                     const Expression* const right) {
  return CreateExpressionForExistingBoundFactory(
      left, right, &BoundOr, "($0 OR $1)");
}

const Expression* AndNot(const Expression* const left,
                         const Expression* const right) {
  return CreateExpressionForExistingBoundFactory(
      left, right, &BoundAndNot, "$0 !&& $1");
}

const Expression* If(const Expression* const condition,
                     const Expression* const then,
                     const Expression* const otherwise) {
  return CreateExpressionForExistingBoundFactory(
      condition, then, otherwise, &BoundIf, "IF $0 THEN $1 ELSE $2");
}

const Expression* NullingIf(const Expression* const condition,
                            const Expression* const then,
                            const Expression* const otherwise) {
  return CreateExpressionForExistingBoundFactory(
      condition, then, otherwise, &BoundIfNulling, "IF $0 THEN $1 ELSE $2");
}

const Expression* IsNull(const Expression* const expression) {
  return CreateExpressionForExistingBoundFactory(
      expression, &BoundIsNull, "ISNULL($0)");
}

const Expression* IfNull(const Expression* const expression,
                         const Expression* const substitute) {
  return CreateExpressionForExistingBoundFactory(
      expression, substitute, &BoundIfNull, "IFNULL($0, $1)");
}

const Expression* BitwiseNot(const Expression* const argument) {
  return CreateExpressionForExistingBoundFactory(
      argument, &BoundBitwiseNot, "(~$0)");
}

const Expression* BitwiseAnd(const Expression* const a,
                             const Expression* const b) {
  return CreateExpressionForExistingBoundFactory(
      a, b, &BoundBitwiseAnd, "($0 & $1)");
}

const Expression* BitwiseAndNot(const Expression* const a,
                                const Expression* const b) {
  return CreateExpressionForExistingBoundFactory(
      a, b, &BoundBitwiseAndNot, "(~$0 & $1)");
}

const Expression* BitwiseOr(const Expression* const a,
                            const Expression* const b) {
  return CreateExpressionForExistingBoundFactory(
      a, b, &BoundBitwiseOr, "($0 | $1)");
}

const Expression* BitwiseXor(const Expression* const a,
                             const Expression* const b) {
  return CreateExpressionForExistingBoundFactory(
      a, b, &BoundBitwiseXor, "($0 ^ $1)");
}

const Expression* ShiftLeft(const Expression* const argument,
                            const Expression* const shift) {
  return CreateExpressionForExistingBoundFactory(
      argument, shift, &BoundShiftLeft, "($0 << $1)");
}

const Expression* ShiftRight(const Expression* const argument,
                             const Expression* const shift) {
  return CreateExpressionForExistingBoundFactory(
      argument, shift, &BoundShiftRight, "($0 >> $1)");
}

}  // namespace supersonic
