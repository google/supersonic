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
// Author: onufry@google.com (Jakub Onufry Wojtaszczyk)
// Author: ptab@google.com (Piotr Tabor)
//
// Basic expression types by arity.

#ifndef SUPERSONIC_EXPRESSION_INFRASTRUCTURE_BASIC_EXPRESSIONS_H_
#define SUPERSONIC_EXPRESSION_INFRASTRUCTURE_BASIC_EXPRESSIONS_H_

#include <memory>
#include <string>
namespace supersonic {using std::string; }

#include "supersonic/utils/macros.h"
#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/expression/base/expression.h"
#include "supersonic/expression/infrastructure/bound_expression_creators.h"

namespace supersonic {

// ----------------------------------------------------------------------------
// Internal building blocks.

// Base class for expressions taking a single child expression.
// It will bind it's child and call CreateBoundUnaryExpression to bind the
// expression itself.
class BufferAllocator;
class TupleSchema;

class UnaryExpression : public Expression {
 public:
  virtual ~UnaryExpression() {}

  virtual FailureOrOwned<BoundExpression> DoBind(
      const TupleSchema& input_schema,
      BufferAllocator* allocator,
      rowcount_t max_row_count) const;

 protected:
  explicit UnaryExpression(const Expression* const child_expression)
      : child_expression_(child_expression) {
  }

 protected:
  std::unique_ptr<const Expression> child_expression_;

 private:
  // Should bind expression knowing that its child is already bound.
  // The returned expression should be ready to use.
  virtual FailureOrOwned<BoundExpression> CreateBoundUnaryExpression(
      const TupleSchema& input_schema,
      BufferAllocator* const allocator,
      rowcount_t row_capacity,
      BoundExpression* child) const = 0;

  DISALLOW_COPY_AND_ASSIGN(UnaryExpression);
};

// Base class for binary expressions with simple (one-attribute) children.
// It will bind it's children and call CreateBoundBinaryExpression to bind
// expression itself.
class BinaryExpression : public Expression {
 public:
  virtual ~BinaryExpression() {}

  virtual FailureOrOwned<BoundExpression> DoBind(
      const TupleSchema& input_schema,
      BufferAllocator* allocator,
      rowcount_t max_row_count) const;

 protected:
  BinaryExpression(const Expression* const left, const Expression* const right)
      : left_(left),
        right_(right) {
  }

  std::unique_ptr<const Expression> left_;
  std::unique_ptr<const Expression> right_;

 private:
  // Should bind expression knowing that its childern are already bound.
  // The returned expression should be ready to use.
  virtual FailureOrOwned<BoundExpression> CreateBoundBinaryExpression(
      const TupleSchema& input_schema,
      BufferAllocator* const allocator,
      rowcount_t row_capacity,
      BoundExpression* left,
      BoundExpression* right) const = 0;

  DISALLOW_COPY_AND_ASSIGN(BinaryExpression);
};

// Base class for ternary expressions with simple (one-attribute) children.
// It will bind it's children and call CreateBoundTernaryExpression to bind
// expression itself.
class TernaryExpression : public Expression {
 public:
  virtual ~TernaryExpression() {}

  virtual FailureOrOwned<BoundExpression> DoBind(
      const TupleSchema& input_schema,
      BufferAllocator* allocator,
      rowcount_t max_row_count) const;

 protected:
  TernaryExpression(const Expression* const left,
                    const Expression* const middle,
                    const Expression* const right)
      : left_(left),
        middle_(middle),
        right_(right) {
  }
  std::unique_ptr<const Expression> left_;
  std::unique_ptr<const Expression> middle_;
  std::unique_ptr<const Expression> right_;

 private:
  // Should bind expression knowing that its childern are already bound.
  // The returned expression should be ready to use.
  virtual FailureOrOwned<BoundExpression> CreateBoundTernaryExpression(
      const TupleSchema& input_schema,
      BufferAllocator* const allocator,
      rowcount_t row_capacity,
      BoundExpression* left,
      BoundExpression* middle,
      BoundExpression* right) const = 0;

  DISALLOW_COPY_AND_ASSIGN(TernaryExpression);
};

// The methods below create an expression that delegates binding to the given
// bound expression factory method.
// It is a common case that expression might be defined as combination of
// other expressions. In such a situation a bound factory method need to be
// defined that uses the other bound factory methods for composition. If you
// need to define the (not bound) expression that describes such an operation,
// the methods do the job for you.

// In the description_pattern "$0" will be replaced with description of the
// child (ex. "FINGERPRINT($0)").
Expression* CreateExpressionForExistingBoundFactory(
    const Expression* const child,
    BoundUnaryExpressionFactory bound_expression_factory,
    const string& description_pattern);

// In the description_pattern "$0" and "$1" will be replaced with description of
// the left and right child (ex. "CONTAINS($0, $1)").
Expression* CreateExpressionForExistingBoundFactory(
    const Expression* const left,
    const Expression* const right,
    BoundBinaryExpressionFactory bound_expression_factory,
    const string& description_pattern);

// In the description_pattern "$0", "$1" and "$2" will be replaced with the
// descriptions of the left, middle and right child respectively.
Expression* CreateExpressionForExistingBoundFactory(
    const Expression* const left,
    const Expression* const middle,
    const Expression* const right,
    BoundTernaryExpressionFactory bound_expression_factory,
    const string& description_pattern);

}  // namespace supersonic

#endif  // SUPERSONIC_EXPRESSION_INFRASTRUCTURE_BASIC_EXPRESSIONS_H_
