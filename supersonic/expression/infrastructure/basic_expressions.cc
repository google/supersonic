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

#include "supersonic/expression/infrastructure/basic_expressions.h"

#include <string>
namespace supersonic {using std::string; }

#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/expression/infrastructure/bound_expression_creators.h"
#include "supersonic/expression/infrastructure/expression_utils.h"
#include "supersonic/utils/strings/substitute.h"

namespace supersonic {

class BufferAllocator;
class TupleSchema;

namespace {

class DelegatingUnaryExpression : public UnaryExpression {
 public:
  DelegatingUnaryExpression(
      const Expression* const child,
      const BoundUnaryExpressionFactory bound_expression_factory,
      const string& description_pattern)
      : UnaryExpression(child),
        bound_expression_factory_(bound_expression_factory),
        description_pattern_(description_pattern) {}

  virtual ~DelegatingUnaryExpression() {}

  virtual string ToString(bool verbose) const {
    return strings::Substitute(description_pattern_,
                               child_expression_->ToString(verbose));
  }

 private:
  virtual FailureOrOwned<BoundExpression> CreateBoundUnaryExpression(
      const TupleSchema& input_schema,
      BufferAllocator* const allocator,
      rowcount_t row_capacity,
      BoundExpression* child) const {
    return bound_expression_factory_(child, allocator, row_capacity);
  }

  const BoundUnaryExpressionFactory bound_expression_factory_;
  const string description_pattern_;
  DISALLOW_COPY_AND_ASSIGN(DelegatingUnaryExpression);
};


class DelegatingBinaryExpression : public BinaryExpression {
 public:
  DelegatingBinaryExpression(
      const Expression* const left,
      const Expression* const right,
      const BoundBinaryExpressionFactory bound_expression_factory,
      const string& description_pattern)
      : BinaryExpression(left, right),
        bound_expression_factory_(bound_expression_factory),
        description_pattern_(description_pattern) {}

  virtual ~DelegatingBinaryExpression() {}

  virtual string ToString(bool verbose) const {
    return strings::Substitute(description_pattern_,
                               left_->ToString(verbose),
                               right_->ToString(verbose));
  }

 private:
  virtual FailureOrOwned<BoundExpression> CreateBoundBinaryExpression(
      const TupleSchema& input_schema,
      BufferAllocator* const allocator,
      rowcount_t row_capacity,
      BoundExpression* left,
      BoundExpression* right) const {
    return bound_expression_factory_(left, right, allocator, row_capacity);
  }

  const BoundBinaryExpressionFactory bound_expression_factory_;
  const string description_pattern_;
  DISALLOW_COPY_AND_ASSIGN(DelegatingBinaryExpression);
};

class DelegatingTernaryExpression : public TernaryExpression {
 public:
  DelegatingTernaryExpression(
      const Expression* const left,
      const Expression* const middle,
      const Expression* const right,
      const BoundTernaryExpressionFactory bound_expression_factory,
      const string& description_pattern)
      : TernaryExpression(left, middle, right),
        bound_expression_factory_(bound_expression_factory),
        description_pattern_(description_pattern) {}

  virtual ~DelegatingTernaryExpression() {}

  virtual string ToString(bool verbose) const {
    return strings::Substitute(description_pattern_,
                               left_->ToString(verbose),
                               middle_->ToString(verbose),
                               right_->ToString(verbose));
  }

 private:
  virtual FailureOrOwned<BoundExpression> CreateBoundTernaryExpression(
      const TupleSchema& input_schema,
      BufferAllocator* const allocator,
      rowcount_t row_capacity,
      BoundExpression* left,
      BoundExpression* middle,
      BoundExpression* right) const {
    return bound_expression_factory_(left, middle, right, allocator,
                                     row_capacity);
  }

  const BoundTernaryExpressionFactory bound_expression_factory_;
  const string description_pattern_;
  DISALLOW_COPY_AND_ASSIGN(DelegatingTernaryExpression);
};

}  // namespace

// ----------------------------------------------------------------------------
// Internal building blocks.

FailureOrOwned<BoundExpression> UnaryExpression::DoBind(
    const TupleSchema& input_schema,
    BufferAllocator* allocator,
    rowcount_t max_row_count) const {
  FailureOrOwned<BoundExpression> bound_child =
      child_expression_->DoBind(input_schema, allocator, max_row_count);
  PROPAGATE_ON_FAILURE(bound_child);
  PROPAGATE_ON_FAILURE(CheckAttributeCount(
      "An operand of unary expression",
      bound_child->result_schema(), 1));
  return CreateBoundUnaryExpression(
      input_schema, allocator, max_row_count, bound_child.release());
}

FailureOrOwned<BoundExpression> BinaryExpression::DoBind(
    const TupleSchema& input_schema,
    BufferAllocator* allocator,
    rowcount_t max_row_count) const {
  FailureOrOwned<BoundExpression> bound_left =
      left_->DoBind(input_schema, allocator, max_row_count);
  PROPAGATE_ON_FAILURE(bound_left);
  FailureOrOwned<BoundExpression> bound_right =
      right_->DoBind(input_schema, allocator, max_row_count);
  PROPAGATE_ON_FAILURE(bound_right);
  PROPAGATE_ON_FAILURE(CheckAttributeCount(
      "A left operand of a binary expression",
      bound_left->result_schema(), 1));
  PROPAGATE_ON_FAILURE(CheckAttributeCount(
      "A right operand of a binary expression",
      bound_right->result_schema(), 1));
  return CreateBoundBinaryExpression(
      input_schema, allocator, max_row_count,
      bound_left.release(), bound_right.release());
}

FailureOrOwned<BoundExpression> TernaryExpression::DoBind(
    const TupleSchema& input_schema,
    BufferAllocator* allocator,
    rowcount_t max_row_count) const {
  FailureOrOwned<BoundExpression> bound_left =
      left_->DoBind(input_schema, allocator, max_row_count);
  PROPAGATE_ON_FAILURE(bound_left);
  FailureOrOwned<BoundExpression> bound_middle =
      middle_->DoBind(input_schema, allocator, max_row_count);
  PROPAGATE_ON_FAILURE(bound_middle);
  FailureOrOwned<BoundExpression> bound_right =
      right_->DoBind(input_schema, allocator, max_row_count);
  PROPAGATE_ON_FAILURE(bound_right);
  PROPAGATE_ON_FAILURE(CheckAttributeCount(
      "A left operand of a ternary expression",
      bound_left->result_schema(), 1));
  PROPAGATE_ON_FAILURE(CheckAttributeCount(
      "A middle operand of a ternary expression",
      bound_middle->result_schema(), 1));
  PROPAGATE_ON_FAILURE(CheckAttributeCount(
      "A right operand of a ternary expression",
      bound_right->result_schema(), 1));
  return CreateBoundTernaryExpression(
      input_schema, allocator, max_row_count, bound_left.release(),
      bound_middle.release(), bound_right.release());
}

Expression* CreateExpressionForExistingBoundFactory(
    const Expression* const child,
    const BoundUnaryExpressionFactory bound_expression_factory,
    const string& description_pattern) {
  return new DelegatingUnaryExpression(child, bound_expression_factory,
                                       description_pattern);
}

Expression* CreateExpressionForExistingBoundFactory(
    const Expression* const left,
    const Expression* const right,
    const BoundBinaryExpressionFactory bound_expression_factory,
    const string& description_pattern) {
  return new DelegatingBinaryExpression(left, right, bound_expression_factory,
                                        description_pattern);
}

Expression* CreateExpressionForExistingBoundFactory(
    const Expression* const left,
    const Expression* const middle,
    const Expression* const right,
    const BoundTernaryExpressionFactory bound_expression_factory,
    const string& description_pattern) {
  return new DelegatingTernaryExpression(left, middle, right,
                                         bound_expression_factory,
                                         description_pattern);
}

}  // namespace supersonic
