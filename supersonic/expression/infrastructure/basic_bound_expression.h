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
//
// Base classes for bound expressions.

#ifndef SUPERSONIC_EXPRESSION_INFRASTRUCTURE_BASIC_BOUND_EXPRESSION_H_
#define SUPERSONIC_EXPRESSION_INFRASTRUCTURE_BASIC_BOUND_EXPRESSION_H_

#include <algorithm>
#include "supersonic/utils/std_namespace.h"
#include <memory>
#include <set>
#include "supersonic/utils/std_namespace.h"
#include <string>
namespace supersonic {using std::string; }

#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/macros.h"
#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/utils/exception/failureor.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/expression/base/expression.h"
#include "supersonic/expression/infrastructure/expression_utils.h"
#include "supersonic/proto/supersonic.pb.h"

namespace supersonic {

// A convenience base class for bound expressions that need to pre-allocate a
// block for their results.
class BoolView;
class BufferAllocator;
class TupleSchema;

class BasicBoundExpression : public BoundExpression {
 public:
  ~BasicBoundExpression() {}

  // Must be called before the expression is first evaluated (normally during
  // binding).
  FailureOrVoid Init(rowcount_t row_capacity, BufferAllocator* allocator);

  virtual rowcount_t row_capacity() const {
    return my_const_block()->row_capacity();
  };

  // A function that checks whether all the children are constant. If so,
  // a precalculation of constants will be performed at Init-time.
  virtual bool can_be_resolved() const = 0;

 protected:
  explicit BasicBoundExpression(const TupleSchema& result_schema,
                                BufferAllocator* const allocator)
      : BoundExpression(result_schema),
        block_(result_schema, allocator) {}

  Block* my_block() { return &block_; }
  const Block* my_const_block() const { return &block_; }

 private:
  // Subclasses can override, to perform post-initialization actions.
  // Note that both the capacity and the allocator can be accessed in this
  // method through my_block().
  virtual FailureOrVoid PostInit() { return Success(); }

  Block block_;
  DISALLOW_COPY_AND_ASSIGN(BasicBoundExpression);
};

class BasicBoundNoArgumentExpression : public BasicBoundExpression {
 protected:
  explicit BasicBoundNoArgumentExpression(const TupleSchema& result_schema,
                                          BufferAllocator* const allocator)
      : BasicBoundExpression(result_schema, allocator) {}

  // This may be surprising. But we do not want to resolve constant expressions
  // - as we resolve to constant expressions this would cause an infinite loop.
  bool can_be_resolved() const { return false; }

  virtual void CollectReferredAttributeNames(
        set<string>* referred_attribute_names) const {}

 private:
  DISALLOW_COPY_AND_ASSIGN(BasicBoundNoArgumentExpression);
};

class BasicBoundConstExpression : public BasicBoundNoArgumentExpression {
 protected:
  explicit BasicBoundConstExpression(const TupleSchema& result_schema,
                                     BufferAllocator* const allocator)
      : BasicBoundNoArgumentExpression(result_schema, allocator) {}

  // Returns a pre-initialized pointer.
  virtual EvaluationResult DoEvaluate(const View& input,
                                      const BoolView& skip_vectors) {
    DCHECK_LE(input.row_count(), my_block()->row_capacity());
    my_view()->set_row_count(input.row_count());
    return Success(*my_view());
  }

  bool is_constant() const { return true; }

 private:
  DISALLOW_COPY_AND_ASSIGN(BasicBoundConstExpression);
};

// Base class for expressions that have one child, with a simple (one-attribute)
// result.
class BoundUnaryExpression : public BasicBoundExpression {
 public:
  BoundUnaryExpression(const TupleSchema& schema,
                       BufferAllocator* allocator,
                       BoundExpression* arg,
                       const DataType expected_arg_type);
  virtual ~BoundUnaryExpression() {}

  virtual rowcount_t row_capacity() const {
    return std::min(my_const_block()->row_capacity(), arg_->row_capacity());
  }

  virtual bool can_be_resolved() const { return argument()->is_constant(); }

  virtual void CollectReferredAttributeNames(
        set<string>* referred_attribute_names) const {
    arg_->CollectReferredAttributeNames(referred_attribute_names);
  }

 protected:
  BoundExpression* const argument() const { return arg_.get(); }

 private:
  const std::unique_ptr<BoundExpression> arg_;
  DISALLOW_COPY_AND_ASSIGN(BoundUnaryExpression);
};

// Base class for expressions that have two children, with a simple
// (one-attribute) result.
class BoundBinaryExpression : public BasicBoundExpression {
 public:
  BoundBinaryExpression(const TupleSchema& schema,
                       BufferAllocator* allocator,
                       BoundExpression* left,
                       const DataType expected_left_type,
                       BoundExpression* right,
                       const DataType expected_right_type);
  virtual ~BoundBinaryExpression() {}

  virtual rowcount_t row_capacity() const {
    return std::min(std::min(left_->row_capacity(), right_->row_capacity()),
                    my_const_block()->row_capacity());
  }

  virtual bool can_be_resolved() const {
    return left()->is_constant() && right()->is_constant();
  }

  virtual void CollectReferredAttributeNames(
      set<string>* referred_attribute_names) const {
    left_->CollectReferredAttributeNames(referred_attribute_names);
    right_->CollectReferredAttributeNames(referred_attribute_names);
  }

 protected:
  BoundExpression* const left() const { return left_.get(); }
  BoundExpression* const right() const { return right_.get(); }

 private:
  const std::unique_ptr<BoundExpression> left_;
  const std::unique_ptr<BoundExpression> right_;
  DISALLOW_COPY_AND_ASSIGN(BoundBinaryExpression);
};

// Base class for expressions that have three children, with a simple result.
// Does not perform type checking upon creation.
class BoundTernaryExpression : public BasicBoundExpression {
 public:
  BoundTernaryExpression(const TupleSchema& schema,
                         BufferAllocator* allocator,
                         BoundExpression* left,
                         const DataType left_type,
                         BoundExpression* middle,
                         const DataType middle_type,
                         BoundExpression* right,
                         const DataType right_type);
  virtual ~BoundTernaryExpression() {}

  virtual rowcount_t row_capacity() const {
    return std::min(std::min(my_const_block()->row_capacity(),
                             left_->row_capacity()),
                    std::min(middle_->row_capacity(), right_->row_capacity()));
  }

  virtual bool can_be_resolved() const {
    return left_->is_constant() && middle_->is_constant()
        && right_->is_constant();
  }

  virtual void CollectReferredAttributeNames(
      set<string>* referred_attribute_names) const {
    left_->CollectReferredAttributeNames(referred_attribute_names);
    middle_->CollectReferredAttributeNames(referred_attribute_names);
    right_->CollectReferredAttributeNames(referred_attribute_names);
  }

 protected:
  const std::unique_ptr<BoundExpression> left_;
  const std::unique_ptr<BoundExpression> middle_;
  const std::unique_ptr<BoundExpression> right_;

  DISALLOW_COPY_AND_ASSIGN(BoundTernaryExpression);
};

// Convenience functions.

// Takes a constant valued bound and resolves it into a single value.
// is_null is a pointer to a boolean value which will be set true if the
// evaluation is a success and evaluates to null.
// Does not takes ownership of input_expression!
template<DataType data_type>
FailureOr<typename TypeTraits<data_type>::hold_type>
    GetConstantBoundExpressionValue(BoundExpression* expression, bool* is_null);

// Takes an expression that is expected to resolve to a constant during binding
// and resolves it into a single value. is_null is a pointer to a boolean value
// which will be set true if the evaluation is a success and evaluates to null.
//
// Does not takes ownership of input_expression!
template<DataType data_type>
FailureOr<typename TypeTraits<data_type>::hold_type> GetConstantExpressionValue(
    const Expression& expression, bool* is_null);

// Takes a bound and initialized expression that can be calculated without
// supplying any inputs, and the result is constant (eg. no randomness and
// no statefulness). Can fail badly or return undefined results if this
// assumption is false, take care! The expression is evaluated, and the
// appropriate constant expression which can be substituted for it after
// binding is returned.
// Takes ownership of expression.
FailureOrOwned<const Expression> ResolveToConstant(BoundExpression* expression);

// Takes an instantiated but uninitialized BasicBoundExpression,
// calls Init() on it, and returns it wrapped in a result upon success, and
// an ERROR_MEMORY_EXCEEDED exception upon failure.
//
// This function also takes care of "resolving constant subtrees". The idea
// is that if a subtree is constant (that is, always evaluates to the same
// result), then we should replace the whole subtree by a single constant
// BoundExpression at binding time, with a gain for performance.
//
// The logic with which this is done is as follows. This happens in a bottom
// up fashion. When a basic expression is not a constant (that is, has
// children), but all its children are constant, it can be resolved. We
// evaluate it, and replace it with an appropriate constant. Thus, if we have
// a large subtree, it will be "eaten up" from the bottom, a layer at a time.
//
// TODO(onufry): we should rewrite the whole binding procedure to allow
// multiple passes through the expression tree, so we can do the following
// (in order):
// - bind the expressions
// - eat up the tree
// - only then allocate the blocks
// to avoid useless allocation and deallocation of memory for expressions that
// die in the resolving proces.

// TODO(onufry): change the order of arguments to expression, allocator,
// row_capacity.
FailureOrOwned<BoundExpression> InitBasicExpression(
    rowcount_t row_capacity,
    BasicBoundExpression* expression,
    BufferAllocator* allocator);

}  // namespace supersonic

#endif  // SUPERSONIC_EXPRESSION_INFRASTRUCTURE_BASIC_BOUND_EXPRESSION_H_
