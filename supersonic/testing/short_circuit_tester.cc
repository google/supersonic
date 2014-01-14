// Copyright 2011 Google Inc. All Rights Reserved.
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

#include "supersonic/testing/short_circuit_tester.h"

#include <memory>

#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/bit_pointers.h"
#include "supersonic/expression/base/expression.h"
#include "supersonic/expression/core/projecting_expressions.h"
#include "supersonic/expression/infrastructure/basic_expressions.h"
#include "supersonic/expression/infrastructure/expression_utils.h"
#include "supersonic/testing/expression_test_helper.h"

namespace supersonic {
namespace {

const rowcount_t kCapacity = 1024;

// The skipper expression takes the left argument (which has to be a
// non-nullable boolean expression) and uses it as the skip vector which will be
// input into the right expression; then returns the result of the right
// expression evaluation. This expression is intended for use in short circuit
// testing.
class BoundSkipperExpression : public BoundExpression {
 public:
  BoundSkipperExpression(BoundExpression* left,
                         BoundExpression* right)
      : BoundExpression(CreateSchema(StrCat("Skipper(",
                                            GetExpressionName(left),
                                            ", ",
                                            GetExpressionName(right),
                                            ")"),
                                     GetExpressionType(right),
                                     NULLABLE)),
        left_child_(CHECK_NOTNULL(left)),
        right_child_(CHECK_NOTNULL(right)) {}

  virtual ~BoundSkipperExpression() {}

  virtual rowcount_t row_capacity() const { return kCapacity; }
  virtual bool is_constant() const { return false; }

  virtual EvaluationResult DoEvaluate(const View& input,
                                      const BoolView& skip_vectors) {
    CHECK_EQ(1, skip_vectors.column_count());
    bool_ptr skip_vector = skip_vectors.column(0);
    rowcount_t rows = input.row_count();
    CHECK_GE(kCapacity, rows);
    CHECK_EQ(0, bit_pointer::PopCount(skip_vector, rows))
        << "The Skipper Expression needs to be a top-level expression.";

    EvaluationResult left_result = left_child_->DoEvaluate(input, skip_vectors);
    PROPAGATE_ON_FAILURE(left_result);
    CHECK_EQ(0, bit_pointer::PopCount(skip_vector, rows));
    bit_pointer::SafeFillFrom(skip_vector,
                              left_result.get().column(0).typed_data<BOOL>(),
                              rows);
    EvaluationResult right_result =
        right_child_->DoEvaluate(input, skip_vectors);
    PROPAGATE_ON_FAILURE(right_result);
    my_view()->ResetFrom(right_result.get());
    my_view()->mutable_column(0)->ResetIsNull(skip_vector);
    return Success(*my_view());
  }

  virtual void CollectReferredAttributeNames(
      set<string>* referred_atribute_names) const {
    left_child_->CollectReferredAttributeNames(referred_atribute_names);
    right_child_->CollectReferredAttributeNames(referred_atribute_names);
  }

 private:
  std::unique_ptr<BoundExpression> left_child_;
  std::unique_ptr<BoundExpression> right_child_;
};

// The skip vector expectation expression calculates the left input with an
// empty skip vector, and then checks that the received skip vector is identical
// to the results of the left expression. After that calculates and returns the
// right expression. This expression is intended for use in short circuit
// testing.
class BoundSkipVectorExpectationExpression : public BoundExpression {
 public:
  BoundSkipVectorExpectationExpression(BoundExpression* left,
                                       BoundExpression* right,
                                       BufferAllocator* allocator)
      : BoundExpression(CreateSchema(StrCat("SkipVectorExpectation(",
                                            GetExpressionName(left),
                                            ", ",
                                            GetExpressionName(right),
                                            ")"),
                                     GetExpressionType(right),
                                     right)),
        left_child_(CHECK_NOTNULL(left)),
        right_child_(CHECK_NOTNULL(right)),
        local_skip_vector_storage_(1, allocator),
        initialized_(false) {}

  virtual ~BoundSkipVectorExpectationExpression() {}

  virtual rowcount_t row_capacity() const { return kCapacity; }
  virtual bool is_constant() const { return false; }

  virtual EvaluationResult DoEvaluate(const View& input,
                                      const BoolView& skip_vectors) {
    // This is an inefficient way to do this - the more efficient would be to
    // override Bind. But this is simpler, and for a test expression we do not
    // really care about efficiency.
    if (!initialized_) {
      PROPAGATE_ON_FAILURE(local_skip_vector_storage_.TryReallocate(kCapacity));
      bit_pointer::FillWithFalse(local_skip_vector_storage_.view().column(0),
                                 kCapacity);
      initialized_ = true;
    }

    CHECK_EQ(1, skip_vectors.column_count());
    bool_ptr skip_vector = skip_vectors.column(0);
    rowcount_t rows = input.row_count();

    EvaluationResult left_result =
        left_child_->DoEvaluate(input, local_skip_vector_storage_.view());
    PROPAGATE_ON_FAILURE(left_result);
    DCHECK_EQ(0, bit_pointer::PopCount(
        local_skip_vector_storage_.view().column(0), rows));

    for (rowid_t row = 0; row < rows; ++row) {
      bool expected = left_result.get().column(0).typed_data<BOOL>()[row];
      bool got = skip_vector[row];
      if (expected != got) {
        THROW(new Exception(ERROR_EVALUATION_ERROR,
                            StrCat("Unexpected skip value in row", row,
                                   ", expected: ", expected,
                                   ", but got: ", got)));
      }
    }

    EvaluationResult right_result =
        right_child_->DoEvaluate(input, skip_vectors);
    PROPAGATE_ON_FAILURE(right_result);
    my_view()->ResetFrom(right_result.get());
    my_view()->mutable_column(0)->ResetIsNull(skip_vector);
    return Success(*my_view());
  }

  virtual void CollectReferredAttributeNames(
      set<string>* referred_atribute_names) const {
    left_child_->CollectReferredAttributeNames(referred_atribute_names);
    right_child_->CollectReferredAttributeNames(referred_atribute_names);
  }

 private:
  std::unique_ptr<BoundExpression> left_child_;
  std::unique_ptr<BoundExpression> right_child_;
  BoolBlock local_skip_vector_storage_;
  bool initialized_;
};

}  // namespace

FailureOrOwned<BoundExpression> BoundSkipper(BoundExpression* skip_vector_ptr,
                                             BoundExpression* input_ptr,
                                             BufferAllocator* allocator,
                                             rowcount_t max_row_count) {
  std::unique_ptr<BoundExpression> skip_vector(skip_vector_ptr);
  std::unique_ptr<BoundExpression> input(input_ptr);
  PROPAGATE_ON_FAILURE(CheckAttributeCount(string("Skipper skip vector"),
                                           skip_vector->result_schema(),
                                           1));
  PROPAGATE_ON_FAILURE(CheckAttributeCount(string("Skipper input"),
                                           input->result_schema(),
                                           1));
  PROPAGATE_ON_FAILURE(CheckExpressionType(BOOL, skip_vector.get()));
  if (GetExpressionNullability(skip_vector.get()) == NULLABLE) {
    THROW(new Exception(ERROR_ATTRIBUTE_IS_NULLABLE,
                        "The skip vector argument has to be non-nullable in "
                        "the short circuit test."));
  }
  return Success(new BoundSkipperExpression(skip_vector.release(),
                                            input.release()));
}

FailureOrOwned<BoundExpression> BoundSkipVectorExpectation(
    BoundExpression* expected_skip_vector_ptr,
    BoundExpression* input_ptr,
    BufferAllocator* allocator,
    rowcount_t max_row_count) {
  std::unique_ptr<BoundExpression> expected_skip_vector(
      expected_skip_vector_ptr);
  std::unique_ptr<BoundExpression> input(input_ptr);
  PROPAGATE_ON_FAILURE(CheckAttributeCount(
      string("Expected skip vector"),
      expected_skip_vector->result_schema(),
      1));
  PROPAGATE_ON_FAILURE(CheckAttributeCount(string("Expecter input"),
                                           input->result_schema(),
                                           1));
  PROPAGATE_ON_FAILURE(CheckExpressionType(BOOL, expected_skip_vector.get()));
  if (GetExpressionNullability(expected_skip_vector.get()) == NULLABLE) {
    THROW(new Exception(ERROR_ATTRIBUTE_IS_NULLABLE,
                        "The expected skip vector argument has to be "
                        "non-nullable in the short circuit test."));
  }
  return Success(new BoundSkipVectorExpectationExpression(
      expected_skip_vector.release(),
      input.release(),
      allocator));
}

const Expression* Skipper(const Expression* skip_vector,
                          const Expression* input) {
  return CreateExpressionForExistingBoundFactory(
      skip_vector, input, &BoundSkipper, "SKIPPER($0, $1)");
}

const Expression* SkipVectorExpectation(const Expression* expected,
                                        const Expression* input) {
  return CreateExpressionForExistingBoundFactory(
      expected, input, &BoundSkipVectorExpectation, "$1");
}

void TestShortCircuitUnary(const Block* block,
                           UnaryExpressionCreator factory) {
  std::unique_ptr<const Block> deleter(block);
  const Expression* expression =
      Skipper(AttributeAt(0), factory(SkipVectorExpectation(AttributeAt(2),
                                                            AttributeAt(1))));
  TestEvaluationCommon(block, true, expression);
}

void TestShortCircuitBinary(const Block* block,
                            BinaryExpressionCreator factory) {
  const Expression* expression =
      Skipper(AttributeAt(0), factory(SkipVectorExpectation(AttributeAt(2),
                                                            AttributeAt(1)),
                                      SkipVectorExpectation(AttributeAt(4),
                                                            AttributeAt(3))));
  std::unique_ptr<const Block> deleter(block);
  TestEvaluationCommon(block, true, expression);
}

void TestShortCircuitTernary(const Block* block,
                             TernaryExpressionCreator factory) {
  const Expression* expression =
      Skipper(AttributeAt(0), factory(SkipVectorExpectation(AttributeAt(2),
                                                            AttributeAt(1)),
                                      SkipVectorExpectation(AttributeAt(4),
                                                            AttributeAt(3)),
                                      SkipVectorExpectation(AttributeAt(6),
                                                            AttributeAt(5))));
  std::unique_ptr<const Block> deleter(block);
  TestEvaluationCommon(block, true, expression);
}

}  // namespace supersonic
