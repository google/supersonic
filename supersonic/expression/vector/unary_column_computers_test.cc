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

#include "supersonic/expression/vector/unary_column_computers.h"

#include <memory>

#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/base/infrastructure/bit_pointers.h"
#include "supersonic/base/infrastructure/block.h"
// TODO(onufry): These includes should go when we start using fake expressions
// for testing instead of real ones.
#include "supersonic/expression/core/date_evaluators.h"  // IWYU pragma: keep
#include "supersonic/expression/core/math_evaluators.h"  // IWYU pragma: keep
#include "supersonic/expression/proto/operators.pb.h"
#include "supersonic/testing/block_builder.h"
#include "gtest/gtest.h"

namespace supersonic {

namespace unary_column_computers {

// This is a convenience wrapper of the BlockBuilder, to avoid calling it with
// explicit template arguments, and to access input/output by getters,
// instead of lengthy block->mutable_column(x)->mutable_typed_data<DOUBLE>().
class UnaryComputersTestBlockWrapper {
 public:
  explicit UnaryComputersTestBlockWrapper(size_t rows)
      : block_(CreateBlock(rows)) {}

  double* input() {
    return block_->mutable_column(0)->mutable_typed_data<DOUBLE>();
  }

  double* output() {
    return block_->mutable_column(1)->mutable_typed_data<DOUBLE>();
  }

  bool_ptr skip_vector() {
    return block_->mutable_column(2)->mutable_is_null();
  }

  const Column& input_column() { return block_->column(0); }

  OwnedColumn* output_column() { return block_->mutable_column(1); }

 private:
  static Block* CreateBlock(size_t rows) {
    BlockBuilder<DOUBLE, DOUBLE, BOOL> builder;
    for (int i = 0; i < rows; ++i)
      builder.AddRow(3., 3., __);
    return builder.Build();
  }

  std::unique_ptr<Block> block_;
};

TEST(UnaryColumnComputersTest, EvaluationFailure) {
  UnaryComputersTestBlockWrapper block(4);
  for (int i = 0; i < 4; ++i) {
    block.input()[i] = -i;
    block.skip_vector()[i] = false;
  }
  ColumnUnaryComputer<OPERATOR_FROMUNIXTIME, DOUBLE, DOUBLE, false> op;

  EXPECT_TRUE(op(block.input_column(), 4, block.output_column(),
                 block.skip_vector()).is_failure());
}

TEST(UnaryColumnComputersTest, EvaluationFailureNotNull) {
  UnaryComputersTestBlockWrapper block(4);
  for (int i = 0; i < 4; ++i) {
    block.input()[i] = -i;
    block.skip_vector()[i] = false;
  }
  ColumnUnaryComputer<OPERATOR_FROMUNIXTIME, DOUBLE, DOUBLE, false> op;

  EXPECT_TRUE(op(block.input_column(), 4,
                 block.output_column(), block.skip_vector()).is_failure());
}

TEST(UnaryColumnComputersTest, EvaluationSuccessOnNulls) {
  UnaryComputersTestBlockWrapper block(4);
  for (int i = 0; i < 4; ++i) {
    block.skip_vector()[i] = i > 1;
    block.input()[i] = 1 - i;
  }
  ColumnUnaryComputer<OPERATOR_FROMUNIXTIME, DOUBLE, DOUBLE, false> op;

  EXPECT_TRUE(op(block.input_column(), 4, block.output_column(),
                 block.skip_vector()).is_success());
}

TEST(UnaryColumnComputersTest, EvaluationCopiesNulls) {
  UnaryComputersTestBlockWrapper block(4);
  for (int i = 0; i < 4; ++i) {
    block.skip_vector()[i] = (i % 3 != 0);
    block.input()[i] = 2;
  }
  ColumnUnaryComputer<OPERATOR_NEGATE, DOUBLE, DOUBLE, false> op;

  EXPECT_TRUE(op(block.input_column(), 4, block.output_column(),
                 block.skip_vector()).is_success());
  for (int i = 0; i < 4; ++i) EXPECT_EQ((i%3) != 0, block.skip_vector()[i]);
}

TEST(UnaryColumnComputersTest, EvaluationDoesNotChangeSource) {
  UnaryComputersTestBlockWrapper block(4);
  for (int i = 0; i < 4; ++i) {
    block.skip_vector()[i] = (i % 3 != 0);
    block.input()[i] = 1 - i;
  }
  ColumnUnaryComputer<OPERATOR_SQRT_NULLING, DOUBLE, DOUBLE, false> op;

  EXPECT_TRUE(op(block.input_column(), 4, block.output_column(),
                 block.skip_vector()).is_success());
  EXPECT_EQ(1., block.input()[0]);
  EXPECT_EQ(0., block.input()[1]);
  EXPECT_EQ(-1., block.input()[2]);
  EXPECT_EQ(-2., block.input()[3]);
}

TEST(UnaryColumnComputersTest, EvaluationIntroducesNulls) {
  UnaryComputersTestBlockWrapper block(4);
  for (int i = 0; i < 4; ++i) {
    block.input()[i] = 1 - i;
    block.skip_vector()[i] = false;
  }
  ColumnUnaryComputer<OPERATOR_SQRT_NULLING, DOUBLE, DOUBLE, false> op;

  EXPECT_TRUE(op(block.input_column(), 4, block.output_column(),
                 block.skip_vector()).is_success());
  for (int i = 0; i < 4; ++i) EXPECT_EQ(i > 1, block.skip_vector()[i]);
}

TEST(UnaryColumnComputersTest, EvaluationPreservesNotNull) {
  UnaryComputersTestBlockWrapper block(4);
  ColumnUnaryComputer<OPERATOR_NEGATE, DOUBLE, DOUBLE, false> op;

  EXPECT_TRUE(op(block.input_column(), 4, block.output_column(),
                 block.skip_vector()).is_success());
  // What we actually check for here is an internal failure of the computer.
}

TEST(UnaryColumnComputersTest, EvalutationWorksForSafe) {
  UnaryComputersTestBlockWrapper block(4);
  block.input()[0] = 4.;
  block.input()[1] = 9.;
  block.input()[2] = -1.;
  block.output()[3] = -1.;
  for (int i = 0; i < 4; ++i) block.skip_vector()[i] = false;
  ColumnUnaryComputer<OPERATOR_SQRT_NULLING, DOUBLE, DOUBLE, false> op;

  EXPECT_TRUE(op(block.input_column(), 3, block.output_column(),
                 block.skip_vector()).is_success());
  EXPECT_EQ(2., block.output()[0]);
  EXPECT_EQ(3., block.output()[1]);
  // We make no assumptions on output[2], this is a NULL.
  EXPECT_EQ(-1., block.output()[3]);  // Should not have been changed.
  EXPECT_EQ(false, block.skip_vector()[0]);
  EXPECT_EQ(false, block.skip_vector()[1]);
  EXPECT_EQ(true, block.skip_vector()[2]);
  EXPECT_EQ(false, block.skip_vector()[3]);
}

// TODO(onufry): a test should be added for non-safe unary operations.
// The problem is that none exist at the moment.

// TODO(onufry): a test suite should be added for allocating unary operations.
// The caveat is the same as above (the ToString operator is allocating, but
// the UnaryColumnComputer for the operator is redefined, so we can't use it
// to test the generic allocating unary column computer).

}  // namespace unary_column_computers.

}  // namespace supersonic
