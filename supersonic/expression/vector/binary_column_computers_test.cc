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

#include "supersonic/expression/vector/binary_column_computers.h"

#include <memory>

#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/base/infrastructure/bit_pointers.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/expression/proto/operators.pb.h"
#include "supersonic/testing/block_builder.h"
#include "gtest/gtest.h"

namespace supersonic {

class BinaryColumnComputersTest : public ::testing::Test {
 public:
  BinaryColumnComputersTest() {}
};

class BinaryComputersTestBlockWrapper {
 public:
  explicit BinaryComputersTestBlockWrapper(size_t rows)
      : block_(CreateBlock(rows)) {}

  double* left() {
    return block_->mutable_column(0)->mutable_typed_data<DOUBLE>();
  }

  double* right() {
    return block_->mutable_column(1)->mutable_typed_data<DOUBLE>();
  }

  double* output() {
    return block_->mutable_column(2)->mutable_typed_data<DOUBLE>();
  }

  bool_ptr skip_vector() {
    return block_->mutable_column(3)->mutable_is_null();
  }

  const Column& left_column() { return block_->column(0); }

  const Column& right_column() { return block_->column(1); }

  OwnedColumn* output_column() { return block_->mutable_column(2); }

 private:
  static Block* CreateBlock(size_t rows) {
    BlockBuilder<DOUBLE, DOUBLE, DOUBLE, BOOL> builder;
    for (int i = 0; i < rows; ++i) {
      builder.AddRow(1., 1., 1., __);
    }
    return builder.Build();
  }

  std::unique_ptr<Block> block_;
};

TEST_F(BinaryColumnComputersTest, ReportError) {
  BinaryComputersTestBlockWrapper block(6);
  FailureOrVoid result = binary_column_computers::ReportError<OPERATOR_OR>(
      ERROR_BAD_PROTO, "Message", block.left_column(), block.right_column());

  EXPECT_TRUE(result.is_failure());
  EXPECT_EQ(ERROR_BAD_PROTO, result.exception().return_code());
  EXPECT_EQ("Message in (col0 OR col1)", result.exception().message());
}

TEST_F(BinaryColumnComputersTest, TrivialFailureCheckReturnsZero) {
  BinaryComputersTestBlockWrapper block(6);
  binary_column_computers::BinaryFailureChecker<OPERATOR_ADD, false,
      double, double> checker;
  int failures = checker(block.left(), block.skip_vector(),
                         block.right(), block.skip_vector(), 5);
  EXPECT_EQ(0, failures);
}

TEST_F(BinaryColumnComputersTest, FailureCheckerCounts) {
  BinaryComputersTestBlockWrapper block(6);
  binary_column_computers::BinaryFailureChecker<OPERATOR_DIVIDE_SIGNALING, true,
      double, double> checker;
  for (int i = 0; i < 5; ++i) {
    block.skip_vector()[i] = false;
    block.left()[i] = (i == 2 || i == 4) ? 0. : 1.;
  }

  int failures = checker(block.left(), block.skip_vector(),
                         block.left(), block.skip_vector(), 3);
  EXPECT_EQ(1, failures);

  failures = checker(block.left(), block.skip_vector(),
                     block.left(), block.skip_vector(), 2);
  EXPECT_EQ(0, failures);

  block.skip_vector()[2] = true;
  failures = checker(block.left(), block.skip_vector(),
                     block.left(), block.skip_vector(), 4);
  EXPECT_EQ(0, failures);
}

TEST_F(BinaryColumnComputersTest, BinaryTrivialNuller) {
  BinaryComputersTestBlockWrapper block(6);
  binary_column_computers::BinaryNuller<OPERATOR_DIVIDE_SIGNALING,
      false, double, double> nuller;
  for (int i = 0; i < 5; ++i) {
    block.right()[i] = 1;
    block.skip_vector()[i] = (i % 2 == 0);
  }

  nuller(block.left(), block.right(), block.skip_vector(), 5);
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ((i % 2 == 0), block.skip_vector()[i]);
  }
}

TEST_F(BinaryColumnComputersTest, BinaryNuller) {
  BinaryComputersTestBlockWrapper block(6);
  binary_column_computers::BinaryNuller<OPERATOR_DIVIDE_NULLING,
      true, double, double> nuller;
  for (int i = 0; i < 5; ++i) {
    block.right()[i] = (i < 2) ? 0. : 1.;
    block.skip_vector()[i] = (i % 2 == 0);
  }

  nuller(block.left(), block.right(), block.skip_vector(), 5);
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ((i % 2 == 0) || (i < 2), block.skip_vector()[i]);
  }
}

TEST_F(BinaryColumnComputersTest, CheckAndNullFailureCount) {
  BinaryComputersTestBlockWrapper block(6);
  for (int i = 0; i < 5; ++i) {
    block.right()[i] = 0.;
    block.skip_vector()[i] = (i % 2 == 0);
  }

  binary_column_computers::CheckAndNull<OPERATOR_DIVIDE_SIGNALING, DOUBLE,
                                        DOUBLE, DOUBLE> check_and_null;
  FailureOrVoid result = check_and_null(block.left_column(),
                                        block.right_column(),
                                        block.skip_vector(), 5);

  ASSERT_TRUE(result.is_failure());
  EXPECT_EQ(ERROR_EVALUATION_ERROR, result.exception().return_code());
  EXPECT_EQ("Evaluation error (invalid data in 2 rows) in (col0 /. col1)",
            result.exception().message());
}

TEST_F(BinaryColumnComputersTest, CheckAndNull) {
  BinaryComputersTestBlockWrapper block(6);
  for (int i = 0; i < 5; ++i) {
    block.right()[i] = (i % 2 == 0) ? 0. : 1.;
    block.skip_vector()[i] = (i % 3 == 0);
  }

  binary_column_computers::CheckAndNull<OPERATOR_DIVIDE_NULLING, DOUBLE,
                                        DOUBLE, DOUBLE> check_and_null;
  FailureOrVoid result = check_and_null(block.left_column(),
                                        block.right_column(),
                                        block.skip_vector(), 5);
  EXPECT_TRUE(result.is_success());
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ((i % 2) == 0 || (i % 3) == 0, block.skip_vector()[i]);
  }
}

TEST_F(BinaryColumnComputersTest, EvaluationFailure) {
  BinaryComputersTestBlockWrapper block(6);
  block.right()[3] = 0.;
  block.skip_vector()[3] = false;

  ColumnBinaryComputer<OPERATOR_DIVIDE_SIGNALING, DOUBLE, DOUBLE, DOUBLE> op;
  FailureOrVoid result = op(block.left_column(), block.right_column(),
                            5, block.output_column(), block.skip_vector());

  EXPECT_TRUE(result.is_failure());
}

TEST_F(BinaryColumnComputersTest, NonSelectiveCalculation) {
  BinaryComputersTestBlockWrapper block(6);
  for (int i = 0; i < 5; ++i) {
    block.left()[i] = 1.;
    block.right()[i] = i;
    block.skip_vector()[i] = (i > 3);
    block.output()[i] = 0.;
  }

  ColumnBinaryComputer<OPERATOR_DIVIDE_NULLING, DOUBLE, DOUBLE, DOUBLE> op;
  FailureOrVoid result = op(block.left_column(), block.right_column(), 5,
                            block.output_column(), block.skip_vector());

  EXPECT_TRUE(result.is_success());
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(1. / static_cast<double>(i), block.output()[i]);
    EXPECT_EQ((i == 0) || (i > 3), block.skip_vector()[i]);
  }
}

TEST_F(BinaryColumnComputersTest, SelectiveCalculation) {
  BinaryComputersTestBlockWrapper block(6);
  for (int i = 0; i < 5; ++i) {
    block.left()[i] = 1.;
    block.right()[i] = (i+1.);
    block.output()[i] = 0.;
    block.skip_vector()[i] = false;
  }

  ColumnBinaryComputer<OPERATOR_CPP_DIVIDE_SIGNALING, DOUBLE, DOUBLE, DOUBLE>
      op;
  FailureOrVoid result = op(block.left_column(), block.right_column(), 5,
                         block.output_column(), block.skip_vector());

  EXPECT_TRUE(result.is_success());
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(1. / (i + 1.), block.output()[i]);
  }
}

}  // namespace supersonic
