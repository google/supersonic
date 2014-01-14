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

#include "supersonic/expression/vector/ternary_column_computers.h"

#include <memory>

#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/expression/proto/operators.pb.h"
#include "supersonic/testing/block_builder.h"
#include "gtest/gtest.h"

namespace supersonic {
class Arena;
}  // namespace supersonic

namespace supersonic {

// Traits for ternary test operators.

namespace operators {

// A addition operator, two variants - one that takes the arena and one that
// doesn't.
struct TernaryAdd {
  double operator()(double a, double b, double c) {
    return a + b + c;
  }

  double operator()(double a, double b, double c, Arena* arena) {
    return a + b + c;
  }
};

}  // namespace operators

namespace failers {

// Returns the number of rows in which the left of the three column is zero.
struct FirstColumnZeroFailer {
  int operator()(const double* left_data,
                 bool_const_ptr left_is_null,
                 const double* middle_data,
                 bool_const_ptr middle_is_null,
                 const double* right_data,
                 bool_const_ptr right_is_null,
                 size_t row_count) {
    int failures = 0;
    if (left_is_null == NULL) {
      for (int i = 0; i < row_count; ++i) {
        failures += (left_data[i] == 0.);
      }
    } else {
      for (int i = 0; i < row_count; ++i) {
        failures += (!left_is_null[i]) && (left_data[i] == 0.);
      }
    }
    return failures;
  }
};

}  // namespace failers

namespace nullers {

// Sets result_is_null for all the rows where the first of three columns is
// zero.
struct FirstColumnZeroNuller {
  void operator()(const double* left_data,
                  const double* middle_data,
                  const double* right_data,
                  bool_ptr result_is_null,
                  size_t row_count) {
    DCHECK(result_is_null != NULL);
    for (int i = 0; i < row_count; ++i) {
      result_is_null[i] |= (left_data[i] == 0.);
    }
  }
};

}  // namespace nullers

template<> struct BaseTernaryExpressionTraits<OPERATOR_TERNARY_NO_CHECK> {
  typedef operators::TernaryAdd basic_operator;
  static const int selectivity_threshold = 0;
  static const string name() { return string("TERNARY_ADD_TEST_NO_CHECK"); }
  static const bool supports_promotions = false;
  static const bool can_fail = false;
  static const bool can_return_null = false;
  static const bool is_safe = true;
  static const bool needs_allocator = false;
};

template<> struct BaseTernaryExpressionTraits<OPERATOR_TERNARY_FULL_CHECK> {
  typedef operators::TernaryAdd basic_operator;
  static const int selectivity_threshold = 100;
  static const string name() { return string("TERNARY_ADD_TEST_WITH_CHECKS"); }
  static const bool supports_promotions = false;
  static const bool can_fail = true;
  static const bool can_return_null = true;
  static const bool is_safe = false;
  static const bool needs_allocator = false;
  typedef failers::FirstColumnZeroFailer CheckFailure;
  typedef nullers::FirstColumnZeroNuller FillNulls;
};

template<> struct BaseTernaryExpressionTraits<OPERATOR_TERNARY_ALLOC> {
  typedef operators::TernaryAdd basic_operator;
  static const int selectivity_threshold = 100;
  static const string name() { return string("TERNARY_ADD_TEST_WITH_ALLOC"); }
  static const bool supports_promotions = false;
  static const bool can_fail = false;
  static const bool can_return_null = false;
  static const bool is_safe = false;
  static const bool needs_allocator = true;
};

namespace ternary_column_computers {

class TernaryComputersTestBlockWrapper {
 public:
  explicit TernaryComputersTestBlockWrapper(size_t rows)
      : block_(CreateBlock(rows)) {}

  double* left() {
    return block_->mutable_column(0)->mutable_typed_data<DOUBLE>();
  }

  double* middle() {
    return block_->mutable_column(1)->mutable_typed_data<DOUBLE>();
  }

  double* right() {
    return block_->mutable_column(2)->mutable_typed_data<DOUBLE>();
  }

  const double* cleft() {
    return block_->column(0).typed_data<DOUBLE>();
  }

  const double* cmiddle() {
    return block_->column(1).typed_data<DOUBLE>();
  }

  const double* cright() {
    return block_->column(2).typed_data<DOUBLE>();
  }

  double* output() {
    return block_->mutable_column(3)->mutable_typed_data<DOUBLE>();
  }

  bool_ptr skip_vector() {
    return block_->mutable_column(4)->mutable_is_null();
  }

  const Column& left_column() { return block_->column(0); }

  const Column& middle_column() { return block_->column(1); }

  const Column& right_column() { return block_->column(2); }

  OwnedColumn* output_column() { return block_->mutable_column(3); }

 private:
  static Block* CreateBlock(size_t rows) {
    BlockBuilder<DOUBLE, DOUBLE, DOUBLE, DOUBLE, BOOL> builder;
    for (int i = 0; i < rows; ++i) {
      builder.AddRow(1., 1., 1., 1., __);
    }
    return builder.Build();
  }

  std::unique_ptr<Block> block_;
};

TEST(TernaryColumnComputersTest, TrivialFailureCheckReturnsZero) {
  TernaryComputersTestBlockWrapper block(6);
  ternary_column_computers::TernaryFailureChecker<OPERATOR_TERNARY_NO_CHECK,
      false, double, double, double> checker;
  for (int i = 0; i < 5; ++i) block.skip_vector()[i] = (i == 1);
  int failures = checker(block.left(), block.skip_vector(),
                         block.middle(), block.skip_vector(),
                         block.right(), block.skip_vector(), 5);
  EXPECT_EQ(0, failures);
}

TEST(TernaryColumnComputersTest, FailureCheckerCounts) {
  TernaryComputersTestBlockWrapper block(6);
  ternary_column_computers::TernaryFailureChecker<OPERATOR_TERNARY_FULL_CHECK,
      true, double, double, double> checker;
  for (int i = 0; i < 5; ++i) {
    block.skip_vector()[i] = false;
    block.left()[i] = (i == 2 || i == 4) ? 0. : 1.;
  }
  int failures = checker(block.cleft(), block.skip_vector(),
                         block.cmiddle(), block.skip_vector(),
                         block.cright(), block.skip_vector(), 3);
  EXPECT_EQ(1, failures);

  failures = checker(block.left(), block.skip_vector(),
                     block.middle(), block.skip_vector(),
                     block.right(), block.skip_vector(), 2);
  EXPECT_EQ(0, failures);

  block.skip_vector()[2] = true;
  failures = checker(block.left(), block.skip_vector(),
                     block.middle(), block.skip_vector(),
                     block.right(), block.skip_vector(), 3);
  EXPECT_EQ(0, failures);
}

TEST(TernaryColumnComputersTest, TernaryTrivialNullerDoesNotDeInitialize) {
  TernaryComputersTestBlockWrapper block(6);
  for (int i = 0; i < 5; ++i) {
    block.skip_vector()[i] = (1 == i);
  }

  TernaryNuller<OPERATOR_TERNARY_NO_CHECK, false,
      double, double, double> nuller;
  nuller(block.cleft(), block.cmiddle(), block.cright(),
         block.skip_vector(), 5);

  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ((1 == i), block.skip_vector()[i]);
  }
}

TEST(TernaryColumnComputersTest, TernaryNullerInitialized) {
  TernaryComputersTestBlockWrapper block(6);
  for (int i = 0; i < 5; ++i) {
    block.skip_vector()[i] = (i % 2 == 0);
    block.left()[i] = (i < 2) ? 0. : 1.;
  }

  TernaryNuller<OPERATOR_TERNARY_FULL_CHECK, true, double, double,
      double> nuller;
  nuller(block.cleft(), block.cmiddle(), block.cright(),
         block.skip_vector(), 5);

  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ((i % 2 == 0) || (i < 2), block.skip_vector()[i]);
  }
}

TEST(TernaryColumnComputersTest, CheckAndNullFailureCount) {
  TernaryComputersTestBlockWrapper block(6);
  for (int i = 0; i < 5; ++i) {
    block.skip_vector()[i] = (i % 2 == 0);
    block.left()[i] = 0.;
  }

  FailureOrVoid result = CheckAndNull<OPERATOR_TERNARY_FULL_CHECK, DOUBLE,
      DOUBLE, DOUBLE, DOUBLE>(block.left_column(), block.middle_column(),
                              block.right_column(), block.skip_vector(), 5);

  ASSERT_TRUE(result.is_failure());
  EXPECT_EQ(ERROR_EVALUATION_ERROR, result.exception().return_code());
  EXPECT_EQ("Evaluation error (invalid data in 2 rows) in "
            "TERNARY_ADD_TEST_WITH_CHECKS(col0, col1, col2)",
            result.exception().message());
}

TEST(TernaryColumnComputersTest, CheckAndNull) {
  TernaryComputersTestBlockWrapper block(6);
  for (int i = 0; i < 5; ++i) {
    block.skip_vector()[i] = (i == 2);
    block.left()[i] = 1.;
  }

  FailureOrVoid result = CheckAndNull<OPERATOR_TERNARY_NO_CHECK, DOUBLE, DOUBLE,
      DOUBLE, DOUBLE>(block.left_column(), block.middle_column(),
                      block.right_column(), block.skip_vector(), 5);

  EXPECT_TRUE(result.is_success());
}

TEST(TernaryColumnComputersTest, EvaluationFailure) {
  TernaryComputersTestBlockWrapper block(6);
  block.left()[3] = 0.;
  block.skip_vector()[3] = false;

  ColumnTernaryComputer<OPERATOR_TERNARY_FULL_CHECK, DOUBLE, DOUBLE, DOUBLE,
      DOUBLE, false> op;
  FailureOrVoid result = op(block.left_column(), block.middle_column(),
                            block.right_column(), 5, block.output_column(),
                            block.skip_vector());

  EXPECT_TRUE(result.is_failure());
}

TEST(TernaryColumnComputersTest, NonSelectiveCalculation) {
  TernaryComputersTestBlockWrapper block(6);
  for (int i = 0; i < 5; ++i) {
    block.left()[i] = 1.;
    block.middle()[i] = 2.;
    block.right()[i] = i;
    block.skip_vector()[i] = (i > 3);
    block.output()[i] = 0.;
  }

  ColumnTernaryComputer<OPERATOR_TERNARY_NO_CHECK, DOUBLE, DOUBLE, DOUBLE,
      DOUBLE, false> op;
  FailureOrVoid result = op(block.left_column(), block.middle_column(),
                            block.right_column(), 5, block.output_column(),
                            block.skip_vector());

  EXPECT_TRUE(result.is_success());
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(3. + i, block.output()[i]);
    EXPECT_EQ((i > 3), block.skip_vector()[i]);
  }
}

TEST(TernaryColumnComputersTest, SelectiveCalculation) {
  TernaryComputersTestBlockWrapper block(6);
  for (int i = 0; i < 5; ++i) {
    block.left()[i] = 1.;
    block.middle()[i] = 2.;
    block.right()[i] = i;
    block.skip_vector()[i] = (i > 3);
    block.output()[i] = 0.;
  }

  ColumnTernaryComputer<OPERATOR_TERNARY_FULL_CHECK, DOUBLE, DOUBLE, DOUBLE,
      DOUBLE, false> op;
  FailureOrVoid result = op(block.left_column(), block.middle_column(),
                            block.right_column(), 5, block.output_column(),
                            block.skip_vector());

  EXPECT_TRUE(result.is_success());
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ((i > 3) ? 0. : 3. + i, block.output()[i]);
    EXPECT_EQ((i > 3), block.skip_vector()[i]);
  }
}

TEST(TernaryColumnComputersTest, AllocatingNullCalculation) {
  TernaryComputersTestBlockWrapper block(6);
  for (int i = 0; i < 5; ++i) {
    block.left()[i] = 1.;
    block.middle()[i] = 2.;
    block.right()[i] = i;
    block.skip_vector()[i] = (i > 3);
    block.output()[i] = 0.;
  }

  ColumnTernaryComputer<OPERATOR_TERNARY_ALLOC, DOUBLE, DOUBLE, DOUBLE,
      DOUBLE, false> op;
  FailureOrVoid result = op(block.left_column(), block.middle_column(),
                            block.right_column(), 5, block.output_column(),
                            block.skip_vector());

  EXPECT_TRUE(result.is_success());
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ((i > 3) ? 0. : 3. + i, block.output()[i]);
    EXPECT_EQ((i > 3), block.skip_vector()[i]);
  }
}

}  // namespace ternary_column_computers

}  // namespace supersonic
