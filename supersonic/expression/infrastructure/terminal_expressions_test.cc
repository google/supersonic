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

#include "supersonic/expression/infrastructure/terminal_expressions.h"

#include <stddef.h>

#include <memory>

#include "supersonic/utils/integral_types.h"
#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/base/infrastructure/bit_pointers.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/expression/base/expression.h"
#include "supersonic/testing/block_builder.h"
#include "supersonic/testing/comparators.h"
#include "supersonic/testing/expression_test_helper.h"
#include "gtest/gtest.h"
#include "supersonic/utils/random.h"
#include "supersonic/utils/random.h"

namespace supersonic {

class TerminalExpressionTest : public testing::Test {
 public:
  TerminalExpressionTest() : block_(CreateBlock()) {}

  const View& input() const { return block_->view(); }

 private:
  static Block* CreateBlock() {
    return BlockBuilder<STRING, INT32, DOUBLE>()
        .AddRow("1", 12, 5.1)
        .AddRow("2", 13, 6.2)
        .AddRow("3", 14, 7.3)
        .AddRow("4", 15, 8.4)
        .AddRow(__,  __, __)
        .Build();
  }

  std::unique_ptr<Block> block_;
};

TEST_F(TerminalExpressionTest, NullsAreNull) {
  std::unique_ptr<BoundExpressionTree> null(
      DefaultBind(input().schema(), 100, Null(DOUBLE)));
  EXPECT_TUPLE_SCHEMAS_EQUAL(TupleSchema::Singleton("NULL", DOUBLE, NULLABLE),
                             null->result_schema());
  const View& result = DefaultEvaluate(null.get(), input());
  EXPECT_EQ(input().row_count(), result.row_count());
  bool_const_ptr is_null = result.column(0).is_null();
  for (int i = 0; i < 5; ++i) EXPECT_TRUE(is_null[i]);
}

TEST_F(TerminalExpressionTest, SequenceProgresses) {
  std::unique_ptr<BoundExpressionTree> sequence(
      DefaultBind(input().schema(), 100, Sequence()));
  EXPECT_TUPLE_SCHEMAS_EQUAL(
      TupleSchema::Singleton("SEQUENCE", INT64, NOT_NULLABLE),
      sequence->result_schema());

  const View& result = DefaultEvaluate(sequence.get(), input());
  bool_const_ptr is_null = result.column(0).is_null();
  const int64* data = result.column(0).typed_data<INT64>();
  EXPECT_EQ(bool_ptr(NULL), is_null);
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(i, data[i]);
  }
}

TEST_F(TerminalExpressionTest, RandInt32WithMTRandom) {
  std::unique_ptr<RandomBase> gen(new MTRandom(0));
  std::unique_ptr<BoundExpressionTree> random(
      DefaultBind(input().schema(), 100, RandInt32(gen->Clone())));
  EXPECT_TUPLE_SCHEMAS_EQUAL(
      TupleSchema::Singleton("RANDINT32", INT32, NOT_NULLABLE),
      random->result_schema());

  const View& result = DefaultEvaluate(random.get(), input());
  const bool* is_null = result.column(0).is_null();
  const int32* data = result.column(0).typed_data<INT32>();
  EXPECT_EQ(NULL, is_null);
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(gen->Rand32(), data[i]);
  }
}

TEST_F(TerminalExpressionTest, ConstInt32) {
  std::unique_ptr<BoundExpressionTree> values(
      DefaultBind(input().schema(), 100, ConstInt32(100)));
  EXPECT_TUPLE_SCHEMAS_EQUAL(
      TupleSchema::Singleton("CONST_INT32", INT32, NOT_NULLABLE),
      values->result_schema());
  const View& result = DefaultEvaluate(values.get(), input());
  EXPECT_TRUE(result.column(0).is_null() == NULL);
  const int32* data = result.column(0).typed_data<INT32>();
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(100, data[i]);
  }
}

TEST_F(TerminalExpressionTest, ConstString) {
  std::unique_ptr<BoundExpressionTree> values(
      DefaultBind(input().schema(), 100, ConstString("Supersonic")));
  EXPECT_TUPLE_SCHEMAS_EQUAL(
      TupleSchema::Singleton("CONST_STRING", STRING, NOT_NULLABLE),
      values->result_schema());
  const View& result = DefaultEvaluate(values.get(), input());
  EXPECT_TRUE(result.column(0).is_null() == NULL);
  const StringPiece* data = result.column(0).typed_data<STRING>();
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ("Supersonic", data[i]);
  }
}

TEST_F(TerminalExpressionTest, ToString) {
  std::unique_ptr<const Expression> str_exp(ConstString("Supersonic"));
  EXPECT_EQ("<STRING>'Supersonic'", str_exp->ToString(true));
  EXPECT_EQ("'Supersonic'", str_exp->ToString(false));

  std::unique_ptr<const Expression> bool_exp(ConstBool(true));
  EXPECT_EQ("<BOOL>TRUE", bool_exp->ToString(true));
  EXPECT_EQ("TRUE", bool_exp->ToString(false));

  std::unique_ptr<const Expression> int_exp(ConstInt32(4));
  EXPECT_EQ("<INT32>4", int_exp->ToString(true));
  EXPECT_EQ("4", int_exp->ToString(false));

  std::unique_ptr<const Expression> null_exp(Null(INT32));
  EXPECT_EQ("<INT32>NULL", null_exp->ToString(true));
  EXPECT_EQ("NULL", null_exp->ToString(false));
}

}  // namespace supersonic
