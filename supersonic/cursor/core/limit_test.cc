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

#include "supersonic/cursor/core/limit.h"

#include <stddef.h>

#include <memory>

#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/utils/stringprintf.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/cursor/base/cursor.h"
#include "supersonic/cursor/base/operation.h"
#include "supersonic/proto/supersonic.pb.h"
#include "supersonic/testing/operation_testing.h"
#include "gtest/gtest.h"

namespace supersonic {

class LimitCursorTest : public testing::Test {
 protected:

  LimitCursorTest() {
  }

  void SetupInput(size_t input_size) {
    TestDataBuilder<STRING, INT64, UINT64, DOUBLE, DATETIME> builder;
    for (int i = 0; i < input_size; ++i) {
      builder.AddRow(StringPrintf("aString%01d", i), i, 2.0 * i,
                     1.0001 * i / 1024, 3.0 * i);
    }
    input_ = builder.Build();
  }

  void CheckData(size_t offset, const View& view) {
    for (int i = 0; i < view.row_count(); i++) {
      int index = i + offset;
      const Column& col1 = view.column(0);
      const Column& col2 = view.column(1);
      const Column& col3 = view.column(2);
      const Column& col4 = view.column(3);
      const Column& col5 = view.column(4);

      EXPECT_EQ(StringPrintf("aString%01d", index),
                col1.typed_data<STRING>()[i]);
      EXPECT_EQ(index,
                col2.typed_data<INT64>()[i]);
      EXPECT_EQ(2 * index,
                col3.typed_data<UINT64>()[i]);
      EXPECT_EQ(1.0001 * index / 1024,
                col4.typed_data<DOUBLE>()[i]);
      EXPECT_EQ(3 * index,
                col5.typed_data<DATETIME>()[i]);
    }
  }

  Operation* input_;
};

// Tests if LimitCursor works properly when called Next(row_count)
// with row_count < limit.
TEST_F(LimitCursorTest, AskForLess) {
  SetupInput(1024);

  std::unique_ptr<Operation> limit(Limit(0, 100, input_));

  FailureOrOwned<Cursor> create_cursor = limit->CreateCursor();
  ASSERT_TRUE(create_cursor.is_success());
  std::unique_ptr<Cursor> limit_cursor(create_cursor.release());
  ResultView result = limit_cursor->Next(50);
  ASSERT_TRUE(result.has_data());
  EXPECT_EQ(50, result.view().row_count());
  CheckData(0, result.view());
  result = limit_cursor->Next(50);
  ASSERT_TRUE(result.has_data());
  EXPECT_EQ(50, result.view().row_count());
  CheckData(50, result.view());
  result = limit_cursor->Next(50);
  EXPECT_TRUE(result.is_eos());
}

// Tests if LimitCursor works properly when called Next(row_count)
// with row_count > limit.
TEST_F(LimitCursorTest, AskForMore) {
  SetupInput(1024);
  std::unique_ptr<Operation> limit(Limit(0, 100, input_));

  FailureOrOwned<Cursor> create_cursor = limit->CreateCursor();
  ASSERT_TRUE(create_cursor.is_success());
  std::unique_ptr<Cursor> limit_cursor(create_cursor.release());
  ResultView result = limit_cursor->Next(120);
  ASSERT_TRUE(result.has_data());
  EXPECT_EQ(100, result.view().row_count());
  CheckData(0, result.view());
  result = limit_cursor->Next(120);
  EXPECT_TRUE(result.is_eos());
}

// Tests if LimitCursor works properly when called Next(row_count)
// with row_count > available input.
TEST_F(LimitCursorTest, AskForMoreThanInput) {
  SetupInput(1024);
  std::unique_ptr<Operation> limit(Limit(0, 2048, input_));

  FailureOrOwned<Cursor> create_cursor = limit->CreateCursor();
  ASSERT_TRUE(create_cursor.is_success());
  std::unique_ptr<Cursor> limit_cursor(create_cursor.release());
  ResultView result = limit_cursor->Next(1536);
  ASSERT_TRUE(result.has_data());
  EXPECT_EQ(1024, result.view().row_count());
  CheckData(0, result.view());
  result = limit_cursor->Next(50);
  EXPECT_TRUE(result.is_eos());
}

TEST_F(LimitCursorTest, LimitWithOffset) {
  SetupInput(1024);
  std::unique_ptr<Operation> limit(Limit(100, 200, input_));

  FailureOrOwned<Cursor> create_cursor = limit->CreateCursor();
  ASSERT_TRUE(create_cursor.is_success());
  std::unique_ptr<Cursor> limit_cursor(create_cursor.release());
  ResultView result = limit_cursor->Next(250);
  ASSERT_TRUE(result.has_data());
  EXPECT_EQ(200, result.view().row_count());
  CheckData(100, result.view());
  result = limit_cursor->Next(50);
  EXPECT_TRUE(result.is_eos());
}

}  // namespace supersonic
