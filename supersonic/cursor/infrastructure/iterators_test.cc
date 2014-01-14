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

#include "supersonic/cursor/infrastructure/iterators.h"

#include <memory>

#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/cursor/base/cursor_transformer.h"
#include "supersonic/cursor/core/scan_view.h"
#include "supersonic/cursor/core/spy.h"
#include "supersonic/cursor/infrastructure/basic_cursor.h"
#include "supersonic/testing/block_builder.h"
#include "supersonic/testing/operation_testing.h"
#include "gtest/gtest.h"

namespace supersonic {

class DestructorCatcher : public BasicDecoratorCursor {
 public:
  DestructorCatcher(Cursor* delegate, bool* destroyed)
      : BasicDecoratorCursor(delegate),
        destroyed_(destroyed) {
    CHECK(!*destroyed);
  }
  virtual ~DestructorCatcher() { *destroyed_ = true; }
 private:
  bool* destroyed_;
};

class BaseIteratorTest : public testing::Test {
 public:
  BaseIteratorTest()
      : schema_(TupleSchema::Singleton("foo", INT32, NULLABLE)),
        data_(TestDataBuilder<INT32>().AddRow(1).AddRow(2).AddRow(3)
                                      .AddRow(4).AddRow(5).AddRow(6).Build()),
        failing_(TestDataBuilder<INT32>().AddRow(1).AddRow(2).AddRow(3)
                                         .ReturnException(ERROR_UNKNOWN_ERROR)
                                         .Build()),
        empty_(TestDataBuilder<INT32>().Build()) {}

  TupleSchema schema() const { return schema_; }
  const View& view() const { return data_->view(); }
  Cursor* CreatePlainCursor() {
    return SucceedOrDie(data_->CreateCursor());
  }
  Cursor* CreateFailingCursor() {
    return SucceedOrDie(failing_->CreateCursor());
  }
  Cursor* CreateEmptyCursor() {
    return SucceedOrDie(empty_->CreateCursor());
  }

 private:
  TupleSchema schema_;
  std::unique_ptr<TestData> data_;
  std::unique_ptr<TestData> failing_;
  std::unique_ptr<TestData> empty_;
};

class ViewIteratorTest : public BaseIteratorTest {};

TEST_F(ViewIteratorTest, NewIteratorIsEmptyAndCantMove) {
  ViewIterator iterator(schema());
  EXPECT_EQ(0, iterator.view().row_count());
  EXPECT_EQ(0, iterator.row_count());
  EXPECT_EQ(1, iterator.column_count());
  EXPECT_EQ(INT32, iterator.column(0).attribute().type());
  EXPECT_FALSE(iterator.next(1));
  EXPECT_FALSE(iterator.next(1));
  EXPECT_FALSE(iterator.truncate(0));
}

TEST_F(ViewIteratorTest, ResetAndTruncateWork) {
  ViewIterator iterator(view());
  EXPECT_EQ(0, iterator.row_count());
  EXPECT_TRUE(iterator.next(100));
  EXPECT_EQ(6, iterator.row_count());
  EXPECT_TRUE(iterator.truncate(2));
  EXPECT_EQ(2, iterator.row_count());
  iterator.reset(view());
  EXPECT_EQ(0, iterator.row_count());
  EXPECT_TRUE(iterator.next(3));
  EXPECT_EQ(3, iterator.row_count());
  EXPECT_TRUE(iterator.next(30));
  EXPECT_EQ(3, iterator.row_count());
  EXPECT_FALSE(iterator.next(100));
}

TEST_F(ViewIteratorTest, RepetitiveTruncateWorks) {
  ViewIterator iterator(view());
  EXPECT_TRUE(iterator.next(50));
  EXPECT_EQ(6, iterator.row_count());
  EXPECT_FALSE(iterator.truncate(8));
  EXPECT_EQ(6, iterator.row_count());
  EXPECT_FALSE(iterator.truncate(6));
  EXPECT_EQ(6, iterator.row_count());
  EXPECT_TRUE(iterator.truncate(5));
  EXPECT_EQ(5, iterator.row_count());
  EXPECT_TRUE(iterator.truncate(3));
  EXPECT_EQ(3, iterator.row_count());
  EXPECT_TRUE(iterator.truncate(0));
  EXPECT_EQ(0, iterator.row_count());
}

TEST_F(ViewIteratorTest, RowByRowIterationWorks) {
  ViewIterator iterator(view());
  for (int i = 0 ; i < 6; ++i) {
    EXPECT_TRUE(iterator.next(1));
    EXPECT_EQ(1, iterator.row_count());
    EXPECT_EQ(i + 1, *iterator.view().column(0).typed_data<INT32>());
  }
  EXPECT_FALSE(iterator.next(1));
}

class ViewRowIteratorTest : public BaseIteratorTest {};

TEST_F(ViewRowIteratorTest, NewIteratorIsEmpty) {
  ViewRowIterator iterator(schema());
  EXPECT_EQ(-1, iterator.current_row_index());
  EXPECT_EQ(0, iterator.total_row_count());
  EXPECT_LT(iterator.current_row_index(), iterator.total_row_count());
  EXPECT_EQ(1, iterator.schema().attribute_count());
  EXPECT_EQ(INT32, iterator.schema().attribute(0).type());
  EXPECT_FALSE(iterator.next());
}

TEST_F(ViewRowIteratorTest, ClearWorks) {
  ViewRowIterator iterator(view());
  iterator.clear();
  EXPECT_EQ(-1, iterator.current_row_index());
  EXPECT_EQ(0, iterator.total_row_count());
}

TEST_F(ViewRowIteratorTest, ResetWorks) {
  ViewRowIterator iterator(view());
  EXPECT_EQ(6, iterator.total_row_count());
  EXPECT_EQ(-1, iterator.current_row_index());
  EXPECT_TRUE(iterator.next());
  EXPECT_TRUE(iterator.next());
  EXPECT_EQ(6, iterator.total_row_count());
  EXPECT_EQ(1, iterator.current_row_index());
  iterator.reset(view());
  EXPECT_EQ(6, iterator.total_row_count());
  EXPECT_EQ(-1, iterator.current_row_index());
  EXPECT_TRUE(iterator.next());
  EXPECT_TRUE(iterator.next());
  EXPECT_TRUE(iterator.next());
  EXPECT_EQ(6, iterator.total_row_count());
  EXPECT_EQ(2, iterator.current_row_index());
}

TEST_F(ViewRowIteratorTest, RowByRowIterationWorks) {
  ViewRowIterator iterator(view());
  for (int i = 0 ; i < 6; ++i) {
    EXPECT_TRUE(iterator.next());
    EXPECT_EQ(i, iterator.current_row_index());
    EXPECT_EQ(6, iterator.total_row_count());
    EXPECT_EQ(i + 1, *iterator.typed_data<INT32>(0));
    EXPECT_FALSE(iterator.is_null(0));
  }
  EXPECT_FALSE(iterator.next());
}

class CursorIteratorTest : public BaseIteratorTest {};

class CursorIteratorSpyTest : public CursorIteratorTest,
                              public testing::WithParamInterface<bool> {
};

TEST_F(CursorIteratorTest, NewIteratorIsAtBOS) {
  CursorIterator iterator(CreatePlainCursor());
  EXPECT_TRUE(iterator.is_bos());
  EXPECT_FALSE(iterator.is_eos());
  EXPECT_FALSE(iterator.is_failure());
}

TEST_F(CursorIteratorTest, EagerIterationWorks) {
  CursorIterator iterator(CreateViewLimiter(4, CreatePlainCursor()));
  EXPECT_TRUE(iterator.EagerNext());
  EXPECT_FALSE(iterator.is_failure());
  EXPECT_FALSE(iterator.is_bos());
  EXPECT_FALSE(iterator.is_eos());
  EXPECT_EQ(4, iterator.view().row_count());
  EXPECT_EQ(0, iterator.current_row_index());
  EXPECT_EQ(1, *iterator.view().column(0).typed_data<INT32>());
  EXPECT_TRUE(iterator.EagerNext());
  EXPECT_FALSE(iterator.is_failure());
  EXPECT_FALSE(iterator.is_eos());
  EXPECT_EQ(2, iterator.view().row_count());
  EXPECT_EQ(4, iterator.current_row_index());
  EXPECT_EQ(5, *iterator.view().column(0).typed_data<INT32>());
  EXPECT_FALSE(iterator.EagerNext());
  EXPECT_TRUE(iterator.is_eos());
  EXPECT_EQ(6, iterator.current_row_index());
  EXPECT_FALSE(iterator.EagerNext());
  EXPECT_TRUE(iterator.is_eos());
  EXPECT_EQ(6, iterator.current_row_index());
}

INSTANTIATE_TEST_CASE_P(SpyUse, CursorIteratorSpyTest, testing::Bool());

TEST_P(CursorIteratorSpyTest, ConstrainedIterationWorks) {
  CursorIterator iterator(CreateViewLimiter(4, CreatePlainCursor()));

  if (GetParam()) {
    std::unique_ptr<CursorTransformer> spy_transform(PrintingSpyTransformer());
    iterator.ApplyToCursor(spy_transform.get());
  }

  EXPECT_TRUE(iterator.Next(3, true));
  EXPECT_FALSE(iterator.is_failure());
  EXPECT_FALSE(iterator.is_bos());
  EXPECT_FALSE(iterator.is_eos());
  EXPECT_EQ(3, iterator.view().row_count());
  EXPECT_EQ(0, iterator.current_row_index());
  EXPECT_EQ(1, *iterator.view().column(0).typed_data<INT32>());
  EXPECT_TRUE(iterator.Next(3, true));
  EXPECT_FALSE(iterator.is_failure());
  EXPECT_FALSE(iterator.is_eos());
  EXPECT_EQ(3, iterator.view().row_count());
  EXPECT_EQ(3, iterator.current_row_index());
  EXPECT_EQ(4, *iterator.view().column(0).typed_data<INT32>());
  EXPECT_FALSE(iterator.EagerNext());
  EXPECT_TRUE(iterator.is_eos());
  EXPECT_EQ(6, iterator.current_row_index());
  EXPECT_FALSE(iterator.EagerNext());
  EXPECT_TRUE(iterator.is_eos());
  EXPECT_EQ(6, iterator.current_row_index());
}

TEST_F(CursorIteratorTest, TruncatingIterationWorks) {
  CursorIterator iterator(CreateViewLimiter(4, CreatePlainCursor()));
  EXPECT_TRUE(iterator.EagerNext());
  ASSERT_FALSE(iterator.is_failure());
  ASSERT_FALSE(iterator.is_eos());
  ASSERT_FALSE(iterator.is_bos());
  ASSERT_EQ(4, iterator.view().row_count());
  EXPECT_EQ(0, iterator.current_row_index());
  EXPECT_TRUE(iterator.truncate(3));
  EXPECT_EQ(3, iterator.view().row_count());
  EXPECT_EQ(0, iterator.current_row_index());
  EXPECT_EQ(1, *iterator.view().column(0).typed_data<INT32>());
  EXPECT_TRUE(iterator.EagerNext());
  ASSERT_FALSE(iterator.is_failure());
  ASSERT_FALSE(iterator.is_eos());
  ASSERT_EQ(1, iterator.view().row_count());
  EXPECT_EQ(3, iterator.current_row_index());
  EXPECT_FALSE(iterator.truncate(3));  // It has just 1 (from previous view).
  EXPECT_TRUE(iterator.EagerNext());
  EXPECT_FALSE(iterator.is_failure());
  EXPECT_FALSE(iterator.is_eos());
  EXPECT_EQ(4, iterator.current_row_index());
  EXPECT_FALSE(iterator.truncate(3));  // It has just 2 now.
  EXPECT_EQ(2, iterator.view().row_count());
  EXPECT_EQ(4, iterator.current_row_index());
  EXPECT_EQ(5, *iterator.view().column(0).typed_data<INT32>());
  EXPECT_FALSE(iterator.EagerNext());
  EXPECT_TRUE(iterator.is_eos());
  EXPECT_EQ(6, iterator.current_row_index());
  EXPECT_FALSE(iterator.EagerNext());
  EXPECT_TRUE(iterator.is_eos());
  EXPECT_EQ(6, iterator.current_row_index());
}

TEST_F(CursorIteratorTest, TerminationNoOpAtEos) {
  CursorIterator iterator(CreatePlainCursor());
  while (iterator.Next(Cursor::kDefaultRowCount, false));
  iterator.Terminate();
  EXPECT_TRUE(iterator.is_eos());
}

TEST_F(CursorIteratorTest, TerminationInterrupts) {
  CursorIterator iterator(CreatePlainCursor());
  iterator.Terminate();
  EXPECT_TRUE(iterator.is_failure());
}

TEST_F(CursorIteratorTest, CursorIsDestroyedAtEos) {
  bool destroyed = false;
  CursorIterator iterator(
      new DestructorCatcher(CreatePlainCursor(), &destroyed));
  EXPECT_FALSE(destroyed);
  while (iterator.Next(Cursor::kDefaultRowCount, false)) {
    EXPECT_FALSE(destroyed);
  }
  EXPECT_TRUE(destroyed);
}

TEST_F(CursorIteratorTest, CursorIsDestroyedAtFailure) {
  bool destroyed = false;
  CursorIterator iterator(
      new DestructorCatcher(CreateFailingCursor(), &destroyed));
  EXPECT_FALSE(destroyed);
  while (iterator.Next(Cursor::kDefaultRowCount, false)) {
    EXPECT_FALSE(destroyed);
  }
  EXPECT_TRUE(destroyed);
}

class CursorRowIteratorTest : public BaseIteratorTest {};

class CursorRowIteratorSpyTest : public CursorRowIteratorTest,
                                 public testing::WithParamInterface<bool> {
};

TEST_F(CursorRowIteratorTest, NewIteratorIsInitialized) {
  CursorRowIterator iterator(CreatePlainCursor());
  EXPECT_EQ(INT32, iterator.schema().attribute(0).type());
  EXPECT_EQ(INT32, iterator.type_info(0).type());
  EXPECT_EQ(-1, iterator.current_row_index());
}

INSTANTIATE_TEST_CASE_P(SpyUse, CursorRowIteratorSpyTest, ::testing::Bool());

TEST_P(CursorRowIteratorSpyTest, RowByRowIterationWorks) {
  CursorRowIterator iterator(
      CreateViewLimiter(4, CreatePlainCursor()));

  if (GetParam()) {
    std::unique_ptr<CursorTransformer> spy_transform(PrintingSpyTransformer());
    iterator.Transform(spy_transform.get());
  }

  for (int i = 0 ; i < 6; ++i) {
    EXPECT_TRUE(iterator.Next());
    EXPECT_EQ(i, iterator.current_row_index());
    EXPECT_EQ(i + 1, *iterator.typed_data<INT32>(0));
    EXPECT_FALSE(iterator.is_null(0));
  }
  EXPECT_FALSE(iterator.Next());
  EXPECT_EQ(6, iterator.current_row_index());
}

TEST_F(CursorRowIteratorTest, TerminationNoOpAtEos) {
  CursorRowIterator iterator(CreatePlainCursor());
  while (iterator.Next());
  iterator.Terminate();
  EXPECT_TRUE(iterator.is_eos());
}

TEST_F(CursorRowIteratorTest, TerminationInterrupts) {
  CursorRowIterator iterator(CreatePlainCursor());
  iterator.Terminate();
  EXPECT_TRUE(iterator.is_failure());
}

TEST_F(CursorRowIteratorTest, CursorIsDestroyedAtEos) {
  bool destroyed = false;
  CursorRowIterator iterator(
      new DestructorCatcher(CreatePlainCursor(), &destroyed));
  EXPECT_FALSE(destroyed);
  while (iterator.Next()) {
    EXPECT_FALSE(destroyed);
  }
  EXPECT_TRUE(destroyed);
}

TEST_F(CursorRowIteratorTest, CursorIsDestroyedAtFailure) {
  bool destroyed = false;
  CursorRowIterator iterator(
      new DestructorCatcher(CreateFailingCursor(), &destroyed));
  EXPECT_FALSE(destroyed);
  while (iterator.Next()) {
    EXPECT_FALSE(destroyed);
  }
  EXPECT_TRUE(destroyed);
}

// Regression test. Previous implementation does not support repeatedly calling
// Next() after EOS.
TEST_F(CursorRowIteratorTest, NextOnEmptyCursor) {
  CursorRowIterator iterator(
      CreateViewLimiter(4, CreateEmptyCursor()));
  EXPECT_FALSE(iterator.Next());
  EXPECT_FALSE(iterator.Next());
}

// Same with above, but with some present data.
TEST_F(CursorRowIteratorTest, RepeatedNextAfterEOSTest) {
  CursorRowIterator iterator(
      CreateViewLimiter(4, CreatePlainCursor()));
  for (int i = 0 ; i < 6; ++i) {
    EXPECT_TRUE(iterator.Next());
  }
  for (int i = 0; i < 10; ++i) {
    EXPECT_FALSE(iterator.Next());
  }
  EXPECT_EQ(6, iterator.current_row_index());
}

// Regression test. Previous implementation does not support repeatedly calling
// Next() after failure.
TEST_F(CursorRowIteratorTest, RepeatedNextAfterFailureTest) {
  CursorRowIterator iterator(CreateFailingCursor());
  while (iterator.Next()) {}
  for (int i = 0; i < 10; ++i) {
    EXPECT_FALSE(iterator.Next());
  }
}

}  // namespace supersonic
