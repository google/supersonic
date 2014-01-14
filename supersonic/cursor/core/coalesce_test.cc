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

#include "supersonic/cursor/core/coalesce.h"

#include <memory>

#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/base/infrastructure/projector.h"
#include "supersonic/cursor/base/cursor.h"
#include "supersonic/cursor/base/cursor_transformer.h"
#include "supersonic/cursor/core/limit.h"
#include "supersonic/cursor/core/project.h"
#include "supersonic/cursor/core/spy.h"
#include "supersonic/proto/supersonic.pb.h"
#include "supersonic/testing/comparable_cursor.h"
#include "supersonic/testing/operation_testing.h"
#include "gtest/gtest.h"
#include "supersonic/utils/container_literal.h"

using util::gtl::Container;

namespace supersonic {

class BoundCoalesceCursorTest : public ::testing::Test {
 public:
  static const int kAttributeCount = 4;
  static const int kRowCount = 16;

  void SetUp() {
    TestDataBuilder<INT32, INT32, INT32, INT32> builder;

    for (int i = 0; i < kRowCount; ++i) {
      const int row_tag = 10 * (i + 1);
      builder.AddRow(row_tag + 1, row_tag + 2, row_tag + 3, row_tag + 4);
    }

    test_data_.reset(builder.Build());
  }

  Cursor* CreateTestCursor() {
    return CreateTestCursor(0, kAttributeCount);
  }

  Cursor* CreateTestCursor(int attribute_offset,
                           int attribute_count) {
    return CreateTestCursor(attribute_offset, attribute_count, kRowCount);
  }

  Cursor* CreateTestCursor(int attribute_offset,
                           int attribute_count,
                           int row_count);

 private:
  std::unique_ptr<TestData> test_data_;
};

class BoundCoalesceCursorSpyTest : public BoundCoalesceCursorTest,
                                   public ::testing::WithParamInterface<bool> {
};

Cursor* BoundCoalesceCursorTest::CreateTestCursor(int attribute_offset,
                                             int attribute_count,
                                             int row_count) {
  DCHECK_GE(attribute_offset, 0);
  DCHECK_GE(attribute_count, 0);
  DCHECK_LE(attribute_offset + attribute_count, kAttributeCount);
  DCHECK_GE(row_count, 0);
  DCHECK_LE(row_count, kRowCount);

  FailureOrOwned<Cursor> cursor = test_data_->CreateCursor();
  DCHECK(cursor.is_success());

  std::unique_ptr<Cursor> limit_cursor(
      BoundLimit(0, row_count, cursor.release()));

  CompoundSingleSourceProjector projector;
  for (int i = attribute_offset;
       i < attribute_offset + attribute_count; i++) {
    projector.add(ProjectAttributeAt(i));
  }

  FailureOrOwned<const BoundSingleSourceProjector> bound_projector =
      projector.Bind(test_data_->schema());
  DCHECK(bound_projector.is_success());

  return BoundProject(bound_projector.release(), limit_cursor.release());
}

TEST_F(BoundCoalesceCursorTest, EmptyVector) {
  vector<Cursor*> children;
  FailureOrOwned<Cursor> cursor = BoundCoalesce(children);
  EXPECT_TRUE(cursor.is_success());

  EXPECT_EQ(0, cursor->column_count());

  ResultView result_view = cursor->Next(Cursor::kDefaultRowCount);
  EXPECT_TRUE(result_view.is_eos());

  children.push_back(cursor.release());
  FailureOrOwned<Cursor> nested_cursor = BoundCoalesce(children);
  EXPECT_TRUE(nested_cursor.is_success());

  EXPECT_EQ(0, nested_cursor->column_count());

  result_view = nested_cursor->Next(Cursor::kDefaultRowCount);
  EXPECT_TRUE(result_view.is_eos());
}

TEST_F(BoundCoalesceCursorTest, OneCursor) {
  std::unique_ptr<Cursor> cursor(CreateTestCursor());

  vector<Cursor*> children;
  children.push_back(cursor.release());
  FailureOrOwned<Cursor> coalesced_cursor = BoundCoalesce(children);
  EXPECT_TRUE(coalesced_cursor.is_success());

  std::unique_ptr<ComparableCursor> coalesce_result(
      new ComparableCursor(coalesced_cursor.release()));

  std::unique_ptr<ComparableCursor> expected_result(
      new ComparableCursor(CreateTestCursor()));

  EXPECT_TRUE(*coalesce_result == *expected_result);
}

TEST_F(BoundCoalesceCursorTest, SameCursor) {
  std::unique_ptr<Cursor> cursor(CreateTestCursor());

  vector<Cursor*> children;
  children.push_back(cursor.get());
  children.push_back(cursor.get());
  children.push_back(cursor.get());
  FailureOrOwned<Cursor> coalesced_cursor = BoundCoalesce(children);
  EXPECT_TRUE(coalesced_cursor.is_failure());
}

TEST_F(BoundCoalesceCursorTest, SameAttributeName) {
  std::unique_ptr<Cursor> cursor1(CreateTestCursor(0, kAttributeCount - 1));
  std::unique_ptr<Cursor> cursor2(CreateTestCursor(kAttributeCount - 2, 2));

  vector<Cursor*> children;
  children.push_back(cursor1.get());
  children.push_back(cursor2.get());
  FailureOrOwned<Cursor> coalesced_cursor = BoundCoalesce(children);
  EXPECT_TRUE(coalesced_cursor.is_failure());
}

INSTANTIATE_TEST_CASE_P(SpyUse, BoundCoalesceCursorSpyTest, ::testing::Bool());

TEST_P(BoundCoalesceCursorSpyTest, SimpleCoalesce) {
  std::unique_ptr<Cursor> cursor1(CreateTestCursor(0, kAttributeCount - 1));
  std::unique_ptr<Cursor> cursor2(CreateTestCursor(kAttributeCount - 1, 1));

  vector<Cursor*> children;
  children.push_back(cursor1.release());
  children.push_back(cursor2.release());
  std::unique_ptr<Cursor> coalesced_cursor(
      SucceedOrDie(BoundCoalesce(children)));

  if (GetParam()) {
    std::unique_ptr<CursorTransformerWithSimpleHistory> spy_transformer(
        PrintingSpyTransformer());
    coalesced_cursor->ApplyToChildren(spy_transformer.get());
    coalesced_cursor.reset(
        spy_transformer->Transform(coalesced_cursor.release()));
  }

  std::unique_ptr<Cursor> expected_cursor(CreateTestCursor());

  ComparableCursor coalesced_result(coalesced_cursor.release());
  ComparableCursor expected_result(expected_cursor.release());
  EXPECT_TRUE(coalesced_result == expected_result);
}

TEST_F(BoundCoalesceCursorTest, ManyCursors) {
  DCHECK_GE(kRowCount, kAttributeCount);
  vector<Cursor*> children;
  for (int i = 0; i < kAttributeCount; i++) {
    std::unique_ptr<Cursor> cursor(CreateTestCursor(i, 1, kRowCount - i));
    children.push_back(cursor.release());
  }
  FailureOrOwned<Cursor> coalesced_cursor = BoundCoalesce(children);
  EXPECT_TRUE(coalesced_cursor.is_success());

  std::unique_ptr<Cursor> expected_cursor(
      CreateTestCursor(0, kAttributeCount, kRowCount - kAttributeCount + 1));

  ComparableCursor coalesced_result(coalesced_cursor.release());
  ComparableCursor expected_result(expected_cursor.release());
  EXPECT_TRUE(coalesced_result == expected_result);
}

TEST_F(BoundCoalesceCursorTest, EmptyCursor) {
  std::unique_ptr<Cursor> cursor1(CreateTestCursor());
  std::unique_ptr<Cursor> cursor2(CreateTestCursor(0, 0));

  vector<Cursor*> children;
  children.push_back(cursor1.release());
  children.push_back(cursor2.release());
  FailureOrOwned<Cursor> coalesced_cursor = BoundCoalesce(children);
  EXPECT_TRUE(coalesced_cursor.is_success());

  std::unique_ptr<Cursor> expected_cursor(CreateTestCursor());

  ComparableCursor coalesced_result(coalesced_cursor.release());
  ComparableCursor expected_result(expected_cursor.release());
  EXPECT_TRUE(coalesced_result == expected_result);
}

TEST_F(BoundCoalesceCursorTest, RowCount) {
  std::unique_ptr<Cursor> cursor1(
      CreateTestCursor(0, kAttributeCount - 1, kRowCount - 1));
  std::unique_ptr<Cursor> cursor2(CreateTestCursor(kAttributeCount - 1, 1));

  vector<Cursor*> children;
  children.push_back(cursor1.release());
  children.push_back(cursor2.release());
  FailureOrOwned<Cursor> coalesced_cursor = BoundCoalesce(children);
  EXPECT_TRUE(coalesced_cursor.is_success());

  std::unique_ptr<Cursor> expected_cursor(
      CreateTestCursor(0, kAttributeCount, kRowCount - 1));

  ComparableCursor coalesced_result(coalesced_cursor.release());
  ComparableCursor expected_result(expected_cursor.release());
  EXPECT_TRUE(coalesced_result == expected_result);
}

TEST_F(BoundCoalesceCursorTest, TransformTest) {
  std::unique_ptr<Cursor> cursor1(CreateTestCursor(0, kAttributeCount - 1));
  std::unique_ptr<Cursor> cursor2(CreateTestCursor(kAttributeCount - 1, 1));

  vector<Cursor*> children;
  children.push_back(cursor1.release());
  children.push_back(cursor2.release());
  FailureOrOwned<Cursor> coalesced_cursor = BoundCoalesce(children);
  EXPECT_TRUE(coalesced_cursor.is_success());

  std::unique_ptr<CursorTransformerWithSimpleHistory> spy_transformer(
      PrintingSpyTransformer());
  coalesced_cursor->ApplyToChildren(spy_transformer.get());

  // Two input children.
  ASSERT_EQ(2, spy_transformer->GetHistoryLength());

  EXPECT_EQ(children[0], spy_transformer->GetEntryAt(0)->original());
  EXPECT_EQ(children[1], spy_transformer->GetEntryAt(1)->original());
}

TEST(CoalesceOperationTests, FailsOnDuplicatedColumnName) {
  vector<Operation*> child_ops;
  TestDataBuilder<INT32, STRING> builder_1;
  builder_1.AddRow(0, "foo");
  child_ops.push_back(builder_1.Build());
  TestDataBuilder<INT32, STRING> builder_2;
  builder_2.AddRow(1, "bar");
  child_ops.push_back(builder_2.Build());
  std::unique_ptr<Operation> op(CHECK_NOTNULL(Coalesce(child_ops)));
  FailureOrOwned<Cursor> cursor(op->CreateCursor());
  EXPECT_TRUE(cursor.is_failure());
}

TEST(CoalesceOperationTests, Succeeds) {
  TestDataBuilder<INT32, STRING> builder_1;
  builder_1.AddRow(0, "foo");
  std::unique_ptr<Operation> op_1(Project(
      ProjectRename(Container("left_1", "left_2"), ProjectAllAttributes()),
      builder_1.Build()));
  TestDataBuilder<INT32, STRING> builder_2;
  builder_2.AddRow(1, "bar");
  std::unique_ptr<Operation> op_2(Project(
      ProjectRename(Container("right_1", "right_2"), ProjectAllAttributes()),
      builder_2.Build()));
  std::unique_ptr<Operation> op(
      CHECK_NOTNULL(Coalesce(Container(op_1.release(), op_2.release()))));
  FailureOrOwned<Cursor> cursor(op->CreateCursor());
  EXPECT_TRUE(cursor.is_success());
}

}  // namespace supersonic
