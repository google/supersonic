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

#include "supersonic/base/infrastructure/block.h"

#include "supersonic/utils/integral_types.h"
#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/base/infrastructure/copy_column.h"
#include "supersonic/base/infrastructure/view_copier.h"
#include "supersonic/testing/block_builder.h"
#include "supersonic/testing/comparators.h"
#include "supersonic/utils/strings/stringpiece.h"
#include "gtest/gtest.h"

namespace supersonic {

class BlockTest : public testing::Test {
 public:
  const rowcount_t Copy(const View& source, Block* destination) {
    ViewCopier copier(source.schema(), true);
    return copier.Copy(source.row_count(), source, 0, destination);
  }
  Block* test_block() {
    return BlockBuilder<STRING, INT32>()
      .AddRow("foo", 5)
      .AddRow("bar", 8)
      .Build();
  }
};

TEST_F(BlockTest, CreatedBlockShouldBeEmptyAndValid) {
  TupleSchema schema;
  schema.add_attribute(Attribute("foo", INT32, NULLABLE));
  schema.add_attribute(Attribute("BAR", STRING, NOT_NULLABLE));
  MemoryLimit limit(0);
  Block block(schema, &limit);
  EXPECT_EQ(0, block.row_capacity());
  EXPECT_EQ(2, block.column_count());
  EXPECT_FALSE(block.column(0).data().is_null());
  EXPECT_FALSE(block.column(1).data().is_null());
  EXPECT_TRUE(block.is_null(0) != NULL);
  EXPECT_TRUE(block.is_null(1) == NULL);
}

TEST_F(BlockTest, SuccessfulReallocShouldPreserveContent) {
  scoped_ptr<Block> test(test_block());
  Block block(test->schema(), HeapBufferAllocator::Get());
  ASSERT_TRUE(block.Reallocate(2));
  ASSERT_EQ(test->row_capacity(), Copy(test->view(), &block));
  EXPECT_VIEWS_EQUAL(test->view(), block.view());
  EXPECT_EQ(2, block.row_capacity());
  ASSERT_TRUE(block.Reallocate(4));
  EXPECT_EQ(4, block.row_capacity());
  View expected(block.schema());
  expected.ResetFromSubRange(block.view(), 0, 2);
  EXPECT_VIEWS_EQUAL(expected, test->view());
}

TEST_F(BlockTest, SuccessfulReallocShouldPreserveContentWithNulls) {
  scoped_ptr<Block> test(BlockBuilder<STRING, INT32>()
                         .AddRow("foo", 7)
                         .AddRow("bar", 10)
                         .AddRow(__,    __)
                         .Build());
  Block block(test->schema(), HeapBufferAllocator::Get());
  ASSERT_TRUE(block.Reallocate(3));
  ASSERT_EQ(test->row_capacity(), Copy(test->view(), &block));
  EXPECT_VIEWS_EQUAL(test->view(), block.view());
  EXPECT_EQ(3, block.row_capacity());
  ASSERT_TRUE(block.Reallocate(4));
  EXPECT_EQ(4, block.row_capacity());
  View expected(block.schema());
  expected.ResetFromSubRange(block.view(), 0, 3);
  EXPECT_VIEWS_EQUAL(expected, test->view());
}

TEST_F(BlockTest, UnsuccessfulReallocShouldAccountToNoOp) {
  scoped_ptr<Block> test(test_block());
  MemoryLimit limit(3 * (sizeof(StringPiece) +
                         sizeof(int32) + 9 /* for string arena */));
  Block block(test->schema(), &limit);
  ASSERT_TRUE(block.Reallocate(2));
  ASSERT_EQ(test->row_capacity(), Copy(test->view(), &block));
  EXPECT_VIEWS_EQUAL(test->view(), block.view());
  EXPECT_EQ(2, block.row_capacity());
  ASSERT_FALSE(block.Reallocate(4));
  EXPECT_EQ(2, block.row_capacity());
  EXPECT_VIEWS_EQUAL(test->view(), block.view());
}

TEST_F(BlockTest, BlockViewShouldPointToBlocksData) {
  scoped_ptr<Block> test(test_block());
  for (int i = 0; i < test->schema().attribute_count(); ++i) {
    EXPECT_EQ(test->mutable_column(i)->mutable_data(),
              test->column(i).data().raw());
    EXPECT_EQ(test->mutable_column(i)->mutable_is_null(),
              test->column(i).is_null());
  }
  // Same should hold true after reallocation.
  ASSERT_TRUE(test->Reallocate(20));
  for (int i = 0; i < test->schema().attribute_count(); ++i) {
    EXPECT_EQ(test->mutable_column(i)->mutable_data(),
              test->column(i).data());
    EXPECT_EQ(test->mutable_column(i)->mutable_is_null(),
              test->column(i).is_null());
  }
}

TEST_F(BlockTest, OffsetsShouldCalculateCorrectly) {
  scoped_ptr<Block> test(test_block());
  EXPECT_EQ(2,
            static_cast<const StringPiece*>(
                test->mutable_column(0)->mutable_data_plus_offset(2)) -
            static_cast<const StringPiece*>(
                test->mutable_column(0)->mutable_data()));
  EXPECT_EQ(2,
            (test->column(0).data_plus_offset(2).as<STRING>()) -
            (test->column(0).data().as<STRING>()));
  EXPECT_EQ(2,
            static_cast<const int32*>(
                test->mutable_column(1)->mutable_data_plus_offset(2)) -
            static_cast<const int32*>(test->mutable_column(1)->mutable_data()));
  EXPECT_EQ(2,
            (test->column(1).data_plus_offset(2).as<INT32>()) -
            (test->column(1).data().as<INT32>()));
}

class ViewTest : public testing::Test {
 public:
  void SetUp() {
    test_block_.reset(BlockBuilder<STRING, INT32>()
        .AddRow("foo", 5)
        .AddRow("bar", 8)
        .AddRow("baz", 4)
        .AddRow("raw", 4)
        .Build());
  }
  const View& test_view() const { return test_block_->view(); }

 private:
  scoped_ptr<Block> test_block_;
};

TEST_F(ViewTest, ResetWorks) {
  View view(test_view().schema());  // Empty view with test_view's schema.
  EXPECT_EQ(0, view.row_count());
  EXPECT_TRUE(view.column(0).data().is_null());
  EXPECT_TRUE(view.column(1).data().is_null());

  view.ResetFrom(test_view());
  EXPECT_EQ(4, view.row_count());
  EXPECT_EQ(test_view().column(0).data(), view.column(0).data());
  EXPECT_EQ(test_view().column(1).data(), view.column(1).data());
  EXPECT_EQ(test_view().column(0).is_null(), view.column(0).is_null());
  EXPECT_EQ(test_view().column(1).is_null(), view.column(1).is_null());
}

TEST_F(ViewTest, SubRangeResetWorks) {
  View view(test_view().schema());
  view.ResetFromSubRange(test_view(), 2, 1);
  EXPECT_EQ(1, view.row_count());
  EXPECT_EQ(test_view().column(0).typed_data<STRING>() + 2,
            view.column(0).data());
  EXPECT_EQ(test_view().column(1).typed_data<INT32>() + 2,
            view.column(1).data());
  EXPECT_EQ(test_view().column(0).is_null_plus_offset(2),
            view.column(0).is_null());
  EXPECT_EQ(test_view().column(1).is_null_plus_offset(2),
            view.column(1).is_null());
}

TEST_F(ViewTest, ConstructFromColumn) {
  View view(test_view().column(1), 3);
  EXPECT_EQ(3, view.row_count());
  EXPECT_EQ(view.column(0).data(), test_view().column(1).data());
}

}  // namespace supersonic
