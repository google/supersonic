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

#include <memory>

#include "supersonic/base/infrastructure/projector.h"
#include "supersonic/cursor/base/cursor.h"
#include "supersonic/cursor/core/aggregate.h"
#include "supersonic/cursor/core/aggregator.h"
#include "supersonic/cursor/core/hybrid_group_utils.h"
#include "supersonic/cursor/core/sort.h"
#include "supersonic/cursor/infrastructure/ordering.h"
#include "supersonic/testing/comparators.h"
#include "supersonic/testing/operation_testing.h"

#include "gtest/gtest.h"
#include "gtest/gtest.h"
#include "supersonic/utils/container_literal.h"

namespace supersonic {

// The example from BoundHybridGroupTransform comment.
TEST(HybridGroupTransformTest, HybridGroupTransformExampleTest) {
  std::unique_ptr<Cursor> input(
      TestDataBuilder<INT32, INT32, INT32, INT32, INT32>()
          .AddRow(1, 2, 3, 4, 5)
          .AddRow(6, 7, 8, 9, 0)
          .BuildCursor());

  std::unique_ptr<Cursor> expected_output(
      TestDataBuilder<INT32, INT32, INT32, INT32, INT32>()
          .AddRow(1, 2, __, __, __)
          .AddRow(6, 7, __, __, __)
          .AddRow(1, __, 3, __, __)
          .AddRow(6, __, 8, __, __)
          .AddRow(1, __, __, 4, 2)
          .AddRow(6, __, __, 9, 7)
          .BuildCursor());

  util::gtl::PointerVector<const SingleSourceProjector> column_groups;
  column_groups.push_back(ProjectNamedAttributeAs("col1", "a"));
  column_groups.push_back(ProjectNamedAttributeAs("col2", "b"));
  column_groups.push_back(ProjectNamedAttributes(
      util::gtl::Container("col3", "col1").As<vector<string> >()));

  std::unique_ptr<Cursor> transformed(SucceedOrDie(
      BoundHybridGroupTransform(ProjectNamedAttribute("col0"), column_groups,
                                HeapBufferAllocator::Get(), input.release())));

  EXPECT_FALSE(transformed->schema().attribute(0).is_nullable());
  EXPECT_TRUE(transformed->schema().attribute(1).is_nullable());
  EXPECT_TRUE(transformed->schema().attribute(2).is_nullable());
  EXPECT_TRUE(transformed->schema().attribute(3).is_nullable());
  EXPECT_TRUE(transformed->schema().attribute(4).is_nullable());
  EXPECT_CURSORS_EQUAL(expected_output.release(),
                       transformed.release());
}

// One column group - it should produce the same number of rows.
TEST(HybridGroupTransformTest, HybridGroupTransformOneGroup) {
  std::unique_ptr<Cursor> input(TestDataBuilder<INT32, INT32, INT32, INT32>()
                                    .AddRow(1, 3, 10, 0)
                                    .AddRow(1, 4, 11, 0)
                                    .AddRow(3, -3, 10, 0)
                                    .AddRow(2, 4, 11, 0)
                                    .AddRow(3, -5, 10, 0)
                                    .BuildCursor());

  std::unique_ptr<Cursor> expected_output(TestDataBuilder<INT32, INT32>()
                                              .AddRow(1, 3)
                                              .AddRow(1, 4)
                                              .AddRow(3, -3)
                                              .AddRow(2, 4)
                                              .AddRow(3, -5)
                                              .BuildCursor());

  util::gtl::PointerVector<const SingleSourceProjector> column_groups;
  column_groups.push_back(ProjectNamedAttribute("col1"));

  std::unique_ptr<Cursor> transformed(SucceedOrDie(
      BoundHybridGroupTransform(ProjectNamedAttribute("col0"), column_groups,
                                HeapBufferAllocator::Get(), input.release())));

  EXPECT_CURSORS_EQUAL(expected_output.release(),
                       transformed.release());
}

TEST(HybridGroupTransformTest, HybridGroupTransformThreeGroups) {
  std::unique_ptr<Cursor> input(TestDataBuilder<INT32, INT32, INT32, INT32>()
                                    .AddRow(1, 3, 10, 0)
                                    .AddRow(1, 4, 11, 0)
                                    .AddRow(3, -3, 10, 0)
                                    .AddRow(2, 4, 11, 0)
                                    .AddRow(3, -5, 10, 0)
                                    .BuildCursor());

  std::unique_ptr<Cursor> expected_output(
      TestDataBuilder<INT32, INT32, INT32, INT32>()
          .AddRow(1, 3, __, __)
          .AddRow(1, 4, __, __)
          .AddRow(3, -3, __, __)
          .AddRow(2, 4, __, __)
          .AddRow(3, -5, __, __)
          .AddRow(1, __, 10, __)
          .AddRow(1, __, 11, __)
          .AddRow(3, __, 10, __)
          .AddRow(2, __, 11, __)
          .AddRow(3, __, 10, __)
          .AddRow(1, __, __, 0)
          .AddRow(1, __, __, 0)
          .AddRow(3, __, __, 0)
          .AddRow(2, __, __, 0)
          .AddRow(3, __, __, 0)
          .BuildCursor());

  util::gtl::PointerVector<const SingleSourceProjector> column_groups;
  column_groups.push_back(ProjectNamedAttribute("col1"));
  column_groups.push_back(ProjectNamedAttribute("col2"));
  column_groups.push_back(ProjectNamedAttribute("col3"));

  std::unique_ptr<Cursor> transformed(SucceedOrDie(
      BoundHybridGroupTransform(ProjectNamedAttribute("col0"), column_groups,
                                HeapBufferAllocator::Get(), input.release())));

  EXPECT_CURSORS_EQUAL(expected_output.release(),
                       transformed.release());
}

TEST(HybridGroupUtilsTest, ExtendByConstantColumnAddsZeroColumn) {
  std::unique_ptr<Cursor> input(TestDataBuilder<INT32, STRING>()
                                    .AddRow(1, "a")
                                    .AddRow(2, "b")
                                    .AddRow(3, __)
                                    .BuildCursor());

  std::unique_ptr<Cursor> expected_result(
      TestDataBuilder<INT32, STRING, INT32>()
          .AddRow(1, "a", 0)
          .AddRow(2, "b", 0)
          .AddRow(3, __, 0)
          .BuildCursor());

  std::unique_ptr<Cursor> transformed(SucceedOrDie(ExtendByConstantColumn(
      "new_column", HeapBufferAllocator::Get(), input.release())));

  EXPECT_EQ(3, transformed->schema().attribute_count());
  EXPECT_FALSE(transformed->schema().attribute(2).is_nullable());
  EXPECT_CURSORS_EQUAL(expected_result.release(),
                       transformed.release());
}

TEST(HybridGroupUtilsTest, MakeSelectedColumnsNotNullableTest) {
  std::unique_ptr<Cursor> input(TestDataBuilder<INT32, STRING>()
                                    .AddRow(1, "a")
                                    .AddRow(__, "b")
                                    .AddRow(3, __)
                                    .BuildCursor());

  std::unique_ptr<Cursor> expected_result(TestDataBuilder<INT32, STRING>()
                                              .AddRow(1, "a")
                                              .AddRow(0, "b")
                                              .AddRow(3, __)
                                              .BuildCursor());

  EXPECT_TRUE(input->schema().attribute(0).is_nullable());

  std::unique_ptr<Cursor> transformed(
      SucceedOrDie(MakeSelectedColumnsNotNullable(
          ProjectAttributeAt(0), HeapBufferAllocator::Get(), input.release())));

  EXPECT_EQ(2, transformed->schema().attribute_count());
  EXPECT_FALSE(transformed->schema().attribute(0).is_nullable());
  EXPECT_CURSORS_EQUAL(expected_result.release(),
                       transformed.release());
}

}  // namespace supersonic
