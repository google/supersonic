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

#include "supersonic/expression/core/projecting_expressions.h"

#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/base/infrastructure/bit_pointers.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/memory/memory.h"
#include "supersonic/proto/supersonic.pb.h"
#include "supersonic/testing/block_builder.h"
#include "supersonic/testing/comparators.h"
#include "supersonic/testing/expression_test_helper.h"
#include "gtest/gtest.h"

namespace supersonic {
namespace {

class ProjectingExpressionTest : public testing::Test {
 public:
  ProjectingExpressionTest() : block_(CreateBlock()) {}

  const View& input() const { return block_->view(); }

 protected:
  void ExpectColumnsEqual(const Column& col1, const Column& col2, size_t rows) {
    ASSERT_EQ(col1.attribute().type(), col2.attribute().type());
    ASSERT_EQ(col1.attribute().nullability(), col2.attribute().nullability());
    // The data is _not_ rewritten, it's the same pointer.
    EXPECT_EQ(col1.data(), col2.data());
    if (col1.attribute().nullability() == NULLABLE) {
      for (int i = 0; i < rows; ++i) {
        EXPECT_EQ(col1.is_null()[i], col2.is_null()[i]);
      }
    }
  }

  const string name(size_t index) {
    CHECK_LT(index, input().column_count());
    CHECK_GE(index, 0);
    return StrCat("col", index);
  }

  const size_t rows() { return input().row_count(); }

 private:
  static Block* CreateBlock() {
    return BlockBuilder<STRING, INT32, DOUBLE, INT32>()
        .AddRow("1", 12, 5.1, 22)
        .AddRow("2", 13, 6.2, 23)
        .AddRow("3", 14, 7.3, 24)
        .AddRow("4", __, 8.4, 25)
        .AddRow(__,  16,  __, 26)
        .Build();
  }

  scoped_ptr<Block> block_;
};

TEST_F(ProjectingExpressionTest, AttributeAtSelects) {
  for (int i = 0; i < input().column_count(); ++i) {
    scoped_ptr<BoundExpressionTree> attribute(
        DefaultBind(input().schema(), 100, AttributeAt(i)));
    const View& result = DefaultEvaluate(attribute.get(), input());
    EXPECT_EQ(1, result.schema().attribute_count());
    EXPECT_COLUMNS_EQUAL(input().column(i), result.column(0), rows());
  }
}

TEST_F(ProjectingExpressionTest, NamedAttributeSelects) {
  for (int i = 0; i < input().column_count(); ++i) {
    scoped_ptr<BoundExpressionTree> attribute(
        DefaultBind(input().schema(), 100, NamedAttribute(name(i))));
    const View& result = DefaultEvaluate(attribute.get(), input());
    EXPECT_EQ(1, result.schema().attribute_count());
    EXPECT_COLUMNS_EQUAL(input().column(i), result.column(0), rows());
  }
}

TEST_F(ProjectingExpressionTest, SingleColumnInputAttributeProjection) {
  vector<string> projected_columns;
  for (int i = 0; i < input().column_count(); ++i) {
    projected_columns.clear();
    projected_columns.push_back(name(i));
    scoped_ptr<const SingleSourceProjector> projector(
        ProjectNamedAttributes(projected_columns));
    scoped_ptr<BoundExpressionTree> projected(DefaultBind(
        input().schema(),
        100,
        InputAttributeProjection(projector.release())));
    const View& result = DefaultEvaluate(projected.get(), input());
    EXPECT_EQ(1, result.schema().attribute_count());
    EXPECT_COLUMNS_EQUAL(input().column(i), result.column(0), rows());
  }
}

TEST_F(ProjectingExpressionTest, TwoColumnInputAttributeProjection) {
  vector<string> projected_columns;
  projected_columns.push_back(name(3));
  projected_columns.push_back(name(1));
  scoped_ptr<const SingleSourceProjector> projector(
      ProjectNamedAttributes(projected_columns));
  scoped_ptr<BoundExpressionTree> projected(DefaultBind(
      input().schema(),
      100,
      InputAttributeProjection(projector.release())));
  const View& result = DefaultEvaluate(projected.get(), input());
  EXPECT_EQ(2, result.schema().attribute_count());
  EXPECT_COLUMNS_EQUAL(input().column(3), result.column(0), rows());
  EXPECT_COLUMNS_EQUAL(input().column(1), result.column(1), rows());
}

TEST_F(ProjectingExpressionTest, TwoColumnInputProjectionShortCircuit) {
  vector<string> projected_columns;
  projected_columns.push_back(name(2));
  projected_columns.push_back(name(0));
  scoped_ptr<const SingleSourceProjector> projector(
      ProjectNamedAttributes(projected_columns));
  scoped_ptr<BoundExpression> projected(DefaultDoBind(
      input().schema(),
      100,
      InputAttributeProjection(projector.release())));
  BoolBlock skip_vectors(2, HeapBufferAllocator::Get());
  ASSERT_TRUE(skip_vectors.TryReallocate(5).is_success());
  bool left_skip[5] = { true, false, false, true, false };
  bool right_skip[5] = { false, true, false, false, true };
  bit_pointer::FillFrom(skip_vectors.view().column(0), left_skip, 5);
  bit_pointer::FillFrom(skip_vectors.view().column(1), right_skip, 5);
  EvaluationResult result =
      projected.get()->DoEvaluate(input(), skip_vectors.view());
  ASSERT_TRUE(result.is_success());
  EXPECT_EQ(2, result.get().schema().attribute_count());
  for (int i = 0; i < input().row_count(); ++i) {
    EXPECT_EQ(input().column(2).is_null()[i] || left_skip[i],
              result.get().column(0).is_null()[i]);
    EXPECT_EQ(input().column(0).is_null()[i] || right_skip[i],
              result.get().column(1).is_null()[i]);
  }
}

TEST_F(ProjectingExpressionTest, Flat) {
  scoped_ptr<BoundExpressionTree> projected(DefaultBind(
      input().schema(),
      100,
      Flat((new ExpressionList())->add(AttributeAt(0))->add(AttributeAt(3)))));
  const View& result = DefaultEvaluate(projected.get(), input());
  EXPECT_EQ(2, result.schema().attribute_count());
  EXPECT_COLUMNS_EQUAL(input().column(0), result.column(0), rows());
  EXPECT_COLUMNS_EQUAL(input().column(3), result.column(1), rows());
}

TEST_F(ProjectingExpressionTest, FlattenEmptyList) {
  scoped_ptr<BoundExpressionTree> projected(DefaultBind(
      input().schema(), 100, Flat(new ExpressionList())));
  const View& result = DefaultEvaluate(projected.get(), input());
  EXPECT_EQ(0, result.schema().attribute_count());
}

TEST_F(ProjectingExpressionTest, Alias) {
  scoped_ptr<BoundExpressionTree> aliased(DefaultBind(
      input().schema(),
      100,
      Alias("Some alias", AttributeAt(0))));
  const View& result = DefaultEvaluate(aliased.get(), input());
  EXPECT_EQ(1, result.schema().attribute_count());
  EXPECT_COLUMNS_EQUAL(input().column(0), result.column(0), rows());
  EXPECT_EQ("Some alias", result.schema().attribute(0).name());
}

TEST_F(ProjectingExpressionTest, AliasFailsOnTooManyColumns) {
  scoped_ptr<const Expression> two_columns(
      Flat((new ExpressionList())->add(AttributeAt(0))->add(AttributeAt(1))));
  scoped_ptr<const Expression> alias(
      Alias(string("Some other alias"), two_columns.release()));
  FailureOrOwned<BoundExpressionTree> bound_alias =
      alias->Bind(input().schema(), HeapBufferAllocator::Get(), 100);
  EXPECT_TRUE(bound_alias.is_failure());
}

TEST_F(ProjectingExpressionTest, AliasFailsOnNoColumns) {
  scoped_ptr<const Expression> no_columns(Flat((new ExpressionList())));
  scoped_ptr<const Expression> alias(
      Alias(string("Yet another alias"), no_columns.release()));
  FailureOrOwned<BoundExpressionTree> bound_alias =
      alias->Bind(input().schema(), HeapBufferAllocator::Get(), 100);
  EXPECT_TRUE(bound_alias.is_failure());
}

}  // namespace
}  // namespace supersonic
