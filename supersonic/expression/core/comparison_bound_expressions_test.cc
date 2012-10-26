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
// Author:  onufry@google.com (Onufry Wojtaszczyk)

#include "supersonic/expression/core/comparison_bound_expressions.h"

#include <vector>
using std::vector;

#include "supersonic/utils/integral_types.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/expression/base/expression.h"
#include "supersonic/expression/core/projecting_bound_expressions.h"
#include "supersonic/testing/expression_test_helper.h"
#include "gtest/gtest.h"
#include "supersonic/utils/container_literal.h"

namespace supersonic {

class BufferAllocator;

namespace {

using util::gtl::Container;

TEST(ComparisonBoundExpressionsTest, BoundEqual) {
  TestBoundBinary(&BoundEqual, INT32, INT64, "($0 == $1)");
}

TEST(ComparisonBoundExpressionsTest, BoundNotEqual) {
  TestBoundBinary(&BoundNotEqual, STRING, STRING, "($0 <> $1)");
}

TEST(ComparisonBoundExpressionsTest, BoundGreater) {
  TestBoundBinary(&BoundGreater, BINARY, BINARY, "($1 < $0)");
  TestBoundBinary(&BoundGreater, DATE, DATE, "($1 < $0)");
  TestBoundBinary(&BoundGreater, DATETIME, DATETIME, "($1 < $0)");
}

TEST(ComparisonBoundExpressionsTest, BoundGreaterOrEqual) {
  TestBoundBinary(&BoundGreaterOrEqual, BOOL, BOOL, "($1 <= $0)");
}

TEST(ComparisonBoundExpressionsTest, BoundLess) {
  TestBoundBinary(&BoundLess, INT32, DOUBLE, "(CAST_INT32_TO_DOUBLE($0) < $1)");
  TestBoundBinary(&BoundLess, DOUBLE, INT32, "($0 < CAST_INT32_TO_DOUBLE($1))");
  TestBoundBinary(&BoundLess, BINARY, BINARY, "($0 < $1)");
}

TEST(ComparisonBoundExpressionsTest, BoundLessOrEqual) {
  TestBoundBinary(&BoundLessOrEqual, FLOAT, FLOAT, "($0 <= $1)");
}

TEST(ComparisonBoundExpressionsTest, BoundIsOdd) {
  TestBoundUnary(&BoundIsOdd, INT32, "IS_ODD($0)");
}

TEST(ComparisonBoundExpressionsTest, BoundIsEven) {
  TestBoundUnary(&BoundIsEven, UINT64, "IS_EVEN($0)");
}

// Helper factory functions (in BoundUnaryExpressionFactory format) for
// the testing of BoundInExpressionSet. This one gives a 3-element set.
FailureOrOwned<BoundExpression> BoundInExpressionSet(
    BoundExpression* arg_base,
    BoundExpression* arg_set0,
    BoundExpression* arg_set1,
    BoundExpression* arg_set2,
    BufferAllocator* allocator,
    rowcount_t max_row_count) {
  BoundExpressionList* explist = new BoundExpressionList();
  explist->add(arg_set0);
  explist->add(arg_set1);
  explist->add(arg_set2);
  return BoundInSet(arg_base, explist, allocator, max_row_count);
}


// Helper factory functions (in BoundUnaryExpressionFactory format) for
// the testing of BoundInExpressionSet. This one gives a 2-element set.
FailureOrOwned<BoundExpression> BoundInExpressionSetSmaller(
    BoundExpression* arg_base,
    BoundExpression* arg_set0,
    BoundExpression* arg_set1,
    BufferAllocator* allocator,
    rowcount_t max_row_count) {
  BoundExpressionList* explist = new BoundExpressionList();
  explist->add(arg_set0);
  explist->add(arg_set1);
  return BoundInSet(arg_base, explist, allocator, max_row_count);
}

// Same as above, but empty.
FailureOrOwned<BoundExpression> BoundInExpressionSetEmpty(
    BoundExpression* arg_base,
    BufferAllocator* allocator,
    rowcount_t max_row_count) {
  BoundExpressionList* explist = new BoundExpressionList();
  return BoundInSet(arg_base, explist, allocator, max_row_count);
}


TEST(ComparisonBoundExpressionsTest, BoundInExpressionSet) {
  TestBoundQuaternary(&BoundInExpressionSet, STRING, STRING, STRING, STRING,
                      "$0 IN ($1, $2, $3)");
  TestBoundQuaternary(&BoundInExpressionSet, INT32, INT32, INT32, INT32,
                      "$0 IN ($1, $2, $3)");
  TestBoundUnary(&BoundInExpressionSetEmpty, INT64, "$0 IN ()");
}

TEST(ComparisonBoundExpressionsTest, BoundInExpressionSetMultiType) {
  TestBoundQuaternary(&BoundInExpressionSet, INT32, INT64, INT64, INT64,
                      "CAST_INT32_TO_INT64($0) IN ($1, $2, $3)");
  TestBoundQuaternary(&BoundInExpressionSet, INT64, INT32, INT64, INT64,
                        "$0 IN (CAST_INT32_TO_INT64($1), $2, $3)");
  TestBoundQuaternary(&BoundInExpressionSet, DOUBLE, INT32, UINT32, UINT64,
                      "$0 IN (CAST_INT32_TO_DOUBLE($1), "
                      "CAST_UINT32_TO_DOUBLE($2), CAST_UINT64_TO_DOUBLE($3))");
  TestBoundQuaternary(&BoundInExpressionSet, INT32, INT32, DOUBLE, UINT64,
                      "CAST_INT32_TO_DOUBLE($0) IN (CAST_INT32_TO_DOUBLE($1), "
                      "$2, CAST_UINT64_TO_DOUBLE($3))");
  TestBoundFactoryFailure(&BoundInExpressionSetSmaller, INT32, STRING, INT32);
  TestBoundFactoryFailure(&BoundInExpressionSetSmaller, STRING, STRING, INT32);
}

TEST(ComparisonBoundExpressionsTest,
     BoundInExpressionSetCollectReferredAttributeNames) {
  TupleSchema schema;
  schema.add_attribute(Attribute("string1", STRING, NULLABLE));
  schema.add_attribute(Attribute("string2", STRING, NOT_NULLABLE));
  schema.add_attribute(Attribute("string3", STRING, NOT_NULLABLE));
  schema.add_attribute(Attribute("string4", STRING, NOT_NULLABLE));
  schema.add_attribute(Attribute("string5", STRING, NOT_NULLABLE));
  FailureOrOwned<BoundExpression> bound_expr = BoundInExpressionSet(
      SucceedOrDie(BoundNamedAttribute(schema, "string4")),
      SucceedOrDie(BoundNamedAttribute(schema, "string3")),
      SucceedOrDie(BoundNamedAttribute(schema, "string1")),
      SucceedOrDie(BoundNamedAttribute(schema, "string3")),
      HeapBufferAllocator::Get(),
      20);
  ASSERT_TRUE(bound_expr.is_success())
      << bound_expr.exception().PrintStackTrace();
  EXPECT_EQ(Container("string4", "string3", "string1").As<set<string> >(),
            bound_expr->referred_attribute_names());
}

}  // namespace

}  // namespace supersonic
