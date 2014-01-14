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

#include "supersonic/expression/core/string_bound_expressions.h"

#include <set>
#include "supersonic/utils/std_namespace.h"
#include <string>
namespace supersonic {using std::string; }

#include "supersonic/utils/exception/failureor.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/memory/memory.h"
#include "supersonic/expression/base/expression.h"
#include "supersonic/expression/core/projecting_bound_expressions.h"
#include "supersonic/proto/supersonic.pb.h"
#include "supersonic/testing/expression_test_helper.h"
#include "gtest/gtest.h"
#include "supersonic/utils/container_literal.h"

namespace supersonic {

namespace {

using util::gtl::Container;

TEST(StringBoundExpressionsTest, BoundToString) {
  TestBoundUnary(&BoundToString, INT32, "TOSTRING($0)");
  TestBoundUnary(&BoundToString, STRING, "$0");
}

TEST(StringBoundExpressionsTest, BoundStringOffset) {
  TestBoundBinary(&BoundStringOffset, STRING, STRING, "STRING_OFFSET($0, $1)");
  TestBoundFactoryFailure(&BoundStringOffset, STRING, INT32);
  TestBoundFactoryFailure(&BoundStringOffset, UINT32, STRING);
}

TEST(StringBoundExpressionsTest, BoundContains) {
  TestBoundBinary(&BoundContains, STRING, STRING,
                  "(CONST_UINT32 < STRING_OFFSET($0, $1))");
  TestBoundFactoryFailure(&BoundContains, STRING, INT32);
}

TEST(StringBoundExpressionsTest, BoundContainsCI) {
  TestBoundBinary(&BoundContainsCI, STRING, STRING,
                  "(CONST_UINT32 < STRING_OFFSET(TO_LOWER($0), TO_LOWER($1)))");
  TestBoundFactoryFailure(&BoundContainsCI, FLOAT, STRING);
}

FailureOrOwned<BoundExpression> BoundOneArgConcat(BoundExpression* arg,
                                                  BufferAllocator* allocator,
                                                  rowcount_t max_row_count) {
  BoundExpressionList* list = new BoundExpressionList();
  list->add(arg);
  return BoundConcat(list, allocator, max_row_count);
}

FailureOrOwned<BoundExpression> BoundTwoArgConcat(BoundExpression* arg1,
                                                  BoundExpression* arg2,
                                                  BufferAllocator* allocator,
                                                  rowcount_t max_row_count) {
  BoundExpressionList* list = new BoundExpressionList();
  list->add(arg1);
  list->add(arg2);
  return BoundConcat(list, allocator, max_row_count);
}

TEST(StringBoundExpressionsTest, BoundConcat) {
  TestBoundUnary(&BoundOneArgConcat, STRING, "CONCAT($0)");
  TestBoundUnary(&BoundOneArgConcat, INT32, "CONCAT(TOSTRING($0))");
  TestBoundBinary(&BoundTwoArgConcat, STRING, INT32,
                  "CONCAT($0, TOSTRING($1))");
}

TEST(StringBoundExpressionsTest, BoundConcatCollectReferredAttributeNames) {
  TupleSchema schema;
  schema.add_attribute(Attribute("string1", STRING, NULLABLE));
  schema.add_attribute(Attribute("string2", STRING, NOT_NULLABLE));
  schema.add_attribute(Attribute("string3", STRING, NOT_NULLABLE));
  FailureOrOwned<BoundExpression> bound_concat = BoundTwoArgConcat(
      SucceedOrDie(BoundNamedAttribute(schema, "string1")),
      SucceedOrDie(BoundNamedAttribute(schema, "string2")),
      HeapBufferAllocator::Get(),
      20);
  ASSERT_TRUE(bound_concat.is_success())
      << bound_concat.exception().PrintStackTrace();
  EXPECT_EQ(Container("string1", "string2").As<set<string> >(),
            bound_concat->referred_attribute_names());
}

TEST(StringBoundExpressionsTest, BoundLength) {
  TestBoundUnary(&BoundLength, STRING, "LENGTH($0)");
}

TEST(StringBoundExpressionsTest, BoundLtrim) {
  TestBoundUnary(&BoundLtrim, STRING, "LTRIM($0)");
}

TEST(StringBoundExpressionsTest, BoundRtrim) {
  TestBoundUnary(&BoundRtrim, STRING, "RTRIM($0)");
}

TEST(StringBoundExpressionsTest, BoundTrim) {
  TestBoundUnary(&BoundTrim, STRING, "TRIM($0)");
}

TEST(StringBoundExpressionsTest, BoundToUpper) {
  TestBoundUnary(&BoundToUpper, STRING, "TO_UPPER($0)");
}

TEST(StringBoundExpressionsTest, BoundToLower) {
  TestBoundUnary(&BoundToLower, STRING, "TO_LOWER($0)");
}

TEST(StringBoundExpressionsTest, BoundTrailingSubstring) {
  TestBoundBinary(&BoundTrailingSubstring, STRING, INT64, "SUBSTRING($0, $1)");
  TestBoundBinary(&BoundTrailingSubstring, STRING, INT32,
                  "SUBSTRING($0, CAST_INT32_TO_INT64($1))");
}

TEST(StringBoundExpressionsTest, BoundSubstring) {
  TestBoundTernary(&BoundSubstring, STRING, INT64, INT64,
                   "SUBSTRING($0, $1, $2)");
  TestBoundTernary(
      &BoundSubstring, STRING, INT32, UINT32,
      "SUBSTRING($0, CAST_INT32_TO_INT64($1), CAST_UINT32_TO_INT64($2))");
}

}  // namespace

}  // namespace supersonic
