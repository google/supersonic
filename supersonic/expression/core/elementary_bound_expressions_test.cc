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

#include "supersonic/expression/core/elementary_bound_expressions.h"

#include <set>
#include "supersonic/utils/std_namespace.h"
#include <string>
namespace supersonic {using std::string; }
#include <vector>
using std::vector;

#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/expression/base/expression.h"
#include "supersonic/expression/core/projecting_bound_expressions.h"
#include "supersonic/testing/expression_test_helper.h"
#include "gtest/gtest.h"
#include "supersonic/utils/container_literal.h"

namespace supersonic {

class BufferAllocator;

namespace {

// ------------------------- Control expressions -------------------------

FailureOrOwned<BoundExpression> BoundCastToInt64(
    BoundExpression* source,
    BufferAllocator* allocator,
    rowcount_t max_row_count) {
  return BoundCastTo(INT64, source, allocator, max_row_count);
}

TEST(ElementaryBoundExpressionsTest, BoundCastTo) {
  TestBoundUnary(&BoundCastToInt64, INT64, "$0");
  TestBoundUnary(&BoundCastToInt64, INT32, "CAST_INT32_TO_INT64($0)");
}

template<DataType out_type>
FailureOrOwned<BoundExpression> TypedBoundParseStringQuiet(
    BoundExpression* source,
    BufferAllocator* allocator,
    rowcount_t max_row_count) {
  return BoundParseStringQuiet(out_type, source, allocator, max_row_count);
}

template<DataType out_type>
FailureOrOwned<BoundExpression> TypedBoundParseStringNulling(
    BoundExpression* source,
    BufferAllocator* allocator,
    rowcount_t max_row_count) {
  return BoundParseStringNulling(out_type, source, allocator, max_row_count);
}

TEST(ElementaryBoundExpressionsTest, BoundParseString) {
  TestBoundUnary(&TypedBoundParseStringQuiet<INT32>, STRING,
                   "PARSE_STRING($0)");
  TestBoundUnary(&TypedBoundParseStringQuiet<STRING>, STRING, "$0");
  TestBoundUnary(&TypedBoundParseStringQuiet<DATE>, STRING,
                   "PARSE_STRING($0)");

  TestBoundUnary(&TypedBoundParseStringNulling<INT32>, STRING,
                   "PARSE_STRING($0)");
  TestBoundUnary(&TypedBoundParseStringNulling<STRING>, STRING, "$0");
  TestBoundUnary(&TypedBoundParseStringNulling<DATE>, STRING,
                   "PARSE_STRING($0)");
}

TEST(ElementaryBoundExpressionsTest, BoundIfNull) {
  TestBoundBinary(&BoundIfNull, INT32, INT64, "CAST_INT32_TO_INT64($0)");
  TestBoundFactoryWithNulls(&BoundIfNull, INT32, true, INT32, false,
                            "IFNULL($0, $1)");
  TestBoundFactoryWithNulls(&BoundIfNull, INT32, false, INT32, true, "$0");
}

TEST(ElementaryBoundExpressionsTest, BoundCase) {
  TestBoundExpressionList(&BoundCase,
      util::gtl::Container(BOOL, INT32, BOOL, INT32).As<vector<DataType> >(),
      "CASE($0, $1, $2, $3)");

  TestBoundExpressionList(&BoundCase,
      util::gtl::Container(STRING, STRING, STRING, STRING)
          .As<vector<DataType> >(),
      "CASE($0, $1, $2, $3)");

  TestBoundExpressionList(&BoundCase,
      util::gtl::Container(UINT32, STRING, INT32, STRING, INT64, STRING,
                           UINT64, STRING)
          .As<vector<DataType> >(),
      "CASE(CAST_UINT32_TO_INT64($0), $1, CAST_INT32_TO_INT64($2), $3, $4, $5,"
      " CAST_UINT64_TO_INT64($6), $7)");

  TestBoundExpressionList(&BoundCase,
      util::gtl::Container(UINT32, STRING, INT32, STRING, INT64, STRING,
                           UINT64, STRING, DOUBLE, STRING, FLOAT, STRING)
          .As<vector<DataType> >(),
      "CASE(CAST_UINT32_TO_DOUBLE($0), $1, CAST_INT32_TO_DOUBLE($2), $3,"
      " CAST_INT64_TO_DOUBLE($4), $5, CAST_UINT64_TO_DOUBLE($6), $7,"
      " $8, $9, CAST_FLOAT_TO_DOUBLE($10), $11)");

  TestBoundExpressionList(&BoundCase,
      util::gtl::Container(STRING, UINT32, STRING, INT32, STRING, INT64,
                           STRING, UINT64, STRING, DOUBLE, STRING, FLOAT)
          .As<vector<DataType> >(),
      "CASE($0, CAST_UINT32_TO_DOUBLE($1), $2, CAST_INT32_TO_DOUBLE($3),"
      " $4, CAST_INT64_TO_DOUBLE($5), $6, CAST_UINT64_TO_DOUBLE($7), $8, $9,"
      " $10, CAST_FLOAT_TO_DOUBLE($11))");

  // Expected at least 2 arguments.
  TestBoundFactoryFailure(&BoundCase,
      util::gtl::Container().As<vector<DataType> >());
  // Expected odd number of arguments.
  TestBoundFactoryFailure(&BoundCase,
      util::gtl::Container(BOOL, INT32, INT64).As<vector<DataType> >());
  // Incompatible returned types.
  TestBoundFactoryFailure(&BoundCase,
      util::gtl::Container(BOOL, INT32, BOOL, INT32, BOOL, STRING)
          .As<vector<DataType> >());
  // Incompatible returned types (STRING in else).
  TestBoundFactoryFailure(&BoundCase,
      util::gtl::Container(BOOL, STRING, BOOL, INT32, BOOL, INT32)
          .As<vector<DataType> >());
  // Incompatible condition types.
  TestBoundFactoryFailure(&BoundCase,
      util::gtl::Container(BOOL, STRING, UINT32, STRING)
          .As<vector<DataType> >());
}

TEST(ElementaryBoundExpressionsTest, BoundCaseCollectReferredAttributeNames) {
  TupleSchema schema;
  schema.add_attribute(Attribute("condition", INT32, NULLABLE));
  schema.add_attribute(Attribute("default", STRING, NULLABLE));
  schema.add_attribute(Attribute("compare1", INT32, NULLABLE));
  schema.add_attribute(Attribute("value1", STRING, NULLABLE));
  schema.add_attribute(Attribute("compare2", INT32, NULLABLE));
  schema.add_attribute(Attribute("value2", STRING, NULLABLE));
  schema.add_attribute(Attribute("not_reffered", DOUBLE, NULLABLE));

  scoped_ptr<BoundExpressionList> list(new BoundExpressionList());
  list->add(SucceedOrDie(BoundNamedAttribute(schema, "condition")));
  list->add(SucceedOrDie(BoundNamedAttribute(schema, "default")));
  list->add(SucceedOrDie(BoundNamedAttribute(schema, "compare1")));
  list->add(SucceedOrDie(BoundNamedAttribute(schema, "value1")));
  list->add(SucceedOrDie(BoundNamedAttribute(schema, "compare2")));
  list->add(SucceedOrDie(BoundNamedAttribute(schema, "value2")));

  FailureOrOwned<BoundExpression> bound_case =
      BoundCase(list.release(), HeapBufferAllocator::Get(), 20);
  ASSERT_TRUE(bound_case.is_success())
      << bound_case.exception().PrintStackTrace();
  EXPECT_EQ(
      util::gtl::Container("condition", "default", "compare1", "value1",
                           "compare2", "value2").As<set<string> >(),
      bound_case->referred_attribute_names());
}

TEST(ElementaryBoundExpressionsTest, BoundIf) {
  TestBoundTernary(&BoundIf, BOOL, INT32, INT64,
                   "IF $0 THEN CAST_INT32_TO_INT64($1) ELSE $2");
  TestBoundTernary(&BoundIf, BOOL, FLOAT, UINT32,
                   "IF $0 THEN $1 ELSE CAST_UINT32_TO_FLOAT($2)");

  TestBoundFactoryFailure(&BoundIf, INT32, STRING, STRING);
  TestBoundFactoryFailure(&BoundIf, BOOL, UINT32, STRING);
}

TEST(ElementaryBoundExpressionsTest, BoundIfNulling) {
  TestBoundTernary(&BoundIfNulling, BOOL, INT32, INT64,
                   "IF $0 THEN CAST_INT32_TO_INT64($1) ELSE $2");
  TestBoundTernary(&BoundIfNulling, BOOL, FLOAT, UINT32,
                   "IF $0 THEN $1 ELSE CAST_UINT32_TO_FLOAT($2)");

  TestBoundFactoryFailure(&BoundIfNulling, INT32, STRING, STRING);
  TestBoundFactoryFailure(&BoundIfNulling, BOOL, UINT32, STRING);
}

// ------------------------------ Logic ---------------------------------

TEST(ElementaryBoundExpressionsTest, BoundNot) {
  TestBoundUnary(&BoundNot, BOOL, "(NOT $0)");
}

TEST(ElementaryBoundExpressionsTest, BoundOr) {
  TestBoundBinary(&BoundOr, BOOL, BOOL, "($0 OR $1)");
}

TEST(ElementaryBoundExpressionsTest, BoundAnd) {
  TestBoundBinary(&BoundAnd, BOOL, BOOL, "($0 AND $1)");
}

TEST(ElementaryBoundExpressionsTest, BoundAndNot) {
  TestBoundBinary(&BoundAndNot, BOOL, BOOL, "($0 !&& $1)");
}

TEST(ElementaryBoundExpressionsTest, BoundXor) {
  TestBoundBinary(&BoundXor, BOOL, BOOL, "($0 XOR $1)");
}

// --------------------- Unary comparisons and checks -----------------------

TEST(ElementaryBoundExpressionsTest, BoundIsNull) {
  TestBoundFactoryWithNulls(&BoundIsNull, FLOAT, true, "ISNULL($0)");
}

}  // namespace

}  // namespace supersonic
