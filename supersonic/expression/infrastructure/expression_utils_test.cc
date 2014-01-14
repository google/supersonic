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

#include "supersonic/expression/infrastructure/expression_utils.h"

#include <memory>

#include "supersonic/utils/integral_types.h"
#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/utils/exception/failureor.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/memory/memory.h"
#include "supersonic/expression/base/expression.h"
#include "supersonic/expression/core/elementary_bound_expressions.h"
#include "supersonic/expression/infrastructure/basic_bound_expression.h"
#include "supersonic/expression/infrastructure/elementary_bound_const_expressions.h"
#include "supersonic/expression/infrastructure/terminal_bound_expressions.h"
#include "supersonic/utils/strings/join.h"
#include "gtest/gtest.h"

namespace supersonic {

class ExpressionUtilsTest : public ::testing::Test {
 protected:
  TupleSchema CreateSchema(int32 attributes) {
    TupleSchema schema;
    for (int32 i = 0; i < attributes; ++i) {
      schema.add_attribute(Attribute(StrCat("Name", i), BOOL, NULLABLE));
    }
    return schema;
  }

  BoundExpression* GetTrue() {
    return SucceedOrDie(InitBasicExpression(10, new BoundConstExpression<BOOL>(
        HeapBufferAllocator::Get(), true),
        HeapBufferAllocator::Get()));
  }

  BoundExpression* GetFalse() {
    return SucceedOrDie(InitBasicExpression(15, new BoundConstExpression<BOOL>(
        HeapBufferAllocator::Get(), false),
        HeapBufferAllocator::Get()));
  }

  BoundExpression* GetNull(DataType type) {
    return SucceedOrDie(BoundNull(type, HeapBufferAllocator::Get(), 5));
  }

  BoundExpression* GetAnd(BoundExpression* left, BoundExpression* right) {
    FailureOrOwned<BoundExpression> and_expression =
        BoundAnd(left, right, HeapBufferAllocator::Get(), 20);
    CHECK(and_expression.is_success());
    return and_expression.release();
  }

  BoundExpressionList* BuildExpressionList(BoundExpression* expression1,
                                           BoundExpression* expression2,
                                           BoundExpression* expression3) {
    std::unique_ptr<BoundExpressionList> scoped_expression_list(
        new BoundExpressionList());
    scoped_expression_list->add(expression1);
    scoped_expression_list->add(expression2);
    scoped_expression_list->add(expression3);
    return scoped_expression_list.release();
  }

  static const int kNumberOfTypes = 12;
  static const DataType kTypes[kNumberOfTypes];
};

const int ExpressionUtilsTest::kNumberOfTypes;
const DataType ExpressionUtilsTest::kTypes[] = {INT32, INT64, UINT32, UINT64,
    FLOAT, DOUBLE, BOOL, DATE, DATETIME, STRING, BINARY, DATA_TYPE};

TEST_F(ExpressionUtilsTest, CheckAttributeCountSucceeds) {
  EXPECT_TRUE(CheckAttributeCount("Name1", CreateSchema(1), 1).is_success());
  EXPECT_TRUE(CheckAttributeCount("Name2", CreateSchema(0), 0).is_success());
  EXPECT_TRUE(CheckAttributeCount("Name3", CreateSchema(99), 99).is_success());
}

TEST_F(ExpressionUtilsTest, CheckAttributeCountFails) {
  EXPECT_TRUE(CheckAttributeCount("Name1", CreateSchema(1), 0).is_failure());
  EXPECT_TRUE(CheckAttributeCount("Name2", CreateSchema(0), 1).is_failure());
  EXPECT_TRUE(CheckAttributeCount("Name3", CreateSchema(2), 1).is_failure());
  EXPECT_TRUE(CheckAttributeCount("Name4", CreateSchema(99), 98).is_failure());
  EXPECT_TRUE(CheckAttributeCount("Name5", CreateSchema(98), 99).is_failure());
}

TEST_F(ExpressionUtilsTest, GetExpressionType) {
  for (int i = 0; i < kNumberOfTypes; ++i) {
    std::unique_ptr<BoundExpression> expr(GetNull(kTypes[i]));
    EXPECT_EQ(kTypes[i], GetExpressionType(expr.get()));
  }
}

TEST_F(ExpressionUtilsTest, CheckExpressionType) {
  for (int i = 0; i < kNumberOfTypes; ++i) {
    std::unique_ptr<const BoundExpression> expr(GetNull(kTypes[i]));
    for (int j = 0; j < kNumberOfTypes; ++j) {
      EXPECT_EQ(i == j,
                CheckExpressionType(kTypes[j], expr.get()).is_success());
    }
  }
}

TEST_F(ExpressionUtilsTest, CheckExpressionListMembersType) {
  int member_count[] = {0, 1, 5};
  for (int i = 0; i < kNumberOfTypes; ++i) {
    for (int j = 0; j < 3; ++j) {
      std::unique_ptr<BoundExpressionList> expression_list(
          new BoundExpressionList);
      for (int k = 0; k < member_count[j]; ++k) {
        expression_list->add(GetNull(kTypes[i]));
      }
      for (int k = 0; k < kNumberOfTypes; ++k) {
        ASSERT_EQ(i == k || j == 0,
                  CheckExpressionListMembersType(
                      kTypes[k], expression_list.get()).is_success());
      }
    }
  }
}

TEST_F(ExpressionUtilsTest, CheckAttributeCountFailureMessage) {
  FailureOrVoid result = CheckAttributeCount("ADD", CreateSchema(1), 2);
  // We'll crash anyway if the result isn't a failure, so we assert, not expect.
  ASSERT_TRUE(result.is_failure());
  EXPECT_EQ(ERROR_ATTRIBUTE_COUNT_MISMATCH, result.exception().return_code());
  EXPECT_EQ("Failed to bind: ADD expects 2 attribute(s), got: 1 (Name0: BOOL)",
            result.exception().message());
}

TEST_F(ExpressionUtilsTest, GetExpressionNullability) {
  std::unique_ptr<BoundExpression> nullable_expression(GetNull(BOOL));
  std::unique_ptr<BoundExpression> not_nullable_expression(GetTrue());
  EXPECT_EQ(GetExpressionNullability(nullable_expression.get()), NULLABLE);
  EXPECT_EQ(GetExpressionNullability(not_nullable_expression.get()),
            NOT_NULLABLE);
}

TEST_F(ExpressionUtilsTest, GetExpressionListNullability) {
  std::unique_ptr<BoundExpressionList> test_list(
      BuildExpressionList(GetNull(BOOL), GetNull(BOOL), GetNull(BOOL)));
  EXPECT_EQ(GetExpressionListNullability(test_list.get()), NULLABLE);
  test_list.reset(BuildExpressionList(GetNull(BOOL), GetTrue(), GetFalse()));
  EXPECT_EQ(GetExpressionListNullability(test_list.get()), NULLABLE);
  test_list.reset(BuildExpressionList(GetTrue(), GetTrue(), GetNull(BOOL)));
  EXPECT_EQ(GetExpressionListNullability(test_list.get()), NULLABLE);
  test_list.reset(BuildExpressionList(GetFalse(), GetTrue(), GetTrue()));
  EXPECT_EQ(GetExpressionListNullability(test_list.get()), NOT_NULLABLE);
}

TEST_F(ExpressionUtilsTest, NullabilityOr) {
  EXPECT_EQ(NullabilityOr(NULLABLE, NULLABLE), NULLABLE);
  EXPECT_EQ(NullabilityOr(NOT_NULLABLE, NULLABLE), NULLABLE);
  EXPECT_EQ(NullabilityOr(NULLABLE, NOT_NULLABLE), NULLABLE);
  EXPECT_EQ(NullabilityOr(NOT_NULLABLE, NOT_NULLABLE), NOT_NULLABLE);
}

TEST_F(ExpressionUtilsTest, GetNameNotPresentInSchema) {
  TupleSchema schema;
  schema.add_attribute(Attribute("a", BOOL, NOT_NULLABLE));
  schema.add_attribute(Attribute("b0", BOOL, NOT_NULLABLE));
  schema.add_attribute(Attribute("b1", BOOL, NOT_NULLABLE));
  schema.add_attribute(Attribute("c0", BOOL, NOT_NULLABLE));
  schema.add_attribute(Attribute("b2", BOOL, NOT_NULLABLE));
  EXPECT_EQ(schema.LookupAttributePosition(
      GetNameNotPresentInSchema(schema, "a")), -1);
  EXPECT_EQ(GetNameNotPresentInSchema(schema, "a").substr(0, 1), "a");
  EXPECT_EQ(schema.LookupAttributePosition(
      GetNameNotPresentInSchema(schema, "b")), -1);
  EXPECT_EQ(GetNameNotPresentInSchema(schema, "b").substr(0, 1), "b");
  EXPECT_EQ(schema.LookupAttributePosition(
      GetNameNotPresentInSchema(schema, "c")), -1);
  EXPECT_EQ(GetNameNotPresentInSchema(schema, "c").substr(0, 1), "c");
  EXPECT_EQ(schema.LookupAttributePosition(
      GetNameNotPresentInSchema(schema, "d")), -1);
  EXPECT_EQ(GetNameNotPresentInSchema(schema, "d").substr(0, 1), "d");
}

TEST_F(ExpressionUtilsTest, CreateUniqueName) {
  TupleSchema schema;
  schema.add_attribute(Attribute("a", BOOL, NOT_NULLABLE));
  schema.add_attribute(Attribute("b0", BOOL, NOT_NULLABLE));
  schema.add_attribute(Attribute("b1", BOOL, NOT_NULLABLE));
  set<string> forbidden_names;
  string name = CreateUniqueName(schema, forbidden_names, "a");
  EXPECT_EQ(schema.LookupAttributePosition(name), -1);
  EXPECT_EQ(forbidden_names.find(name), forbidden_names.end());
  forbidden_names.insert("a0");
  name = CreateUniqueName(schema, forbidden_names, "a");
  EXPECT_EQ(schema.LookupAttributePosition(name), -1);
  EXPECT_EQ(forbidden_names.find(name), forbidden_names.end());
  forbidden_names.insert("a1");
  name = CreateUniqueName(schema, forbidden_names, "a");
  EXPECT_EQ(schema.LookupAttributePosition(name), -1);
  EXPECT_EQ(forbidden_names.find(name), forbidden_names.end());
  forbidden_names.insert("b0");
  name = CreateUniqueName(schema, forbidden_names, "b");
  EXPECT_EQ(schema.LookupAttributePosition(name), -1);
  EXPECT_EQ(forbidden_names.find(name), forbidden_names.end());
  forbidden_names.insert("b2");
  name = CreateUniqueName(schema, forbidden_names, "b");
  EXPECT_EQ(schema.LookupAttributePosition(name), -1);
  EXPECT_EQ(forbidden_names.find(name), forbidden_names.end());
}

}  // namespace supersonic
