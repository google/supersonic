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
// Author: onufry@google.com (Onufry Wojtaszczyk)

#include "supersonic/expression/infrastructure/basic_bound_expression.h"

#include <memory>
#include <string>
namespace supersonic {using std::string; }

#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/memory/memory.h"
#include "supersonic/expression/core/arithmetic_bound_expressions.h"
#include "supersonic/expression/core/elementary_bound_expressions.h"
#include "supersonic/expression/core/projecting_bound_expressions.h"
#include "supersonic/expression/infrastructure/elementary_bound_const_expressions.h"
#include "supersonic/expression/infrastructure/terminal_bound_expressions.h"
#include "supersonic/expression/infrastructure/terminal_expressions.h"
#include "supersonic/expression/core/projecting_expressions.h"
#include "gtest/gtest.h"
#include "supersonic/utils/container_literal.h"

namespace supersonic {

namespace {

using util::gtl::Container;

class BasicBoundExpressionTest : public ::testing::Test {
 protected:
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

  BoundExpression* GetNull() {
    FailureOrOwned<BoundExpression> null =
        BoundNull(BOOL, HeapBufferAllocator::Get(), 5);
    CHECK(null.is_success());
    return SucceedOrDie(null);
  }

  template<DataType data_type>
  void TestGetConstantBoundExpressionValue(
      typename TypeTraits<data_type>::hold_type value) {
    std::unique_ptr<BoundExpression> expression(SucceedOrDie(
        InitBasicExpression(15, new BoundConstExpression<data_type>(
                                    HeapBufferAllocator::Get(), value),
                            HeapBufferAllocator::Get())));
    bool is_null;
    FailureOr<typename TypeTraits<data_type>::hold_type> result =
        GetConstantBoundExpressionValue<data_type>(
        expression.get(), &is_null);
    EXPECT_TRUE(result.is_success());
    EXPECT_EQ(result.get(), value);
    EXPECT_FALSE(is_null);
  }

  BoundExpression* GetNamedAttribute(const string attribute_name) {
    return SucceedOrDie(BoundNamedAttribute(GetTestSchema(), attribute_name));
  }

  BoundExpression* GetAnd(BoundExpression* left, BoundExpression* right) {
    FailureOrOwned<BoundExpression> and_expression =
        BoundAnd(left, right, HeapBufferAllocator::Get(), 20);
    CHECK(and_expression.is_success());
    return SucceedOrDie(and_expression);
  }

  BoundExpression* GetIf(BoundExpression* left, BoundExpression* middle,
                         BoundExpression* right) {
    FailureOrOwned<BoundExpression> if_expression =
        BoundIf(left, middle,  right, HeapBufferAllocator::Get(), 20);
    CHECK(if_expression.is_success());
    return SucceedOrDie(if_expression);
  }

  BoundExpression* GetNot(BoundExpression* arg) {
    FailureOrOwned<BoundExpression> not_expression =
        BoundNot(arg, HeapBufferAllocator::Get(), 20);
    CHECK(not_expression.is_success());
    return SucceedOrDie(not_expression);
  }

  void TestResolve(BoundExpression* expression_ptr, const string& description) {
    std::unique_ptr<BoundExpression> expression(expression_ptr);
    EXPECT_EQ(description,
              expression->result_schema().GetHumanReadableSpecification());
  }

  void TestreferredAttributeNames(
      BoundExpression* expression_ptr,
      const set<string>& exprected_referred_attribute_names) {
    std::unique_ptr<BoundExpression> expression(expression_ptr);
    EXPECT_EQ(exprected_referred_attribute_names,
              expression->referred_attribute_names());
  }

  TupleSchema GetTestSchema() {
    TupleSchema schema;
    CHECK(schema.add_attribute(Attribute("double", DOUBLE, NULLABLE)));
    CHECK(schema.add_attribute(Attribute("int32_nn",  INT32, NOT_NULLABLE)));
    CHECK(schema.add_attribute(Attribute("bool1", BOOL, NULLABLE)));
    CHECK(schema.add_attribute(Attribute("bool2_nn", BOOL, NOT_NULLABLE)));
    CHECK(schema.add_attribute(Attribute("bool3_nn", BOOL, NOT_NULLABLE)));
    CHECK(schema.add_attribute(Attribute("bool4", BOOL, NULLABLE)));
    return schema;
  }
};


// These tests assume Resolve gets called somewhere inside
// binding and checks that what we get are the expected resolved expressions.
// There should be a better way to test this than this "implicit" test, the
// problem is we have no access to constructors of BoundExpressions, and have
// to way to lay our hands on BoundExpressions for which Init has not been
// called.
TEST_F(BasicBoundExpressionTest, ResolveToConstantConstants) {
  TestResolve(GetAnd(GetTrue(), GetTrue()), "CONST_BOOL: BOOL NOT NULL");
}

TEST_F(BasicBoundExpressionTest, ResolveToConstantConstantAndNullToConstant) {
  TestResolve(GetAnd(GetFalse(), GetNull()), "CONST_BOOL: BOOL NOT NULL");
}

TEST_F(BasicBoundExpressionTest, ResolveToConstantConstantAndNullToNull) {
  TestResolve(GetAnd(GetNull(), GetTrue()), "NULL: BOOL");
}

TEST_F(BasicBoundExpressionTest, ResolveComplicatedToConstant) {
  TestResolve(GetAnd(GetAnd(GetTrue(), GetNull()),
                     GetAnd(GetNull(), GetFalse())),
              "CONST_BOOL: BOOL NOT NULL");
}

TEST_F(BasicBoundExpressionTest, ResolvingFailureDoesNotCrash) {
  FailureOrOwned<BoundExpression> one =
      BoundConstInt32(1, HeapBufferAllocator::Get(), 10);
  ASSERT_TRUE(one.is_success());
  FailureOrOwned<BoundExpression> zero =
      BoundConstInt32(0, HeapBufferAllocator::Get(), 10);
  ASSERT_TRUE(zero.is_success());

  FailureOrOwned<BoundExpression> modulus =
      BoundModulusSignaling(one.release(), zero.release(),
                            HeapBufferAllocator::Get(), 10);
  EXPECT_TRUE(modulus.is_failure());
}

// --------------- CollectReferredAttributeNames tests ------------------------

TEST_F(BasicBoundExpressionTest, TestCollectReferredAttributeNamesOnConstant) {
  TestreferredAttributeNames(GetFalse(), set<string>());
}

TEST_F(BasicBoundExpressionTest, TestCollectReferredAttributeNamesOnName) {
  TestreferredAttributeNames(GetNamedAttribute("double"),
                             Container("double"));
}

TEST_F(BasicBoundExpressionTest, TestCollectReferredAttributeNamesOnUnary) {
  TestreferredAttributeNames(GetNot(GetNamedAttribute("bool1")),
                             Container("bool1"));
}

TEST_F(BasicBoundExpressionTest, TestCollectReferredAttributeNamesOnBinary) {
  TestreferredAttributeNames(
      GetAnd(GetAnd(GetNamedAttribute("bool1"),
                    GetNamedAttribute("bool2_nn")),
             GetAnd(GetNull(),
                    GetNamedAttribute("bool2_nn"))),
      Container("bool1", "bool2_nn"));
}

TEST_F(BasicBoundExpressionTest, TestCollectReferredAttributeNamesOnTernary) {
  TestreferredAttributeNames(
      GetIf(GetNamedAttribute("bool1"),
            GetNamedAttribute("double"),
            GetNamedAttribute("int32_nn")),
      Container("bool1", "double", "int32_nn"));
}

TEST_F(BasicBoundExpressionTest, GetConstantBoundExpressionValueNotNull) {
  bool is_null;
  std::unique_ptr<BoundExpression> expression(GetTrue());
  FailureOr<bool> result =
      GetConstantBoundExpressionValue<BOOL>(expression.get(), &is_null);
  EXPECT_TRUE(result.is_success());
  EXPECT_TRUE(result.get());
  EXPECT_FALSE(is_null);

  expression.reset(GetFalse());
  result = GetConstantBoundExpressionValue<BOOL>(expression.get(), &is_null);
  EXPECT_TRUE(result.is_success());
  EXPECT_FALSE(result.get());
  EXPECT_FALSE(is_null);

  TestGetConstantBoundExpressionValue<STRING>("!mmm'4_G00000DLE_p4$$wrd!");
  TestGetConstantBoundExpressionValue<INT32>(290890);
  TestGetConstantBoundExpressionValue<INT64>(290890);
  TestGetConstantBoundExpressionValue<UINT32>(290890);
  TestGetConstantBoundExpressionValue<UINT64>(290890);
}

TEST_F(BasicBoundExpressionTest, GetConstantBoundExpressionValueNull) {
  bool is_null;
  std::unique_ptr<BoundExpression> expression(GetNull());
  FailureOr<bool> result =
      GetConstantBoundExpressionValue<BOOL>(expression.get(), &is_null);
  EXPECT_TRUE(result.is_success());
  EXPECT_TRUE(is_null);
}

TEST_F(BasicBoundExpressionTest, GetConstantExpressionValueNotNull) {
  bool is_null;
  std::unique_ptr<const Expression> expression(ConstBool(true));
  FailureOr<bool> result =
      GetConstantExpressionValue<BOOL>(*expression, &is_null);
  EXPECT_TRUE(result.is_success());
  EXPECT_TRUE(result.get());
  EXPECT_FALSE(is_null);

  expression.reset(ConstBool(false));
  result = GetConstantExpressionValue<BOOL>(*expression, &is_null);
  EXPECT_TRUE(result.is_success());
  EXPECT_FALSE(result.get());
  EXPECT_FALSE(is_null);
}

TEST_F(BasicBoundExpressionTest, GetConstantExpressionValueNull) {
  bool is_null;
  std::unique_ptr<const Expression> expression(Null(BOOL));
  FailureOr<bool> result =
      GetConstantExpressionValue<BOOL>(*expression, &is_null);
  EXPECT_TRUE(result.is_success());
  EXPECT_TRUE(is_null);
}

TEST_F(BasicBoundExpressionTest, GetConstantExpressionValueNotConst) {
  bool is_null;
  std::unique_ptr<const Expression> expression(NamedAttribute("any"));
  FailureOr<bool> result =
      GetConstantExpressionValue<BOOL>(*expression, &is_null);
  EXPECT_FALSE(result.is_success());
}

}  // namespace
}  // namespace supersonic
