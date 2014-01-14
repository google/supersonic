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

#include "supersonic/expression/templated/cast_bound_expression.h"

#include <set>
#include "supersonic/utils/std_namespace.h"
#include <string>
namespace supersonic {using std::string; }

#include "supersonic/expression/base/expression.h"
#include "supersonic/expression/core/projecting_bound_expressions.h"
#include "supersonic/proto/supersonic.pb.h"
#include "supersonic/testing/expression_test_helper.h"
#include "gtest/gtest.h"
#include "supersonic/utils/container_literal.h"

namespace supersonic {

class BufferAllocator;

namespace {

using util::gtl::Container;

template <DataType to_type, bool is_implicit>
FailureOrOwned<BoundExpression> BoundUnaryCast(
    BoundExpression* child_ptr,
    BufferAllocator* const allocator,
    rowcount_t row_capacity) {
  return BoundInternalCast(allocator, row_capacity, child_ptr, to_type,
                           is_implicit);
}

TEST(CastBoundExpressionTest, NotImplicitBoundCastExpression) {
  TestBoundUnary(&(BoundUnaryCast<INT32, false>), INT32, "$0");
  TestBoundUnary(&(BoundUnaryCast<INT32, false>), UINT32,
                 "CAST_UINT32_TO_INT32($0)");

  TestBoundUnary(&(BoundUnaryCast<INT64, false>), INT32,
                 "CAST_INT32_TO_INT64($0)");
  TestBoundUnary(&(BoundUnaryCast<INT32, false>), INT64,
                 "CAST_INT64_TO_INT32($0)");

  TestBoundUnary(&(BoundUnaryCast<DOUBLE, false>), FLOAT,
                 "CAST_FLOAT_TO_DOUBLE($0)");
  TestBoundUnary(&(BoundUnaryCast<FLOAT, false>), DOUBLE,
                 "CAST_DOUBLE_TO_FLOAT($0)");

  TestBoundUnary(&(BoundUnaryCast<STRING, false>), BINARY,
                 "CAST_BINARY_TO_STRING($0)");
  TestBoundUnary(&(BoundUnaryCast<BINARY, false>), STRING,
                 "CAST_STRING_TO_BINARY($0)");

  TestBoundFactoryFailure(&(BoundUnaryCast<INT32, false>), STRING);
  TestBoundFactoryFailure(&(BoundUnaryCast<STRING, false>), INT32);
}

TEST(CastBoundExpressionTest, ImplicitBoundCastExpression) {
  TestBoundUnary(&(BoundUnaryCast<INT32, true>), INT32, "$0");
  TestBoundUnary(&(BoundUnaryCast<INT32, true>), UINT32,
                 "CAST_UINT32_TO_INT32($0)");

  TestBoundUnary(&(BoundUnaryCast<INT64, true>), INT32,
                 "CAST_INT32_TO_INT64($0)");
  TestBoundFactoryFailure(&(BoundUnaryCast<INT32, true>), INT64);

  TestBoundUnary(&(BoundUnaryCast<DOUBLE, true>), FLOAT,
                 "CAST_FLOAT_TO_DOUBLE($0)");
  TestBoundFactoryFailure(&(BoundUnaryCast<FLOAT, true>), DOUBLE);

  TestBoundUnary(&(BoundUnaryCast<STRING, true>), BINARY,
                 "CAST_BINARY_TO_STRING($0)");
  TestBoundUnary(&(BoundUnaryCast<BINARY, true>), STRING,
                 "CAST_STRING_TO_BINARY($0)");
  TestBoundFactoryFailure(&(BoundUnaryCast<INT32, true>), STRING);
  TestBoundFactoryFailure(&(BoundUnaryCast<STRING, true>), INT32);
}

TEST(CastBoundExpressionTest, ProjectingCastCollectReferredAttributeNames) {
  TupleSchema schema;
  schema.add_attribute(Attribute("uint32", UINT32, NULLABLE));
  schema.add_attribute(Attribute("int32",  INT32, NOT_NULLABLE));
  schema.add_attribute(Attribute("string", STRING, NOT_NULLABLE));
  FailureOrOwned<BoundExpression> bound_cast = BoundInternalCast(
        HeapBufferAllocator::Get(), 20,
        SucceedOrDie(BoundNamedAttribute(schema, "int32")),
        UINT32,
        true);
  ASSERT_TRUE(bound_cast.is_success())
      << bound_cast.exception().PrintStackTrace();
  EXPECT_EQ(Container("int32").As<set<string> >(),
            bound_cast->referred_attribute_names());
}

}  // namespace

}  // namespace supersonic
