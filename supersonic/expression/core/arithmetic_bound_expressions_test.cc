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

#include "supersonic/expression/core/arithmetic_bound_expressions.h"

#include "supersonic/proto/supersonic.pb.h"
#include "supersonic/testing/expression_test_helper.h"
#include "gtest/gtest.h"

namespace supersonic {
namespace {

TEST(ElementaryBoundExpressionsTest, BoundNegate) {
  TestBoundUnary(&BoundNegate, INT32, "(-$0)");
  TestBoundUnary(&BoundNegate, UINT32, "(-$0)");
}

TEST(ElementaryBoundExpressionsTest, BoundPlus) {
  TestBoundBinary(&BoundPlus, INT32, INT64, "(CAST_INT32_TO_INT64($0) + $1)");
}

TEST(ElementaryBoundExpressionsTest, BoundMultiply) {
  TestBoundBinary(&BoundMultiply, INT32, INT64,
                  "(CAST_INT32_TO_INT64($0) * $1)");
}

TEST(ElementaryBoundExpressionsTest, BoundMinus) {
  TestBoundBinary(&BoundMinus, INT32, INT64, "(CAST_INT32_TO_INT64($0) - $1)");
}

TEST(ElementaryBoundExpressionsTest, BoundDivideSignaling) {
  TestBoundBinary(&BoundDivideSignaling, UINT32, UINT64,
                  "(CAST_UINT32_TO_DOUBLE($0) /. CAST_UINT64_TO_DOUBLE($1))");
}

TEST(ElementaryBoundExpressionsTest, BoundDivideNulling) {
  TestBoundBinary(&BoundDivideNulling, UINT32, UINT64,
                  "(CAST_UINT32_TO_DOUBLE($0) /. CAST_UINT64_TO_DOUBLE($1))");
}

TEST(ElementaryBoundExpressionsTest, BoundDivideQuiet) {
  TestBoundBinary(&BoundDivideQuiet, UINT32, UINT64,
                  "(CAST_UINT32_TO_DOUBLE($0) /. CAST_UINT64_TO_DOUBLE($1))");
}

TEST(ElementaryBoundExpressionsTest, BoundCppDivideSignaling) {
  TestBoundBinary(&BoundCppDivideSignaling, UINT32, UINT64,
                  "(CAST_UINT32_TO_UINT64($0) / $1)");
}

TEST(ElementaryBoundExpressionsTest, BoundCppDivideNulling) {
  TestBoundBinary(&BoundCppDivideNulling, UINT32, UINT64,
                  "(CAST_UINT32_TO_UINT64($0) / $1)");
}

TEST(ElementaryBoundExpressionsTest, BoundModulusSignaling) {
  TestBoundBinary(&BoundModulusSignaling, UINT32, UINT64,
                  "(CAST_UINT32_TO_UINT64($0) % $1)");
}

TEST(ElementaryBoundExpressionsTest, BoundModulusNulling) {
  TestBoundBinary(&BoundModulusNulling, UINT32, UINT64,
                  "(CAST_UINT32_TO_UINT64($0) % $1)");
}
}  // namespace
}  // namespace supersonic
