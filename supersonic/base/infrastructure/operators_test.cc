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

#include "supersonic/base/infrastructure/operators.h"

#include "supersonic/utils/integral_types.h"
#include "gtest/gtest.h"

namespace supersonic {

static const int32 kInt32_3 = 3;
static const int32 kInt32_5 = 5;
static const int32 kInt32_Neg_5 = -5;

static const int64 kInt64_3 = 3;
static const int64 kInt64_5 = 5;
static const int64 kInt64_Neg_5 = -5;

static const uint32 kUInt32_3 = 3;
static const uint32 kUInt32_5 = 5;
static const uint32 kUInt32_Neg_5 = static_cast<uint32>(-5);

static const uint64 kUInt64_3 = 3;
static const uint64 kUInt64_5 = 5;
static const uint64 kUInt64_Neg_5 = static_cast<uint64>(-5);

class EqualTest : public testing::Test {};

TEST_F(EqualTest, MixedNumerics) {
  operators::Equal eq;
  EXPECT_TRUE(eq(kInt32_3, kInt32_3));
  EXPECT_TRUE(eq(kInt32_3, kUInt32_3));
  EXPECT_TRUE(eq(kInt32_3, kInt64_3));
  EXPECT_TRUE(eq(kInt32_3, kUInt64_3));

  EXPECT_TRUE(eq(kUInt32_3, kInt32_3));
  EXPECT_TRUE(eq(kUInt32_3, kUInt32_3));
  EXPECT_TRUE(eq(kUInt32_3, kInt64_3));
  EXPECT_TRUE(eq(kUInt32_3, kUInt64_3));

  EXPECT_TRUE(eq(kInt64_3, kInt32_3));
  EXPECT_TRUE(eq(kInt64_3, kUInt32_3));
  EXPECT_TRUE(eq(kInt64_3, kInt64_3));
  EXPECT_TRUE(eq(kInt64_3, kUInt64_3));

  EXPECT_TRUE(eq(kUInt64_3, kInt32_3));
  EXPECT_TRUE(eq(kUInt64_3, kUInt32_3));
  EXPECT_TRUE(eq(kUInt64_3, kInt64_3));
  EXPECT_TRUE(eq(kUInt64_3, kUInt64_3));

  EXPECT_FALSE(eq(kInt32_Neg_5, kUInt32_Neg_5));
  EXPECT_FALSE(eq(kInt32_Neg_5, kUInt64_Neg_5));
  EXPECT_FALSE(eq(kInt64_Neg_5, kUInt32_Neg_5));
  EXPECT_FALSE(eq(kInt64_Neg_5, kUInt64_Neg_5));

  EXPECT_FALSE(eq(kUInt32_Neg_5, kInt32_Neg_5));
  EXPECT_FALSE(eq(kUInt32_Neg_5, kInt64_Neg_5));
  EXPECT_FALSE(eq(kUInt64_Neg_5, kInt32_Neg_5));
  EXPECT_FALSE(eq(kUInt64_Neg_5, kInt64_Neg_5));

  EXPECT_FALSE(eq(kInt32_3, kInt64_5));
}

TEST_F(EqualTest, Bool) {
  operators::Equal equal;
  EXPECT_TRUE(equal(false, false));
  EXPECT_FALSE(equal(false, true));
  EXPECT_FALSE(equal(true, false));
  EXPECT_TRUE(equal(true, true));
}

TEST_F(EqualTest, StringAndBinary) {
  operators::Equal equal;
  StringPiece a("a");
  StringPiece aa("aa");
  EXPECT_TRUE(equal(a, a));
  EXPECT_FALSE(equal(a, aa));
  EXPECT_FALSE(equal(aa, a));
  EXPECT_TRUE(equal(aa, aa));
}

class LessTest : public testing::Test {};

TEST_F(LessTest, Trivial) {
  operators::Less less;
  EXPECT_TRUE(less(kInt32_3, kInt32_5));
  EXPECT_TRUE(less(kInt32_Neg_5, kInt32_3));
  EXPECT_FALSE(less(kInt32_5, kInt32_Neg_5));
}

TEST_F(LessTest, MixedNumerics) {
  operators::Less less;
  // Double-check that C++ is insane.
  EXPECT_TRUE(kInt32_Neg_5 > kUInt32_5);
  // But not Supersonics.
  EXPECT_TRUE(less(kInt32_Neg_5, kUInt32_5));
  EXPECT_TRUE(less(kInt32_Neg_5, kUInt64_5));
  EXPECT_TRUE(less(kInt64_Neg_5, kUInt32_5));
  EXPECT_TRUE(less(kInt64_Neg_5, kUInt64_5));

  EXPECT_FALSE(less(kUInt32_5, kInt32_Neg_5));
  EXPECT_FALSE(less(kUInt64_5, kInt32_Neg_5));
  EXPECT_FALSE(less(kUInt32_5, kInt64_Neg_5));
  EXPECT_FALSE(less(kUInt64_5, kInt64_Neg_5));

  EXPECT_TRUE(less(kInt32_3, kUInt32_5));
  EXPECT_TRUE(less(kInt32_3, kUInt64_5));
  EXPECT_TRUE(less(kInt64_3, kUInt32_5));
  EXPECT_TRUE(less(kInt64_3, kUInt64_5));

  EXPECT_FALSE(less(kUInt32_5, kInt32_3));
  EXPECT_FALSE(less(kUInt64_5, kInt32_3));
  EXPECT_FALSE(less(kUInt32_5, kInt64_3));
  EXPECT_FALSE(less(kUInt64_5, kInt64_3));
}

TEST_F(LessTest, Bool) {
  operators::Less less;
  EXPECT_FALSE(less(false, false));
  EXPECT_TRUE(less(false, true));
  EXPECT_FALSE(less(true, false));
  EXPECT_FALSE(less(true, true));
}

TEST_F(LessTest, StringAndBinary) {
  operators::Less less;
  StringPiece a("a");
  StringPiece aa("aa");
  EXPECT_FALSE(less(a, a));
  EXPECT_TRUE(less(a, aa));
  EXPECT_FALSE(less(aa, a));
  EXPECT_FALSE(less(aa, aa));
}

class ComplementsTest : public testing::Test {};

TEST_F(ComplementsTest, Greater) {
  operators::Greater op;
  EXPECT_TRUE(op(5, 3));
  EXPECT_FALSE(op(5, 5));
  EXPECT_FALSE(op(3, 5));
}

TEST_F(ComplementsTest, LessOrEqual) {
  operators::LessOrEqual op;
  EXPECT_FALSE(op(5, 3));
  EXPECT_TRUE(op(5, 5));
  EXPECT_TRUE(op(3, 5));
}

TEST_F(ComplementsTest, GreaterOrEqual) {
  operators::GreaterOrEqual op;
  EXPECT_TRUE(op(5, 3));
  EXPECT_TRUE(op(5, 5));
  EXPECT_FALSE(op(3, 5));
}

TEST_F(ComplementsTest, NotEqual) {
  operators::NotEqual op;
  EXPECT_TRUE(op(5, 3));
  EXPECT_FALSE(op(5, 5));
  EXPECT_TRUE(op(3, 5));
}

}  // namespace supersonic
