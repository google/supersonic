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

#include "supersonic/expression/core/math_bound_expressions.h"

#include "supersonic/proto/supersonic.pb.h"
#include "supersonic/testing/expression_test_helper.h"
#include "gtest/gtest.h"

namespace supersonic {

namespace {

// ------------------------ Exponents, logarithms, powers ----------------------

TEST(MathBoundExpressionsTest, BoundExp) {
  TestBoundUnary(&BoundExp, DOUBLE, "EXP($0)");
  TestBoundUnary(&BoundExp, INT32, "EXP(CAST_INT32_TO_DOUBLE($0))");
}

TEST(MathBoundExpressionsTest, BoundLnNulling) {
  TestBoundUnary(&BoundLnNulling, DOUBLE, "LN($0)");
}

TEST(MathBoundExpressionsTest, BoundLnQuiet) {
  TestBoundUnary(&BoundLnQuiet, DOUBLE, "LN($0)");
}

TEST(MathBoundExpressionsTest, BoundLog10Nulling) {
  TestBoundUnary(&BoundLog10Nulling, DOUBLE, "LOG10($0)");
}

TEST(MathBoundExpressionsTest, BoundLog2Quiet) {
  TestBoundUnary(&BoundLog2Quiet, DOUBLE, "LOG2($0)");
}

TEST(MathBoundExpressionsTest, BoundLogNulling) {
  TestBoundBinary(&BoundLogNulling, DOUBLE, DOUBLE, "(LN($1) /. LN($0))");
}

TEST(MathBoundExpressionsTest, BoundPowerSignaling) {
  TestBoundBinary(&BoundPowerSignaling, DOUBLE, DOUBLE, "POW($0, $1)");
}

TEST(MathBoundExpressionsTest, BoundPowerNulling) {
  TestBoundBinary(&BoundPowerNulling, DOUBLE, DOUBLE, "POW($0, $1)");
}

TEST(MathBoundExpressionsTest, BoundPowerQuiet) {
  TestBoundBinary(&BoundPowerQuiet, DOUBLE, DOUBLE, "POW($0, $1)");
}

TEST(MathBoundExpressionsTest, BoundSqrtSignaling) {
  TestBoundUnary(&BoundSqrtSignaling, DOUBLE, "SQRT($0)");
}

TEST(MathBoundExpressionsTest, BoundSqrtNulling) {
  TestBoundUnary(&BoundSqrtNulling, DOUBLE, "SQRT($0)");
}

TEST(MathBoundExpressionsTest, BoundSqrtQuiet) {
  TestBoundUnary(&BoundSqrtQuiet, DOUBLE, "SQRT($0)");
}

// ------------------------------ Trigonometry ---------------------------------

TEST(MathBoundExpressionsTest, BoundSin) {
  TestBoundUnary(&BoundSin, DOUBLE, "SIN($0)");
}

TEST(MathBoundExpressionsTest, BoundCos) {
  TestBoundUnary(&BoundCos, DOUBLE, "COS($0)");
}

TEST(MathBoundExpressionsTest, BoundTanQuiet) {
  TestBoundUnary(&BoundTanQuiet, DOUBLE, "TAN($0)");
}

// ------------------------------------ Rounding -------------------------------

TEST(MathBoundExpressionsTest, BoundRound) {
  TestBoundUnary(&BoundRound, DOUBLE, "ROUND($0)");
  TestBoundUnary(&BoundRound, INT32, "$0");
}

TEST(MathBoundExpressionsTest, BoundRoundToInt) {
  // TODO(ptab): Should be native ROUND_TO_INT($0) when llround issue is fixed
  // (b/5183960).
  // TestBoundUnary(&BoundRoundToInt, DOUBLE, "ROUND_TO_INT($0)");
  TestBoundUnary(&BoundRoundToInt, DOUBLE, "CEIL_TO_INT(ROUND($0))");
  TestBoundUnary(&BoundRoundToInt, UINT32, "$0");
}

TEST(MathBoundExpressionsTest, BoundFloor) {
  TestBoundUnary(&BoundFloor, DOUBLE, "FLOOR($0)");
  TestBoundUnary(&BoundFloor, INT32, "$0");
}

TEST(MathBoundExpressionsTest, BoundFloorToInt) {
  TestBoundUnary(&BoundFloorToInt, DOUBLE, "FLOOR_TO_INT($0)");
  TestBoundUnary(&BoundFloorToInt, INT32, "$0");
}

TEST(MathBoundExpressionsTest, BoundCeil) {
  TestBoundUnary(&BoundCeil, DOUBLE, "CEIL($0)");
  TestBoundUnary(&BoundCeil, INT32, "$0");
}

TEST(MathBoundExpressionsTest, BoundCeilToInt) {
  TestBoundUnary(&BoundCeilToInt, DOUBLE, "CEIL_TO_INT($0)");
  TestBoundUnary(&BoundCeilToInt, INT32, "$0");
}

TEST(MathBoundExpressionsTest, BoundTrunc) {
  TestBoundUnary(&BoundTrunc, DOUBLE, "TRUNC($0)");
  TestBoundUnary(&BoundTrunc, UINT64, "$0");
}

// ------------------------------ IEEE 754 Checks ------------------------------

TEST(MathBoundExpressionsTest, BoundIsFinite) {
  TestBoundUnary(&BoundIsFinite, DOUBLE, "IS_FINITE($0)");
}

TEST(MathBoundExpressionsTest, BoundIsNormal) {
  TestBoundUnary(&BoundIsNormal, DOUBLE, "IS_NORMAL($0)");
}

TEST(MathBoundExpressionsTest, BoundIsNaN) {
  TestBoundUnary(&BoundIsNaN, DOUBLE, "IS_NAN($0)");
}

TEST(MathBoundExpressionsTest, BoundIsInf) {
  TestBoundUnary(&BoundIsInf, DOUBLE, "IS_INF($0)");
}

// ----------------------------------- Other -----------------------------------

TEST(MathBoundExpressionsTest, BoundFormatSignaling) {
  TestBoundBinary(&BoundFormatSignaling, DOUBLE, INT32, "FORMAT($0, $1)");
}

}  // namespace

}  // namespace supersonic
