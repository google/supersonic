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
// Author: onufry@google.com (Jakub Onufry Wojtaszczyk)

#include "supersonic/expression/core/math_expressions.h"

#include <cmath>
#include <limits>
#include "supersonic/utils/std_namespace.h"

#include "supersonic/utils/integral_types.h"
#include "supersonic/proto/supersonic.pb.h"
#include "supersonic/testing/block_builder.h"
#include "supersonic/testing/expression_test_helper.h"
#include "gtest/gtest.h"

namespace supersonic {

// The documentation of the whole test fixture scheme (TestBinding and
// TestEvaluation) is in expression_test_helper.h.
TEST(MathExpressionTest, BindingSuccess) {
  TestUnaryBinding(&Ceil,            DOUBLE, "CEIL($0)",         DOUBLE, false);
  TestUnaryBinding(&Ceil,            UINT32, "$0",               UINT32, false);
  TestUnaryBinding(&CeilToInt,       DOUBLE, "CEIL_TO_INT($0)",  INT64,  false);
  TestUnaryBinding(&CeilToInt,       UINT64, "$0",               UINT64, false);
  TestUnaryBinding(&Exp,             DOUBLE, "EXP($0)",          DOUBLE, false);
  TestUnaryBinding(&Floor,           DOUBLE, "FLOOR($0)",        DOUBLE, false);
  TestUnaryBinding(&Floor,           FLOAT,  "FLOOR($0)",        FLOAT,  false);
  TestUnaryBinding(&FloorToInt,      FLOAT,  "FLOOR_TO_INT($0)", INT64,  false);
  TestUnaryBinding(&FloorToInt,      INT64,  "$0",               INT64,  false);
  TestUnaryBinding(&LnNulling,       DOUBLE, "LN($0)",           DOUBLE, true);
  TestUnaryBinding(&LnQuiet,         DOUBLE, "LN($0)",           DOUBLE, false);
  TestUnaryBinding(&Log10Nulling,    DOUBLE, "LOG10($0)",        DOUBLE, true);
  TestUnaryBinding(&Log10Quiet,      DOUBLE, "LOG10($0)",        DOUBLE, false);
  TestUnaryBinding(&Log2Nulling,     DOUBLE, "LOG2($0)",         DOUBLE, true);
  TestUnaryBinding(&Log2Quiet,       DOUBLE, "LOG2($0)",         DOUBLE, false);
  TestUnaryBinding(&Round,           DOUBLE, "ROUND($0)",        DOUBLE, false);
  TestUnaryBinding(&Round,           FLOAT,  "ROUND($0)",        FLOAT,  false);
  TestUnaryBinding(&Round,           INT32,  "$0",               INT32,  false);
  // TODO(ptab): Should be native ROUND_TO_INT($0) when llround issue is fixed.
  TestUnaryBinding(&RoundToInt, DOUBLE, "CEIL_TO_INT(ROUND($0))", INT64, false);
  TestUnaryBinding(&RoundToInt,      UINT32, "$0",               UINT32, false);
  TestUnaryBinding(&SqrtQuiet,       DOUBLE, "SQRT($0)",         DOUBLE, false);
  TestUnaryBinding(&SqrtNulling,     DOUBLE, "SQRT($0)",         DOUBLE, true);
  TestUnaryBinding(&SqrtSignaling,   DOUBLE, "SQRT($0)",         DOUBLE, false);
  TestUnaryBinding(&Trunc,           DOUBLE, "TRUNC($0)",        DOUBLE, false);
  TestUnaryBinding(&Sin,             DOUBLE, "SIN($0)",          DOUBLE, false);
  TestUnaryBinding(&Cos,             DOUBLE, "COS($0)",          DOUBLE, false);
  TestUnaryBinding(&Tan,             DOUBLE, "TAN($0)",          DOUBLE, false);
  TestUnaryBinding(&Cot,    DOUBLE, "(CONST_DOUBLE /. TAN($0))", DOUBLE, false);
  TestUnaryBinding(&Asin,            DOUBLE, "ASIN($0)",         DOUBLE, false);
  TestUnaryBinding(&Acos,            DOUBLE, "ACOS($0)",         DOUBLE, false);
  TestUnaryBinding(&Atan,            DOUBLE, "ATAN($0)",         DOUBLE, false);
  TestUnaryBinding(&Sinh,            DOUBLE, "SINH($0)",         DOUBLE, false);
  TestUnaryBinding(&Cosh,            DOUBLE, "COSH($0)",         DOUBLE, false);
  TestUnaryBinding(&Tanh,            DOUBLE, "TANH($0)",         DOUBLE, false);
  TestUnaryBinding(&Asinh,           DOUBLE, "ASINH($0)",        DOUBLE, false);
  TestUnaryBinding(&Acosh,           DOUBLE, "ACOSH($0)",        DOUBLE, false);
  TestUnaryBinding(&Atanh,           DOUBLE, "ATANH($0)",        DOUBLE, false);
  TestUnaryBinding(&ToDegrees,    DOUBLE, "($0 * CONST_DOUBLE)", DOUBLE, false);
  TestUnaryBinding(&ToRadians,    DOUBLE, "($0 * CONST_DOUBLE)", DOUBLE, false);
  TestUnaryBinding(&IsFinite,        DOUBLE, "IS_FINITE($0)",    BOOL,   false);
  TestUnaryBinding(&IsNormal,        DOUBLE, "IS_NORMAL($0)",    BOOL,   false);
  TestUnaryBinding(&IsNaN,           DOUBLE, "IS_NAN($0)",       BOOL,   false);
  TestUnaryBinding(&IsInf,           DOUBLE, "IS_INF($0)",       BOOL,   false);

  TestBinaryBindingNotNullable(
      &Atan2,          DOUBLE, DOUBLE, "ATAN2($0, $1)",      DOUBLE);
  TestBinaryBindingNotNullable(
      &Format,         DOUBLE, INT32,  "FORMAT($0, $1)",     STRING);
  TestBinaryBindingNotNullable(
      &LogQuiet,       DOUBLE, DOUBLE, "(LN($1) /. LN($0))", DOUBLE);
  TestBinaryBindingNotNullable(
      &PowerSignaling, DOUBLE, DOUBLE, "POW($0, $1)",        DOUBLE);
  TestBinaryBindingNotNullable(
      &PowerQuiet,     DOUBLE, DOUBLE, "POW($0, $1)",        DOUBLE);

  TestBinaryBindingNullable(
      &PowerNulling, DOUBLE, DOUBLE, "POW($0, $1)",        DOUBLE);
  TestBinaryBindingNullable(
      &LogNulling,   DOUBLE, DOUBLE, "(LN($1) /. LN($0))", DOUBLE);
}

// Precise testing, as custom binding function is present.
TEST(MathExpressionTest, RoundWithPrecisionBinding) {
  TestBinaryBindingNotNullable(
      &RoundWithPrecision, FLOAT, INT32,
      "ROUND_WITH_MULTIPLIER("
          "CAST_FLOAT_TO_DOUBLE($0), "
          "POW(CONST_DOUBLE, CAST_INT32_TO_DOUBLE($1)))",
      DOUBLE);

  TestBinaryBindingNotNullable(
      &RoundWithPrecision, UINT32, UINT64,
      "ROUND_WITH_MULTIPLIER("
          "CAST_UINT32_TO_DOUBLE($0), "
          "POW(CONST_DOUBLE, CAST_UINT64_TO_DOUBLE($1)))",
      DOUBLE);

  TestBindingFailure(&RoundWithPrecision, DOUBLE, DOUBLE);
}

TEST(MathExpressionTest, BindingFailure) {
  TestBindingFailure(&Ceil, STRING);

  TestBindingFailure(&LogNulling, DATETIME, INT32);
  TestBindingFailure(&LogNulling, DOUBLE,   DATE);
  TestBindingFailure(&Format,     DOUBLE,   DOUBLE);
}

TEST(MathExpressionTest, BindingWithCast) {
  TestUnaryBinding(
      &Exp, FLOAT,  "EXP(CAST_FLOAT_TO_DOUBLE($0))",  DOUBLE, false);
  TestUnaryBinding(
      &Sin, INT32,  "SIN(CAST_INT32_TO_DOUBLE($0))",  DOUBLE, false);
  TestUnaryBinding(
      &Cos, UINT32, "COS(CAST_UINT32_TO_DOUBLE($0))", DOUBLE, false);
  TestUnaryBinding(
      &Tan, UINT64, "TAN(CAST_UINT64_TO_DOUBLE($0))", DOUBLE, false);
  TestUnaryBinding(
      &Sin, INT64,  "SIN(CAST_INT64_TO_DOUBLE($0))",  DOUBLE, false);
}

TEST(MathExpressionTest, AbsBinding) {
  TestUnaryBinding(&Abs, INT32,    "ABS($0)", UINT32, false);
  TestUnaryBinding(&Abs, UINT32,   "$0",      UINT32, false);
  TestUnaryBinding(&Abs, INT64,    "ABS($0)", UINT64, false);
  TestUnaryBinding(&Abs, UINT64,   "$0",      UINT64, false);
  TestUnaryBinding(&Abs, FLOAT,    "ABS($0)", FLOAT,  false);
  TestUnaryBinding(&Abs, DOUBLE,   "ABS($0)", DOUBLE, false);
  TestBindingFailure(&Abs, DATETIME);
  TestBindingFailure(&Abs, STRING);
}

TEST(MathExpressionTest, Round) {
  TestEvaluation(BlockBuilder<DOUBLE, DOUBLE>()
      .AddRow(4.,        4.)
      .AddRow(0.5,       1.)
      .AddRow(-0.5,     -1.)
      .AddRow(0.49,      0.)
      .AddRow(-0.49,     0.)
      .AddRow(2345.,  2345.)
      .Build(), &Round);
}

TEST(MathExpressionTest, RoundFloat) {
  TestEvaluation(BlockBuilder<FLOAT, FLOAT>()
      .AddRow(4.,        4.)
      .AddRow(0.5,       1.)
      .AddRow(-0.5,     -1.)
      .AddRow(0.49,      0.)
      .AddRow(-0.49,     0.)
      .AddRow(2345.,  2345.)
      .Build(), &Round);
}

TEST(MathExpressionTest, RoundToIntOnDouble) {
  TestEvaluation(BlockBuilder<DOUBLE, INT64>()
      .AddRow(4.,       4LL)
      .AddRow(0.5,      1LL)
      .AddRow(-0.5,    -1LL)
      .AddRow(0.49,     0LL)
      .AddRow(__,        __)
      .AddRow(-0.49,    0LL)
      .AddRow(2345., 2345LL)
      .AddRow(-3.65309740835E17, -365309740835000000LL)
      .Build(), &RoundToInt);
}

TEST(MathExpressionTest, RoundToIntOnFloat) {
  TestEvaluation(BlockBuilder<FLOAT, INT64>()
      .AddRow(4.f,       4LL)
      .AddRow(0.5f,      1LL)
      .AddRow(-0.5f,    -1LL)
      .AddRow(0.49f,     0LL)
      .AddRow(__,        __)
      .AddRow(-0.49f,    0LL)
      .AddRow(2345.f, 2345LL)
      .AddRow(-3.65309E5f, -365309LL)
      .Build(), &RoundToInt);
}

TEST(MathExpressionTest, RoundWithPrecision) {
  TestEvaluation(BlockBuilder<DOUBLE, INT32, DOUBLE>()
      .AddRow(4.,       0,   4.)
      .AddRow(4.,       2,   4.)
      .AddRow(4.,      -1,   0.)
      .AddRow(1024,    -3,   1000.)
      .AddRow(3.14,     1,   3.1)
      .AddRow(3.141592, 4,   3.1416)
      .AddRow(3.141592, 5,   3.14159)
      .AddRow(-0.4,     0,   0.)
      .AddRow(-0.6,     0,  -1.)
      .AddRow(0.5,      0,   1.)
      .AddRow(0.1,     20,   0.1)
      .Build(), &RoundWithPrecision);
}

TEST(MathExpressionTest, Ceil) {
  TestEvaluation(BlockBuilder<DOUBLE, DOUBLE>()
      .AddRow(3.,     3.)
      .AddRow(-3.,   -3.)
      .AddRow(-2.9,  -2.)
      .AddRow(1.9,    2.)
      .AddRow(-2.1,  -2.)
      .AddRow(1.001,  2.)
      .Build(), &Ceil);

  TestEvaluation(BlockBuilder<FLOAT, FLOAT>()
      .AddRow(0.1,  1.)
      .AddRow(-0.9, 0.)
      .Build(), &Ceil);

  TestEvaluation(BlockBuilder<INT32, INT32>()
      .AddRow(7,  7)
      .AddRow(-1, -1)
      .Build(), &Ceil);
}

TEST(MathExpressionTest, CeilToInt) {
  TestEvaluation(BlockBuilder<DOUBLE, INT64>()
      .AddRow(3.,     3)
      .AddRow(-3.,   -3)
      .AddRow(-2.9,  -2)
      .AddRow(1.9,    2)
      .AddRow(-2.1,  -2)
      .AddRow(1.001,  2)
      .Build(), &CeilToInt);

  TestEvaluation(BlockBuilder<FLOAT, INT64>()
      .AddRow(0.1,  1)
      .AddRow(-0.9, 0)
      .Build(), &CeilToInt);

  TestEvaluation(BlockBuilder<INT32, INT32>()
      .AddRow(7,  7)
      .AddRow(-1, -1)
      .Build(), &CeilToInt);
}

TEST(MathExpressionTest, Floor) {
  TestEvaluation(BlockBuilder<DOUBLE, DOUBLE>()
      .AddRow(3.,     3.)
      .AddRow(-3.,   -3.)
      .AddRow(-2.9,  -3.)
      .AddRow(1.9,    1.)
      .AddRow(-2.1,  -3.)
      .AddRow(1.001,  1.)
      .Build(), &Floor);

  TestEvaluation(BlockBuilder<FLOAT, FLOAT>()
      .AddRow(0.1,   0.)
      .AddRow(-0.9, -1.)
      .Build(), &Floor);

  TestEvaluation(BlockBuilder<INT32, INT32>()
      .AddRow(7,  7)
      .AddRow(-1, -1)
      .Build(), &Floor);
}

TEST(MathExpressionTest, FloorToInt) {
  TestEvaluation(BlockBuilder<DOUBLE, INT64>()
      .AddRow(3.,     3)
      .AddRow(-3.,   -3)
      .AddRow(-2.9,  -3)
      .AddRow(1.9,    1)
      .AddRow(-2.1,  -3)
      .AddRow(1.001,  1)
      .Build(), &FloorToInt);

  TestEvaluation(BlockBuilder<FLOAT, INT64>()
      .AddRow(0.1,  0)
      .AddRow(-0.9, -1)
      .Build(), &FloorToInt);

  TestEvaluation(BlockBuilder<INT32, INT32>()
      .AddRow(7,  7)
      .AddRow(-1, -1)
      .Build(), &FloorToInt);
}

TEST(MathExpressionTest, Trunc) {
  TestEvaluation(BlockBuilder<DOUBLE, DOUBLE>()
      .AddRow(3.,     3.)
      .AddRow(-3.,   -3.)
      .AddRow(-2.9,  -2.)
      .AddRow(1.9,    1.)
      .AddRow(-2.1,  -2.)
      .AddRow(1.001,  1.)
      .Build(), &Trunc);

  TestEvaluation(BlockBuilder<FLOAT, FLOAT>()
      .AddRow(0.1,  0.)
      .AddRow(__,   __)
      .Build(), &Trunc);

  TestEvaluation(BlockBuilder<UINT32, UINT32>()
      .AddRow(7, 7)
      .AddRow(0, 0)
      .Build(), &Trunc);
}

TEST(MathExpressionTest, Abs) {
  TestEvaluation(BlockBuilder<INT32, UINT32>()
      .AddRow(0, 0)
      .AddRow(1, 1)
      .AddRow(-3, 3)
      .AddRow(static_cast<int32>(-2147483648LL),
              static_cast<uint32>(2147483648LL))  // MININT
      .Build(), &Abs);
}

TEST(MathExpressionTest, SqrtNulling) {
  TestEvaluation(BlockBuilder<DOUBLE, DOUBLE>()
      .AddRow(0.25,    0.5)
      .AddRow(1.,      1.)
      .AddRow(40000.,  200.)
      .AddRow(0.,      0.)
      .AddRow(-1,      __)
      .Build(), &SqrtNulling);
}

TEST(MathExpressionTest, SqrtSignaling) {
  TestEvaluation(BlockBuilder<DOUBLE, DOUBLE>()
      .AddRow(0.25,    0.5)
      .AddRow(1.,      1.)
      .AddRow(__,      __)
      .AddRow(0.,      0.)
      .Build(), &SqrtSignaling);

  TestEvaluationFailure(BlockBuilder<DOUBLE>()
      .AddRow(-1.)
      .AddRow(-123321.)
      .Build(), &SqrtSignaling);
}

TEST(MathExpressionTest, SqrtQuiet) {
  double nan = numeric_limits<double>::quiet_NaN();
  TestEvaluation(BlockBuilder<DOUBLE, DOUBLE>()
      .AddRow(0.25,    0.5)
      .AddRow(1.,      1.)
      .AddRow(-123.,   nan)
      .AddRow(40000.,  200.)
      .AddRow(0.,      0.)
      .AddRow(-1,      nan)
      .Build(), &SqrtQuiet);
}

TEST(MathExpressionTest, LnNulling) {
  TestEvaluation(BlockBuilder<DOUBLE, DOUBLE>()
      .AddRow(1.,     0.)
      .AddRow(1000.,  log(1000.))
      .AddRow(exp(1),  1.)
      .AddRow(0.,     __)
      .AddRow(-1,     __)
      .Build(), &LnNulling);
}

TEST(MathExpressionTest, LnQuiet) {
  double inf = 1./0.;
  double nan = inf - inf;
  TestEvaluation(BlockBuilder<DOUBLE, DOUBLE>()
      .AddRow(1.,     0.)
      .AddRow(1000.,  log(1000.))
      .AddRow(0.,     -inf)
      .AddRow(-1,     nan)
      .Build(), &LnQuiet);
}

TEST(MathExpresionTest, Log10Nulling) {
  TestEvaluation(BlockBuilder<DOUBLE, DOUBLE>()
      .AddRow(1.,     0.)
      .AddRow(1000.,  3.)
      .AddRow(0.,     __)
      .AddRow(-1.,    __)
      .Build(), &Log10Nulling);
}

TEST(MathExpressionTest, Log10Quiet) {
  double inf = 1./0.;
  double nan = inf - inf;
  TestEvaluation(BlockBuilder<DOUBLE, DOUBLE>()
      .AddRow(1.,     0.)
      .AddRow(1000.,  3.)
      .AddRow(0.,     -inf)
      .AddRow(-1,     nan)
      .Build(), &Log10Quiet);
}

TEST(MathExpressionTest, Log2Nulling) {
  TestEvaluation(BlockBuilder<DOUBLE, DOUBLE>()
      .AddRow(1.,     0.)
      .AddRow(1000.,  log2(1000.))
      .AddRow(1024.,  10.)
      .AddRow(0.,     __)
      .AddRow(-1,     __)
      .Build(), &Log2Nulling);
}

TEST(MathExpressionTest, Log2Quiet) {
  double inf = 1./0.;
  double nan = inf - inf;
  TestEvaluation(BlockBuilder<DOUBLE, DOUBLE>()
      .AddRow(1.,     0.)
      .AddRow(1000.,  log2(1000.))
      .AddRow(1024.,  10.)
      .AddRow(0.,     -inf)
      .AddRow(-1,     nan)
      .Build(), &Log2Quiet);
}

TEST(MathExpressionTest, Exp) {
  TestEvaluation(BlockBuilder<DOUBLE, DOUBLE>()
      .AddRow(0.,     1.)
      .AddRow(1000.,  exp(1000.))
      .AddRow(-1,     exp(-1.))
      .Build(), &Exp);
}

TEST(MathExpressionTest, ExpWithIntInputType) {
  TestEvaluation(BlockBuilder<INT32, DOUBLE>()
      .AddRow(-4, exp(-4.))
      .AddRow(4,  exp(4.))
      .Build(), &Exp);
}

TEST(MathExpressionTest, LogNulling) {
  TestEvaluation(BlockBuilder<DOUBLE, INT32, DOUBLE>()
      .AddRow(2.,   4,  2.)
      .AddRow(4.,   4,  1.)
      .AddRow(10.,  2,  log(2.) / log(10.))
      .AddRow(0.,   2,  __)
      .AddRow(2.,   0,  __)
      .AddRow(-1.,  5,  __)
      .AddRow(5.,  -3,  __)
      .AddRow(-8., -8,  __)
      .Build(), &LogNulling);
}

TEST(MathExpressionTest, LogQuiet) {
  double inf = 1./0.;
  double nan = inf - inf;
  TestEvaluation(BlockBuilder<DOUBLE, INT32, DOUBLE>()
      .AddRow(2.,   4,  2.)
      .AddRow(4.,   4,  1.)
      .AddRow(10.,  2,  log(2.) / log(10.))
      // I'm not sure about what I want to be the result here, the -0. is rather
      // implementation driven, I'd probably prefer a NaN - but it's not worth
      // the performance cost of doing a direct check.
      .AddRow(0.,   2,  -0.)
      .AddRow(2.,   0,  -inf)
      .AddRow(-1.,  5,  nan)
      .AddRow(5.,  -3,  nan)
      .AddRow(-8., -8,  nan)
      .Build(), &LogQuiet);
}

TEST(MathExpressionTest, LogInvalidInputType) {
  TestBindingFailure(&Ln, STRING);
}

TEST(MathExpressionTest, PowerSignaling) {
  TestEvaluation(BlockBuilder<DOUBLE, DOUBLE, DOUBLE>()
      .AddRow(1.,   0.,   1.)
      .AddRow(2.,   2.,   4.)
      .AddRow(2.5,  2.,   6.25)
      .AddRow(4.,   0.5,  2.)
      .AddRow(-1.,  2.,   1.)
      .AddRow(0.,   0.,   1.)
      .AddRow(0.,   0.5,  0.)
      .AddRow(6.25, 0.5,  2.5)
      .AddRow(0.5,  -1,   2)
      .AddRow(-1.,  -1.,  -1.)
      .AddRow(3.,   -0.3, pow(3, -0.3))
      .Build(), &PowerSignaling);

  TestEvaluationFailure(BlockBuilder<DOUBLE, DOUBLE>()
      .AddRow(-1., 0.5)
      .AddRow(-1,  -0.5)
      .Build(), &PowerSignaling);
}

TEST(MathExpressionTest, PowerNulling) {
  TestEvaluation(BlockBuilder<DOUBLE, DOUBLE, DOUBLE>()
      .AddRow(1.,   0.,   1.)
      .AddRow(2.,   2.,   4.)
      .AddRow(2.5,  2.,   6.25)
      .AddRow(4.,   0.5,  2.)
      .AddRow(-1.,  2.,   1.)
      .AddRow(0.,   0.,   1.)
      .AddRow(0.,   0.5,  0.)
      .AddRow(6.25, 0.5,  2.5)
      .AddRow(0.5,  -1,   2)
      .AddRow(-1.,  -1.,  -1.)
      .AddRow(3,    -0.3, pow(3, -0.3))
      .AddRow(-1.,  0.5,  __)
      .AddRow(-1,   -0.5, __)
      .Build(), &PowerNulling);
}

TEST(MathExpressionTest, PowerQuiet) {
  double nan = numeric_limits<double>::quiet_NaN();

  TestEvaluation(BlockBuilder<DOUBLE, DOUBLE, DOUBLE>()
      .AddRow(1.,   0.,   1.)
      .AddRow(2.,   2.,   4.)
      .AddRow(2.5,  2.,   6.25)
      .AddRow(4.,   0.5,  2.)
      .AddRow(-1.,  2.,   1.)
      .AddRow(0.,   0.,   1.)
      .AddRow(0.,   0.5,  0.)
      .AddRow(6.25, 0.5,  2.5)
      .AddRow(0.5,  -1,   2)
      .AddRow(-1.,  -1.,  -1.)
      .AddRow(3,    -0.3, pow(3, -0.3))
      .AddRow(-1.,  0.5,  nan)
      .AddRow(-1,   -0.5, nan)
      .Build(), &PowerQuiet);
}

// Note that one has to be rather careful when selecting values for testing
// trigonometric functions, as the implementation is hardly exact. The values
// tested here are either exact (that is, sin(0) is _exactly_ 0), or values
// where the derivative is equal to zero (thus the error introduced by rounding
// disappears) easily. One may note that for instance (cos(acos(0)) == 0) fails.
TEST(MathExpressionTest, Sin) {
  TestEvaluation(BlockBuilder<DOUBLE, DOUBLE>()
      .AddRow(0.,           0.)
      .AddRow(acos(0),      1.)
      .AddRow(1,            sin(1))
      .Build(), &Sin);
}

TEST(MathExpressionTest, Cos) {
  TestEvaluation(BlockBuilder<DOUBLE, DOUBLE>()
      .AddRow(0.,       1.)
      .AddRow(acos(-1), -1.)
      .AddRow(1,        cos(1))
      .Build(), &Cos);
}

TEST(MathExpressionTest, Tan) {
  TestEvaluation(BlockBuilder<DOUBLE, DOUBLE>()
      .AddRow(0.,      0.)
      .AddRow(123.,    tan(123))
      .AddRow(1,       tan(1))
      .Build(), &Tan);
}

TEST(MathExpressionTest, Cot) {
  TestEvaluation(BlockBuilder<DOUBLE, DOUBLE>()
      .AddRow(1.,   1. / tan(1.))
      .AddRow(2.,   1. / tan(2.))
      .AddRow(3.14, 1. / tan(3.14))
      .Build(), &Cot);
}

TEST(MathExpressionTest, Asin) {
  TestEvaluation(BlockBuilder<DOUBLE, DOUBLE>()
      .AddRow(0.5,   asin(0.5))
      .AddRow(-0.5,  asin(-0.5))
      .AddRow(0.14,  asin(0.14))
      .Build(), &Asin);
}

TEST(MathExpressionTest, Acos) {
  TestEvaluation(BlockBuilder<DOUBLE, DOUBLE>()
      .AddRow(0.5,   acos(0.5))
      .AddRow(-0.5,   acos(-0.5))
      .AddRow(0.14,   acos(0.14))
      .Build(), &Acos);
}

TEST(MathExpressionTest, Atan) {
  TestEvaluation(BlockBuilder<DOUBLE, DOUBLE>()
      .AddRow(1.,   atan(1.))
      .AddRow(2.,   atan(2.))
      .AddRow(3.14, atan(3.14))
      .Build(), &Atan);
}

TEST(MathExpressionTest, Atan2) {
  TestEvaluation(BlockBuilder<DOUBLE, DOUBLE, DOUBLE>()
      .AddRow(1.,   1.,  atan2(1., 1.))
      .AddRow(2.,   0.,  atan2(2., 0.))
      .AddRow(3.14, 0.,  atan2(3.14, 0.))
      .Build(), &Atan2);
}

TEST(MathExpressionTest, Sinh) {
  TestEvaluation(BlockBuilder<DOUBLE, DOUBLE>()
      .AddRow(0.,           sinh(0.))
      .AddRow(1.3,          sinh(1.3))
      .AddRow(2.1,          sinh(2.1))
      .Build(), &Sinh);
}

TEST(MathExpressionTest, Cosh) {
  TestEvaluation(BlockBuilder<DOUBLE, DOUBLE>()
      .AddRow(0.,       cosh(0.))
      .AddRow(1.,       cosh(1.))
      .AddRow(2.,       cosh(2.))
      .Build(), &Cosh);
}

TEST(MathExpressionTest, Tanh) {
  TestEvaluation(BlockBuilder<DOUBLE, DOUBLE>()
      .AddRow(0.,      tanh(0.))
      .AddRow(123.,    tanh(123))
      .AddRow(1,       tanh(1))
      .Build(), &Tanh);
}

TEST(MathExpressionTest, Asinh) {
  TestEvaluation(BlockBuilder<DOUBLE, DOUBLE>()
      .AddRow(0.5,   asinh(0.5))
      .AddRow(-0.5,  asinh(-0.5))
      .AddRow(0.14,  asinh(0.14))
      .Build(), &Asinh);
}

TEST(MathExpressionTest, Acosh) {
  TestEvaluation(BlockBuilder<DOUBLE, DOUBLE>()
      .AddRow(0.5,   acosh(0.5))
      .AddRow(-0.5,   acosh(-0.5))
      .AddRow(0.14,   acosh(0.14))
      .Build(), &Acosh);
}

TEST(MathExpressionTest, Atanh) {
  TestEvaluation(BlockBuilder<DOUBLE, DOUBLE>()
      .AddRow(1.,   atanh(1.))
      .AddRow(2.,   atanh(2.))
      .AddRow(3.14, atanh(3.14))
      .Build(), &Atanh);
}

TEST(MathExpressionTest, ToDegrees) {
  TestEvaluation(BlockBuilder<DOUBLE, DOUBLE>()
      .AddRow(0.,        0.)
      .AddRow(M_PI,      180.)
      .AddRow(M_PI / 2., 90)
      .Build(), &ToDegrees);
}

TEST(MathExpressionTest, ToRadians) {
  TestEvaluation(BlockBuilder<DOUBLE, DOUBLE>()
      .AddRow(0.,        0.)
      .AddRow(180.,      M_PI)
      .AddRow(90.,       M_PI / 2.)
      .Build(), &ToRadians);
}

TEST(MathExpressionTest, Pi) {
  TestEvaluation(BlockBuilder<DOUBLE>()
      .AddRow(2. * acos(0))
      .Build(), &Pi);
}

TEST(MathExpressionTest, Format) {
  TestEvaluation(BlockBuilder<DOUBLE, INT32, STRING>()
      .AddRow(4.,       2,  "4.00")
      .AddRow(10.,      2,  "10.00")
      .AddRow(9.9999,   1,  "10.0")
      .AddRow(9.949,    1,  "9.9")
      .AddRow(0.5,      0,  "0")
      .AddRow(0.50001,  0,  "1")
      .AddRow(0.05,     1,  "0.1")
      .AddRow(-0.5,     0,  "-0")
      .AddRow(-0.001,   0,  "-0")
      .AddRow(-1.001,   1,  "-1.0")
      .Build(), &Format);
}

TEST(MathExpressionTest, FormatSmall) {
  TestEvaluation(BlockBuilder<DOUBLE, INT32, STRING>()
      .AddRow(0.,       0, "0")
      .AddRow(0.0001,   0, "0")
      .AddRow(0.53,     0, "1")
      .AddRow(0.,       3, "0.000")
      .AddRow(-0.53,    3, "-0.530")
      .Build(), &Format);
}

TEST(MathExpressionTest, FormatLarge) {
  // 2^170, a double should represent this exactly.
  double large_power = 1496577676626844588240573268701473812127674924007424.;
  char large[55] = "1496577676626844588240573268701473812127674924007424";
  char zeroes[404] = "0.";
  for (int i = 2; i < 402; ++i) zeroes[i] = '0';
  zeroes[402] = '\0';

  TestEvaluation(BlockBuilder<DOUBLE, INT32, STRING>()
      .AddRow(0.,          400, zeroes)
      .AddRow(large_power, 0,   large)
      .Build(), &Format);
}

TEST(MathExpressionTest, IsFinite) {
  float inf = numeric_limits<float>::infinity();
  float nan = numeric_limits<float>::quiet_NaN();
  TestEvaluation(BlockBuilder<FLOAT, BOOL>()
      .AddRow(0.,     true)
      .AddRow(1234.,  true)
      .AddRow(inf,    false)
      .AddRow(nan,    false)
      .AddRow(__,     __)
      .Build(), &IsFinite);
}

TEST(MathExpressionTest, IsInf) {
  float inf = numeric_limits<float>::infinity();
  float nan = numeric_limits<float>::quiet_NaN();
  TestEvaluation(BlockBuilder<DOUBLE, BOOL>()
      .AddRow(0.,     false)
      .AddRow(1234.,  false)
      .AddRow(inf,    true)
      .AddRow(nan,    false)
      .AddRow(__,     __)
      .Build(), &IsInf);
}

TEST(MathExpressionTest, IsNaN) {
  double inf = numeric_limits<double>::infinity();
  double nan = numeric_limits<double>::quiet_NaN();
  TestEvaluation(BlockBuilder<FLOAT, BOOL>()
      .AddRow(0.,     false)
      .AddRow(1234.,  false)
      .AddRow(inf,    false)
      .AddRow(nan,    true)
      .AddRow(__,     __)
      .Build(), &IsNaN);
}

TEST(MathExpressionTest, IsNormal) {
  double inf = numeric_limits<double>::infinity();
  double nan = numeric_limits<double>::quiet_NaN();
  TestEvaluation(BlockBuilder<DOUBLE, BOOL>()
      .AddRow(0.,     false)
      .AddRow(1234.,  true)
      .AddRow(inf,    false)
      .AddRow(nan,    false)
      .AddRow(__,     __)
      .Build(), &IsNormal);
}

}  // namespace supersonic
