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

#include "supersonic/expression/core/math_expressions.h"

#include <math.h>

#include "supersonic/expression/core/math_bound_expressions.h"
#include "supersonic/expression/core/math_evaluators.h"  // IWYU pragma: keep
#include "supersonic/expression/infrastructure/basic_expressions.h"
#include "supersonic/expression/infrastructure/terminal_expressions.h"

namespace supersonic {

// Exponent and logarithm.
class Expression;

const Expression* Exp(const Expression* const argument) {
  return CreateExpressionForExistingBoundFactory(
      argument, &BoundExp, "EXP($0)");
}

const Expression* LnNulling(const Expression* const argument) {
  return CreateExpressionForExistingBoundFactory(
      argument, &BoundLnNulling, "LN($0)");
}

const Expression* LnQuiet(const Expression* const argument) {
  return CreateExpressionForExistingBoundFactory(
      argument, &BoundLnQuiet, "LN($0)");
}

const Expression* Log10Nulling(const Expression* const argument) {
  return CreateExpressionForExistingBoundFactory(
      argument, &BoundLog10Nulling, "LOG10($0)");
}

const Expression* Log10Quiet(const Expression* const argument) {
  return CreateExpressionForExistingBoundFactory(
      argument, &BoundLog10Quiet, "LOG10($0)");
}

const Expression* Log2Nulling(const Expression* const argument) {
  return CreateExpressionForExistingBoundFactory(
      argument, &BoundLog2Nulling, "LOG2($0)");
}

const Expression* Log2Quiet(const Expression* const argument) {
  return CreateExpressionForExistingBoundFactory(
      argument, &BoundLog2Quiet, "LOG2($0)");
}

const Expression* LogNulling(const Expression* base,
                             const Expression* argument) {
  return CreateExpressionForExistingBoundFactory(
      base, argument, &BoundLogNulling, "LOG($0, $1)");
}

const Expression* LogQuiet(const Expression* base,
                           const Expression* argument) {
  return CreateExpressionForExistingBoundFactory(
      base, argument, &BoundLogQuiet, "LOG($0, $1)");
}

// Absolute value.
const Expression* Abs(const Expression* const argument) {
  return CreateExpressionForExistingBoundFactory(
      argument, &BoundAbs, "ABS($0)");
}

// Various rounding functions.
const Expression* Floor(const Expression* const argument) {
  return CreateExpressionForExistingBoundFactory(
      argument, &BoundFloor, "FLOOR($0)");
}

const Expression* Ceil(const Expression* const argument) {
  return CreateExpressionForExistingBoundFactory(
      argument, &BoundCeil, "CEIL($0)");
}

const Expression* CeilToInt(const Expression* const argument) {
  return CreateExpressionForExistingBoundFactory(
      argument, &BoundCeilToInt, "CEIL_TO_INT($0)");
}

const Expression* FloorToInt(const Expression* const argument) {
  return CreateExpressionForExistingBoundFactory(
      argument, &BoundFloorToInt, "FLOOR_TO_INT($0)");
}

const Expression* Trunc(const Expression* const argument) {
  return CreateExpressionForExistingBoundFactory(
      argument, &BoundTrunc, "TRUNC($0)");
}

const Expression* Round(const Expression* const argument) {
  return CreateExpressionForExistingBoundFactory(
      argument, &BoundRound, "ROUND($0)");
}

const Expression* RoundToInt(const Expression* const argument) {
  return CreateExpressionForExistingBoundFactory(
      argument, &BoundRoundToInt, "ROUND_TO_INT($0)");
}

const Expression* RoundWithPrecision(const Expression* const argument,
                                     const Expression* const precision) {
  return CreateExpressionForExistingBoundFactory(
      argument, precision, &BoundRoundWithPrecision,
      "ROUND_WITH_PRECISION($0, $1)");
}

// Square root.
const Expression* SqrtSignaling(const Expression* const argument) {
  return CreateExpressionForExistingBoundFactory(
      argument, &BoundSqrtSignaling, "SQRT($0)");
}

const Expression* SqrtNulling(const Expression* const argument) {
  return CreateExpressionForExistingBoundFactory(
      argument, &BoundSqrtNulling, "SQRT($0)");
}

const Expression* SqrtQuiet(const Expression* const argument) {
  return CreateExpressionForExistingBoundFactory(
      argument, &BoundSqrtQuiet, "SQRT($0)");
}

// Power.
const Expression* PowerSignaling(const Expression* const base,
                                 const Expression* const exponent) {
  return CreateExpressionForExistingBoundFactory(
      base, exponent, &BoundPowerSignaling, "POW($0, $1)");
}

const Expression* PowerNulling(const Expression* const base,
                               const Expression* const exponent) {
  return CreateExpressionForExistingBoundFactory(
      base, exponent, &BoundPowerNulling, "POW($0, $1)");
}

const Expression* PowerQuiet(const Expression* const base,
                             const Expression* const exponent) {
  return CreateExpressionForExistingBoundFactory(
      base, exponent, &BoundPowerQuiet, "POW($0, $1)");
}

// Trigonometry.
const Expression* Sin(const Expression* const radians) {
  return CreateExpressionForExistingBoundFactory(
      radians, &BoundSin, "SIN($0)");
}

const Expression* Cos(const Expression* const radians) {
  return CreateExpressionForExistingBoundFactory(
      radians, &BoundCos, "COS($0)");
}

const Expression* Tan(const Expression* const radians) {
  return CreateExpressionForExistingBoundFactory(
      radians, &BoundTanQuiet, "TAN($0)");
}

const Expression* Cot(const Expression* const radians) {
  return CreateExpressionForExistingBoundFactory(
      radians, &BoundCot, "COT($0)");
}

const Expression* Asin(const Expression* const argument) {
  return CreateExpressionForExistingBoundFactory(
      argument, &BoundAsin, "ASIN($0)");
}

const Expression* Acos(const Expression* const argument) {
  return CreateExpressionForExistingBoundFactory(
      argument, &BoundAcos, "ACOS($0)");
}

const Expression* Atan(const Expression* const argument) {
  return CreateExpressionForExistingBoundFactory(
      argument, &BoundAtan, "ATAN($0)");
}

const Expression* Atan2(const Expression* const x,
                        const Expression* const y) {
  return CreateExpressionForExistingBoundFactory(
      x, y, &BoundAtan2, "ATAN2($0, $1)");
}

const Expression* Sinh(const Expression* const argument) {
  return CreateExpressionForExistingBoundFactory(
      argument, &BoundSinh, "SINH($0)");
}

const Expression* Cosh(const Expression* const argument) {
  return CreateExpressionForExistingBoundFactory(
      argument, &BoundCosh, "COSH($0)");
}

const Expression* Tanh(const Expression* const argument) {
  return CreateExpressionForExistingBoundFactory(
      argument, &BoundTanh, "TANH($0)");
}

const Expression* Asinh(const Expression* const argument) {
  return CreateExpressionForExistingBoundFactory(
      argument, &BoundAsinh, "ASINH($0)");
}

const Expression* Acosh(const Expression* const argument) {
  return CreateExpressionForExistingBoundFactory(
      argument, &BoundAcosh, "ACOSH($0)");
}

const Expression* Atanh(const Expression* const argument) {
  return CreateExpressionForExistingBoundFactory(
      argument, &BoundAtanh, "ATANH($0)");
}

const Expression* ToDegrees(const Expression* const radians) {
  return CreateExpressionForExistingBoundFactory(
      radians, &BoundToDegrees, "DEGREES($0)");
}

const Expression* ToRadians(const Expression* const degrees) {
  return CreateExpressionForExistingBoundFactory(
      degrees, &BoundToRadians, "RADIANS($0)");
}

const Expression* Pi() {
  return ConstDouble(M_PI);
}

// IEEE checks.
const Expression* IsFinite(const Expression* number) {
  return CreateExpressionForExistingBoundFactory(
      number, &BoundIsFinite, "IS_FINITE($0)");
}

const Expression* IsInf(const Expression* number) {
  return CreateExpressionForExistingBoundFactory(
      number, &BoundIsInf, "IS_INF($0)");
}

const Expression* IsNaN(const Expression* number) {
  return CreateExpressionForExistingBoundFactory(
      number, &BoundIsNaN, "IS_NAN($0)");
}

const Expression* IsNormal(const Expression* number) {
  return CreateExpressionForExistingBoundFactory(
      number, &BoundIsNormal, "IS_NORMAL($0)");
}

// Other.
const Expression* Format(const Expression* number,
                         const Expression* precision) {
  return CreateExpressionForExistingBoundFactory(
      number, precision, &BoundFormatSignaling, "FORMAT($0, $1)");
}

// Deprecated.
const Expression* Sqrt(const Expression* const e) {
  return SqrtQuiet(e);
}

const Expression* Ln(const Expression* const e) {
  return LnNulling(e);
}

const Expression* Log10(const Expression* const e) {
  return Log10Nulling(e);
}

const Expression* Log(const Expression* base, const Expression* argument) {
  return LogNulling(base, argument);
}

}  // namespace supersonic
