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
//
// Mathematical functions.

#ifndef SUPERSONIC_EXPRESSION_CORE_MATH_EXPRESSIONS_H_
#define SUPERSONIC_EXPRESSION_CORE_MATH_EXPRESSIONS_H_

namespace supersonic {

// Selected <math> functions.
//
// The *Signaling, *Nulling and *Quiet versions differ in their treatment
// of improper arguments (for instance of negative inputs to Sqrt or Log).
// The Signaling versions stop the evaluation with an error. The Nulling
// versions take the result to be NULL. The Quiet versions do what the
// underlying C++ functions do (so usually return a NaN, and generally behave
// according to IEE 754). If you do not care, as you trust your input data to
// be correct, choose the Quiet version, as it is the most efficient (the
// Nulling version tends to be least efficient).

// Exponent (e to the power argument).
class Expression;

const Expression* Exp(const Expression* const argument);
// TODO(onufry): add also a signaling version for the logarithms. If anybody
// needs those, please contact the Supersonic team, we'll add them ASAP.
// Various versions of logarithms - natural, base 10, base 2, and arbitrary
// base. The specialized versions are quicker and preferred to the arbitrary
// base one.
const Expression* LnNulling(const Expression* const argument);
const Expression* LnQuiet(const Expression* const argument);
const Expression* Log10Nulling(const Expression* const argument);
const Expression* Log10Quiet(const Expression* const argument);
const Expression* Log2Nulling(const Expression* const argument);
const Expression* Log2Quiet(const Expression* const argument);
const Expression* LogNulling(const Expression* base,
                             const Expression* argument);
const Expression* LogQuiet(const Expression* base,
                           const Expression* argument);

// Trigonometry.
const Expression* Sin(const Expression* const radians);
const Expression* Cos(const Expression* const radians);
const Expression* Tan(const Expression* const radians);
const Expression* Cot(const Expression* const radians);
// Arc trigonometry.
const Expression* Asin(const Expression* const argument);
const Expression* Acos(const Expression* const argument);
const Expression* Atan(const Expression* const argument);
const Expression* Atan2(const Expression* const x,
                        const Expression* const y);
// Hyperbolic trigonometry.
const Expression* Sinh(const Expression* const argument);
const Expression* Cosh(const Expression* const argument);
const Expression* Tanh(const Expression* const argument);
// Hyperbolic arc trigonometry.
const Expression* Asinh(const Expression* const argument);
const Expression* Acosh(const Expression* const argument);
const Expression* Atanh(const Expression* const argument);
// Various others trigonometry-related.
const Expression* ToDegrees(const Expression* const radians);  // From radians.
const Expression* ToRadians(const Expression* const degrees);  // From degrees.
const Expression* Pi();

// Absolute value.
const Expression* Abs(const Expression* const argument);
// The result type is equal to the input type.
const Expression* Round(const Expression* argument);
const Expression* Ceil(const Expression* const argument);
const Expression* Floor(const Expression* const argument);
const Expression* Trunc(const Expression* const argument);
// The result type is integer.
const Expression* RoundToInt(const Expression* argument);
const Expression* CeilToInt(const Expression* argument);
const Expression* FloorToInt(const Expression* argument);
// The result type is always double. The precision has to be an integer. Rounds
// up to precision decimal places. If precision is negative, the argument gets
// rounded to the nearest multiple of 1E-precision.
const Expression* RoundWithPrecision(const Expression* argument,
                                     const Expression* precision);
// Square root. For the semantics of Signaling, Nulling and Quiet see notes at
// the beginning of the document.
const Expression* SqrtSignaling(const Expression* const argument);
const Expression* SqrtNulling(const Expression* const argument);
const Expression* SqrtQuiet(const Expression* const argument);

// Three versions of the Power function (base raised to the power exponent).
// The three versions differ in their treatment of incorrect arguments (to
// be more specific - their treatment of the case where the base is negative
// and the exponent is not an integer). See the comments at the beginning of
// the file for the descriptions of the policies.
// Note that all the versions assume that zero to the power zero is equal to
// one, and zero to negative powers is equal to infinity.
const Expression* PowerSignaling(const Expression* const base,
                                 const Expression* const exponent);
const Expression* PowerNulling(const Expression* const base,
                               const Expression* const exponent);
const Expression* PowerQuiet(const Expression* const base,
                             const Expression* const exponent);


const Expression* Format(const Expression* const number,
                         const Expression* const precision);

// Create bool expressions that compute the respective C99 classifications
// of floating point numbers.  The result is NULL if the argument is NULL.
// See http://www.opengroup.org/onlinepubs/000095399/functions/isfinite.html
// and, and it's SEE ALSO section.
// For integers is equivalent to the IsNull operator, but less effective.
const Expression* IsFinite(const Expression* number);
const Expression* IsNormal(const Expression* number);
const Expression* IsNaN(const Expression* number);
const Expression* IsInf(const Expression* number);

// Creates an expression of type DOUBLE that will, upon evaluation against
// a cursor, return a uniformly distributed pseudo random number in [0, 1].
const Expression* RandomDouble();  // Not implemented.

// DEPRECATED, use the policy-conscious versions.
// instead.
const Expression* Ln(const Expression* const e);
const Expression* Log10(const Expression* const e);
const Expression* Log(const Expression* base, const Expression* argument);
// DEPRECATED, use the policy-conscious version.
const Expression* Sqrt(const Expression* const e);

}  // namespace supersonic

#endif  // SUPERSONIC_EXPRESSION_CORE_MATH_EXPRESSIONS_H_
