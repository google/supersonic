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
//
// Evaluators for expressions defined in math_expressions.h.

#ifndef SUPERSONIC_EXPRESSION_CORE_MATH_EVALUATORS_H_
#define SUPERSONIC_EXPRESSION_CORE_MATH_EVALUATORS_H_

#include <math.h>
#include <stdio.h>

#include <algorithm>
#include "supersonic/utils/std_namespace.h"

#include "supersonic/utils/mathlimits.h"

#include "supersonic/base/memory/arena.h"

namespace supersonic {
namespace operators {

// TODO(onufry): There is no signal if the Arena allocation fails. We should
// probably pass a boolean flag, set it on failure, and then return a false
// from the vector primitive or evaluation routine if the flag is set (and
// obviously return from this function when we set the flag).
struct Format {
  StringPiece operator()(double number, int32 precision, Arena* arena) {
    double number_copy = number;
    // We need to precalculate the size of the resulting string, to allocate
    // the right amount of memory. This is:
    //   - the presicion digits after the decimal point
    //   - a single digit for the decimal point itself, unless precision == 0
    //   - a single digit for the minus, if the number is negative
    //   - and ceil(log10(number)) digits before the decimal point.
    //
    // All the calculations are done by hand and arithmetically, it's quicker
    // than doing it with inbuilt functions.
    //
    // Note - format sometimes allocates an extra unnecessary byte,
    // TODO(onufry): Fix this when the Arena allows to decrease the number
    // of bytes allocated at the last call (cutting of the array).

    // snprintf seems to work with negative arguments, but just in case we'll
    // remove the problem (the cost of the max disappears in the cost of the
    // calculations anyway).
    precision = std::max(precision, 0);
    size_t length = precision + 1 + (number_copy < 0.) - (precision == 0);
    // equivalent to: number_copy = fabsl(number_copy), but arithmetic instead
    // of branching.
    number_copy *= (1 - 2 * (number_copy < 0.));
    // This is an equivalent of a base 10 integer logarithm.
    do {
      length += 1;
      number_copy /= 10.;
    } while (number_copy >= 1.);
    // There is the danger of 9.9 being rounded to 10.0 and gaining one byte.
    length += (number_copy > 0.89);
    // We allocate one byte more than necessary, for null-termination, although
    // we don't actually need the null, to avoid printf writing it on
    // unassigned memory
    char* new_str = static_cast<char *>(arena->AllocateBytes(length+1));
    CHECK_NOTNULL(new_str);
    size_t new_length =
        snprintf(new_str, length + 1, "%.*lf", precision, number);
    CHECK(new_length < length + 1) << "Format length calculation error";
    return StringPiece(new_str, new_length);
  }
};

struct Ceil {
  double operator()(double arg) { return ceil(arg); }
};

struct CeilToInt {
  int64 operator()(double arg) { return static_cast<int64>(ceil(arg)); }
  int64 operator()(float arg) { return static_cast<int64>(ceil(arg)); }
};

struct Exp {
  double operator()(double arg) { return exp(arg); }
};

struct Floor {
  double operator()(double arg) { return floor(arg); }
};

struct FloorToInt {
  int64 operator()(double arg) { return static_cast<int64>(floor(arg)); }
  int64 operator()(float arg) { return static_cast<int64>(floor(arg)); }
};

struct Round {
  double operator()(double arg) { return round(arg); }
  float operator()(float arg) { return roundf(arg); }
};

// The native RoundToInt is not used because of b/518396.
// TODO(ptab): Reenable it (in BoundRoundToInt) when the b/518396 is fixed.
struct RoundToInt {
  int64 operator()(float arg) { return llroundf(arg); }
  int64 operator()(double arg) { return llround(arg); }
};

struct RoundWithMultiplier {
  double operator()(double arg, double multiplier) {
    return round(arg * multiplier) / multiplier;
  }
};

struct Trunc {
  double operator()(double arg) { return trunc(arg); }
};

struct Abs {
  uint32 operator()(int32 arg) { return (arg < 0) ? -arg : arg; }
  uint64 operator()(int64 arg) { return (arg < 0) ? -arg : arg; }
  float  operator()(float arg) { return (arg < 0) ? -arg : arg; }
  double operator()(double arg) { return (arg < 0) ? -arg : arg; }
};

struct Ln {
  double operator()(double arg) { return log(arg); }
};

struct Log10 {
  double operator()(double arg) { return log10(arg); }
};

struct Log2 {
  double operator()(double arg) { return log2(arg); }
};

struct Sqrt {
  double operator()(double arg) { return sqrt(arg); }
};

struct Sin {
  double operator()(double arg) { return sin(arg); }
};

struct Cos {
  double operator()(double arg) { return cos(arg); }
};

struct Tan {
  double operator()(double arg) { return tan(arg); }
};

struct Asin {
  double operator()(double arg) { return asin(arg); }
};

struct Acos {
  double operator()(double arg) { return acos(arg); }
};

struct Atan {
  double operator()(double arg) { return atan(arg); }
};

struct Atan2 {
  double operator()(double x, double y) { return atan2(x, y); }
};

struct Sinh {
  double operator()(double arg) { return sinh(arg); }
};

struct Cosh {
  double operator()(double arg) { return cosh(arg); }
};

struct Tanh {
  double operator()(double arg) { return tanh(arg); }
};

struct Asinh {
  double operator()(double arg) { return asinh(arg); }
};

struct Acosh {
  double operator()(double arg) { return acosh(arg); }
};

struct Atanh {
  double operator()(double arg) { return atanh(arg); }
};

struct Pow {
  double operator()(double arg1, double arg2) { return pow(arg1, arg2); }
};

struct IsFinite {
  bool operator()(double arg) { return std::isfinite(arg); }
};

struct IsNaN {
  bool operator()(double arg) { return std::isnan(arg); }
};

struct IsInf {
  bool operator()(double arg) { return std::isinf(arg); }
};

struct IsNormal {
  bool operator()(double arg) { return std::isnormal(arg); }
};

}  // namespace operators
}  // namespace supersonic

#endif  // SUPERSONIC_EXPRESSION_CORE_MATH_EVALUATORS_H_
