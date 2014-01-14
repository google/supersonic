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
//
// Expression traits for expressions.
// Contains information about a particular expression type.

#ifndef SUPERSONIC_EXPRESSION_VECTOR_EXPRESSION_TRAITS_H_
#define SUPERSONIC_EXPRESSION_VECTOR_EXPRESSION_TRAITS_H_

#include <string>
namespace supersonic {using std::string; }
#include <vector>
using std::vector;

#include "supersonic/utils/stringprintf.h"

#include "supersonic/expression/vector/column_validity_checkers.h"
#include "supersonic/expression/vector/expression_evaluators.h"
#include "supersonic/expression/proto/operators.pb.h"
#include "supersonic/base/infrastructure/operators.h"
#include "supersonic/expression/vector/simd_operators.h"
#include "supersonic/base/infrastructure/types.h"

namespace supersonic {
// Note: for usage with AbstractExpressions the following fields should be
// defined:
// - typedef basic_operator (a struct with the () operator overloaded)
// - name (a static function returning a string)
// - a FormatDescription(...) method, taking as many arguments, as there are
//   arguments to the expression.
// - a FormatBoundDescription(...) method, taking for every arguments string
//   describing it and type of it. It can be added by template.
// - output_type (a static DataType)
// - supports_promotions (boolean, default false. Set to true if numeric
//   promotions are desirable (usually the case with numeric inputs)).
// - can_fail (boolean, default false. Set to true if the function can fail,
//   i.e., return an error due to improper arguments)
// - can_return_null, similar to the above, only concerned with NULL returns.
//   Note that a NULL input is always assumed to result in a null output
//   (except for IF_NULL), and does not have to be specified here.
// Note: only one of can_fail and can_return_null should be specified, this
// choice basically settles the behaviour of the function when unexpected input
// is encountered.

// -------------------------- Forward declarations -----------------
//
// Forward declarations of evaluators. Evaluators themselves are defined in
// appropriate *_evaluators.h files, along with the appropriate expressions.

namespace operators {
// String expression evaluators, defined in core/string_evaluators.h.
  struct Length;
  struct Ltrim;
  struct Rtrim;
  struct ToUpper;
  struct ToLower;
  struct Trim;
  struct RegexpPartial;
  struct RegexpFull;
  struct SubstringBinary;
  struct SubstringTernary;
  struct StringOffset;
  struct StringReplaceEvaluator;

// Datetime expression evaluators, defined in core/date_evaluators.h.
  struct UnixTimestamp;
  struct FromUnixTime;
  struct Year;
  struct YearLocal;
  struct Quarter;
  struct QuarterLocal;
  struct Month;
  struct MonthLocal;
  struct Day;
  struct DayLocal;
  struct Weekday;
  struct WeekdayLocal;
  struct YearDay;
  struct YearDayLocal;
  struct Hour;
  struct HourLocal;
  struct Minute;
  struct MinuteLocal;
  struct Second;
  struct Microsecond;
  struct DateFormat;
  struct DateFormatLocal;
  struct MakeDate;
  struct AddMinute;
  struct AddMinutes;
  struct AddDay;
  struct AddDays;
  struct AddMonth;
  struct AddMonths;

// Math expression evaluators, defined in core/math_evaluators.h.
  struct Format;
  struct Ceil;
  struct CeilToInt;
  struct Exp;
  struct Floor;
  struct FloorToInt;
  struct Round;
  struct RoundToInt;
  struct RoundWithMultiplier;
  struct Trunc;
  struct Abs;
  struct Ln;
  struct Log10;
  struct Log2;
  struct Sqrt;
  struct Sin;
  struct Cos;
  struct Tan;
  struct Asin;
  struct Acos;
  struct Atan;
  struct Atan2;
  struct Sinh;
  struct Cosh;
  struct Tanh;
  struct Asinh;
  struct Acosh;
  struct Atanh;
  struct Pow;
  struct IsFinite;
  struct IsNaN;
  struct IsInf;
  struct IsNormal;

// Hashing evaluators, defined in ext/hashing/hashing_evaluators.h.
  struct HashEvaluator;
  struct FingerprintEvaluator;
}  // namespace operators

namespace failers {
// Specific failers for datetime functions
  struct MakeDateFailer;
}  // namespace failers

// -------------------------- Unary expression traits --------------

// Standard unary expression, with defaults as above. Needs only
// basic_operator, name and output_type definitions.
struct StandardUnaryExpression {
  static const bool supports_promotions = false;
  static const bool can_fail = false;
  static const bool can_return_null = false;
  // Whether the operator can actually crash. For operators that do not
  // crash, but simply return garbage on garbage input, it will frequently
  // be quicker to evaluate anyway, instead of checking.
  static const bool is_safe = true;
  static const bool needs_allocator = false;
};

// A unary expression that can fail. Needs additionally is_invalid_argument.
struct FailingUnaryExpression {
  static const bool supports_promotions = false;
  static const bool needs_allocator = false;
  static const bool can_fail = true;
  static const bool can_return_null = false;
};

// A unary expression that can fail and supports promotion. Needs as above.
struct FailingPromotingUnaryExpression {
  static const bool supports_promotions = true;
  static const bool needs_allocator = false;
  static const bool can_fail = true;
  static const bool can_return_null = false;
};

// A unary expression that does not fail nor null, but supports numeric
// promotions. Needs as StandardUnaryExpression.
struct PromotingUnaryExpression {
  static const bool supports_promotions = true;
  static const bool needs_allocator = false;
  static const bool can_fail = false;
  static const bool can_return_null = false;
  static const bool is_safe = true;
};

// A unary expression that never fails, but can return a null for non-null
// inputs.
struct NullingUnaryExpression {
  static const bool supports_promotions = false;
  static const bool needs_allocator = false;
  static const bool can_fail = false;
  static const bool can_return_null = true;
};

// A unary expression that never fails, but can return a null for non-null
// inputs. It also supports promotion.
struct NullingPromotingUnaryExpression {
  static const bool supports_promotions = true;
  static const bool needs_allocator = false;
  static const bool can_fail = false;
  static const bool can_return_null = true;
};

// A unary expression that needs to allocate additional space in the arena
// for its results. We assume it doesn't fail or return null for non-null
// arguments, and is not safe (see comment below).
struct AllocatingUnaryExpression {
  static const bool supports_promotions = false;
  static const bool can_fail = false;
  static const bool can_return_null = false;
  static const bool is_safe = false;
  static const bool needs_allocator = true;
};

// A simple (non-promoting, non-failing, non-nulling) expression that is not
// safe. An expression is safe if we can run it on any input (even on
// non-initialized data like StringPieces), and it is guaranteed not to crash
// or loop.
struct UnsafeUnaryExpression {
  static const bool supports_promotions = false;
  static const bool can_fail = false;
  static const bool can_return_null = false;
  static const bool needs_allocator = false;
  static const bool is_safe = false;
  static const int selectivity_threshold = 100;
};

template<OperatorId op> struct BaseUnaryExpressionTraits
    : public StandardUnaryExpression {
  static const OperatorId type = op;
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_NEGATE>
    : public StandardUnaryExpression {
  typedef operators::Negate basic_operator;
  static const int selectivity_threshold = 0;
  static const string name() { return string("NEGATE"); }
  static const string FormatDescription(const string& child) {
    return StrCat("(-", child, ")");
  }
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_CAST_QUIET>
    : public StandardUnaryExpression {
  typedef operators::Cast basic_operator;
  static const int selectivity_threshold = 0;
  static const string name() { return string("CAST"); }
  static const string FormatDescription(const string& child) {
    return StringPrintf("CAST(%s)", child.c_str());
  }
  static const string FormatDescription(const string& child, DataType to_type) {
      return StringPrintf("CAST<%s>(%s)",
                          GetTypeInfo(to_type).name().c_str(),
                          child.c_str());
  }
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_PARSE_STRING_QUIET>
    : public UnsafeUnaryExpression {
  // No basic_operator is needed, the evaluation is performed by a
  // specialization performed at the UnaryColumnComputer level.
  //
  // The evalutation code in in the TypedParseString struct in
  // expression_evaluators.
  //
  // We mark this as not safe, as we do not want to parse something random
  // in the memory (and the uninitialized StringPiece could point to
  // anything).
  static const string name() { return string("PARSE_STRING"); }
  static const string FormatDescription(const string& child) {
    return StringPrintf("%s(%s)", name().c_str(), child.c_str());
  }
  static const string FormatDescription(const string& child, DataType to_type) {
      return StringPrintf("%s<%s>(%s)",
                          name().c_str(),
                          GetTypeInfo(to_type).name().c_str(),
                          child.c_str());
  }
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_PARSE_STRING_NULLING>
    : public NullingUnaryExpression {
  // No basic_operator is needed, the evaluation is performed by a
  // specialization performed at the UnaryColumnComputer level.
  //
  // The evalutation code in in the TypedParseString struct in
  // expression_evaluators.
  static const bool is_safe = false;
  static const int selectivity_threshold = 100;
  static const string name() { return string("PARSE_STRING"); }
  static const string FormatDescription(const string& child) {
    return StringPrintf("%s(%s)", name().c_str(), child.c_str());
  }
  static const string FormatDescription(const string& child, DataType to_type) {
      return StringPrintf("%s<%s>(%s)",
                          name().c_str(),
                          GetTypeInfo(to_type).name().c_str(),
                          child.c_str());
  }
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_NOT>
    : public StandardUnaryExpression {
  typedef operators::Not basic_operator;
  static const int selectivity_threshold = 0;
  static const string name() { return string("NOT"); }
  static const string FormatDescription(const string& child) {
    return StringPrintf("(NOT %s)", child.c_str());
  }
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_COPY>
    : public StandardUnaryExpression {
  typedef operators::Copy basic_operator;
  static const int selectivity_threshold = 0;
  static const string name() { return string("COPY"); }
  static const string FormatDescription(const string& child) {
    return StringPrintf("COPY(%s)", child.c_str());
  }
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_NUMERIC_TO_BOOL>
    : public StandardUnaryExpression {
  typedef operators::Cast basic_operator;
  static const int selectivity_threshold = 0;
  static const string name() { return string("NUMERIC_TO_BOOL"); }
  static const string FormatDescription(const string& child) {
    return StringPrintf("TO_BOOL(%s)", child.c_str());
  }
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_BOOL_TO_NUMERIC>
    : public StandardUnaryExpression {
  typedef operators::Cast basic_operator;
  static const int selectivity_threshold = 0;
  static const string name() { return string("BOOL_TO_NUMERIC"); }
  static const string FormatDescription(const string& child) {
    return StringPrintf("FROM_BOOL(%s)", child.c_str());
  }
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_BITWISE_NOT>
    : public PromotingUnaryExpression {
  typedef operators::BitwiseNot basic_operator;
  static const int selectivity_threshold = 0;
  static const string name() { return string("BITWISE_NOT"); }
  static const string FormatDescription(const string& child) {
    return StringPrintf("(~%s)", child.c_str());
  }
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_IS_ODD>
    : public PromotingUnaryExpression {
  typedef operators::IsOdd basic_operator;
  static const int selectivity_threshold = 3;
  static const string name() { return string("IS_ODD"); }
  static const string FormatDescription(const string& child) {
    return StringPrintf("IS_ODD(%s)", child.c_str());
  }
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_IS_EVEN>
    : public StandardUnaryExpression {
  typedef operators::IsEven basic_operator;
  static const int selectivity_threshold = 3;
  static const string name() { return string("IS_EVEN"); }
  static const string FormatDescription(const string& child) {
    return StringPrintf("IS_EVEN(%s)", child.c_str());
  }
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_UNIXTIMESTAMP>
    : public PromotingUnaryExpression {
  typedef operators::UnixTimestamp basic_operator;
  static const int selectivity_threshold = 0;
  static const string name() { return string("UNIXTIMESTAMP"); }
  static const string FormatDescription(const string& child) {
    return StringPrintf("UNIXTIMESTAMP(%s)", child.c_str());
  }
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_FROMUNIXTIME>
    : public FailingPromotingUnaryExpression {
  typedef operators::FromUnixTime basic_operator;
  static const int selectivity_threshold = 0;
  static const string name() { return string("FROMUNIXTIME"); }
  static const string FormatDescription(const string& child) {
    return StringPrintf("FROMUNIXTIME(%s)", child.c_str());
  }
  static const bool is_safe = true;
  typedef failers::IsNegativeFailer CheckFailure;
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_DATE_TO_DATETIME>
    : public StandardUnaryExpression {
  typedef operators::DateToDatetime basic_operator;
  static const int selectivity_threshold = 0;
  static const string name() { return "CAST_DATE_TO_DATETIME"; }
  static const string FormatDescription(const string& child) {
    return StrCat("CAST_DATE_TO_DATETIME(", child, ")");
  }
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_YEAR>
    : public PromotingUnaryExpression {
  typedef operators::Year basic_operator;
  static const int selectivity_threshold = 80;
  static const string name() { return "YEAR"; }
  static const string FormatDescription(const string& child) {
    return StrCat(name(), "(", child, ")");
  }
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_YEAR_LOCAL>
    : public PromotingUnaryExpression {
  typedef operators::YearLocal basic_operator;
  static const int selectivity_threshold = 80;
  static const string name() { return "YEAR_LOCAL"; }
  static const string FormatDescription(const string& child) {
    return StrCat(name(), "(", child, ")");
  }
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_QUARTER>
    : public PromotingUnaryExpression {
  typedef operators::Quarter basic_operator;
  static const int selectivity_threshold = 80;
  static const string name() { return "QUARTER"; }
  static const string FormatDescription(const string& child) {
    return StrCat("QUARTER(", child, ")");
  }
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_QUARTER_LOCAL>
    : public PromotingUnaryExpression {
  typedef operators::QuarterLocal basic_operator;
  static const int selectivity_threshold = 80;
  static const string name() { return "QUARTER_LOCAL"; }
  static const string FormatDescription(const string& child) {
    return StrCat(name(), "(", child, ")");
  }
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_MONTH>
    : public PromotingUnaryExpression {
  typedef operators::Month basic_operator;
  static const int selectivity_threshold = 80;
  static const string name() { return "MONTH"; }
  static const string FormatDescription(const string& child) {
    return StrCat("MONTH(", child, ")");
  }
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_MONTH_LOCAL>
    : public PromotingUnaryExpression {
  typedef operators::MonthLocal basic_operator;
  static const int selectivity_threshold = 80;
  static const string name() { return "MONTH_LOCAL"; }
  static const string FormatDescription(const string& child) {
    return StrCat(name(), "(", child, ")");
  }
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_DAY>
    : public PromotingUnaryExpression {
  typedef operators::Day basic_operator;
  static const int selectivity_threshold = 20;
  static const string name() { return "DAY"; }
  static const string FormatDescription(const string& child) {
    return StrCat("DAY(", child, ")");
  }
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_DAY_LOCAL>
    : public PromotingUnaryExpression {
  typedef operators::DayLocal basic_operator;
  static const int selectivity_threshold = 80;
  static const string name() { return "DAY_LOCAL"; }
  static const string FormatDescription(const string& child) {
    return StrCat(name(), "(", child, ")");
  }
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_WEEKDAY>
    : public PromotingUnaryExpression {
  typedef operators::Weekday basic_operator;
  static const int selectivity_threshold = 20;
  static const string name() { return "WEEKDAY"; }
  static const string FormatDescription(const string& child) {
    return StrCat("WEEKDAY(", child, ")");
  }
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_WEEKDAY_LOCAL>
    : public PromotingUnaryExpression {
  typedef operators::WeekdayLocal basic_operator;
  static const int selectivity_threshold = 80;
  static const string name() { return "WEEKDAY_LOCAL"; }
  static const string FormatDescription(const string& child) {
    return StrCat(name(), "(", child, ")");
  }
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_YEARDAY>
    : public PromotingUnaryExpression {
  typedef operators::YearDay basic_operator;
  static const int selectivity_threshold = 80;
  static const string name() { return "YEARDAY"; }
  static const string FormatDescription(const string& child) {
    return StrCat("YEARDAY(", child, ")");
  }
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_YEARDAY_LOCAL>
    : public PromotingUnaryExpression {
  typedef operators::YearDayLocal basic_operator;
  static const int selectivity_threshold = 80;
  static const string name() { return "YEAR_DAY_LOCAL"; }
  static const string FormatDescription(const string& child) {
    return StrCat(name(), "(", child, ")");
  }
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_HOUR>
    : public StandardUnaryExpression {
  typedef operators::Hour basic_operator;
  // This is simple division, selectivity is lower.
  static const int selectivity_threshold = 10;
  static const string name() { return "HOUR"; }
  static const string FormatDescription(const string& child) {
    return StrCat("HOUR(", child, ")");
  }
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_HOUR_LOCAL>
    : public PromotingUnaryExpression {
  typedef operators::HourLocal basic_operator;
  // While this is still a date operation.
  static const int selectivity_threshold = 80;
  static const string name() { return "HOUR_LOCAL"; }
  static const string FormatDescription(const string& child) {
    return StrCat(name(), "(", child, ")");
  }
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_MINUTE>
    : public StandardUnaryExpression {
  typedef operators::Minute basic_operator;
  static const int selectivity_threshold = 10;
  static const string name() { return "MINUTE"; }
  static const string FormatDescription(const string& child) {
    return StrCat("MINUTE(", child, ")");
  }
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_MINUTE_LOCAL>
    : public PromotingUnaryExpression {
  typedef operators::MinuteLocal basic_operator;
  static const int selectivity_threshold = 80;
  static const string name() { return "MINUTE_LOCAL"; }
  static const string FormatDescription(const string& child) {
    return StrCat(name(), "(", child, ")");
  }
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_SECOND>
    : public StandardUnaryExpression {
  typedef operators::Second basic_operator;
  static const int selectivity_threshold = 10;
  static const string name() { return "SECOND"; }
  static const string FormatDescription(const string& child) {
    return StrCat("SECOND(", child, ")");
  }
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_MICROSECOND>
    : public StandardUnaryExpression {
  typedef operators::Microsecond basic_operator;
  static const int selectivity_threshold = 10;
  static const string name() { return "MICROSECOND"; }
  static const string FormatDescription(const string& child) {
    return StrCat("MICROSECOND(", child, ")");
  }
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_TOSTRING>
    : public AllocatingUnaryExpression {
  // No basic_operator is needed, this function is implemented through a
  // specialization of UnaryColumnComputers. The evaluation logic is in the
  // TypedToString struct in expression_evaluators.h.
  static const int selectivity_threshold = 100;
  static const string name() { return "TOSTRING"; }
  static const string FormatDescription(const string& child) {
    return StrCat(name(), "(", child, ")");
  }
};

// It's actually safe, however surprising that is.
template<>
struct BaseUnaryExpressionTraits<OPERATOR_LENGTH>
    : public StandardUnaryExpression {
  typedef operators::Length basic_operator;
  // This is ignored anyway, as the function takes a string input. This could
  // possibly be optimized at some point.
  static const int selectivity_threshold = 0;
  static const string name() { return "LENGTH"; }
  static const string FormatDescription(const string& child) {
    return StrCat(name(), "(", child, ")");
  }
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_LTRIM>
    : public UnsafeUnaryExpression {
  typedef operators::Ltrim basic_operator;
  static const string name() { return "LTRIM"; }
  static const string FormatDescription(const string& child) {
    return StrCat(name(), "(", child, ")");
  }
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_RTRIM>
    : public UnsafeUnaryExpression {
  typedef operators::Rtrim basic_operator;
  static const string name() { return "RTRIM"; }
  static const string FormatDescription(const string& child) {
    return StrCat(name(), "(", child, ")");
  }
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_TOUPPER>
    : public AllocatingUnaryExpression {
  typedef operators::ToUpper basic_operator;
  static const int selectivity_threshold = 100;
  static const string name() { return "TO_UPPER"; }
  static const string FormatDescription(const string& child) {
    return StrCat(name(), "(", child, ")");
  }
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_TOLOWER>
    : public AllocatingUnaryExpression {
  typedef operators::ToLower basic_operator;
  static const int selectivity_threshold = 100;
  static const string name() { return "TO_LOWER"; }
  static const string FormatDescription(const string& child) {
    return StrCat(name(), "(", child, ")");
  }
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_TRIM>
    : public UnsafeUnaryExpression {
  typedef operators::Trim basic_operator;
  static const string name() { return "TRIM"; }
  static const string FormatDescription(const string& child) {
    return StrCat(name(), "(", child, ")");
  }
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_CEIL>
    : public PromotingUnaryExpression {
  typedef operators::Ceil basic_operator;
  static const int selectivity_threshold = 10;
  static const string name() { return "CEIL"; }
  static const string FormatDescription(const string& child) {
    return StrCat(name(), "(", child, ")");
  }
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_EXP>
    : public PromotingUnaryExpression {
  typedef operators::Exp basic_operator;
  static const int selectivity_threshold = 30;
  static const string name() { return "EXP"; }
  static const string FormatDescription(const string& child) {
    return StrCat(name(), "(", child, ")");
  }
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_FLOOR>
    : public PromotingUnaryExpression {
  typedef operators::Floor basic_operator;
  static const int selectivity_threshold = 10;
  static const string name() { return "FLOOR"; }
  static const string FormatDescription(const string& child) {
    return StrCat(name(), "(", child, ")");
  }
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_ROUND>
    : public PromotingUnaryExpression {
  typedef operators::Round basic_operator;
  static const int selectivity_threshold = 10;
  static const string name() { return "ROUND"; }
  static const string FormatDescription(const string& child) {
    return StrCat(name(), "(", child, ")");
  }
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_ROUND_TO_INT>
    : public PromotingUnaryExpression {
  typedef operators::RoundToInt basic_operator;
  static const int selectivity_threshold = 10;
  static const string name() { return "ROUND_TO_INT"; }
  static const string FormatDescription(const string& child) {
    return StrCat(name(), "(", child, ")");
  }
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_CEIL_TO_INT>
    : public PromotingUnaryExpression {
  typedef operators::CeilToInt basic_operator;
  static const int selectivity_threshold = 10;
  static const string name() { return "CEIL_TO_INT"; }
  static const string FormatDescription(const string& child) {
    return StrCat(name(), "(", child, ")");
  }
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_FLOOR_TO_INT>
    : public PromotingUnaryExpression {
  typedef operators::FloorToInt basic_operator;
  static const int selectivity_threshold = 10;
  static const string name() { return "FLOOR_TO_INT"; }
  static const string FormatDescription(const string& child) {
    return StrCat(name(), "(", child, ")");
  }
};
template<>
struct BaseUnaryExpressionTraits<OPERATOR_TRUNC>
    : public PromotingUnaryExpression {
  typedef operators::Trunc basic_operator;
  static const int selectivity_threshold = 10;
  static const string name() { return "TRUNC"; }
  static const string FormatDescription(const string& child) {
    return StrCat(name(), "(", child, ")");
  }
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_ABS>
    : public StandardUnaryExpression {
  typedef operators::Abs basic_operator;
  static const int selectivity_threshold = 0;
  static const string name() { return "ABS"; }
  static const string FormatDescription(const string& child) {
    return StrCat(name(), "(", child, ")");
  }
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_IS_FINITE>
    : public PromotingUnaryExpression {
  typedef operators::IsFinite basic_operator;
  static const int selectivity_threshold = 10;
  static const string name() { return "IS_FINITE"; }
  static const string FormatDescription(const string& child) {
    return StrCat(name(), "(", child, ")");
  }
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_IS_INF>
    : public PromotingUnaryExpression {
  typedef operators::IsInf basic_operator;
  static const int selectivity_threshold = 10;
  static const string name() { return "IS_INF"; }
  static const string FormatDescription(const string& child) {
    return StrCat(name(), "(", child, ")");
  }
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_IS_NAN>
    : public PromotingUnaryExpression {
  typedef operators::IsNaN basic_operator;
  static const int selectivity_threshold = 10;
  static const string name() { return "IS_NAN"; }
  static const string FormatDescription(const string& child) {
    return StrCat(name(), "(", child, ")");
  }
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_IS_NORMAL>
    : public PromotingUnaryExpression {
  typedef operators::IsNormal basic_operator;
  static const int selectivity_threshold = 10;
  static const string name() { return "IS_NORMAL"; }
  static const string FormatDescription(const string& child) {
    return StrCat(name(), "(", child, ")");
  }
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_LN_NULLING>
    : public NullingPromotingUnaryExpression {
  typedef operators::Ln basic_operator;
  static const int selectivity_threshold = 30;
  static const string name() { return "LN"; }
  static const string FormatDescription(const string& child) {
    return StrCat(name(), "(", child, ")");
  }

  static const bool is_safe = true;
  typedef nullers::IsNonPositiveNuller FillNulls;
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_LN_QUIET>
    : public PromotingUnaryExpression {
  typedef operators::Ln basic_operator;
  static const int selectivity_threshold = 30;
  static const string name() { return "LN"; }
  static const string FormatDescription(const string& child) {
    return StrCat(name(), "(", child, ")");
  }
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_LOG10_NULLING>
    : public NullingPromotingUnaryExpression {
  typedef operators::Log10 basic_operator;
  static const int selectivity_threshold = 30;
  static const string name() { return "LOG10"; }
  static const string FormatDescription(const string& child) {
    return StrCat(name(), "(", child, ")");
  }
  static const bool is_safe = true;
  typedef nullers::IsNonPositiveNuller FillNulls;
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_LOG10_QUIET>
    : public PromotingUnaryExpression {
  typedef operators::Log10 basic_operator;
  static const int selectivity_threshold = 30;
  static const string name() { return "LOG10"; }
  static const string FormatDescription(const string& child) {
    return StrCat(name(), "(", child, ")");
  }
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_LOG2_NULLING>
    : public NullingPromotingUnaryExpression {
  typedef operators::Log2 basic_operator;
  static const int selectivity_threshold = 30;
  static const string name() { return "LOG2"; }
  static const string FormatDescription(const string& child) {
    return StrCat(name(), "(", child, ")");
  }
  static const bool is_safe = true;
  typedef nullers::IsNonPositiveNuller FillNulls;
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_LOG2_QUIET>
    : public PromotingUnaryExpression {
  typedef operators::Log2 basic_operator;
  static const int selectivity_threshold = 30;
  static const string name() { return "LOG2"; }
  static const string FormatDescription(const string& child) {
    return StrCat(name(), "(", child, ")");
  }
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_SQRT_SIGNALING>
    : public FailingPromotingUnaryExpression {
  typedef operators::Sqrt basic_operator;
  static const int selectivity_threshold = 30;
  static const string name() { return "SQRT"; }
  static const string FormatDescription(const string& child) {
    return StrCat(name(), "(", child, ")");
  }
  static const bool is_safe = true;
  typedef failers::IsNegativeFailer CheckFailure;
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_SQRT_NULLING>
    : public NullingPromotingUnaryExpression {
  typedef operators::Sqrt basic_operator;
  static const int selectivity_threshold = 30;
  static const string name() { return "SQRT"; }
  static const string FormatDescription(const string& child) {
    return StrCat(name(), "(", child, ")");
  }

  static const bool is_safe = true;
  typedef nullers::IsNegativeNuller FillNulls;
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_SQRT_QUIET>
    : public PromotingUnaryExpression {
  typedef operators::Sqrt basic_operator;
  static const int selectivity_threshold = 30;
  static const string name() { return "SQRT"; }
  static const string FormatDescription(const string& child) {
    return StrCat(name(), "(", child, ")");
  }
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_SIN>
    : public PromotingUnaryExpression {
  typedef operators::Sin basic_operator;
  static const int selectivity_threshold = 30;
  static const string name() { return "SIN"; }
  static const string FormatDescription(const string& child) {
    return StrCat(name(), "(", child, ")");
  }
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_COS>
    : public PromotingUnaryExpression {
  typedef operators::Cos basic_operator;
  static const int selectivity_threshold = 30;
  static const string name() { return "COS"; }
  static const string FormatDescription(const string& child) {
    return StrCat(name(), "(", child, ")");
  }
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_TAN>
    : public PromotingUnaryExpression {
  typedef operators::Tan basic_operator;
  static const int selectivity_threshold = 30;
  static const string name() { return "TAN"; }
  static const string FormatDescription(const string& child) {
    return StrCat(name(), "(", child, ")");
  }
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_ASIN>
    : public PromotingUnaryExpression {
  typedef operators::Asin basic_operator;
  static const int selectivity_threshold = 30;
  static const string name() { return "ASIN"; }
  static const string FormatDescription(const string& child) {
    return StrCat(name(), "(", child, ")");
  }
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_ACOS>
    : public PromotingUnaryExpression {
  typedef operators::Acos basic_operator;
  static const int selectivity_threshold = 30;
  static const string name() { return "ACOS"; }
  static const string FormatDescription(const string& child) {
    return StrCat(name(), "(", child, ")");
  }
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_ATAN>
    : public PromotingUnaryExpression {
  typedef operators::Atan basic_operator;
  static const int selectivity_threshold = 30;
  static const string name() { return "ATAN"; }
  static const string FormatDescription(const string& child) {
    return StrCat(name(), "(", child, ")");
  }
};


template<>
struct BaseUnaryExpressionTraits<OPERATOR_SINH>
    : public PromotingUnaryExpression {
  typedef operators::Sinh basic_operator;
  static const int selectivity_threshold = 30;
  static const string name() { return "SINH"; }
  static const string FormatDescription(const string& child) {
    return StrCat(name(), "(", child, ")");
  }
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_COSH>
    : public PromotingUnaryExpression {
  typedef operators::Cosh basic_operator;
  static const int selectivity_threshold = 30;
  static const string name() { return "COSH"; }
  static const string FormatDescription(const string& child) {
    return StrCat(name(), "(", child, ")");
  }
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_TANH>
    : public PromotingUnaryExpression {
  typedef operators::Tanh basic_operator;
  static const int selectivity_threshold = 30;
  static const string name() { return "TANH"; }
  static const string FormatDescription(const string& child) {
    return StrCat(name(), "(", child, ")");
  }
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_ASINH>
    : public PromotingUnaryExpression {
  typedef operators::Asinh basic_operator;
  static const int selectivity_threshold = 30;
  static const string name() { return "ASINH"; }
  static const string FormatDescription(const string& child) {
    return StrCat(name(), "(", child, ")");
  }
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_ACOSH>
    : public PromotingUnaryExpression {
  typedef operators::Acosh basic_operator;
  static const int selectivity_threshold = 30;
  static const string name() { return "ACOSH"; }
  static const string FormatDescription(const string& child) {
    return StrCat(name(), "(", child, ")");
  }
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_ATANH>
    : public PromotingUnaryExpression {
  typedef operators::Atanh basic_operator;
  static const int selectivity_threshold = 30;
  static const string name() { return "ATANH"; }
  static const string FormatDescription(const string& child) {
    return StrCat(name(), "(", child, ")");
  }
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_REGEXP_PARTIAL>
    : public UnsafeUnaryExpression {
  typedef operators::RegexpPartial basic_operator;
  static const string name() { return "REGEXP_PARTIAL_MATCH"; }
  static const string FormatDescription(const string& child) {
    return StrCat(name(), "(", child, ")");
  }
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_REGEXP_FULL>
    : public UnsafeUnaryExpression {
  typedef operators::RegexpFull basic_operator;
  static const string name() { return "REGEXP_FULL_MATCH"; }
  static const string FormatDescription(const string& child) {
    return StrCat(name(), "(", child, ")");
  }
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_REGEXP_EXTRACT>
    : public UnsafeUnaryExpression {
  // No operator needed, evaluation is implemented by hand due to different
  // nullability applications.
  static const string name() { return "REGEXP_EXTRACT"; }
  static const string FormatDescription(const string& child) {
    return StrCat(name(), "(", child, ")");
  }
};

template<>
struct BaseUnaryExpressionTraits<OPERATOR_FINGERPRINT>
    : public UnsafeUnaryExpression {
  typedef operators::FingerprintEvaluator basic_operator;
  static const int selectivity_threshold = 20;
  static const string name() { return "FINGERPRINT"; }
  static const string FormatDescription(const string& child) {
    return StrCat(name(), "(", child, ")");
  }
};

template<OperatorId op>
struct UnaryExpressionTraits : public BaseUnaryExpressionTraits<op> {
  static const string FormatBoundDescription(
        const string& child,
        DataType from_type,
        DataType to_type) {
    return BaseUnaryExpressionTraits<op>::FormatDescription(child);
  }
};

template<>
struct UnaryExpressionTraits<OPERATOR_CAST_QUIET>
    : public BaseUnaryExpressionTraits<OPERATOR_CAST_QUIET> {
  static const string FormatBoundDescription(const string& argument,
                                             DataType from_type,
                                             DataType to_type) {
    return StringPrintf("CAST_%s_TO_%s(%s)",
                        GetTypeInfo(from_type).name().c_str(),
                        GetTypeInfo(to_type).name().c_str(),
                        argument.c_str());
  }
};

// ---------------------------- Binary Expression Traits ---------------
struct ArithmeticExpressionTraits {
  static const bool supports_promotions = true;
  static const bool can_fail = false;
  static const bool can_return_null = false;
  static const bool needs_allocator = false;
  static const bool is_safe = true;
};

struct ComparisonExpressionTraits {
  static const bool supports_promotions = true;
  static const bool can_fail = false;
  static const bool can_return_null = false;
  static const bool needs_allocator = false;
  // All comparisons are not safe in the case of STRING or BINARY inputs.
  // TODO(onufry): some workaround is needed, to allow the SIMDing of
  // comparisons. Maybe a templated (with input/output types) version of
  // is_safe would be suitable? Or (ptab's idea) let the comparison operator
  // be bound to a different internal operatorId for variable length types?
  static const bool is_safe = false;
};

struct LogicExpressionTraits {
  static const bool supports_promotions = false;
  static const bool can_fail = false;
  static const bool can_return_null = false;
  static const bool needs_allocator = false;
  static const bool is_safe = true;
};

struct FailingArithmeticExpressionTraits {
  static const bool supports_promotions = true;
  static const bool can_fail = true;
  static const bool can_return_null = false;
  static const bool needs_allocator = false;
};

struct NullingArithmeticExpressionTraits {
  static const bool supports_promotions = true;
  static const bool can_fail = false;
  static const bool can_return_null = true;
  static const bool needs_allocator = false;
};

template<OperatorId op> struct BaseBinaryExpressionTraits {};

template<>
struct BaseBinaryExpressionTraits<OPERATOR_ADD>
    : public ArithmeticExpressionTraits {
  typedef operators::Plus basic_operator;
  static const int selectivity_threshold = 0;
  static const string name() { return "PLUS"; }
  static const string FormatDescription(const string& left,
                                        const string& right) {
    return StringPrintf("(%s + %s)", left.c_str(), right.c_str());
  }
};

template<>
struct BaseBinaryExpressionTraits<OPERATOR_MULTIPLY>
    : public ArithmeticExpressionTraits {
  typedef operators::Multiply basic_operator;
  static const int selectivity_threshold = 0;
  static const string name() { return "MULTIPLY"; }
  static const string FormatDescription(const string& left,
                                        const string& right) {
    return StrCat("(", left, " * " , right, ")");
  }
};

template<>
struct BaseBinaryExpressionTraits<OPERATOR_DIVIDE_SIGNALING>
    : public FailingArithmeticExpressionTraits {
  typedef operators::Divide basic_operator;
  static const int selectivity_threshold = 0;
  static const string name() { return "DIVIDE"; }
  static const string FormatDescription(const string& left,
                                        const string& right) {
    return StrCat("(", left,  " /. ", right, ")");
  }
  // The standard DIVIDE operator casts its arguments to DOUBLE, and double
  // division cannot fail at runtime.
  static const bool is_safe = true;
  typedef failers::SecondColumnZeroFailer CheckFailure;
};

template<>
struct BaseBinaryExpressionTraits<OPERATOR_DIVIDE_NULLING>
    : public NullingArithmeticExpressionTraits {
  typedef operators::Divide basic_operator;
  static const int selectivity_threshold = 0;
  static const string name() { return "DIVIDE"; }
  static const string FormatDescription(const string& left,
                                        const string& right) {
    return StrCat("(", left,  " /. ", right, ")");
  }
  // The standard DIVIDE operator casts its arguments to DOUBLE, and double
  // division cannot fail at runtime.
  static const bool is_safe = true;
  typedef nullers::SecondColumnZeroNuller FillNulls;
};

template<>
struct BaseBinaryExpressionTraits<OPERATOR_DIVIDE_QUIET>
    : public ArithmeticExpressionTraits {
  typedef operators::Divide basic_operator;
  static const int selectivity_threshold = 0;
  static const string name() { return "DIVIDE"; }
  static const string FormatDescription(const string& left,
                                        const string& right) {
    return StrCat("(", left,  " /. ", right, ")");
  }
};

template<>
struct BaseBinaryExpressionTraits<OPERATOR_CPP_DIVIDE_SIGNALING>
    : public FailingArithmeticExpressionTraits {
  // The division is the same, only the casts performed beforehand are
  // different (we cast to the smallest common containing type).
  typedef operators::Divide basic_operator;
  static const int selectivity_threshold = 100;
  static const string name() { return "CPP_DIVIDE"; }
  static const string FormatDescription(const string& left,
                                        const string& right) {
    return StrCat("(", left,  " / ", right, ")");
  }
  typedef failers::SecondColumnZeroFailer CheckFailure;
};

template<>
struct BaseBinaryExpressionTraits<OPERATOR_CPP_DIVIDE_NULLING>
    : public NullingArithmeticExpressionTraits {
  // The division is the same, only the casts performed beforehand are
  // different (we cast to the smallest common containing type).
  typedef operators::Divide basic_operator;
  static const int selectivity_threshold = 100;
  static const string name() { return "CPP_DIVIDE"; }
  static const string FormatDescription(const string& left,
                                        const string& right) {
    return StrCat("(", left,  " / ", right, ")");
  }
  // This can fail, because integer division can fail at runtime.
  typedef nullers::SecondColumnZeroNuller FillNulls;
};

template<>
struct BaseBinaryExpressionTraits<OPERATOR_MODULUS_SIGNALING>
    : public FailingArithmeticExpressionTraits {
  typedef operators::Modulus basic_operator;
  static const int selectivity_threshold = 100;
  static const string name() { return "MODULUS"; }
  static const string FormatDescription(const string& left,
                                        const string& right) {
    return StrCat("(", left,  " % ", right, ")");
  }
  typedef failers::SecondColumnZeroFailer CheckFailure;
};

template<>
struct BaseBinaryExpressionTraits<OPERATOR_MODULUS_NULLING>
    : public NullingArithmeticExpressionTraits {
  typedef operators::Modulus basic_operator;
  static const int selectivity_threshold = 100;
  static const string name() { return "MODULUS"; }
  static const string FormatDescription(const string& left,
                                        const string& right) {
    return StrCat("(", left,  " % ", right, ")");
  }
  typedef nullers::SecondColumnZeroNuller FillNulls;
};

template<>
struct BaseBinaryExpressionTraits<OPERATOR_POW_SIGNALING>
    : public FailingArithmeticExpressionTraits {
  typedef operators::Pow basic_operator;
  static const int selectivity_threshold = 30;
  static const string name() { return "POW"; }
  static const string FormatDescription(const string& left,
                                        const string& right) {
    return StrCat(name(), "(", left, ", ", right, ")");
  }
  // The standard DIVIDE operator casts its arguments to DOUBLE, and double
  // division cannot fail at runtime.
  static const bool is_safe = true;
  typedef failers::FirstColumnNegativeAndSecondNonIntegerFailer CheckFailure;
};

template<>
struct BaseBinaryExpressionTraits<OPERATOR_POW_NULLING>
    : public NullingArithmeticExpressionTraits {
  typedef operators::Pow basic_operator;
  static const int selectivity_threshold = 30;
  static const string name() { return "POW"; }
  static const string FormatDescription(const string& left,
                                        const string& right) {
    return StrCat(name(), "(", left, ", ", right, ")");
  }
  // The standard DIVIDE operator casts its arguments to DOUBLE, and double
  // division cannot fail at runtime.
  static const bool is_safe = true;
  typedef nullers::FirstColumnNegativeAndSecondNonIntegerNuller FillNulls;
};

template<>
struct BaseBinaryExpressionTraits<OPERATOR_POW_QUIET>
    : public ArithmeticExpressionTraits {
  typedef operators::Pow basic_operator;
  static const int selectivity_threshold = 30;
  static const string name() { return "POW"; }
  static const string FormatDescription(const string& left,
                                        const string& right) {
    return StrCat(name(), "(", left, ", ", right, ")");
  }
};

template<>
struct BaseBinaryExpressionTraits<OPERATOR_ROUND_WITH_MULTIPLIER>
    : public ArithmeticExpressionTraits {
  typedef operators::RoundWithMultiplier basic_operator;
  static const int selectivity_threshold = 10;
  static const string name() { return "ROUND_WITH_MULTIPLIER"; }
  static const string FormatDescription(const string& left,
                                        const string& right) {
    return StrCat(name(), "(", left, ", ", right, ")");
  }
};

template<>
struct BaseBinaryExpressionTraits<OPERATOR_ATAN2>
    : public ArithmeticExpressionTraits {
  typedef operators::Atan2 basic_operator;
  static const int selectivity_threshold = 30;
  static const string name() { return "ATAN2"; }
  static const string FormatDescription(const string& left,
                                        const string& right) {
    return StrCat(name(), "(", left, ", ", right, ")");
  }
};

template<>
struct BaseBinaryExpressionTraits<OPERATOR_SUBTRACT>
    : public ArithmeticExpressionTraits {
  typedef operators::Minus basic_operator;
  static const int selectivity_threshold = 0;
  static const string name() { return "SUBTRACT"; }
  static const string FormatDescription(const string& left,
                                        const string& right) {
    return StrCat("(", left,  " - ", right, ")");
  }
};

template<>
struct BaseBinaryExpressionTraits<OPERATOR_NOT_EQUAL>
    : public ComparisonExpressionTraits {
  typedef operators::NotEqual basic_operator;
  static const int selectivity_threshold = 0;
  static const string name() { return "NOT EQUAL"; }
  static const string FormatDescription(const string& left,
                                        const string& right) {
    return StrCat("(", left,  " <> ", right, ")");
  }
};

template<>
struct BaseBinaryExpressionTraits<OPERATOR_EQUAL>
    : public ComparisonExpressionTraits {
  typedef operators::Equal basic_operator;
  static const int selectivity_threshold = 0;
  static const string name() { return "EQUAL"; }
  static const string FormatDescription(const string& left,
                                        const string& right) {
    return StrCat("(", left,  " == ", right, ")");
  }
};

template<>
struct BaseBinaryExpressionTraits<OPERATOR_LESS_OR_EQUAL>
    : public ComparisonExpressionTraits {
  typedef operators::LessOrEqual basic_operator;
  static const int selectivity_threshold = 0;
  static const string name() { return "LESS OR EQUAL"; }
  static const string FormatDescription(const string& left,
                                        const string& right) {
    return StrCat("(", left,  " <= ", right, ")");
  }
};

template<>
struct BaseBinaryExpressionTraits<OPERATOR_LESS>
    : public ComparisonExpressionTraits {
  typedef operators::Less basic_operator;
  static const int selectivity_threshold = 0;
  static const string name() { return "LESS"; }
  static const string FormatDescription(const string& left,
                                        const string& right) {
    return StrCat("(", left,  " < ", right, ")");
  }
};

template<>
struct BaseBinaryExpressionTraits<OPERATOR_AND> : public LogicExpressionTraits {
  typedef operators::And basic_operator;
  static const string name() { return string("AND"); }
  static const int selectivity_threshold = 0;
  static const string FormatDescription(const string& left,
                                        const string& right) {
    return StrCat("(", left,  " AND ", right, ")");
  }
};

// andnot(a,b) = (~a) && b.
template<>
struct BaseBinaryExpressionTraits<OPERATOR_AND_NOT>
    : public LogicExpressionTraits {
  typedef operators::AndNot basic_operator;
  static const int selectivity_threshold = 0;
  static const string name() { return string("AND_NOT"); }
  static const string FormatDescription(const string& left,
                                        const string& right) {
    return StrCat("(", left,  " !&& ", right, ")");
  }
};

template<>
struct BaseBinaryExpressionTraits<OPERATOR_OR> : public LogicExpressionTraits {
  typedef operators::Or basic_operator;
  static const int selectivity_threshold = 0;
  static const string name() { return string("OR"); }
  static const string FormatDescription(const string& left,
                                        const string& right) {
    return StrCat("(", left,  " OR ", right, ")");
  }
};

template<>
struct BaseBinaryExpressionTraits<OPERATOR_XOR> : public LogicExpressionTraits {
  typedef operators::Xor basic_operator;
  static const string name() { return string("XOR"); }
  static const int selectivity_threshold = 0;
  static const string FormatDescription(const string& left,
                                        const string& right) {
    return StrCat("(", left,  " XOR ", right, ")");
  }
};

template<>
struct BaseBinaryExpressionTraits<OPERATOR_BITWISE_AND>
    : public ArithmeticExpressionTraits {
  typedef operators::BitwiseAnd basic_operator;
  static const int selectivity_threshold = 0;
  static const string name() { return "BITWISE AND"; }
  static const string FormatDescription(const string& left,
                                        const string& right) {
    return StrCat("(", left,  " & ", right, ")");
  }
};

template<>
struct BaseBinaryExpressionTraits<OPERATOR_BITWISE_OR>
    : public ArithmeticExpressionTraits {
  typedef operators::BitwiseOr basic_operator;
  static const int selectivity_threshold = 0;
  static const string name() { return "BITWISE OR"; }
  static const string FormatDescription(const string& left,
                                        const string& right) {
    return StrCat("(", left,  " | ", right, ")");
  }
};

template<>
struct BaseBinaryExpressionTraits<OPERATOR_BITWISE_ANDNOT>
    : public ArithmeticExpressionTraits {
  typedef operators::BitwiseAndNot basic_operator;
  static const int selectivity_threshold = 0;
  static const string name() { return "BITWISE AND NOT"; }
  static const string FormatDescription(const string& left,
                                        const string& right) {
    return StrCat("(~", left,  " & ", right, ")");
  }
};

template<>
struct BaseBinaryExpressionTraits<OPERATOR_BITWISE_XOR>
    : public ArithmeticExpressionTraits {
  typedef operators::BitwiseXor basic_operator;
  static const int selectivity_threshold = 0;
  static const string name() { return "BITWISE XOR"; }
  static const string FormatDescription(const string& left,
                                        const string& right) {
    return StrCat("(", left,  " ^ ", right, ")");
  }
};

template<>
struct BaseBinaryExpressionTraits<OPERATOR_SHIFT_LEFT>
    : public ArithmeticExpressionTraits {
  typedef operators::LeftShift basic_operator;
  static const int selectivity_threshold = 0;
  static const string name() { return "SHIFT LEFT"; }
  static const string FormatDescription(const string& left,
                                        const string& right) {
    return StrCat("(", left,  " << ", right, ")");
  }
};

template<>
struct BaseBinaryExpressionTraits<OPERATOR_SHIFT_RIGHT>
    : public ArithmeticExpressionTraits {
  typedef operators::RightShift basic_operator;
  static const int selectivity_threshold = 0;
  static const string name() { return "SHIFT RIGHT"; }
  static const string FormatDescription(const string& left,
                                        const string& right) {
    return StrCat("(", left,  " >> ", right, ")");
  }
};

template<>
struct BaseBinaryExpressionTraits<OPERATOR_IF_NULL> {
  // We don't need to define the basic operator, the implementation of IFNULL
  // is through a specialized subtemplate of column computers.
  static const string name() { return "IFNULL"; }
  static const int selectivity_threshold = 0;
  static const bool supports_promotions = true;
  static const bool can_fail = false;
  static const bool can_return_null = false;
  static const bool needs_allocator = false;
  static const bool is_safe = true;
  static const string FormatDescription(const string& left,
                                        const string& right) {
    return StringPrintf("IFNULL(%s, %s)", left.c_str(), right.c_str());
  }
};

template<>
struct BaseBinaryExpressionTraits<OPERATOR_STRING_OFFSET> {
  typedef operators::StringOffset basic_operator;
  static const string name() { return string("STRING_OFFSET"); }
  static const int selectivity_threshold = 100;
  static const bool supports_promotions = false;
  static const bool can_fail = false;
  static const bool can_return_null = false;
  static const bool needs_allocator = false;
  static const bool is_safe = false;
  static const string FormatDescription(const string& left,
                                        const string& right) {
    return StringPrintf("%s(%s, %s)", name().c_str(), left.c_str(),
                        right.c_str());
  }
};

template<>
struct BaseBinaryExpressionTraits<OPERATOR_SUBSTRING_SIGNALING> {
  typedef operators::SubstringBinary basic_operator;
  static const string name() { return string("SUBSTRING"); }
  static const int selectivity_threshold = 100;
  static const bool supports_promotions = true;
  static const bool can_fail = false;
  static const bool can_return_null = false;
  static const bool needs_allocator = false;
  // Substring on StringPiece can reach outside its memory buffer with
  // negative arguments. It can also read bad memory if the StringPiece is
  // uninitialized.
  static const bool is_safe = false;
  static const string FormatDescription(const string& left,
                                        const string& right) {
    return StringPrintf("SUBSTRING(%s, %s)", left.c_str(), right.c_str());
  }
};

template<>
struct BaseBinaryExpressionTraits<OPERATOR_FORMAT_SIGNALING> {
  typedef operators::Format basic_operator;
  static const string name() { return "FORMAT"; }
  static const int selectivity_threshold = 95;
  static const bool supports_promotions = true;
  static const bool can_fail = true;
  static const bool can_return_null = false;
  static const bool needs_allocator = true;
  static const bool is_safe = true;
  typedef failers::SecondColumnNegativeFailer CheckFailure;
  static const string FormatDescription(const string& left,
                                        const string& right) {
    return StrCat("FORMAT(", left, ", ", right, ")");
  }
};

template<>
struct BaseBinaryExpressionTraits<OPERATOR_DATEFORMAT> {
  typedef operators::DateFormat basic_operator;
  static const string name() { return "DATE_FORMAT"; }
  static const int selectivity_threshold = 100;
  static const bool supports_promotions = true;
  static const bool can_fail = false;
  static const bool can_return_null = false;
  static const bool needs_allocator = true;
  // We do not want to read random bytes as format input.
  static const bool is_safe = false;
  static const string FormatDescription(const string& left,
                                        const string& right) {
    return StrCat("DATEFORMAT(", left, ", ", right, ")");
  }
};

template<>
struct BaseBinaryExpressionTraits<OPERATOR_DATEFORMAT_LOCAL> {
  typedef operators::DateFormatLocal basic_operator;
  static const string name() { return "DATE_FORMAT_LOCAL"; }
  static const int selectivity_threshold = 100;
  static const bool supports_promotions = true;
  static const bool can_fail = false;
  static const bool can_return_null = false;
  static const bool needs_allocator = true;
  // We do not want to read random bytes as format input.
  static const bool is_safe = false;
  static const string FormatDescription(const string& left,
                                        const string& right) {
    return StrCat(name(), "(", left, ", ", right, ")");
  }
};

template<>
struct BaseBinaryExpressionTraits<OPERATOR_ADD_MINUTES>
    : ArithmeticExpressionTraits {
  typedef operators::AddMinutes basic_operator;
  static const int selectivity_threshold = 5;
  static const string name() { return "ADD_MINUTES"; }
  static const string FormatDescription(const string& left,
                                        const string& right) {
    return StrCat(name(), "(", left, ", ", right, ")");
  }
};

template<>
struct BaseBinaryExpressionTraits<OPERATOR_ADD_DAYS>
    : ArithmeticExpressionTraits {
  typedef operators::AddDays basic_operator;
  static const int selectivity_threshold = 5;
  static const string name() { return "ADD_DAYS"; }
  static const string FormatDescription(const string& left,
                                        const string& right) {
    return StrCat(name(), "(", left, ", ", right, ")");
  }
};

template<>
struct BaseBinaryExpressionTraits<OPERATOR_ADD_MONTHS>
    : ArithmeticExpressionTraits {
  typedef operators::AddMonths basic_operator;
  static const int selectivity_threshold = 80;
  static const string name() { return "ADD_MONTHS"; }
  static const string FormatDescription(const string& left,
                                        const string& right) {
    return StrCat(name(), "(", left, ", ", right, ")");
  }
};

template<>
struct BaseBinaryExpressionTraits<OPERATOR_REGEXP_REPLACE> {
  // No basic operator, the evaluation is hand-implemented.
  static const string name() { return "REGEXP_REPLACE"; }
  // Is not a safe operator - do not change the selection level!
  static const int selectivity_threshold = 100;
  static const bool supports_promotions = false;
  static const bool can_fail = false;
  static const bool can_return_null = false;
  static const bool needs_allocator = true;
  // We do not want to read from random memory on uninitialized data.
  static const bool is_safe = false;
  static const string FormatDescription(const string& left,
                                        const string& right) {
    return StrCat(name(), "(", left, ", ", right, ")");
  }
};

template<>
struct BaseBinaryExpressionTraits<OPERATOR_HASH> {
  typedef operators::HashEvaluator basic_operator;
  static const string name() { return "HASH"; }
  static const int selectivity_threshold = 20;
  static const bool supports_promotions = false;
  static const bool can_fail = false;
  static const bool can_return_null = false;
  static const bool needs_allocator = false;
  // We do not want to read from random memory on uninitialized data.
  static const bool is_safe = false;
  static const string FormatDescription(const string& left,
                                        const string& right) {
    return StrCat(name(), "(", left, ", ", right, ")");
  }
};

template<OperatorId op>
struct BinaryExpressionTraits : public BaseBinaryExpressionTraits<op> {
  static const string FormatBoundDescription(
      const string& left,
      DataType left_type,
      const string& right,
      DataType right_type,
      DataType to_type) {
    return BaseBinaryExpressionTraits<op>::FormatDescription(left, right);
  }
};

// --------------------- Ternary Expression Traits -------------
template<OperatorId op> struct BaseTernaryExpressionTraits {
  static const OperatorId type = op;
};

template<> struct BaseTernaryExpressionTraits<OPERATOR_MAKEDATE> {
  typedef operators::MakeDate basic_operator;
  static const int selectivity_threshold = 90;
  static const string name() { return string("MAKEDATE"); }
  static const DataType output_type = INT64;
  static const bool supports_promotions = true;
  static const bool can_fail = true;
  static const bool can_return_null = false;
  static const bool needs_allocator = false;
  static const bool is_safe = true;
  typedef failers::MakeDateFailer CheckFailure;
};

template<> struct BaseTernaryExpressionTraits<OPERATOR_SUBSTRING_SIGNALING> {
  typedef operators::SubstringTernary basic_operator;
  static const int selectivity_threshold = 100;
  static const string name() { return string("SUBSTRING"); }
  static const DataType output_type = STRING;
  static const bool supports_promotions = true;
  static const bool can_fail = false;
  static const bool can_return_null = false;
  static const bool needs_allocator = false;
  // Not 100% sure about this, but the code analysis shows this should work
  // correctly even on invalid pointers - there is no dereferencing, just
  // pointer arithmetic.
  static const bool is_safe = true;
};

template<> struct BaseTernaryExpressionTraits<OPERATOR_STRING_REPLACE> {
  typedef operators::StringReplaceEvaluator basic_operator;
  static const int selectivity_threshold = 100;
  static const string name() { return string("STRING_REPLACE"); }
  static const bool supports_promotions = true;
  static const bool can_fail = false;
  static const bool can_return_null = false;
  static const bool needs_allocator = true;
  static const bool is_safe = false;
};

template<OperatorId op>
struct TernaryExpressionTraits : public BaseTernaryExpressionTraits<op> {
  static const string FormatDescription(
      const string& left,
      const string& middle,
      const string& right) {
    return StrCat(BaseTernaryExpressionTraits<op>::name(),
                  "(", left, ", ", middle, ", ", right, ")");
  }
  static const string FormatBoundDescription(
      const string& left,
      DataType left_type,
      const string& middle,
      DataType middle_type,
      const string& right,
      DataType right_type,
      DataType to_type) {
    return FormatDescription(left, middle, right);
  }
};

// ----------------------- SIMD Traits -----------------------------------------

template<OperatorId op, typename DataType> struct SimdTraits {
  static const bool simd_supported = false;
};

// Define SIMD traits if we are building for targets supporting SSE2.
#ifdef __SSE2__

// Defines SimdTraits<operation, dataType> that supports SIMD and delegates
// SIMD evaluation to 'simdOperator' functor.
#define SIMD_TRAITS_FOR(op_type, DataType, SimdOperator)                       \
  template <> struct SimdTraits<op_type, DataType> {                           \
    static const bool simd_supported = true;                                   \
    typedef SimdOperator<DataType, DataType> simd_operator;                    \
  };

SIMD_TRAITS_FOR(OPERATOR_ADD, int32,  simd_operators::SimdPlus);
SIMD_TRAITS_FOR(OPERATOR_ADD, int64,  simd_operators::SimdPlus);
SIMD_TRAITS_FOR(OPERATOR_ADD, uint32, simd_operators::SimdPlus);
SIMD_TRAITS_FOR(OPERATOR_ADD, uint64, simd_operators::SimdPlus);
SIMD_TRAITS_FOR(OPERATOR_ADD, float,  simd_operators::SimdPlus);
SIMD_TRAITS_FOR(OPERATOR_ADD, double, simd_operators::SimdPlus);

SIMD_TRAITS_FOR(OPERATOR_SUBTRACT, int32,  simd_operators::SimdSubtract);
SIMD_TRAITS_FOR(OPERATOR_SUBTRACT, int64,  simd_operators::SimdSubtract);
SIMD_TRAITS_FOR(OPERATOR_SUBTRACT, uint32, simd_operators::SimdSubtract);
SIMD_TRAITS_FOR(OPERATOR_SUBTRACT, uint64, simd_operators::SimdSubtract);
SIMD_TRAITS_FOR(OPERATOR_SUBTRACT, float,  simd_operators::SimdSubtract);
SIMD_TRAITS_FOR(OPERATOR_SUBTRACT, double, simd_operators::SimdSubtract);

SIMD_TRAITS_FOR(OPERATOR_MULTIPLY, float,  simd_operators::SimdMultiply);
SIMD_TRAITS_FOR(OPERATOR_MULTIPLY, double, simd_operators::SimdMultiply);

SIMD_TRAITS_FOR(OPERATOR_DIVIDE_SIGNALING, float,  simd_operators::SimdDivide);
SIMD_TRAITS_FOR(OPERATOR_DIVIDE_SIGNALING, double, simd_operators::SimdDivide);

SIMD_TRAITS_FOR(OPERATOR_OR, bool,   simd_operators::SimdOr);
SIMD_TRAITS_FOR(OPERATOR_OR, int32,  simd_operators::SimdOr);
SIMD_TRAITS_FOR(OPERATOR_OR, int64,  simd_operators::SimdOr);
SIMD_TRAITS_FOR(OPERATOR_OR, uint32, simd_operators::SimdOr);
SIMD_TRAITS_FOR(OPERATOR_OR, uint64, simd_operators::SimdOr);

SIMD_TRAITS_FOR(OPERATOR_AND, bool,   simd_operators::SimdAnd);
SIMD_TRAITS_FOR(OPERATOR_AND, int32,  simd_operators::SimdAnd);
SIMD_TRAITS_FOR(OPERATOR_AND, int64,  simd_operators::SimdAnd);
SIMD_TRAITS_FOR(OPERATOR_AND, uint32, simd_operators::SimdAnd);
SIMD_TRAITS_FOR(OPERATOR_AND, uint64, simd_operators::SimdAnd);

SIMD_TRAITS_FOR(OPERATOR_AND_NOT, bool,   simd_operators::SimdAndNot);
SIMD_TRAITS_FOR(OPERATOR_AND_NOT, int32,  simd_operators::SimdAndNot);
SIMD_TRAITS_FOR(OPERATOR_AND_NOT, int64,  simd_operators::SimdAndNot);
SIMD_TRAITS_FOR(OPERATOR_AND_NOT, uint32, simd_operators::SimdAndNot);
SIMD_TRAITS_FOR(OPERATOR_AND_NOT, uint64, simd_operators::SimdAndNot);

SIMD_TRAITS_FOR(OPERATOR_XOR, bool, simd_operators::SimdXor);

SIMD_TRAITS_FOR(OPERATOR_BITWISE_OR, bool,   simd_operators::SimdOr);
SIMD_TRAITS_FOR(OPERATOR_BITWISE_OR, int32,  simd_operators::SimdOr);
SIMD_TRAITS_FOR(OPERATOR_BITWISE_OR, int64,  simd_operators::SimdOr);
SIMD_TRAITS_FOR(OPERATOR_BITWISE_OR, uint32, simd_operators::SimdOr);
SIMD_TRAITS_FOR(OPERATOR_BITWISE_OR, uint64, simd_operators::SimdOr);

SIMD_TRAITS_FOR(OPERATOR_BITWISE_AND, bool,   simd_operators::SimdAnd);
SIMD_TRAITS_FOR(OPERATOR_BITWISE_AND, int32,  simd_operators::SimdAnd);
SIMD_TRAITS_FOR(OPERATOR_BITWISE_AND, int64,  simd_operators::SimdAnd);
SIMD_TRAITS_FOR(OPERATOR_BITWISE_AND, uint32, simd_operators::SimdAnd);
SIMD_TRAITS_FOR(OPERATOR_BITWISE_AND, uint64, simd_operators::SimdAnd);

SIMD_TRAITS_FOR(OPERATOR_BITWISE_ANDNOT, bool,   simd_operators::SimdAndNot);
SIMD_TRAITS_FOR(OPERATOR_BITWISE_ANDNOT, int32,  simd_operators::SimdAndNot);
SIMD_TRAITS_FOR(OPERATOR_BITWISE_ANDNOT, int64,  simd_operators::SimdAndNot);
SIMD_TRAITS_FOR(OPERATOR_BITWISE_ANDNOT, uint32, simd_operators::SimdAndNot);
SIMD_TRAITS_FOR(OPERATOR_BITWISE_ANDNOT, uint64, simd_operators::SimdAndNot);

#endif  // __SSE2__

}  // namespace supersonic

#endif  // SUPERSONIC_EXPRESSION_VECTOR_EXPRESSION_TRAITS_H_
