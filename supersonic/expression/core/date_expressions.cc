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

#include "supersonic/expression/core/date_expressions.h"

#include <string>
namespace supersonic {using std::string; }

#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/utils/walltime.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/expression/base/expression.h"
#include "supersonic/expression/core/date_bound_expressions.h"
#include "supersonic/expression/core/date_evaluators.h"  // IWYU pragma: keep
#include "supersonic/expression/core/elementary_expressions.h"
#include "supersonic/expression/infrastructure/basic_expressions.h"
#include "supersonic/expression/infrastructure/terminal_expressions.h"
#include "supersonic/expression/proto/operators.pb.h"
#include "supersonic/expression/templated/abstract_expressions.h"
#include "supersonic/proto/supersonic.pb.h"
#include "supersonic/utils/strings/join.h"

namespace supersonic {

class BufferAllocator;
class TupleSchema;

namespace {

// The MakeDatetime expression. We don't have a senary expression framework, so
// this is implemented separately.
class MakeDatetimeExpression : public Expression {
 public:
  MakeDatetimeExpression(const Expression* const year,
                         const Expression* const month,
                         const Expression* const day,
                         const Expression* const hour,
                         const Expression* const minute,
                         const Expression* const second)
      : year_(year), month_(month), day_(day), hour_(hour), minute_(minute),
        second_(second) {}
 private:
  virtual FailureOrOwned<BoundExpression> DoBind(
      const TupleSchema& input_schema,
      BufferAllocator* allocator,
      rowcount_t max_row_count) const {
    FailureOrOwned<BoundExpression> bound_year = year_->DoBind(
        input_schema, allocator, max_row_count);
    PROPAGATE_ON_FAILURE(bound_year);

    FailureOrOwned<BoundExpression> bound_month = month_->DoBind(
        input_schema, allocator, max_row_count);
    PROPAGATE_ON_FAILURE(bound_month);

    FailureOrOwned<BoundExpression> bound_day = day_->DoBind(
        input_schema, allocator, max_row_count);
    PROPAGATE_ON_FAILURE(bound_day);

    FailureOrOwned<BoundExpression> bound_hour = hour_->DoBind(
        input_schema, allocator, max_row_count);
    PROPAGATE_ON_FAILURE(bound_hour);

    FailureOrOwned<BoundExpression> bound_minute = minute_->DoBind(
        input_schema, allocator, max_row_count);
    PROPAGATE_ON_FAILURE(bound_minute);

    FailureOrOwned<BoundExpression> bound_second = second_->DoBind(
        input_schema, allocator, max_row_count);
    PROPAGATE_ON_FAILURE(bound_second);

    return BoundMakeDatetime(bound_year.release(), bound_month.release(),
                             bound_day.release(), bound_hour.release(),
                             bound_minute.release(), bound_second.release(),
                             allocator, max_row_count);
  }

  virtual string ToString(bool verbose) const {
    // StrCat takes at most 12 arguments, which necessitates the double call.
    return StrCat(
        StrCat("MAKE_DATETIME(", year_->ToString(verbose), ", ",
               month_->ToString(verbose), ", ", day_->ToString(verbose)),
        ", ", hour_->ToString(verbose), ", ", minute_->ToString(verbose),
        ", ", second_->ToString(verbose), ")");
  }

  const scoped_ptr<const Expression> year_;
  const scoped_ptr<const Expression> month_;
  const scoped_ptr<const Expression> day_;
  const scoped_ptr<const Expression> hour_;
  const scoped_ptr<const Expression> minute_;
  const scoped_ptr<const Expression> second_;
};

}  // namespace

const Expression* ConstDateTime(const StringPiece& value) {
  // This implementation looks bad, because it looks like the resolving of the
  // parsing will be performed at evaluation time, for each row separately.
  // Fortunately, we do have constant folding in Supersonic, so actually this
  // calculation will be performed once, during binding time, and the expression
  // will be collapsed to some const DateTime.
  return ParseStringNulling(DATETIME, ConstString(value));
}

const Expression* ConstDateTimeFromMicrosecondsSinceEpoch(const int64& value) {
  return ConstDateTime(value);
}

const Expression* ConstDateTimeFromSecondsSinceEpoch(const double& value) {
  return ConstDateTime(static_cast<int64>(value * 1000000.));
}

const Expression* Now() {
  return ConstDateTime(static_cast<int64>(WallTime_Now() * 1000000.));
}

const Expression* UnixTimestamp(const Expression* const datetime) {
  return CreateExpressionForExistingBoundFactory(
      datetime, &BoundUnixTimestamp, "UNIXTIMESTAMP($0)");
}

const Expression* FromUnixTime(const Expression* const timestamp) {
  return CreateExpressionForExistingBoundFactory(
      timestamp, &BoundFromUnixTime, "FROMUNIXTIME($0)");
}

const Expression* MakeDate(const Expression* const year,
                           const Expression* const month,
                           const Expression* const day) {
  return CreateExpressionForExistingBoundFactory(
      year, month, day, &BoundMakeDate, "MAKEDATE($0, $1, $2)");
}

const Expression* MakeDatetime(const Expression* const year,
                               const Expression* const month,
                               const Expression* const day,
                               const Expression* const hour,
                               const Expression* const minute,
                               const Expression* const second) {
  return new MakeDatetimeExpression(year, month, day, hour, minute, second);
}

const Expression* Year(const Expression* const datetime) {
  return CreateExpressionForExistingBoundFactory(
      datetime, &BoundYear, "YEAR($0)");
}

const Expression* Quarter(const Expression* const datetime) {
  return CreateExpressionForExistingBoundFactory(
      datetime, &BoundQuarter, "QUARTER($0)");
}

const Expression* Month(const Expression* const datetime) {
  return CreateExpressionForExistingBoundFactory(
      datetime, &BoundMonth, "MONTH($0)");
}

const Expression* Day(const Expression* const datetime) {
  return CreateExpressionForExistingBoundFactory(
      datetime, &BoundDay, "DAY($0)");
}

const Expression* AddMinutes(const Expression* const datetime,
                             const Expression* const number_of_minutes) {
  return new TypedBinaryExpression<OPERATOR_ADD_MINUTES, DATETIME,
      INT64, DATETIME>(datetime, number_of_minutes);
}

const Expression* AddMinute(const Expression* const datetime) {
  return AddMinutes(datetime, ConstInt64(1LL));
}

const Expression* AddDays(const Expression* const datetime,
                          const Expression* const number_of_days) {
  return new TypedBinaryExpression<OPERATOR_ADD_DAYS, DATETIME,
      INT64, DATETIME>(datetime, number_of_days);
}

const Expression* AddDay(const Expression* const datetime) {
  return AddDays(datetime, ConstInt64(1LL));
}

const Expression* AddMonths(const Expression* const datetime,
                                  const Expression* const number_of_months) {
  return new TypedBinaryExpression<OPERATOR_ADD_MONTHS, DATETIME,
      INT64, DATETIME>(datetime, number_of_months);
}

const Expression* AddMonth(const Expression* const datetime) {
  return AddMonths(datetime, ConstInt64(1LL));
}

const Expression* Weekday(const Expression* const datetime) {
  return CreateExpressionForExistingBoundFactory(
      datetime, &BoundWeekday, "WEEKDAY($0)");
}

const Expression* YearDay(const Expression* const datetime) {
  return CreateExpressionForExistingBoundFactory(
      datetime, &BoundYearDay, "YEARDAY($0)");
}

const Expression* Hour(const Expression* const datetime) {
  return CreateExpressionForExistingBoundFactory(
      datetime, &BoundHour, "HOUR($0)");
}

const Expression* Minute(const Expression* const datetime) {
  return CreateExpressionForExistingBoundFactory(
      datetime, &BoundMinute, "MINUTE($0)");
}

const Expression* Second(const Expression* const datetime) {
  return CreateExpressionForExistingBoundFactory(
      datetime, &BoundSecond, "SECOND($0)");
}

const Expression* Microsecond(const Expression* const datetime) {
  return CreateExpressionForExistingBoundFactory(
      datetime, &BoundMicrosecond, "MICROSECOND($0)");
}

const Expression* YearLocal(const Expression* const datetime) {
  return CreateExpressionForExistingBoundFactory(
      datetime, &BoundYearLocal, "YEAR_LOCAL($0)");
}

const Expression* QuarterLocal(const Expression* const datetime) {
  return CreateExpressionForExistingBoundFactory(
      datetime, &BoundQuarterLocal, "QUARTER_LOCAL($0)");
}

const Expression* MonthLocal(const Expression* const datetime) {
  return CreateExpressionForExistingBoundFactory(
      datetime, &BoundMonthLocal, "MONTH_LOCAL($0)");
}

const Expression* DayLocal(const Expression* const datetime) {
  return CreateExpressionForExistingBoundFactory(
      datetime, &BoundDayLocal, "DAY_LOCAL($0)");
}

const Expression* WeekdayLocal(const Expression* const datetime) {
  return CreateExpressionForExistingBoundFactory(
      datetime, &BoundWeekdayLocal, "WEEKDAY_LOCAL($0)");
}

const Expression* YearDayLocal(const Expression* const datetime) {
  return CreateExpressionForExistingBoundFactory(
      datetime, &BoundYearDayLocal, "YEAR_DAY_LOCAL($0)");
}

const Expression* HourLocal(const Expression* const datetime) {
  return CreateExpressionForExistingBoundFactory(
      datetime, &BoundHourLocal, "HOUR_LOCAL($0)");
}

const Expression* MinuteLocal(const Expression* const datetime) {
  return CreateExpressionForExistingBoundFactory(
      datetime, &BoundMinuteLocal, "MINUTE_LOCAL($0)");
}

const Expression* SecondLocal(const Expression* const datetime) {
  return Second(datetime);
}

const Expression* MicrosecondLocal(const Expression* const datetime) {
  return Microsecond(datetime);
}

const Expression* DateFormat(const Expression* const datetime,
                             const Expression* const format) {
  return CreateExpressionForExistingBoundFactory(
      datetime, format, &BoundDateFormat, "DATE_FORMAT($0, $1)");
}

const Expression* DateFormatLocal(const Expression* const datetime,
                                  const Expression* const format) {
  return CreateExpressionForExistingBoundFactory(
      datetime, format, &BoundDateFormatLocal, "DATE_FORMAT_LOCAL($0, $1)");
}

}  // namespace supersonic
