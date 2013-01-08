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
// Definitions of the individual operations for expressions defined in
// date_expressions.h

#ifndef SUPERSONIC_EXPRESSION_CORE_DATE_EVALUATORS_H_
#define SUPERSONIC_EXPRESSION_CORE_DATE_EVALUATORS_H_

#include <stddef.h>

#include "supersonic/utils/integral_types.h"
#include "supersonic/base/infrastructure/bit_pointers.h"
#include "supersonic/utils/strings/stringpiece.h"

namespace supersonic {

class Arena;

namespace operators {

static const int64 kMillion = 1000000LL;

// Returns a datetime from a unix timestamp.
// We store the datetime as timestamps already, only calculated in microseconds,
// not seconds.
struct FromUnixTime {
  int64 operator()(const int64 arg) const { return arg * kMillion; }
};

// Returns unix timestamp from datetime.
// We store the datetime as timestamps already, only calculated in microseconds,
// not seconds.
struct UnixTimestamp {
  int64 operator()(const int64 arg) const { return arg / kMillion; }
};

struct MakeDate {
  int64 operator()(const int64 year, const int64 month, const int64 day);
};

struct Year {
  int32 operator()(const int64 datetime);
};

struct YearLocal {
  int32 operator()(const int64 datetime);
};

struct Quarter {
  int32 operator()(const int64 datetime);
};

struct QuarterLocal {
  int32 operator()(const int64 datetime);
};

struct Month {
  int32 operator()(const int64 datetime);
};

struct MonthLocal {
  int32 operator()(const int64 datetime);
};

struct Day {
  int32 operator()(const int64 datetime);
};

struct DayLocal {
  int32 operator()(const int64 datetime);
};

struct Weekday {
  int32 operator()(const int64 datetime);
};

struct WeekdayLocal {
  int32 operator()(const int64 datetime);
};

struct YearDay {
  int32 operator()(const int64 datetime);
};

struct YearDayLocal {
  int32 operator()(const int64 datetime);
};

struct Hour {
  int32 operator()(const int64 datetime);
};

struct HourLocal {
  int32 operator()(const int64 datetime);
};

struct Minute {
  int32 operator()(const int64 datetime);
};

struct MinuteLocal {
  int32 operator()(const int64 datetime);
};

struct Second {
  int32 operator()(const int64 datetime);
};

struct Microsecond {
  int32 operator()(const int64 datetime);
};

struct AddMinutes {
  int64 operator()(const int64 datetime, const int64 number_of_minutes) {
    return datetime + number_of_minutes * 60LL * kMillion;
  }
};

struct AddDays {
  int64 operator()(const int64 datetime, const int64 number_of_days) {
    return datetime + number_of_days * 24LL * 3600LL * kMillion;
  }
};

struct AddMonths {
  int64 operator()(const int64 datetime, const int64 number_of_months);
};

// Note - this is not INT64-compliant.
struct DateFormat {
  StringPiece operator()(int64 datetime, const StringPiece& format,
                         Arena* arena);
};

struct DateFormatLocal {
  StringPiece operator()(int64 datetime, const StringPiece& format,
                         Arena* arena);
};

}  // namespace operators
namespace failers {

// Returns the number of rows where the MakeDate function fails.
struct MakeDateFailer {
  int operator()(const int64* left_data,
                 bool_const_ptr left_is_null,
                 const int64* middle_data,
                 bool_const_ptr middle_is_null,
                 const int64* right_data,
                 bool_const_ptr right_is_null,
                 size_t row_count);
};

}  // namespace failers
}  // namespace supersonic

#endif  // SUPERSONIC_EXPRESSION_CORE_DATE_EVALUATORS_H_
