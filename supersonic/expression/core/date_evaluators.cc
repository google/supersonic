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

#include "supersonic/expression/core/date_evaluators.h"

#include <cstring>
#include <ctime>
#include <string>
namespace supersonic {using std::string; }

#include "supersonic/utils/integral_types.h"
#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/base/memory/arena.h"

namespace supersonic {

namespace {
// This code is in the large part shamelessly stolen from base/time_support.h.
// The reason I do this is that time_support.h only offers functions that return
// long and time_t, which causes our makedate to fail on 32-bit architectures,
// which seem to define time_t as int32.
int64 mkgmtime_int64(int64 year, int64 month, int64 day) {
  static const int64 month_day[12] = {0, 31, 59, 90, 120, 151,
                                      181, 212, 243, 273, 304, 334};

  // The calculation is basically easy; leap years are the main difficulty.
  int64 real_month = month % 12;
  // Here we deal with months outside the [0,11] range. Note that we depend on
  // this functionality, do not remove this.
  int64 real_year = year + month / 12;
  if (real_month < 0) {  // Negative values % 12 are still negative.
    real_month += 12;
    --real_year;
  }

  // The number of full Februaries since 1970.
  const int64 februaries = (real_month > 1) ? real_year + 1 : real_year;
  int64 result = 60 * 60 * 24           // seconds in a day.
      * (month_day[static_cast<int>(real_month)] + day - 1  // days from 1.01.
      + 365 * real_year                 // Year = 365 days.
      + (februaries + 1) / 4            // Every 4 years is leap...
      - (februaries + 69) / 100         // Except centuries...
      + (februaries + 369) / 400);      // Except 400s.
  return result < 0 ? -1 : result;
}

}  // namespace

namespace operators {

int64 MakeDate::operator()(const int64 year,
                           const int64 month,
                           const int64 day) {
  return kMillion * mkgmtime_int64(year - 1970LL, month - 1LL, day);
}

int64 AddMonths::operator()(const int64 datetime,
                            const int64 number_of_months) {
  tm result;
  time_t offset_time = datetime / kMillion;
  gmtime_r(&offset_time, &result);
  // We take advantage of the fact that mkgmtime_int64 is written so that
  // it can deal with the month argument being outside the [0,11] range.
  return kMillion * mkgmtime_int64(result.tm_year - 70LL,
                                   result.tm_mon + number_of_months,
                                   result.tm_mday) +
      // The offset from a full day.
      datetime % (24LL * 3600LL * kMillion);
}

int32 Year::operator()(const int64 datetime) {
  tm result;
  time_t offset_time = datetime / kMillion;
  gmtime_r(&offset_time, &result);
  // tm has years from 1900.
  return result.tm_year + 1900;
}

int32 YearLocal::operator()(const int64 datetime) {
  tm result;
  time_t offset_time = datetime / kMillion;
  localtime_r(&offset_time, &result);
  // tm has years from 1900.
  return result.tm_year + 1900;
}

int32 Quarter::operator()(const int64 datetime) {
  tm result;
  time_t offset_time = datetime / 1000000LL;
  gmtime_r(&offset_time, &result);
  return (result.tm_mon / 3) + 1;
}

int32 QuarterLocal::operator()(const int64 datetime) {
  tm result;
  time_t offset_time = datetime / 1000000LL;
  localtime_r(&offset_time, &result);
  return (result.tm_mon / 3) + 1;
}

int32 Month::operator()(const int64 datetime) {
  tm result;
  time_t offset_time = datetime / kMillion;
  gmtime_r(&offset_time, &result);
  // tm has months in 0..11 range.
  return result.tm_mon + 1;
}

int32 MonthLocal::operator()(const int64 datetime) {
  tm result;
  time_t offset_time = datetime / kMillion;
  localtime_r(&offset_time, &result);
  // tm has months in 0..11 range.
  return result.tm_mon + 1;
}

int32 Day::operator()(const int64 datetime) {
  tm result;
  time_t offset_time = datetime / kMillion;
  gmtime_r(&offset_time, &result);
  return result.tm_mday;
}

int32 DayLocal::operator()(const int64 datetime) {
  tm result;
  time_t offset_time = datetime / kMillion;
  localtime_r(&offset_time, &result);
  return result.tm_mday;
}

int32 Weekday::operator()(const int64 datetime) {
  tm result;
  time_t offset_time = datetime / kMillion;
  gmtime_r(&offset_time, &result);
  // tm counts weekdays from Sunday.
  return (result.tm_wday + 6) % 7;
}

int32 WeekdayLocal::operator()(const int64 datetime) {
  tm result;
  time_t offset_time = datetime / kMillion;
  localtime_r(&offset_time, &result);
  // tm counts weekdays from Sunday.
  return (result.tm_wday + 6) % 7;
}

int32 YearDay::operator()(const int64 datetime) {
  tm result;
  time_t offset_time = datetime / kMillion;
  gmtime_r(&offset_time, &result);
  // tm counts yeardays from 0.
  return result.tm_yday + 1;
}

int32 YearDayLocal::operator()(const int64 datetime) {
  tm result;
  time_t offset_time = datetime / kMillion;
  localtime_r(&offset_time, &result);
  // tm counts yeardays from 0.
  return result.tm_yday + 1;
}

int32 Hour::operator()(int64 datetime) {
  if (datetime < 0LL) {
    // Move to same hour, 1st January 1970.
    datetime %= kMillion * 3600LL * 24LL;
    datetime += kMillion * 3600LL * 24LL;
  }
  int64 hours_since_epoch = datetime / (kMillion * 3600LL);
  return hours_since_epoch % 24LL;
}

int32 HourLocal::operator()(int64 datetime) {
  tm result;
  time_t offset_time = datetime / kMillion;
  localtime_r(&offset_time, &result);
  return result.tm_hour;
}

int32 Minute::operator()(int64 datetime) {
  if (datetime < 0LL) {
    // Move to the same minute, 1st January 1970, before 1AM.
    datetime %= kMillion * 60LL * 60LL;
    datetime += kMillion * 60LL * 60LL;
  }
  int64 minutes_since_epoch = datetime / (kMillion * 60LL);
  return minutes_since_epoch % 60LL;
}

int32 MinuteLocal::operator()(int64 datetime) {
  tm result;
  time_t offset_time = datetime / kMillion;
  localtime_r(&offset_time, &result);
  return result.tm_min;
}

int32 Second::operator()(int64 datetime) {
  if (datetime < 0LL) {
    // Move to the same second, 1st January 1970, before 0:01AM.
    datetime %= kMillion * 60LL;
    datetime += kMillion * 60LL;
  }
  int64 seconds_since_epoch = datetime / kMillion;
  return seconds_since_epoch % 60LL;
}

int32 Microsecond::operator()(const int64 datetime) {
  return (datetime < 0LL) ? kMillion - 1LL + ((datetime + 1LL) % kMillion)
      : datetime % kMillion;
}

// Note - this is not INT64-compliant.
StringPiece DateFormat::operator()(int64 datetime, const StringPiece& format,
                                   Arena* arena) {
  char buffer[33];
  struct tm time_result;
  time_t offset_time = static_cast<time_t>(datetime / kMillion);
  gmtime_r(&offset_time, &time_result);
  // StringPiece stores non-NULL-terminated bytes. We have to allocate
  // a place only to copy the whole format string, and terminate with a NULL.
  // Which is weird, maybe it would be better to write our own strftime.
  string format_string = format.ToString();
  size_t length =
      strftime(buffer, 33, format_string.c_str(), &time_result);
  char* new_str = static_cast<char *>(arena->AllocateBytes(length + 1));
  // TODO(onufry): Replace this with a gentler mechanism. Maybe the allocating
  // operators could take a bool*, on which they would set the error code?
  CHECK_NOTNULL(new_str);
  // TODO(onufry): modify this when the Arena is modified to allow cutting,
  // see the comment in Format.
  strncpy(new_str, buffer, length + 1);
  return StringPiece(new_str, length);
}

// See comments for DateFormat above..
StringPiece DateFormatLocal::operator()(int64 datetime,
                                        const StringPiece& format,
                                        Arena* arena) {
  char buffer[33];
  struct tm time_result;
  time_t offset_time = static_cast<time_t>(datetime / kMillion);
  localtime_r(&offset_time, &time_result);
  string format_string = format.ToString();
  size_t length =
      strftime(buffer, 33, format_string.c_str(), &time_result);
  char* new_str = static_cast<char *>(arena->AllocateBytes(length + 1));
  CHECK_NOTNULL(new_str);
  strncpy(new_str, buffer, length + 1);
  return StringPiece(new_str, length);
}

}  // namespace operators
namespace failers {

// TODO(onufry): Add a mechanism allowing to make failure checks at evaluation
// time to avoid double evaluation.
int MakeDateFailer::operator()(const int64* left_data,
                               bool_const_ptr left_is_null,
                               const int64* middle_data,
                               bool_const_ptr middle_is_null,
                               const int64* right_data,
                               bool_const_ptr right_is_null,
                               size_t row_count) {
  int failures = 0;
  for (int i = 0; i < row_count; ++i) {
    failures += ((!((left_is_null != NULL && left_is_null[i]) ||
                    (middle_is_null != NULL && middle_is_null[i]) ||
                    (right_is_null != NULL && right_is_null[i]))) &&
                 (mkgmtime_int64(left_data[i] - 1970, middle_data[i] - 1,
                                 right_data[i]) == -1LL));
  }
  return failures;
}

}  // namespace failers
}  // namespace supersonic
