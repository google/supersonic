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

#include "supersonic/proto/supersonic.pb.h"
#include "supersonic/testing/block_builder.h"
#include "supersonic/testing/expression_test_helper.h"
#include "gtest/gtest.h"

namespace supersonic {
class Expression;

namespace {

// Utility class which sets up the specified time zone flag and runs tzset() on
// construction and reverts the change on destruction. Will fail if there are
// errors during calls to setenv() and/or unsetenv() .
class TimeZoneFlagSaver {
 public:
  TimeZoneFlagSaver(const string& time_zone_flag, const string& time_zone)
      : original_time_zone_(NULL),
        time_zone_flag_(time_zone_flag),
        time_zone_(time_zone) {
    InitFlag();
  }

  ~TimeZoneFlagSaver() {
    RestoreFlag();
  }

 private:
  void InitFlag() {
    original_time_zone_ = getenv(time_zone_flag_.c_str());
    ASSERT_EQ(0, setenv(time_zone_flag_.c_str(), time_zone_.c_str(), 1));
    tzset();
  }

  void RestoreFlag() {
    if (original_time_zone_ == NULL) {
      ASSERT_EQ(0, unsetenv(time_zone_flag_.c_str()));
    } else {
      ASSERT_EQ(0, setenv(time_zone_flag_.c_str(), original_time_zone_, 1));
    }
    tzset();
  }

  const char* original_time_zone_;
  string time_zone_flag_;
  string time_zone_;

  DISALLOW_COPY_AND_ASSIGN(TimeZoneFlagSaver);
};

// ---------------------- Unix Timestamp -------------------

TEST(DateTimeExpressionTest, BindingSuccessTimestamps) {
  TestUnaryBinding(
      &UnixTimestamp, DATETIME, "UNIXTIMESTAMP($0)", INT64,    false);
  TestUnaryBinding(
      &FromUnixTime,  INT64,    "FROMUNIXTIME($0)",  DATETIME, false);
}

TEST(DateTimeExpressionTest, BindingSuccessDateFunctions) {
  TestUnaryBinding(&Year,        DATETIME, "YEAR($0)",        INT32, false);
  TestUnaryBinding(&Month,       DATETIME, "MONTH($0)",       INT32, false);
  TestUnaryBinding(&Day,         DATETIME, "DAY($0)",         INT32, false);
  TestUnaryBinding(&Weekday,     DATETIME, "WEEKDAY($0)",     INT32, false);
  TestUnaryBinding(&YearDay,     DATETIME, "YEARDAY($0)",     INT32, false);
  TestUnaryBinding(&Hour,        DATETIME, "HOUR($0)",        INT32, false);
  TestUnaryBinding(&Minute,      DATETIME, "MINUTE($0)",      INT32, false);
  TestUnaryBinding(&Second,      DATETIME, "SECOND($0)",      INT32, false);
  TestUnaryBinding(&Microsecond, DATETIME, "MICROSECOND($0)", INT32, false);
}

TEST(DateTimeExpressionTest, BindingSuccessMakeDate) {
  TestTernaryBinding(&MakeDate, UINT32, INT64, INT64,
                     "MAKEDATE(CAST_UINT32_TO_INT64($0), $1, $2)",
                     DATETIME, false);
}

TEST(DateTimeExpressionTest, BindingSuccessWithCast) {
  TestUnaryBinding(
      &Year, DATE, "YEAR(CAST_DATE_TO_DATETIME($0))", INT32, false);
}

TEST(DateTimeExpressionTest, BindingFailures) {
  TestBindingFailure(&UnixTimestamp, INT64);
  TestBindingFailure(&FromUnixTime, DATETIME);
  TestBindingFailure(&Hour, INT32);
  TestBindingFailure(&Year, STRING);
  TestBindingFailure(&UnixTimestamp, DOUBLE);
  TestBindingFailure(&MakeDate, DOUBLE, INT32, INT32);
}

// Several instantiations of the ConstDateTime functions for testing puproses.
const Expression* Epoch() {
  return ConstDateTime("1970/01/01-00:00:00");
}
const Expression* Today() {
  return ConstDateTime("2011/03/22-16:51:44");
}
const Expression* BeforeEpoch() {
  return ConstDateTime("1969/03/12-13.11.11");
}
const Expression* MalformedDateTime() {
  return ConstDateTime("1980:03:03-12:00:00");
}

// Time zone constants.
const char* kTimeZoneFlag = "TZ";
const char* kCESTZone = "CET-1CEST,M3.5.0,M10.5.0/3";
const char* kNPTModifiedZone = "NPT+5:45";
const char* kPacificZone = "US/Pacific";

TEST(DateTimeExpressionTest, ConstDateTime) {
  TestEvaluation(BlockBuilder<DATETIME>().AddRow(0LL).Build(), &Epoch);
  TestEvaluation(BlockBuilder<DATETIME>().AddRow(1300812704000000LL).Build(),
                 &Today);
  TestEvaluation(BlockBuilder<DATETIME>().AddRow(__).Build(), &BeforeEpoch);
  TestEvaluation(BlockBuilder<DATETIME>().AddRow(__).Build(),
                 &MalformedDateTime);
}

template<int64 value>
const Expression* DateTimeFromMicroseconds() {
  return ConstDateTimeFromMicrosecondsSinceEpoch(value);
}

TEST(DateTimeExpressionTest, ConstDateTimeFromMicrosecondsSinceEpoch) {
  TestEvaluation(BlockBuilder<DATETIME>().AddRow(12312LL).Build(),
                 &DateTimeFromMicroseconds<12312LL>);
  TestEvaluation(BlockBuilder<DATETIME>().AddRow(1300812704000000LL).Build(),
                 &DateTimeFromMicroseconds<1300812704000000LL>);
}

// Value is nanoseconds since epoch.
template<int64 value>
const Expression* DateTimeFromSeconds() {
  return ConstDateTimeFromSecondsSinceEpoch(
      static_cast<double>(value) / 1000000000.);
}

TEST(DateTimeExpressionTest, ConstDateTimeFromSecondsSinceEpoch) {
  TestEvaluation(BlockBuilder<DATETIME>().AddRow(123123123LL).Build(),
                 &DateTimeFromSeconds<123123123000LL>);
  TestEvaluation(BlockBuilder<DATETIME>().AddRow(0LL).Build(),
                 &DateTimeFromSeconds<100LL>);
}

// Now obviously isn't deterministic. If you want to check it still works,
// uncomment the code, the test will fail, but you will learn what Supersonic
// considers to be "Now" (remember this is UTC time).
//
// TEST(DateTimeExpressionTest, Now) {
//   TestEvaluation(BlockBuilder<DATETIME>().AddRow(0LL).Build(), &Now);
// }

TEST(DateTimeExpressionTest, UnixTimestamp) {
  TestEvaluation(BlockBuilder<DATETIME, INT64>()
      .AddRow(0LL,                0LL)
      .AddRow(__,                 __)
      .AddRow(1234000LL,          1LL)
      .AddRow(1287417385000000LL, 1287417385LL)
      .Build(), &UnixTimestamp);
}

TEST(DateTimeExpressionTest, FromUnixTime) {
  TestEvaluation(BlockBuilder<INT64, DATETIME>()
      .AddRow(0LL,                0LL)
      .AddRow(__,                 __)
      .AddRow(1234LL,             1234000000LL)
      .AddRow(1287417385LL,       1287417385000000LL)
      .Build(), &FromUnixTime);

  TestEvaluation(BlockBuilder<INT32, DATETIME>()
      .AddRow(0,     0LL)
      .AddRow(123,   123000000LL)
      .AddRow(17,    17000000LL)
      .Build(), &FromUnixTime);

  TestEvaluationFailure(BlockBuilder<INT64>()
      .AddRow(-1LL)
      .AddRow(-213213LL)
      .AddRow(-1234567890LL)
      .Build(), &FromUnixTime);
}

TEST(DateTimeExpressionTest, Year) {
  TestEvaluation(BlockBuilder<DATETIME, INT32>()
      .AddRow(1287417385000000LL,  2010)
      .AddRow(1291382646000000LL,  2010)
      .AddRow(0LL,                 1970)
      .AddRow(-1000000LL,          1969)
      .Build(), &Year);
}

// Tests if Year and other datetime functions using gmtime_r support 'big'
// years (>2038). See b/6024922.
TEST(DateTimeExpressionTest, BigYear) {
#if defined(__x86_64__)
  TestEvaluation(BlockBuilder<DATETIME, INT32>()
      // 10000/01/11-13:46:40
      .AddRow(253403214400000000LL, 10000)
      // 10003/03/13-23:35:00
      .AddRow(253503214500000000LL, 10003)
      .Build(), &Year);
#endif
}

TEST(DateTimeExpressionTest, YearWithCast) {
  TestEvaluation(BlockBuilder<DATE, INT32>()
      .AddRow(0,    1970)
      .AddRow(364,  1970)
      .AddRow(365,  1971)
      .AddRow(1095, 1972)
      .AddRow(1096, 1973)
      .Build(), &Year);
}

TEST(DateTimeExpressionTest, Month) {
  TestEvaluation(BlockBuilder<DATETIME, INT32>()
      .AddRow(1287417385000000LL,  10)
      .AddRow(1291382646000000LL,  12)
      .AddRow(0LL,                 1)
      .AddRow(-1000000LL,          12)
      .Build(), &Month);
}

TEST(DateTimeExpressionTest, Quarter) {
  TestEvaluation(BlockBuilder<DATETIME, INT32>()
      .AddRow(1287417385000000LL,  4)
      .AddRow(1291382646000000LL,  4)
      .AddRow(0LL,                 1)
      .AddRow(1183784400000000LL,  3)  // 7.07.2007.
      .Build(), &Quarter);
}

TEST(DateTimeExpressionTest, Day) {
  TestEvaluation(BlockBuilder<DATETIME, INT32>()
      .AddRow(1287417385000000LL,  18)
      .AddRow(1291382646000000LL,  3)
      .AddRow(0LL,                 1)
      .AddRow(-1000000LL,          31)
      .Build(), &Day);
}

TEST(DateTimeExpressionTest, Weekday) {
  TestEvaluation(BlockBuilder<DATETIME, INT32>()
      .AddRow(1287417385000000LL,  0)
      .AddRow(1291382646000000LL,  4)
      .AddRow(0LL,                 3)
      .AddRow(-1000000LL,          2)
      .AddRow(__,                  __)
      .Build(), &Weekday);
}

TEST(DateTimeExpressionTest, YearDay) {
  TestEvaluation(BlockBuilder<DATETIME, INT32>()
      .AddRow(1287417385000000LL,  291)
      .AddRow(1291382646000000LL,  337)
      .AddRow(0LL,                 1)
      .AddRow(-1000000LL,          365)
      .Build(), &YearDay);
}

TEST(DateTimeExpressionTest, Hour) {
  TestEvaluation(BlockBuilder<DATETIME, INT32>()
      .AddRow(1287417385000000LL,  15)
      .AddRow(1291382646000000LL,  13)
      .AddRow(__,                  __)
      .AddRow(0LL,                 0)
      .AddRow(-1000000LL,          23)
      .Build(), &Hour);
}

TEST(DateTimeExpressionTest, Minute) {
  TestEvaluation(BlockBuilder<DATETIME, INT32>()
      .AddRow(1287417385000000LL,  56)
      .AddRow(1291382646000000LL,  24)
      .AddRow(0LL,                 0)
      .AddRow(-1000000LL,          59)
      .Build(), &Minute);
}

// I'm running local tests under CET/CEST, which is -1 or -2 to UTC (depending
// on local savings time).
TEST(DateTimeExpressionTest, YearLocal) {
  TimeZoneFlagSaver tz_saver(kTimeZoneFlag, kCESTZone);
  TestEvaluation(BlockBuilder<DATETIME, INT32>()
      .AddRow(1293832810000000LL, 2010)  // 31.12.2010 22:00:10 UTC.
      .AddRow(1293836410000000LL, 2011)  // 31.12.2010 23:00:10 UTC.
      .AddRow(1293840010000000LL, 2011)  //  1.01.2011  0:00:10 UTC.
      .AddRow(1293850810000000LL, 2011)  //  1.01.2011  3:00:10 UTC.
      .AddRow(1293850810000000LL, 2011)  //  1.01.2011  6:00:10 UTC.
      .AddRow(1293865210000000LL, 2011)  //  1.01.2011  7:00:10 UTC.
      .AddRow(1293868810000000LL, 2011)  //  1.01.2011  8:00:10 UTC.
      .AddRow(1293872410000000LL, 2011)  //  1.01.2011  9:00:10 UTC.
      .Build(), &YearLocal);
}

TEST(DateTimeExpressionTest, QuarterLocal) {
  TimeZoneFlagSaver tz_saver(kTimeZoneFlag, kCESTZone);
  TestEvaluation(BlockBuilder<DATETIME, INT32>()
      .AddRow(1293832810000000LL, 4)  // 31.12.2010 22:00:10 UTC.
      .AddRow(1293836410000000LL, 1)  // 31.12.2010 23:00:10 UTC.
      .Build(), &QuarterLocal);
}

TEST(DateTimeExpressionTest, MonthLocal) {
  TimeZoneFlagSaver tz_saver(kTimeZoneFlag, kCESTZone);
  TestEvaluation(BlockBuilder<DATETIME, INT32>()
      .AddRow(1293832810000000LL, 12)  // 31.12.2010 22:00:10 UTC.
      .AddRow(1293836410000000LL, 1)   // 31.12.2010 23:00:10 UTC.
      .Build(), &MonthLocal);
}

TEST(DateTimeExpressionTest, DayLocal) {
  TimeZoneFlagSaver tz_saver(kTimeZoneFlag, kCESTZone);
  TestEvaluation(BlockBuilder<DATETIME, INT32>()
      .AddRow(1293832810000000LL, 31)  // 31.12.2010 22:00:10 UTC.
      .AddRow(1293836410000000LL, 1)   // 31.12.2010 23:00:10 UTC.
      .Build(), &DayLocal);
}

TEST(DateTimeExpressionTest, WeekdayLocal) {
  TimeZoneFlagSaver tz_saver(kTimeZoneFlag, kCESTZone);
  TestEvaluation(BlockBuilder<DATETIME, INT32>()
      .AddRow(1293832810000000LL, 4)  // 31.12.2010 22:00:10 UTC.
      .AddRow(1293836410000000LL, 5)  // 31.12.2010 23:00:10 UTC.
      .Build(), &WeekdayLocal);
}

TEST(DateTimeExpressionTest, YearDayLocal) {
  TimeZoneFlagSaver tz_saver(kTimeZoneFlag, kCESTZone);
  TestEvaluation(BlockBuilder<DATETIME, INT32>()
      .AddRow(1293832810000000LL, 365)  // 31.12.2010 22:00:10 UTC.
      .AddRow(1293836410000000LL, 1)    // 31.12.2010 23:00:10 UTC.
      .Build(), &YearDayLocal);
}

TEST(DateTimeExpressionTest, HourLocal) {
  TimeZoneFlagSaver tz_saver(kTimeZoneFlag, kCESTZone);
  TestEvaluation(BlockBuilder<DATETIME, INT32>()
      .AddRow(1293832810000000LL, 23)  // 31.12.2010 22:00:10 UTC.
      .AddRow(1293836410000000LL, 0)   // 31.12.2010 23:00:10 UTC.
      .AddRow(1319929210000000LL, 1)   // 29.10.2011 23:00:10 UTC
      .AddRow(1319932810000000LL, 2)   // 30.10.2011  0:00:10 UTC.
      // And now Daylight Savings kicks in (or rather, out).
      .AddRow(1319936410000000LL, 2)   // 30.10.2011  1:00:10 UTC.
      .AddRow(1319940010000000LL, 3)   // 30.10.2011  2:00:10 UTC.
      .Build(), &HourLocal);
}

// Testing the MinuteLocal function in CET is pretty useless, so I'm testing it
// in Nepal time (which is UTC +5:45).
TEST(DateTimeExpressionTest, MinuteLocal) {
  TimeZoneFlagSaver tz_saver(kTimeZoneFlag, kNPTModifiedZone);
  TestEvaluation(BlockBuilder<DATETIME, INT32>()
      .AddRow(1287417385000000LL,  11)
      .AddRow(1291382646000000LL,  39)
      .AddRow(0LL,                 15)
      .AddRow(-1000000LL,          14)
      .Build(), &MinuteLocal);
}

TEST(DateTimeExpressionTest, Second) {
  TestEvaluation(BlockBuilder<DATETIME, INT32>()
      .AddRow(__,                  __)
      .AddRow(1287417385000000LL,  25)
      .AddRow(1291382646000000LL,  6)
      .AddRow(0LL,                 0)
      .AddRow(-1000000LL,          59)
      .Build(), &Second);
}

TEST(DateTimeExpressionTest, Microsecond) {
  TestEvaluation(BlockBuilder<DATETIME, INT32>()
      .AddRow(1287417385000000LL,  0)
      .AddRow(1291382646000020LL,  20)
      .AddRow(0LL,                 0)
      .AddRow(-1LL,                999999)
      .Build(), &Microsecond);
}

TEST(DateTimeExpressionTest, AddMinute) {
  TestEvaluation(BlockBuilder<DATETIME, DATETIME>()
      .AddRow(825465600000000LL,    825465660000000LL)
      .AddRow(825552000000000LL,    825552060000000LL)
      .AddRow(1014854400000000LL,   1014854460000000LL)
      .Build(), &AddMinute);
}

TEST(DateTimeExpressionTest, AddMinutes) {
  TestEvaluation(BlockBuilder<DATETIME, INT64, DATETIME>()
      .AddRow(1014940800000000LL,  0LL,  1014940800000000LL)
      .AddRow(825552000000000LL,   1LL,   825552060000000LL)
      .AddRow(951696000000000LL,   2LL,   951696120000000LL)
      .AddRow(1287619200000000LL, 50LL,  1287622200000000LL)
      .Build(), &AddMinutes);
}

TEST(DateTimeExpressionTest, AddDay) {
  TestEvaluation(BlockBuilder<DATETIME, DATETIME>()
      .AddRow(825465600000000LL,    825552000000000LL)   // 28.02.1996
      .AddRow(825552000000000LL,    825638400000000LL)   // 29.02.1996
      .AddRow(1014854400000000LL,   1014940800000000LL)  // 28.02.2002
      .Build(), &AddDay);
}

TEST(DateTimeExpressionTest, AddDays) {
  TestEvaluation(BlockBuilder<DATETIME, INT64, DATETIME>()
      .AddRow(1014940800000000LL,  0LL,  1014940800000000LL)
      .AddRow(825552000000000LL,   1LL,   825638400000000LL)
      .AddRow(951696000000000LL,   2LL,   951868800000000LL)
      .AddRow(1287619200000000LL, 50LL,  1291939200000000LL)
      .Build(), &AddDays);
}

TEST(DateTimeExpressionTest, AddMonth) {
  TestEvaluation(BlockBuilder<DATETIME, DATETIME>()
      .AddRow(1297836000000000LL,  1300255200000000LL)
      .AddRow(1293170412345678LL,  1295848812345678LL)
      .Build(), &AddMonth);
}

TEST(DateTimeExpressionTest, AddMonths) {
  TestEvaluation(BlockBuilder<DATETIME, INT64, DATETIME>()
      .AddRow(1014940800000000LL,  0LL,  1014940800000000LL)
      .AddRow(1297836000000000LL,  1LL,  1300255200000000LL)
      .AddRow(951868800001234LL,  24LL,  1014940800001234LL)
      .AddRow(__,                  7LL,  __)
      .Build(), &AddMonths);
}

TEST(DateTimeExpressionTest, DateFormat) {
  // The timestamps used throughout the test are the midnight of 20th July 1980
  // and the noon of 1st Jan 1970.
  TestEvaluation(BlockBuilder<DATETIME, STRING, STRING>()
      .AddRow(332899200000000LL, "%C %d %D",    "19 20 07/20/80")
      .AddRow(332899200000000LL, "%F %H %I",    "1980-07-20 00 12")
      .AddRow(43200000000LL,     "%F %H %d %p", "1970-01-01 12 01 PM")
      .AddRow(332899200000000LL, "%T %M",       "00:00:00 00")
      .Build(), &DateFormat);

  // The max length of the output is 32 bytes.
  TestEvaluation(BlockBuilder<DATETIME, STRING, STRING>()
      .AddRow(__,  "",          __)
      .AddRow(0LL, "%F%F",        "1970-01-011970-01-01")
      .AddRow(0LL, "%F %F %D..",  "1970-01-01 1970-01-01 01/01/70..")
      .AddRow(0LL, "%F %F %D ..", "")
      .AddRow(0LL, "%F%F%F%F",    "")
      .Build(), &DateFormat);
}

TEST(DateTimeExpressionTest, DateFormatLocal) {
  TimeZoneFlagSaver tz_saver(kTimeZoneFlag, kPacificZone);
  TestEvaluation(BlockBuilder<DATETIME, STRING, STRING>()
      .AddRow(332899200000000LL, "%T",  "17:00:00")
      .Build(), &DateFormatLocal);
}

// ---------------------- Make Date --------------------------

TEST(DateTimeExpressionTest, MakeDate) {
  TestEvaluation(BlockBuilder<INT32, INT32, INT32, DATETIME>()
      .AddRow(1970,  1,  1, 0LL)                 // Epoch.
      .AddRow(1996,  2, 28, 825465600000000LL)   // Leap year.
      .AddRow(1996,  2, 29, 825552000000000LL)
      .AddRow(1996,  3,  1, 825638400000000LL)
      .AddRow(2000,  2, 28, 951696000000000LL)   // Also a leap year.
      .AddRow(2000,  2, 29, 951782400000000LL)
      .AddRow(2000,  3,  1, 951868800000000LL)
      .AddRow(2002,  2, 28, 1014854400000000LL)  // Not a leap year.
      .AddRow(2002,  3,  1, 1014940800000000LL)
      .AddRow(2010, 10, 21, 1287619200000000LL)  // Random date (around now).
      .AddRow(2010, 12, 10, 1291939200000000LL)  // Random date (around now).
      .AddRow(2012,  6, 12, 1339459200000000LL)  // Random date (Euro 2012).
      .AddRow(2112,  6, 12, 4495132800000000LL)  // Out of int32 range.
      .Build(), &MakeDate);

  TestEvaluation(BlockBuilder<INT32, INT64, UINT64, DATETIME>()
      .AddRow(__,    1LL,  1LL,  __)
      .AddRow(2010,  __,   21LL, __)
      .AddRow(2012,  6LL,  __,   __)
      .AddRow(2005,  1LL,  1LL,  1104537600000000LL)
      .Build(), &MakeDate);

  TestEvaluationFailure(BlockBuilder<INT32, INT32, INT32>()
      .AddRow(1969, 12, 30)
      .AddRow(0,    0,  0)
      .AddRow(-1,   0,  0)
      .Build(), &MakeDate);
}

TEST(DateTimeExpressionTest, MakeDatetime) {
  TestEvaluation(BlockBuilder<INT32, INT32, INT32, INT32, INT32, INT32,
                              DATETIME>()
      .AddRow(1970,  1,  1,   0,   0,   0, 0LL)                 // Epoch.
      .AddRow(2000,  2, 28,   1,   0,   0, 951699600000000LL)   // Leap year.
      .AddRow(2000,  2, 28,  23,  59,  59, 951782399000000LL)
      .AddRow(2000,  3,  1,   0,   0,   0, 951868800000000LL)
      .AddRow(2002,  2, 28,  12,  12,  12, 1014898332000000LL)  // No leap.
      .AddRow(2012,  6, 12,  18,   0,   0, 1339524000000000LL)  // Euro 2012.
      // Out of int32 range.
      .AddRow(2112,  6, 12,   0,   0,   0, 4495132800000000LL)
      .Build(), &MakeDatetime);
}

TEST(DateTimeExpressionTest, MakeDatetimeNulls) {
  TestEvaluation(BlockBuilder<INT32, INT32, INT32, INT32, INT32, INT32,
                              DATETIME>()
      .AddRow(1970,  1,  1,   0,   0,  __, __)
      .AddRow(1970,  1,  1,   0,   0,   0, 0LL)
      .AddRow(1970,  1,  1,   0,  __,   0, __)
      .AddRow(1970,  1,  1,  __,   0,   0, __)
      .AddRow(1969, 12, 31,  23,  59,  59, __)
      .AddRow(1970,  1, __,   0,   0,   0, __)
      .AddRow(1980, __,  1,   0,   0,   0, __)
      .AddRow(__,    1,  1,   0,   0,   0, __)
      .Build(), &MakeDatetime);
}

}  // namespace
}  // namespace supersonic
