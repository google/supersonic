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
// Author: onufry@google.com (Onufry Wojtaszczyk)
//
// Expressions on DATE and DATETIME, bound versions. See date_expressions.h for
// semantics.

#ifndef SUPERSONIC_EXPRESSION_CORE_DATE_BOUND_EXPRESSIONS_H_
#define SUPERSONIC_EXPRESSION_CORE_DATE_BOUND_EXPRESSIONS_H_



#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/types.h"

namespace supersonic {

// Returns the INT64 timestamp (microseconds from January 1st, 1970) of
// the specified date. Returns NULL if the date is NULL.
class BoundExpression;
class BufferAllocator;

FailureOrOwned<BoundExpression> BoundUnixTimestamp(BoundExpression* date,
                                                   BufferAllocator* allocator,
                                                   rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundFromUnixTime(BoundExpression* timestamp,
                                                  BufferAllocator* allocator,
                                                  rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundMakeDate(BoundExpression* year,
                                              BoundExpression* month,
                                              BoundExpression* day,
                                              BufferAllocator* allocator,
                                              rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundMakeDatetime(BoundExpression* year,
                                                  BoundExpression* month,
                                                  BoundExpression* day,
                                                  BoundExpression* hour,
                                                  BoundExpression* minute,
                                                  BoundExpression* second,
                                                  BufferAllocator* allocator,
                                                  rowcount_t max_row_count);

// Date specifics retrieval - UTC versions.
FailureOrOwned<BoundExpression> BoundYear(BoundExpression* datetime,
                                          BufferAllocator* allocator,
                                          rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundQuarter(BoundExpression* datetime,
                                             BufferAllocator* allocator,
                                             rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundMonth(BoundExpression* datetime,
                                           BufferAllocator* allocator,
                                           rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundDay(BoundExpression* datetime,
                                         BufferAllocator* allocator,
                                         rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundWeekday(BoundExpression* datetime,
                                             BufferAllocator* allocator,
                                             rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundYearDay(BoundExpression* datetime,
                                             BufferAllocator* allocator,
                                             rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundHour(BoundExpression* datetime,
                                          BufferAllocator* allocator,
                                          rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundMinute(BoundExpression* datetime,
                                            BufferAllocator* allocator,
                                            rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundSecond(BoundExpression* datetime,
                                            BufferAllocator* allocator,
                                            rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundMicrosecond(BoundExpression* datetime,
                                                 BufferAllocator* allocator,
                                                 rowcount_t max_row_count);

// Date specifics retrieval - local timezone versions. Deprecated.
FailureOrOwned<BoundExpression> BoundYearLocal(BoundExpression* datetime,
                                               BufferAllocator* allocator,
                                               rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundQuarterLocal(BoundExpression* datetime,
                                                  BufferAllocator* allocator,
                                                  rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundMonthLocal(BoundExpression* datetime,
                                                BufferAllocator* allocator,
                                                rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundDayLocal(BoundExpression* datetime,
                                              BufferAllocator* allocator,
                                              rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundWeekdayLocal(BoundExpression* datetime,
                                                  BufferAllocator* allocator,
                                                  rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundYearDayLocal(BoundExpression* datetime,
                                                  BufferAllocator* allocator,
                                                  rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundHourLocal(BoundExpression* datetime,
                                               BufferAllocator* allocator,
                                               rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundMinuteLocal(BoundExpression* datetime,
                                                 BufferAllocator* allocator,
                                                 rowcount_t max_row_count);

// Dateformat expressions.
FailureOrOwned<BoundExpression> BoundDateFormat(BoundExpression* datetime,
                                                BoundExpression* format,
                                                BufferAllocator* allocator,
                                                rowcount_t max_row_count);

FailureOrOwned<BoundExpression> BoundDateFormatLocal(BoundExpression* datetime,
                                                     BoundExpression* format,
                                                     BufferAllocator* allocator,
                                                     rowcount_t max_row_count);

}  // namespace supersonic

#endif  // SUPERSONIC_EXPRESSION_CORE_DATE_BOUND_EXPRESSIONS_H_
