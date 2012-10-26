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

#ifndef SUPERSONIC_TESTING_COMPARATORS_H_
#define SUPERSONIC_TESTING_COMPARATORS_H_

#include "supersonic/testing/comparable_cursor.h"
#include "supersonic/testing/comparable_tuple_schema.h"
#include "supersonic/testing/comparable_view.h"
#include "supersonic/testing/row.h"

// The macros below are gUnit-compatible assertions that allow convenient and
// thorough comparison of basic Supersonic data objects. In case any of these
// assertions fail, it prints out verbose string representation of the two
// objects and detailed reason for inequality.
//
// Sample output from EXPECT_VIEWS_EQUAL
//
// value mismatch in row 1, column 0
// in ViewsEqual(*aN_a2_, *a1_b2_)
//
// *aN_a2_ = View; rows: 2; schema: STRING "col0", INT64 "col1" (N)
//   a, NULL
//   a, 2
//
// *aN_b2_ = View; rows: 2; schema: STRING "col0", INT64 "col1" (N)
//   a, NULL
//   b, 2

// Cursor* a, Cursor* b - takes ownership of Cursors and modifies (reads) them.
#define EXPECT_CURSORS_EQUAL(a, b) \
  EXPECT_PRED_FORMAT2(CursorsEqual, a, b)

#define ASSERT_CURSORS_EQUAL(a, b) \
  ASSERT_PRED_FORMAT2(CursorsEqual, a, b)

// See testing/row.h for other ways of comparing Rows.
// const Row& a, const Row& b
#define EXPECT_ROWS_EQUAL(a, b) \
  EXPECT_PRED_FORMAT2(RowsEqual, a, b)

// const TupleSchema& a, const TupleSchema& b
#define EXPECT_TUPLE_SCHEMAS_EQUAL(a, b)       \
  EXPECT_PRED_FORMAT2(TupleSchemasStrictEqual, a, b)

// const View& a, const View& b
#define EXPECT_VIEWS_EQUAL(a, b) \
  EXPECT_PRED_FORMAT2(ViewsEqual, a, b)

// const View& a, const View& b
#define ASSERT_VIEWS_EQUAL(a, b) \
  ASSERT_PRED_FORMAT2(ViewsEqual, a, b)

// const Column& a, const Column& b, rowcount_t c
#define EXPECT_COLUMNS_EQUAL(a, b, c) \
    EXPECT_PRED_FORMAT3(ColumnsEqual, a, b, c)

#endif  // SUPERSONIC_TESTING_COMPARATORS_H_
