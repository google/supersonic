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

#include "supersonic/expression/templated/cast_expression.h"

#include <string>
namespace supersonic {using std::string; }

#include "supersonic/utils/integral_types.h"
#include "supersonic/utils/stringprintf.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/expression/infrastructure/bound_expression_creators.h"
#include "supersonic/proto/supersonic.pb.h"
#include "supersonic/testing/block_builder.h"
#include "supersonic/testing/expression_test_helper.h"
#include "gtest/gtest.h"

namespace supersonic {

class Expression;

namespace {

template<DataType to_type, bool is_implicit>
const Expression* Cast(const Expression* const source) {
  return InternalCast(to_type, source, is_implicit);
}

// The main point of using this template is that it makes sure we make no
// copy-paste errors. This assures that a cast from type from, to type to does
// indeed make this particular cast (if it works).
//
// The only place where mistakes are possible is in allowing (or disallowing)
// particular casts.
template<DataType from, DataType to, bool is_implicit>
void TestCastBinding(bool success) {
  string name = StringPrintf("CAST_%s_TO_%s($0)",
                             GetTypeInfo(from).name().c_str(),
                             GetTypeInfo(to).name().c_str());
  if (from == to) name = "$0";
  if (success) {
    UnaryExpressionCreator creator = &Cast<to, is_implicit>;
    TestUnaryBinding(creator, from, name.c_str(), to, false);
  } else {
    TestBindingFailure(&Cast<to, is_implicit>, from);
  }
}

// TODO(onufry): add a implicit/explicit enum, to make the code readable.
TEST(CastExpressionTest, BindingFromInt32) {
// INT32 casts to any numeric, and to nothing else (implicity nonwithstanding).
  TestCastBinding<INT32,    INT32,      true>(true);
  TestCastBinding<INT32,    UINT32,     true>(true);
  TestCastBinding<INT32,    INT64,      true>(true);
  TestCastBinding<INT32,    UINT64,     true>(true);
  TestCastBinding<INT32,    FLOAT,      true>(true);
  TestCastBinding<INT32,    DOUBLE,     true>(true);

  TestCastBinding<INT32,    DATE,       true>(false);
  TestCastBinding<INT32,    DATETIME,   true>(false);
  TestCastBinding<INT32,    STRING,     true>(false);
  TestCastBinding<INT32,    BOOL,       true>(false);
  TestCastBinding<INT32,    BINARY,     true>(false);
  TestCastBinding<INT32,    DATA_TYPE,  true>(false);
}

TEST(CastExpressionTest, BindingFromUInt32) {
// UINT32 casts to any numeric, and to nothing else (implicity nonwithstanding).
  TestCastBinding<UINT32,   INT32,      false>(true);
  TestCastBinding<UINT32,   UINT32,     false>(true);
  TestCastBinding<UINT32,   INT64,      false>(true);
  TestCastBinding<UINT32,   UINT64,     false>(true);
  TestCastBinding<UINT32,   FLOAT,      false>(true);
  TestCastBinding<UINT32,   DOUBLE,     false>(true);

  TestCastBinding<UINT32,   DATE,       false>(false);
  TestCastBinding<UINT32,   DATETIME,   false>(false);
  TestCastBinding<UINT32,   STRING,     false>(false);
  TestCastBinding<UINT32,   BINARY,     false>(false);
  TestCastBinding<UINT32,   BOOL,       false>(false);
  TestCastBinding<UINT32,   DATA_TYPE,  false>(false);
}

TEST(CastExpressionTest, BindingFromInt64) {
// Downcasts to INT32/UINT32/FLOAT are allowed only if explicit.
  TestCastBinding<INT64,    INT32,      false>(true);
  TestCastBinding<INT64,    UINT32,     false>(true);
  TestCastBinding<INT64,    FLOAT,      false>(true);

  TestCastBinding<INT64,    UINT32,     true>(false);
  TestCastBinding<INT64,    INT32,      true>(false);
  TestCastBinding<INT64,    FLOAT,      true>(false);

// Sideways casts are always allowed.
  TestCastBinding<INT64,    INT64,      true>(true);
  TestCastBinding<INT64,    UINT64,     true>(true);
  TestCastBinding<INT64,    UINT64,     false>(true);

// Allowed explicit casts to double.
  TestCastBinding<INT64,    DOUBLE,     true>(true);
// And no others.
  TestCastBinding<INT64,    DATE,       false>(false);
  TestCastBinding<INT64,    DATETIME,   false>(false);
  TestCastBinding<INT64,    STRING,     false>(false);
  TestCastBinding<INT64,    BINARY,     false>(false);
  TestCastBinding<INT64,    BOOL,       false>(false);
  TestCastBinding<INT64,    DATA_TYPE,  false>(false);
}

TEST(CastExpressionTest, BindingFromUInt64) {
// Downcasts to INT32/UINT32/FLOAT are allowed only if explicit.
  TestCastBinding<UINT64,   INT32,      false>(true);
  TestCastBinding<UINT64,   UINT32,     false>(true);
  TestCastBinding<UINT64,   FLOAT,      false>(true);

  TestCastBinding<UINT64,   UINT32,     true>(false);
  TestCastBinding<UINT64,   INT32,      true>(false);
  TestCastBinding<UINT64,   FLOAT,      true>(false);

// Sideways casts are always allowed.
  TestCastBinding<UINT64,   UINT64,     false>(true);
  TestCastBinding<UINT64,   INT64,      true>(true);
  TestCastBinding<UINT64,   INT64,      false>(true);

// Allowed explicit casts to double.
  TestCastBinding<UINT64,   DOUBLE,     true>(true);
// And no others.
  TestCastBinding<UINT64,   DATE,       true>(false);
  TestCastBinding<UINT64,   DATETIME,   true>(false);
  TestCastBinding<UINT64,   STRING,     true>(false);
  TestCastBinding<UINT64,   BINARY,     true>(false);
  TestCastBinding<UINT64,   BOOL,       true>(false);
  TestCastBinding<UINT64,   DATA_TYPE,  true>(false);
}

TEST(CastExpressionTest, BindingFromFloat) {
// Downcasts to integer types not allowed at all.
  TestCastBinding<FLOAT,    INT32,      true>(false);
  TestCastBinding<FLOAT,    UINT32,     true>(false);
  TestCastBinding<FLOAT,    INT64,      true>(false);
  TestCastBinding<FLOAT,    UINT64,     true>(false);
  TestCastBinding<FLOAT,    INT32,      false>(false);
  TestCastBinding<FLOAT,    UINT32,     false>(false);
  TestCastBinding<FLOAT,    INT64,      false>(false);
  TestCastBinding<FLOAT,    UINT64,     false>(false);

// Upcasts to DOUBLE (and no-casts to FLOAT) allowed.
  TestCastBinding<FLOAT,    DOUBLE,     false>(true);
  TestCastBinding<FLOAT,    DOUBLE,     true>(true);
  TestCastBinding<FLOAT,    FLOAT,      false>(true);

// No other types allowed.
  TestCastBinding<FLOAT,    DATE,       true>(false);
  TestCastBinding<FLOAT,    DATETIME,   true>(false);
  TestCastBinding<FLOAT,    STRING,     true>(false);
  TestCastBinding<FLOAT,    BINARY,     true>(false);
  TestCastBinding<FLOAT,    BOOL,       true>(false);
  TestCastBinding<FLOAT,    DATA_TYPE,  true>(false);
}

TEST(CastExpressionTest, BindingFromDouble) {
// Downcasts to integer types not allowed at all.
  TestCastBinding<DOUBLE,   INT32,      true>(false);
  TestCastBinding<DOUBLE,   UINT32,     false>(false);
  TestCastBinding<DOUBLE,   INT64,      true>(false);
  TestCastBinding<DOUBLE,   UINT64,     false>(false);

// Downcasts to FLOAT allowed only if explicit.
  TestCastBinding<DOUBLE,   FLOAT,      false>(true);
  TestCastBinding<DOUBLE,   FLOAT,      true>(false);
  TestCastBinding<DOUBLE,   DOUBLE,     true>(true);

// No other types allowed.
  TestCastBinding<DOUBLE,   DATE,       true>(false);
  TestCastBinding<DOUBLE,   DATETIME,   true>(false);
  TestCastBinding<DOUBLE,   STRING,     true>(false);
  TestCastBinding<DOUBLE,   BINARY,     true>(false);
  TestCastBinding<DOUBLE,   BOOL,       true>(false);
  TestCastBinding<DOUBLE,   DATA_TYPE,  true>(false);
}

TEST(CastExpressionTest, BindingFromDate) {
// No-op cast allowed.
  TestCastBinding<DATE,     DATE,       true>(true);
  TestCastBinding<DATE,     DATE,       false>(true);
// Up-cast to datetime allowed.
  TestCastBinding<DATE,     DATETIME,   true>(true);
  TestCastBinding<DATE,     DATETIME,   false>(true);
// No other casts allowed.
  TestCastBinding<DATE,     INT32,      false>(false);
  TestCastBinding<DATE,     UINT32,     false>(false);
  TestCastBinding<DATE,     INT64,      false>(false);
  TestCastBinding<DATE,     UINT64,     false>(false);
  TestCastBinding<DATE,     FLOAT,      false>(false);
  TestCastBinding<DATE,     DOUBLE,     false>(false);
  TestCastBinding<DATE,     STRING,     false>(false);
  TestCastBinding<DATE,     BINARY,     false>(false);
  TestCastBinding<DATE,     BOOL,       false>(false);
  TestCastBinding<DATE,     DATA_TYPE,  false>(false);
}

TEST(CastExpressionTest, BindingFromDatetime) {
// No-op cast allowed.
  TestCastBinding<DATETIME, DATETIME,   true>(true);
  TestCastBinding<DATETIME, DATETIME,   false>(true);
// No other casts allowed.
  TestCastBinding<DATETIME, INT32,      true>(false);
  TestCastBinding<DATETIME, UINT32,     true>(false);
  TestCastBinding<DATETIME, INT64,      true>(false);
  TestCastBinding<DATETIME, UINT64,     true>(false);
  TestCastBinding<DATETIME, FLOAT,      true>(false);
  TestCastBinding<DATETIME, DOUBLE,     true>(false);
  TestCastBinding<DATETIME, DATE,       true>(false);
  TestCastBinding<DATETIME, STRING,     true>(false);
  TestCastBinding<DATETIME, BINARY,     true>(false);
  TestCastBinding<DATETIME, BOOL,       true>(false);
  TestCastBinding<DATETIME, DATA_TYPE,  true>(false);
}

TEST(CastExpressionTest, BindingFromBool) {
// No-op cast allowed.
  TestCastBinding<BOOL,     BOOL,       true>(true);
  TestCastBinding<BOOL,     BOOL,       false>(true);
// No other casts allowed.
  TestCastBinding<BOOL,     INT32,      true>(false);
  TestCastBinding<BOOL,     UINT32,     true>(false);
  TestCastBinding<BOOL,     INT64,      true>(false);
  TestCastBinding<BOOL,     UINT64,     true>(false);
  TestCastBinding<BOOL,     FLOAT,      true>(false);
  TestCastBinding<BOOL,     DOUBLE,     true>(false);
  TestCastBinding<BOOL,     DATE,       true>(false);
  TestCastBinding<BOOL,     DATETIME,   true>(false);
  TestCastBinding<BOOL,     STRING,     true>(false);
  TestCastBinding<BOOL,     BINARY,     true>(false);
  TestCastBinding<BOOL,     DATA_TYPE,  true>(false);
}

TEST(CastExpressionTest, BindingFromString) {
// No-op cast allowed.
  TestCastBinding<STRING,   STRING,     true>(true);
  TestCastBinding<STRING,   STRING,     false>(true);
// Projecting cast to BINARY allowed.
  TestCastBinding<STRING,   BINARY,     true>(true);
  TestCastBinding<STRING,   BINARY,     false>(true);
// No other casts allowed.
  TestCastBinding<STRING,   INT32,      false>(false);
  TestCastBinding<STRING,   UINT32,     false>(false);
  TestCastBinding<STRING,   INT64,      false>(false);
  TestCastBinding<STRING,   UINT64,     false>(false);
  TestCastBinding<STRING,   FLOAT,      false>(false);
  TestCastBinding<STRING,   DOUBLE,     false>(false);
  TestCastBinding<STRING,   DATE,       false>(false);
  TestCastBinding<STRING,   DATETIME,   false>(false);
  TestCastBinding<STRING,   BOOL,       false>(false);
  TestCastBinding<STRING,   DATA_TYPE,  false>(false);
}

TEST(CastExpressionTest, BindingFromDataType) {
// No-op cast allowed.
  TestCastBinding<DATA_TYPE, DATA_TYPE, true>(true);
  TestCastBinding<DATA_TYPE, DATA_TYPE, false>(true);
// No other casts allowed.
  TestCastBinding<DATA_TYPE, INT32,      false>(false);
  TestCastBinding<DATA_TYPE, UINT32,     false>(false);
  TestCastBinding<DATA_TYPE, INT64,      false>(false);
  TestCastBinding<DATA_TYPE, UINT64,     false>(false);
  TestCastBinding<DATA_TYPE, FLOAT,      false>(false);
  TestCastBinding<DATA_TYPE, DOUBLE,     false>(false);
  TestCastBinding<DATA_TYPE, DATE,       false>(false);
  TestCastBinding<DATA_TYPE, DATETIME,   false>(false);
  TestCastBinding<DATA_TYPE, BOOL,       false>(false);
  TestCastBinding<DATA_TYPE, STRING,     false>(false);
  TestCastBinding<DATA_TYPE, BINARY,     false>(false);
}

TEST(CastExpressionTest, ProjectingCast) {
  TestEvaluation(BlockBuilder<INT32, UINT32>()
      .AddRow(1,  1)
      .AddRow(13, 13)
      .AddRow(-1, static_cast<uint32>(-1))
      .Build(), &Cast<UINT32, false>);

  TestEvaluation(BlockBuilder<UINT64, INT64>()
      .AddRow(1LL,                          1LL)
      .AddRow(static_cast<uint64>(-19LL),  -19LL)
      .AddRow(1234567LL,                    1234567LL)
      .Build(), &Cast<INT64, true>);
}

TEST(CastExpressionTest, NoOpCast) {
  TestEvaluation(BlockBuilder<DOUBLE, DOUBLE>()
      .AddRow(3.14,   3.14)
      .AddRow(1.41,   1.41)
      .AddRow(9.81,   9.81)
      .AddRow(2.71,   2.71)
      .Build(), &Cast<DOUBLE, false>);

  TestEvaluation(BlockBuilder<DATA_TYPE, DATA_TYPE>()
      .AddRow(INT32,     INT32)
      .AddRow(DATA_TYPE, DATA_TYPE)
      .AddRow(DATE,      DATE)
      .Build(), &Cast<DATA_TYPE, true>);
}

TEST(CastExpressionTest, StandardCast) {
  TestEvaluation(BlockBuilder<INT32, INT64>()
      .AddRow(476,    476LL)      // Fall of the roman empire.
      .AddRow(1054,   1054LL)     // The great schism.
      .AddRow(1453,   1453LL)     // The fall of Constantinople.
      .AddRow(1517,   1517LL)     // The thesis of Luter.
      .AddRow(1914,   1914LL)     // Outbreak of the first world war.
      .AddRow(1939,   1939LL)     // Outbreak of the second world war.
      .AddRow(2011,   2011LL)     // The writing of this document :).
      .Build(), &Cast<INT64, true>);

  TestEvaluation(BlockBuilder<UINT64, UINT32>()
      .AddRow(1234LL, 1234)
      .AddRow(static_cast<uint64>(-1LL), static_cast<uint32>(-1LL))
      .AddRow(0LL,    0)
      .Build(), &Cast<UINT32, false>);
}

TEST(CastExpressionTest, DateToDatetimeCast) {
  TestEvaluation(BlockBuilder<DATE, DATETIME>()
      .AddRow(1,     86400000000LL)
      .AddRow(100,   8640000000000LL)
      .AddRow(14600, 1261440000000000LL)
      .Build(), &Cast<DATETIME, true>);
}

}  // namespace

}  // namespace supersonic
