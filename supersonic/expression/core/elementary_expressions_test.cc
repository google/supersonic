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

#include "supersonic/expression/core/elementary_expressions.h"

#include "supersonic/utils/exception/failureor.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/expression/core/arithmetic_expressions.h"
#include "supersonic/expression/core/comparison_expressions.h"
#include "supersonic/expression/infrastructure/terminal_expressions.h"
#include "supersonic/expression/templated/bound_expression_factory.h"
#include "supersonic/testing/block_builder.h"
#include "supersonic/testing/expression_test_helper.h"
#include "supersonic/testing/short_circuit_tester.h"
#include "gtest/gtest.h"

namespace supersonic {
// TODO(onufry): Move these tests where they belong.

class Expression;

namespace {

DataType CalculateCommonTypeOrDie(DataType a, DataType b) {
  return SucceedOrDie(CalculateCommonType(a, b));
}

TEST(CalculateCommonTypeTest, CorrectOutputTypesForDouble) {
  EXPECT_EQ(DOUBLE, CalculateCommonTypeOrDie(DOUBLE, INT32));
  EXPECT_EQ(DOUBLE, CalculateCommonTypeOrDie(DOUBLE, INT64));
  EXPECT_EQ(DOUBLE, CalculateCommonTypeOrDie(DOUBLE, UINT32));
  EXPECT_EQ(DOUBLE, CalculateCommonTypeOrDie(DOUBLE, UINT64));
  EXPECT_EQ(DOUBLE, CalculateCommonTypeOrDie(DOUBLE, FLOAT));
}

TEST(CalculateCommonTypeTest, CorrectOutputTypesForFloat) {
  EXPECT_EQ(FLOAT, CalculateCommonTypeOrDie(FLOAT, INT32));
  EXPECT_EQ(DOUBLE, CalculateCommonTypeOrDie(FLOAT, INT64));
  EXPECT_EQ(FLOAT, CalculateCommonTypeOrDie(FLOAT, UINT32));
  EXPECT_EQ(DOUBLE, CalculateCommonTypeOrDie(FLOAT, UINT64));
  EXPECT_EQ(DOUBLE, CalculateCommonTypeOrDie(FLOAT, DOUBLE));
}

TEST(CalculateCommonTypeTest, CorrectOutputTypesForInt64) {
  EXPECT_EQ(INT64, CalculateCommonTypeOrDie(INT64, INT32));
  EXPECT_EQ(DOUBLE, CalculateCommonTypeOrDie(INT64, FLOAT));
  EXPECT_EQ(INT64, CalculateCommonTypeOrDie(INT64, UINT32));
  EXPECT_EQ(INT64, CalculateCommonTypeOrDie(INT64, UINT64));
  EXPECT_EQ(DOUBLE, CalculateCommonTypeOrDie(INT64, DOUBLE));
}

TEST(CalculateCommonTypeTest, CorrectOutputTypesForInt32) {
  EXPECT_EQ(INT64, CalculateCommonTypeOrDie(INT32, INT64));
  EXPECT_EQ(FLOAT, CalculateCommonTypeOrDie(INT32, FLOAT));
  EXPECT_EQ(INT64, CalculateCommonTypeOrDie(INT32, UINT32));
  EXPECT_EQ(INT64, CalculateCommonTypeOrDie(INT32, UINT64));
  EXPECT_EQ(DOUBLE, CalculateCommonTypeOrDie(INT32, DOUBLE));
}

TEST(CalculateCommonTypeTest, CorrectOutputTypesForUInt32) {
  EXPECT_EQ(INT64, CalculateCommonTypeOrDie(UINT32, INT64));
  EXPECT_EQ(FLOAT, CalculateCommonTypeOrDie(UINT32, FLOAT));
  EXPECT_EQ(INT64, CalculateCommonTypeOrDie(UINT32, INT32));
  EXPECT_EQ(UINT64, CalculateCommonTypeOrDie(UINT32, UINT64));
  EXPECT_EQ(DOUBLE, CalculateCommonTypeOrDie(UINT32, DOUBLE));
}

TEST(CalculateCommonTypeTest, CorrectOutputTypesForUInt64) {
  EXPECT_EQ(INT64, CalculateCommonTypeOrDie(UINT64, INT64));
  EXPECT_EQ(DOUBLE, CalculateCommonTypeOrDie(UINT64, FLOAT));
  EXPECT_EQ(INT64, CalculateCommonTypeOrDie(UINT64, INT32));
  EXPECT_EQ(UINT64, CalculateCommonTypeOrDie(UINT64, UINT32));
  EXPECT_EQ(DOUBLE, CalculateCommonTypeOrDie(UINT64, DOUBLE));
}

// ------------------------------ Unary Expressions ----------------------------

template<DataType to_type>
const Expression* TypedCast(const Expression* const e) {
  return CastTo(to_type, e);
}

TEST(UnaryExpressionTest, Cast) {
  TestUnaryBinding(&TypedCast<DOUBLE>, INT32, "CAST_INT32_TO_DOUBLE($0)",
              DOUBLE, false);
  TestUnaryBinding(&TypedCast<DOUBLE>, DOUBLE, "$0", DOUBLE, false);
  TestEvaluation(BlockBuilder<INT32, DOUBLE>()
      .AddRow(1,  1.)
      .AddRow(2,  2.)
      .AddRow(-1, -1.)
      .Build(), &TypedCast<DOUBLE>);

  TestBindingFailure(&TypedCast<INT32>, DOUBLE);
  TestBindingFailure(&TypedCast<INT32>, STRING);
  TestBindingFailure(&TypedCast<STRING>, INT32);
  TestBindingFailure(&TypedCast<DATE>, STRING);
}

// Template helpers, to fit the ParseString functions (which take one additional
// argument) fit into the factory testing scheme.
template<DataType type>
const Expression* TypedParseStringQuiet(const Expression* const e) {
  return ParseStringQuiet(type, e);
}

template<DataType type>
const Expression* TypedParseStringNulling(const Expression* const e) {
  return ParseStringNulling(type, e);
}

// We do not test for the behaviour of ParseStringQuiet on invalid inputs, as
// the behaviour is undefined.
TEST(UnaryExpressionTest, ParseStringQuietToInteger) {
  TestEvaluation(BlockBuilder<STRING, INT32>()
      .AddRow("3",   3)
      .AddRow("-3",  -3)
      .AddRow("401", 401)
      .Build(), &TypedParseStringQuiet<INT32>);

  TestEvaluation(BlockBuilder<STRING, UINT32>()
      .AddRow("3",   3)
      .AddRow("13",  13)
      .AddRow("401", 401)
      .Build(), &TypedParseStringQuiet<UINT32>);

  TestEvaluation(BlockBuilder<STRING, INT64>()
      .AddRow("3",    3)
      .AddRow("-3",  -3)
      .AddRow("401", 401)
      .AddRow("1000000000000", 1000000000000LL)
      .Build(), &TypedParseStringQuiet<INT64>);

  TestEvaluation(BlockBuilder<STRING, UINT64>()
      .AddRow("3",    3)
      .AddRow("401", 401)
      .AddRow(__,    __)
      .AddRow("1000000000000", 1000000000000LL)
      .Build(), &TypedParseStringQuiet<UINT64>);
}

TEST(UnaryExpressionTest, ParseStringNullingToInteger) {
  TestEvaluation(BlockBuilder<STRING, INT32>()
      .AddRow("Random String", __)
      .AddRow(__,              __)
      .AddRow("123",           123)
      .AddRow("12345678901",   __)  // Out of range.
      .Build(), &TypedParseStringNulling<INT32>);

  TestEvaluation(BlockBuilder<STRING, UINT64>()
      .AddRow("No Such Number",  __)
      .AddRow("125 OOPS",        __)
      .AddRow("OOPS 125",        __)
      .AddRow("-1",              __)  // Out of range.
      .AddRow("1000000000000",   1000000000000LL)
      .AddRow("1000000000000000000000", __)  // Out of range.
      .Build(), &TypedParseStringNulling<UINT64>);
}

TEST(UnaryExpressionTest, ParseStringQuietToFloatingPoint) {
  TestEvaluation(BlockBuilder<STRING, FLOAT>()
      .AddRow("12",   12.)
      .AddRow("0.1",  0.1)
      .AddRow("-12",  -12.)
      .Build(), &TypedParseStringQuiet<FLOAT>);

  TestEvaluation(BlockBuilder<STRING, DOUBLE>()
      .AddRow("12",   12.)
      .AddRow("0.1",  0.1)
      .AddRow("-12",  -12.)
      .Build(), &TypedParseStringQuiet<DOUBLE>);
}

TEST(UnaryExpressionTest, ParseStringNullingToFloatingPoint) {
  TestEvaluation(BlockBuilder<STRING, FLOAT>()
      .AddRow("-12",      -12)
      .AddRow("-1.",      -1)
      .AddRow("1.OOPS",   __)
      .AddRow("1.1.",     __)
      .AddRow("--12",     __)
      .AddRow("NaN",      0. / 0.)  // NaN.
      .Build(), &TypedParseStringNulling<FLOAT>);
}

TEST(UnaryExpressionTest, ParseStringQuietToBool) {
  TestEvaluation(BlockBuilder<STRING, BOOL>()
      .AddRow("True",     true)
      .AddRow("true",     true)
      .AddRow("  TRUE  ", true)
      .AddRow("FaLsE  ",  false)
      .AddRow("YeS",      true)
      .AddRow("No",       false)
      .Build(), &TypedParseStringQuiet<BOOL>);
}

TEST(UnaryExpressionTest, ParseStringNullingToBool) {
  TestEvaluation(BlockBuilder<STRING, BOOL>()
      .AddRow("I really hope it's true", __)
      .AddRow("OK",                      __)
      .AddRow("1",                       __)  // Booleans are not integers!
      .AddRow("....",                    __)
      .AddRow("TRUE",                    true)
      .Build(), &TypedParseStringNulling<BOOL>);
}

TEST(UnaryExpressionTest, ParseStringQuietToDate) {
  TestEvaluation(BlockBuilder<STRING, DATE>()
      .AddRow("1970/1/1",   0)
      .AddRow("1970/1/2",   1)
      .AddRow("2010/12/22", 14965)
      .Build(), &TypedParseStringQuiet<DATE>);
}

TEST(UnaryExpressionTest, ParseStringNullingToDate) {
  TestEvaluation(BlockBuilder<STRING, DATE>()
      .AddRow("1969/1/1",  __)
      .AddRow("9999/9/9",  __)
      .AddRow("2000:2:2",  __)
      .AddRow("12th of January, 2000", __)
      .Build(), &TypedParseStringNulling<DATE>);
}

TEST(UnaryExpressionTest, ParseStringToDateTime) {
  TestEvaluation(BlockBuilder<STRING, DATETIME>()
      .AddRow("1970/1/1-00:00:00",  0LL)
      .AddRow("2002/2/28-00:00:00", 1014854400000000LL)
      .AddRow("2012/6/12-00:00:01", 1339459201000000LL)
      .AddRow("2012/6/12-01:02:03", 1339462923000000LL)
      .Build(), &TypedParseStringQuiet<DATETIME>);

  TestEvaluation(BlockBuilder<STRING, DATETIME>()
      .AddRow("1970/1/1", __)
      .AddRow("Now",      __)
      .AddRow("123",      __)
      .Build(), &TypedParseStringNulling<DATETIME>);
}

TEST(UnaryExpressionTest, ParseStringToString) {
  TestUnaryBinding(
      &TypedParseStringQuiet<STRING>,   STRING, "$0", STRING, false);
  TestUnaryBinding(
      &TypedParseStringNulling<STRING>, STRING, "$0", STRING, false);
}

TEST(UnaryExpressionTest, NotBinding) {
  TestUnaryBinding(&Not, BOOL, "(NOT $0)", BOOL, false);

  TestBindingFailure(&Not, STRING);
  TestBindingFailure(&Not, DATE);
  TestBindingFailure(&Not, INT32);
}

TEST(UnaryExpressionTest, Not) {
  TestEvaluation(BlockBuilder<BOOL, BOOL>()
      .AddRow(false, true)
      .AddRow(true,  false)
      .AddRow(__,    __)
      .Build(), &Not);
}

TEST(UnaryExpressionTest, IsNull) {
  TestEvaluation(BlockBuilder<INT32, BOOL>()
      .AddRow(1,  false)
      .AddRow(__, true)
      .AddRow(4,  false)
      .AddRow(1,  false)
      .AddRow(0,  false)
      .AddRow(__, true)
      .Build(), &IsNull);
}

TEST(UnaryExpressionTest, IsNullNotNull) {
  TestEvaluation(BlockBuilder<STRING, BOOL>()
      .AddRow("I am", false)
      .AddRow("You are", false)
      .AddRow("He/she/it is", false)
      .AddRow("We are", false)
      .AddRow("You are", false)
      .AddRow("They are", false)
      .AddRow("", false)
      .Build(), &IsNull);
}

TEST(UnaryExpressionTest, IsNullShortCircuit) {
  TestShortCircuitUnary(BlockBuilder<BOOL, INT32, BOOL, BOOL>()
      .AddRow(false, 1,  false, false)
      .AddRow(false, __, false, true)
      .AddRow(true,  1,  true,  __)
      .AddRow(true,  __, true,  __)
      .Build(), &IsNull);
}

TEST(UnaryExpressionTest, BitwiseNot) {
  TestUnaryBinding(&BitwiseNot, INT32, "(~$0)", INT32, false);
  TestBindingFailure(&BitwiseNot, BOOL);
  TestBindingFailure(&BitwiseNot, DATE);

  TestEvaluation(BlockBuilder<INT32, INT32>()
      .AddRow(__,      __)
      .AddRow(0,       ~0)
      .AddRow(1234567, ~1234567)
      .Build(), &BitwiseNot);

  TestEvaluation(BlockBuilder<UINT64, UINT64>()
      .AddRow(0LL,         ~(0LL))
      .AddRow(123456789LL, ~(123456789LL))
      .Build(), &BitwiseNot);
}

// --------------------------- Binary Expressions ------------------------------

TEST(BinaryExpressionTest, BindingSuccess) {
  TestBinaryBindingNotNullable(&And,    BOOL,   BOOL,   "($0 AND $1)", BOOL);
  TestBinaryBindingNotNullable(&AndNot, BOOL,   BOOL,   "($0 !&& $1)", BOOL);
  TestBinaryBindingNotNullable(&Or,     BOOL,   BOOL,   "($0 OR $1)",  BOOL);
  TestBinaryBindingNotNullable(&Xor,    BOOL,   BOOL,   "($0 XOR $1)", BOOL);
}

TEST(BinaryExpressionTest, BindingIfNull) {
  TestBindingWithNull(&IfNull,  INT32, false, INT32, false, "$0", INT32, false);
  TestBindingWithNull(&IfNull, INT32, true, INT32, false, "IFNULL($0, $1)",
                      INT32, false);
  TestBindingWithNull(&IfNull, INT64, false, INT64, true, "$0", INT64, false);
  TestBindingWithNull(&IfNull, INT32, true, INT32, true, "IFNULL($0, $1)",
                      INT32, true);

  // This is a non-obvious design decision. But the reasoning here is that if
  // the user writes something like this, he/she doesn't know that the left
  // argument is not nullable, and thus expects an INT64 in the output. So we
  // oblige him/her.
  TestBinaryBindingNotNullable(
      &IfNull, INT32, INT64, "CAST_INT32_TO_INT64($0)", INT64);

  // The same here - the user probably wants this to fail on a type-check,
  // instead of receiving the left hand side, even though it is not nullable.
  TestBindingFailure(&IfNull, BOOL, INT32);
}

TEST(BinaryExpressionTest, BindingIfNullWithCast) {
  TestBindingWithNull(&IfNull, INT32, true, FLOAT, false,
                      "IFNULL(CAST_INT32_TO_FLOAT($0), $1)", FLOAT, false);
}

TEST(BinaryExpressionTest, IfNull) {
  TestEvaluation(BlockBuilder<INT64, INT64, INT64>()
      .AddRow(1,  20,  1)
      .AddRow(10, 20, 10)
      .AddRow(__, 20, 20)
      .AddRow(__, 15, 15)
      .AddRow(7,  __,  7)
      .AddRow(__, __, __)
      .Build(), &IfNull);
}

TEST(BinaryExpressionTest, IfNullWithCast) {
  TestEvaluation(BlockBuilder<INT64, INT32, INT64>()
      .AddRow(1,  20,  1)
      .AddRow(10, 20, 10)
      .AddRow(__, 20, 20)
      .AddRow(__, 15, 15)
      .AddRow(7,  __,  7)
      .AddRow(__, __, __)
      .Build(), &IfNull);
}

TEST(BinaryExpressionTest, IfNullShortCircuit) {
  // Order of columns:
  // input skip vector, left input, skip vector passed to left input,
  // right input, skip vector passed to right input, result.
  TestShortCircuitBinary(
      BlockBuilder<BOOL, STRING, BOOL, STRING, BOOL, STRING>()
          .AddRow(false, "One",  false, "Ring", true,  "One")
          .AddRow(false, "To",   false, "Rule", true,  "To")
          .AddRow(false, "Them", false, __,     true,  "Them")
          .AddRow(false, __,     false, "All",  false, "All")
          .AddRow(false, __,     false, __,     false, __)
          .AddRow(true,  "One",  true,  "Ring", true,  __)
          .AddRow(true,  "To",   true,  __,     true,  __)
          .AddRow(true,  __,     true,  "Bind", true,  __)
          .AddRow(true,  __,     true,  __,     true,  __)
          .AddRow(false, __,     false, "Them", false, "Them")
          .Build(), &IfNull);
}

TEST(BinaryExpressionTest, Xor) {
  TestBinaryBindingNotNullable(&Xor, BOOL, BOOL, "($0 XOR $1)", BOOL);
  TestBindingFailure(&Xor, BOOL, INT32);
  TestEvaluation(BlockBuilder<BOOL, BOOL, BOOL>()
      .AddRow(true,  true,  false)
      .AddRow(true,  false, true)
      .AddRow(true,  __,    __)
      .AddRow(false, true,  true)
      .AddRow(false, false, false)
      .AddRow(false, __,    __)
      .AddRow(__,    true,  __)
      .AddRow(__,    false, __)
      .AddRow(__,    __,    __)
      .Build(), &Xor);
}

TEST(BinaryExpressionTest, XorWithoutNulls) {
  TestEvaluation(BlockBuilder<BOOL, BOOL, BOOL>()
      .AddRow(true,  true,  false)
      .AddRow(true,  false, true)
      .AddRow(false, true,  true)
      .AddRow(false, false, false)
      .Build(), &Xor);
}

TEST(BinaryExpressionTest, And) {
  TestEvaluation(BlockBuilder<BOOL, BOOL, BOOL>()
    .AddRow(false, false, false)
    .AddRow(false, true,  false)
    .AddRow(false, __,    false)
    .AddRow(true,  false, false)
    .AddRow(true,  true,  true)
    .AddRow(true,  __,    __)
    .AddRow(__,    false, false)
    .AddRow(__,    true,  __)
    .AddRow(__,    __,    __)
    .Build(), &And);
}

TEST(BinaryExpressionTest, AndWithoutNulls) {
  TestEvaluation(BlockBuilder<BOOL, BOOL, BOOL>()
      .AddRow(false, false, false)
      .AddRow(false, true,  false)
      .AddRow(true,  false, false)
      .AddRow(true,  true,  true)
      .Build(), &And);
}

TEST(BinaryExpressionTest, AndShortCircuit) {
  // Order of columns:
  // input skip vector, left input, skip vector passed to left input,
  // right input, skip vector passed to right input, result.
  TestShortCircuitBinary(BlockBuilder<BOOL, BOOL, BOOL, BOOL, BOOL, BOOL>()
      .AddRow(false, false, false, false, true,  false)
      .AddRow(false, false, false, __,    true,  false)
      .AddRow(false, true,  false, false, false, false)
      .AddRow(false, true,  false, __,    false, __)
      .AddRow(false, __,    false, true,  false, __)
      .AddRow(false, __,    false, false, false, false)
      .AddRow(false, true,  false, true,  false, true)
      .AddRow(true,  false, true,  false, true,  __)
      .AddRow(true,  __,    true,  __,    true,  __)
      .AddRow(true,  true,  true,  true,  true,  __)
      .Build(), &And);
}

TEST(BinaryExpressionTest, Or) {
  TestEvaluation(BlockBuilder<BOOL, BOOL, BOOL>()
    .AddRow(false, false, false)
    .AddRow(false, true,  true)
    .AddRow(false, __,    __)
    .AddRow(true,  false, true)
    .AddRow(true,  true,  true)
    .AddRow(true,  __,    true)
    .AddRow(__,    false, __)
    .AddRow(__,    true,  true)
    .AddRow(__,    __,    __)
    .Build(), &Or);
}

TEST(BinaryExpressionTest, OrShortCircuit) {
  // Order of columns:
  // input skip vector, left input, skip vector passed to left input,
  // right input, skip vector passed to right input, result.
  TestShortCircuitBinary(BlockBuilder<BOOL, BOOL, BOOL, BOOL, BOOL, BOOL>()
      .AddRow(false, false, false, false, false, false)
      .AddRow(false, false, false, __,    false, __)
      .AddRow(false, true,  false, false, true,  true)
      .AddRow(false, true,  false, __,    true,  true)
      .AddRow(false, __,    false, true,  false, true)
      .AddRow(false, __,    false, __,    false, __)
      .AddRow(false, __,    false, false, false, __)
      .AddRow(true,  false, true,  false, true,  __)
      .AddRow(true,  false, true,  __,    true,  __)
      .AddRow(true,  __,    true,  false, true,  __)
      .AddRow(true,  __,    true,  __,    true,  __)
      .Build(), &Or);
}

TEST(BinaryExpressionTest, OrNotNullable) {
  TestEvaluation(BlockBuilder<BOOL, BOOL, BOOL>()
      .AddRow(false, false, false)
      .AddRow(true,  false, true)
      .AddRow(false, true,  true)
      .AddRow(true,  true,  true)
      .Build(), &Or);
}

TEST(BinaryExpressionTest, AndNot) {
  TestEvaluation(BlockBuilder<BOOL, BOOL, BOOL>()
      .AddRow(false, false, false)
      .AddRow(false, true,  true)
      .AddRow(false, __,    __)
      .AddRow(true,  false, false)
      .AddRow(true,  true,  false)
      .AddRow(true,  __,    false)
      .AddRow(__,    false, false)
      .AddRow(__,    true,  __)
      .AddRow(__,    __,    __)
      .Build(), &AndNot);
}

TEST(BinaryExpressionTest, AndNotShortCircuit) {
  // Order of columns:
  // input skip vector, left input, skip vector passed to left input,
  // right input, skip vector passed to right input, result.
  TestShortCircuitBinary(BlockBuilder<BOOL, BOOL, BOOL, BOOL, BOOL, BOOL>()
      .AddRow(false, false, false, true,  false, true)
      .AddRow(false, false, false, __,    false, __)
      .AddRow(false, true,  false, false, true,  false)
      .AddRow(false, true,  false, __,    true,  false)
      .AddRow(false, __,    false, false, false, false)
      .AddRow(false, __,    false, __,    false, __)
      .AddRow(false, __,    false, true,  false, __)
      .AddRow(true,  false, true,  false, true,  __)
      .AddRow(true,  false, true,  __,    true,  __)
      .AddRow(true,  __,    true,  false, true,  __)
      .AddRow(true,  __,    true,  __,    true,  __)
      .Build(), &AndNot);
}

TEST(BinaryExpressionTest, BitwiseAnd) {
  TestBinaryBindingNotNullable(
      &BitwiseAnd, INT32, UINT64,
      "(CAST_INT32_TO_INT64($0) & CAST_UINT64_TO_INT64($1))", INT64);
  TestBindingFailure(&BitwiseAnd, BOOL, INT32);
  TestBindingFailure(&BitwiseAnd, BOOL, BOOL);

  TestEvaluation(BlockBuilder<INT32, INT64, INT64>()
      .AddRow(12,   12LL,            12LL)
      .AddRow(12,   1000000000000LL, 0LL)
      .AddRow(-1,   12345LL,         12345LL)
      .Build(), &BitwiseAnd);
}

TEST(BinaryExpressionTest, BitwiseAndNot) {
  TestBinaryBindingNotNullable(&BitwiseAndNot, INT32, INT32,
                               "(~$0 & $1)", INT32);

  TestEvaluation(BlockBuilder<INT32, INT32, INT32>()
      .AddRow(1,   1,   0)
      .AddRow(3,   7,   4)
      .AddRow(10,  7,   5)
      .AddRow(__,  8,   __)
      .Build(), &BitwiseAndNot);
}

TEST(BinaryExpressionTest, BitwiseOr) {
  TestBinaryBindingNotNullable(
      &BitwiseOr, INT32, UINT64,
      "(CAST_INT32_TO_INT64($0) | CAST_UINT64_TO_INT64($1))", INT64);

  TestEvaluation(BlockBuilder<INT64, INT64, INT64>()
      .AddRow(1LL,             0LL,            1LL)
      .AddRow(1099511627776LL, 549755813888LL, 1649267441664LL)  // 2^40 + 2^39.
      .AddRow(3LL,             5LL,            7LL)
      .Build(), &BitwiseOr);
}

TEST(BinaryExpressionTest, BitwiseXor) {
  TestBinaryBindingNotNullable(
      &BitwiseXor, UINT32, UINT64, "(CAST_UINT32_TO_UINT64($0) ^ $1)", UINT64);

  TestEvaluation(BlockBuilder<UINT32, UINT32, UINT32>()
      .AddRow(1,   1,  0)
      .AddRow(2,   1,  3)
      .AddRow(3,   1,  2)
      .AddRow(5,  10,  15)
      .AddRow(19, 39,  52)
      .AddRow(__,  1,  __)
      .AddRow(1,  __,  __)
      .AddRow(__, __,  __)
      .Build(), &BitwiseXor);
}

TEST(BinaryExpressionTest, ShiftLeft) {
  TestBinaryBindingNotNullable(
      &ShiftLeft, UINT32, INT64, "($0 << $1)", UINT32);
  TestBinaryBindingNotNullable(&ShiftLeft, INT64, UINT32, "($0 << $1)", INT64);
  TestBindingFailure(&ShiftLeft, UINT32, BOOL);
  TestBindingFailure(&ShiftLeft, BOOL, INT64);
  TestBindingFailure(&ShiftLeft, DATE, INT32);
  TestBindingFailure(&ShiftLeft, INT32, DATA_TYPE);

  TestEvaluation(BlockBuilder<UINT32, INT32, UINT32>()
      .AddRow(1, 4,  16)
      .AddRow(3, 2,  12)
      .AddRow(5, 31, 2147483648U)
      .Build(), &ShiftLeft);
}

TEST(BinaryExpressionTest, ShiftRight) {
  TestBinaryBindingNotNullable(&ShiftRight, INT32, UINT64, "($0 >> $1)", INT32);

  TestEvaluation(BlockBuilder<INT32, INT32, INT32>()
      .AddRow(1,  1,  0)
      .AddRow(2,  1,  1)
      .AddRow(-1, 1, -1)
      .AddRow(-4, 1, -2)
      .AddRow(__, 1, __)
      .Build(), &ShiftRight);
}

// -------------------------------- Ternary Expressions ------------------------

TEST(TernaryExpressionTest, BasicIf) {
  TestTernaryBinding(
      &If, BOOL, INT32, INT32, "IF $0 THEN $1 ELSE $2", INT32, false);

  TestEvaluation(BlockBuilder<BOOL, INT32, INT32, INT32>()
      .AddRow(true,  1, 2, 1)
      .AddRow(false, 3, 4, 4)
      .AddRow(true,  1, 1, 1)
      .Build(), &If);
}

TEST(TernaryExpressionTest, BasicNullingIf) {
  TestTernaryBinding(
      &NullingIf, BOOL, INT32, INT64,
      "IF $0 THEN CAST_INT32_TO_INT64($1) ELSE $2", INT64, false);

  TestEvaluation(BlockBuilder<BOOL, INT32, INT64, INT64>()
      .AddRow(true,  1, 2LL, 1LL)
      .AddRow(false, 3, 4LL, 4LL)
      .AddRow(true,  1, 1LL, 1LL)
      .Build(), &NullingIf);
}

TEST(TernaryExpressionTest, IfWithNullCondition) {
  TestEvaluation(BlockBuilder<BOOL, INT32, INT32, INT32>()
      .AddRow(false, 1, 2, 2)
      .AddRow(__,    1, 2, 2)
      .AddRow(true,  1, 2, 1)
      .Build(), &If);

  TestEvaluation(BlockBuilder<BOOL, INT32, INT32, INT32>()
      .AddRow(false, 1, 2, 2)
      .AddRow(__,    1, 2, __)
      .AddRow(true,  1, 2, 1)
      .Build(), &NullingIf);
}

TEST(TernaryExpressionTest, IfWithNullThen) {
  TestEvaluation(BlockBuilder<BOOL, DATE, DATE, DATE>()
      .AddRow(true,   1, 2, 1)
      .AddRow(true,  __, 2, __)
      .AddRow(false, __, 2, 2)
      .Build(), &If);

  TestEvaluation(BlockBuilder<BOOL, DATE, DATE, DATE>()
      .AddRow(true,   1, 2, 1)
      .AddRow(true,  __, 2, __)
      .AddRow(false, __, 2, 2)
      .Build(), &NullingIf);
}

TEST(TernaryExpressionTest, IfWithNullOtherwise) {
  TestEvaluation(BlockBuilder<BOOL, BOOL, BOOL, BOOL>()
      .AddRow(true,  true,  false, true)
      .AddRow(true,  true,  __,    true)
      .AddRow(false, true,  __,    __)
      .Build(), &If);

  TestEvaluation(BlockBuilder<BOOL, BOOL, BOOL, BOOL>()
      .AddRow(true,  true,  false, true)
      .AddRow(true,  true,  __,    true)
      .AddRow(false, true,  __,    __)
      .Build(), &NullingIf);
}

TEST(TernaryExpressionTest, IfWithAllNullable) {
  TestEvaluation(BlockBuilder<BOOL, INT32, INT32, INT32>()
      .AddRow(true,  1,  2,  1)
      .AddRow(false, 1,  2,  2)
      .AddRow(__,    1,  2,  2)
      .AddRow(true,  __, 2,  __)
      .AddRow(false, __, 2,  2)
      .AddRow(__,    __, 2,  2)
      .AddRow(true,  1,  __, 1)
      .AddRow(false, 1,  __, __)
      .AddRow(__,    1,  __, __)
      .AddRow(true,  __, __, __)
      .AddRow(false, __, __, __)
      .AddRow(__,    __, __, __)
      .Build(), &If);

  TestEvaluation(BlockBuilder<BOOL, INT32, INT32, INT32>()
      .AddRow(true,  1,  2,  1)
      .AddRow(false, 1,  2,  2)
      .AddRow(__,    1,  2,  __)
      .AddRow(true,  __, 2,  __)
      .AddRow(false, __, 2,  2)
      .AddRow(__,    __, 2,  __)
      .AddRow(true,  1,  __, 1)
      .AddRow(false, 1,  __, __)
      .AddRow(__,    1,  __, __)
      .AddRow(true,  __, __, __)
      .AddRow(false, __, __, __)
      .AddRow(__,    __, __, __)
      .Build(), &NullingIf);
}

TEST(TernaryExpressionTest, IfShortCircuit) {
  // Order of columns:
  // input skip vector, left input, skip vector passed to left input,
  // middle input, skip vector passed to middle input, right input, right skip,
  // result.
  TestShortCircuitTernary(
      BlockBuilder<BOOL, BOOL, BOOL, INT32, BOOL, INT32, BOOL, INT32>()
          .AddRow(false, true,  false, 1,  false, 2,  true,  1)
          .AddRow(false, true,  false, __, false, 2,  true,  __)
          .AddRow(false, true,  false, 1,  false, __, true,  1)
          .AddRow(false, false, false, __, true,  2,  false, 2)
          .AddRow(false, false, false, 1,  true,  __, false, __)
          .AddRow(false, __,    false, 1,  true,  2,  false, 2)
          .AddRow(true,  true,  true,  1,  true,  2,  true,  __)
          .AddRow(true,  false, true,  1,  true,  2,  true,  __)
          .AddRow(true,  __,    true,  1,  true,  2,  true,  __)
          .Build(), &If);
}

TEST(TernaryExpressionTest, NullingIfShortCircuit) {
  // Order of columns:
  // input skip vector, left input, skip vector passed to left input,
  // middle input, skip vector passed to middle input, right input, right skip,
  // result.
  TestShortCircuitTernary(
      BlockBuilder<BOOL, BOOL, BOOL, INT32, BOOL, INT32, BOOL, INT32>()
          .AddRow(false, true,  false, 1,  false, 2,  true,  1)
          .AddRow(false, true,  false, __, false, 2,  true,  __)
          .AddRow(false, true,  false, 1,  false, __, true,  1)
          .AddRow(false, false, false, __, true,  2,  false, 2)
          .AddRow(false, false, false, 1,  true,  __, false, __)
          .AddRow(false, __,    false, 1,  true,  2,  true,  __)
          .AddRow(true,  true,  true,  1,  true,  2,  true,  __)
          .AddRow(true,  false, true,  1,  true,  2,  true,  __)
          .AddRow(true,  __,    true,  1,  true,  2,  true,  __)
          .Build(), &NullingIf);
}

}  // namespace

}  // namespace supersonic
