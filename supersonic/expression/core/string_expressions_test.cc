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

#include "supersonic/expression/core/string_expressions.h"

#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/expression/base/expression.h"
#include "supersonic/expression/core/projecting_expressions.h"
#include "supersonic/proto/supersonic.pb.h"
#include "supersonic/testing/block_builder.h"
#include "supersonic/testing/comparators.h"
#include "supersonic/testing/expression_test_helper.h"
#include "gtest/gtest.h"

namespace supersonic {
// The test suite (TestBinding, TestEvaluation, and the failure tests) is
// described in
// datawarehouse/common/supersonic/testing/expression_test_helper.h.

namespace {

TEST(StringExpressionTest, Binding) {
  TestUnaryBinding(&Ltrim,     STRING,    "LTRIM($0)",     STRING, false);
  TestUnaryBinding(&Rtrim,     STRING,    "RTRIM($0)",     STRING, false);
  TestUnaryBinding(&Trim,      STRING,    "TRIM($0)",      STRING, false);
  TestUnaryBinding(&Length,    STRING,    "LENGTH($0)",    UINT32, false);
  TestUnaryBinding(&ToUpper,   STRING,    "TO_UPPER($0)",  STRING, false);
  TestUnaryBinding(&ToLower,   STRING,    "TO_LOWER($0)",  STRING, false);
  TestUnaryBinding(&ToString,  BOOL,      "TOSTRING($0)",  STRING, false);
  TestUnaryBinding(&ToString,  DATETIME,  "TOSTRING($0)",  STRING, false);
  TestUnaryBinding(&ToString,  DATE,      "TOSTRING($0)",  STRING, false);
  TestUnaryBinding(&ToString,  FLOAT,     "TOSTRING($0)",  STRING, false);
  TestUnaryBinding(&ToString,  DOUBLE,    "TOSTRING($0)",  STRING, false);
  TestUnaryBinding(&ToString,  INT32,     "TOSTRING($0)",  STRING, false);
  TestUnaryBinding(&ToString,  UINT32,    "TOSTRING($0)",  STRING, false);
  TestUnaryBinding(&ToString,  UINT64,    "TOSTRING($0)",  STRING, false);
  TestUnaryBinding(&ToString,  INT64,     "TOSTRING($0)",  STRING, false);
  TestUnaryBinding(&ToString,  STRING,    "$0",            STRING, false);
  TestUnaryBinding(&ToString,  BINARY,    "TOSTRING($0)",  STRING, false);
  TestBinaryBindingNotNullable(
      &StringOffset, STRING, STRING,   "STRING_OFFSET($0, $1)", INT32);
  TestBinaryBindingNotNullable(&StringContains, STRING, STRING,
      "(CONST_UINT32 < STRING_OFFSET($0, $1))", BOOL);
  TestBinaryBindingNotNullable(&StringContainsCI, STRING, STRING,
      "(CONST_UINT32 < STRING_OFFSET(TO_LOWER(9), TO_LOWER(9)))", BOOL);

  TestBinaryBindingNotNullable(
      &TrailingSubstring, STRING, INT64, "SUBSTRING($0, $1)", STRING);

  TestTernaryBinding(&Substring, STRING, INT64, INT64, "SUBSTRING($0, $1, $2)",
              STRING, false);
  TestTernaryBinding(&StringReplace, STRING, STRING, STRING,
                     "STRING_REPLACE($0, $1, $2)", STRING, false);
}

TEST(StringExpressionTest, BindingWithCast) {
  TestTernaryBinding(&Substring, STRING, INT32, UINT32,
      "SUBSTRING($0, CAST_INT32_TO_INT64($1), CAST_UINT32_TO_INT64($2))",
      STRING, false);
}

TEST(StringExpressionTest, BindingFailures) {
  TestBindingFailure(&Ltrim, BOOL);
  TestBindingFailure(&Trim, DATETIME);
  TestBindingFailure(&Length, BINARY);
  TestBindingFailure(&ToLower, UINT32);
  TestBindingFailure(&ToUpper, DOUBLE);

  TestBindingFailure(&Substring, STRING, INT64, DOUBLE);
  TestBindingFailure(&Substring, STRING, FLOAT, UINT32);
  TestBindingFailure(&StringReplace, INT32, INT32, INT32);
}

TEST(StringExpressionTest, LTrim) {
  TestEvaluation(BlockBuilder<STRING, STRING>()
      .AddRow("",             "")
      .AddRow("LTrim ",       "LTrim ")
      .AddRow("\n\t",         "\n\t")
      .AddRow("   ",          "")
      .AddRow("  String  ",   "String  ")
      .AddRow("\t",           "\t")
      .AddRow("A little dog",  "A little dog")
      .Build(), &Ltrim);

  TestEvaluation(BlockBuilder<STRING, STRING>()
      .AddRow("  ",    "")
      .AddRow(__,      __)
      .AddRow("LTrim", "LTrim")
      .Build(), &Ltrim);
}

TEST(StringExpressionTest, RTrim) {
  TestEvaluation(BlockBuilder<STRING, STRING>()
      .AddRow("",               "")
      .AddRow(" RTrim",         " RTrim")
      .AddRow("\n\t",           "\n\t")
      .AddRow("      ",         "")
      .AddRow("  AnyString  ",  "  AnyString")
      .AddRow("\n",             "\n")
      .AddRow("A small cat ",   "A small cat")
      .Build(), &Rtrim);

  TestEvaluation(BlockBuilder<STRING, STRING>()
      .AddRow("  Goldfish  ",   "  Goldfish")
      .AddRow("RTrim",          "RTrim")
      .AddRow(__,               __)
      .Build(), &Rtrim);
}

TEST(StringExpressionTest, Trim) {
  TestEvaluation(BlockBuilder<STRING, STRING>()
      .AddRow("",               "")
      .AddRow(" Trim",          "Trim")
      .AddRow("Swordfish",      "Swordfish")
      .AddRow(" ",              "")
      .AddRow("    ",           "")
      .AddRow("RightSpace ",    "RightSpace")
      .AddRow(" Little Bird ",  "Little Bird")
      .Build(), &Trim);

  TestEvaluation(BlockBuilder<STRING, STRING>()
      .AddRow("   Parrot   ",  "Parrot")
      .AddRow(__,              __)
      .AddRow("",              "")
      .Build(), &Trim);
}

TEST(StringExpressionTest, Length) {
  TestEvaluation(BlockBuilder<STRING, UINT32>()
      .AddRow("",             0)
      .AddRow(" ",            1)
      .AddRow("\n",           1)
      .AddRow("SuperSonic",   10)
      .AddRow(__,             __)
      .AddRow("S________c",   10)
      .Build(), &Length);
}

TEST(StringExpressionTest, ToUpper) {
  TestEvaluation(BlockBuilder<STRING, STRING>()
      .AddRow("",                     "")
      .AddRow("S",                    "S")
      .AddRow("SuperSonic",           "SUPERSONIC")
      .AddRow("S________c",           "S________C")
      .AddRow("Dog\nCat ",            "DOG\nCAT ")
      .Build(), &ToUpper);

  TestEvaluation(BlockBuilder<STRING, STRING>()
      .AddRow("MOUSE",  "MOUSE")
      .AddRow("rat",    "RAT")
      .AddRow(__,       __)
      .Build(), &ToUpper);
}

TEST(StringExpressionTest, ToLower) {
  TestEvaluation(BlockBuilder<STRING, STRING>()
      .AddRow("",                     "")
      .AddRow("?",                   "?")
      .AddRow("SuperSonic", "supersonic")
      .AddRow("S P Q R",       "s p q r")
      .Build(), &ToLower);

  TestEvaluation(BlockBuilder<STRING, STRING>()
      .AddRow(__,       __)
      .AddRow("MOUSE",  "mouse")
      .AddRow("rat",    "rat")
      .Build(), &ToLower);
}

TEST(StringExpressionTest, Substring) {
  TestEvaluation(BlockBuilder<STRING, INT32, UINT64, STRING>()
      .AddRow("SuperSonic",  3,  4,    "perS")
      .AddRow("SuperSonic",  6,  8,    "Sonic")
      .AddRow("Cyan",        1,  __,   __)
      .AddRow("Magenta",     4,  0,    "")
      .AddRow("Sepia",       1,  1,    "S")
      .AddRow("Tourmaline",  14, 12,   "")
      .AddRow(__,            1,  1,    __)
      .AddRow("Ochre",       4,  1,    "r")
      .Build(), &Substring);
}

TEST(StringExpressionTest, SubstringNegatives) {
  TestEvaluation(BlockBuilder<STRING, INT32, INT32, STRING>()
      .AddRow("SuperSonic",        0,  1,    "")
      .AddRow("SuperSonic",       -1,  1,    "c")
      .AddRow("SuperSonic",       -2,  1,    "i")
      .AddRow("Beige",            -4,  9,    "eige")
      .AddRow("Ultramarine",       5, -1,    "")
      .AddRow("Aquamarine",        0, 20,    "")
      .AddRow("Turquoise",       -10,  2,    "Tu")
      .AddRow("Fuchsia",         -10, 20,    "Fuchsia")
      .Build(), &Substring);
}

TEST(StringExpressionTest, TrailingSubstring) {
  TestEvaluation(BlockBuilder<STRING, UINT32, STRING>()
      .AddRow("Ash",        5,    "")
      .AddRow("Maple",      3,    "ple")
      .AddRow("Pine",       1,    "Pine")
      .AddRow("  Oak  ",    4,    "ak  ")
      .Build(), &TrailingSubstring);

  TestEvaluation(BlockBuilder<STRING, INT64, STRING>()
      .AddRow(__, 1, __)
      .Build(), &TrailingSubstring);
}

TEST(StringExpressionTest, TrailingSubstringNegatives) {
  TestEvaluation(BlockBuilder<STRING, INT32, STRING>()
      .AddRow("Alder",      0,   "")
      .AddRow("Chestnut",  -1,   "t")
      .AddRow("Hornbeam",  -4,   "beam")
      .AddRow("Beech",    -10,   "Beech")
      .AddRow("Sycamore",  -8,   "Sycamore")
      .AddRow(__,          -2,   __)
      .AddRow("Spruce",    __,   __)
      .AddRow("Fir",       -2,   "ir")
      .Build(), &TrailingSubstring);
}

TEST(StringExpressionTest, StringToString) {
  TestEvaluation(BlockBuilder<STRING, STRING>()
      .AddRow("",            "")
      .AddRow("SuperSonic",  "SuperSonic")
      .AddRow("A b C",       "A b C")
      .Build(), &ToString);
}

TEST(StringExpressionTest, DoubleToString) {
  TestEvaluation(BlockBuilder<DOUBLE, STRING>()
      .AddRow(-1.,         "-1")
      .AddRow(3.14159265,  "3.14159265")
      .AddRow(0.02,        "0.02")
      .Build(), &ToString);
}

TEST(StringExpressionTest, BoolToString) {
  TestEvaluation(BlockBuilder<BOOL, STRING>()
      .AddRow(true,   "TRUE")
      .AddRow(false,  "FALSE")
      .AddRow(__,     __)
      .Build(), &ToString);
}

TEST(StringExpressionTest, StringOffset) {
  TestEvaluation(BlockBuilder<STRING, STRING, INT32>()
      .AddRow("Dog",          "Cat",    0)
      .AddRow("Dog",          "Dog",    1)
      .AddRow("Two dogs",     "Dog",    0)
      .AddRow("Two dogs",     "dog",    5)
      .AddRow(__,             "eagle",  __)
      .AddRow("Three eagles", __,       __)
      .AddRow("",             "a cat",  0)
      .AddRow("",             "",       1)
      .AddRow("Cow",          "",       1)
      .Build(), &StringOffset);
}

TEST(StringExpressionTest, StringContains) {
  TestEvaluation(BlockBuilder<STRING, STRING, BOOL>()
      .AddRow("Dog",          "Cat",    false)
      .AddRow("Dog",          "Dog",    true)
      .AddRow("Two dogs",     "Dog",    false)
      .AddRow("Two dogs",     "dog",    true)
      .AddRow(__,             "eagle",  __)
      .AddRow("Three eagles", __,       __)
      .AddRow("",             "a cat",  false)
      .AddRow("",             "",       true)
      .AddRow("Cow",          "",       true)
      .Build(), &StringContains);
}

TEST(StringExpressionTest, StringContainsCI) {
  TestEvaluation(BlockBuilder<STRING, STRING, BOOL>()
      .AddRow("Dog",          "Cat",    false)
      .AddRow("Dog",          "Dog",    true)
      .AddRow("Two dogs",     "Dog",    true)
      .AddRow("Two dogs",     "dog",    true)
      .AddRow(__,             "eagle",  __)
      .AddRow("Three eagles", __,       __)
      .AddRow("",             "a cat",  false)
      .AddRow("",             "",       true)
      .AddRow("Cow",          "",       true)
      .Build(), &StringContainsCI);
}

// The Regexp expression have to be wrapped to fit into the general testing
// scheme. We will want to use several different patterns. As we want to
// have a rather general testing structure, but cannot template upon the
// pattern, we will use enums for the purpose.
enum PatternType {
  EXTRACT,
  FOOBAR,
  STAR,
  X,
  XPLUS,
  WORDS,
  WRONG
};

StringPiece ResolvePattern(PatternType pattern) {
  switch (pattern) {
    case EXTRACT: return "f(\\w+)r";
    case FOOBAR: return "fo+b*a.";
    case STAR: return "[a-z]*";
    case X: return "X";
    case XPLUS: return "X+";
    case WORDS: return "\\w+";
    case WRONG: return "\\W\\Y";
  }
  return "";
}

template<PatternType pattern>
const Expression* RegexpFullPatterned(const Expression* str) {
  return RegexpFullMatch(str, ResolvePattern(pattern));
}

template<PatternType pattern>
const Expression* RegexpPartialPatterned(const Expression* str) {
  return RegexpPartialMatch(str, ResolvePattern(pattern));
}

template<PatternType pattern>
const Expression* RegexpExtractPatterned(const Expression* str) {
  return RegexpExtract(str, ResolvePattern(pattern));
}

template<PatternType pattern>
const Expression* RegexpReplacePatterned(const Expression* str,
                                         const Expression* sub) {
  return RegexpReplace(str, ResolvePattern(pattern), sub);
}

TEST(StringExpressionTest, RegexpBinding) {
  TestUnaryBinding(&RegexpFullPatterned<X>, STRING, "REGEXP_FULL_MATCH($0)",
                   BOOL, false);
  TestUnaryBinding(&RegexpPartialPatterned<WORDS>, STRING,
                   "REGEXP_PARTIAL_MATCH($0)", BOOL, false);
  TestUnaryBinding(&RegexpExtractPatterned<XPLUS>, STRING, "REGEXP_EXTRACT($0)",
                   STRING, true);
  TestBinaryBindingNotNullable(&RegexpReplacePatterned<FOOBAR>, STRING, STRING,
                               "REGEXP_REPLACE($0, $1)", STRING);
}

TEST(StringExpressionTest, RegexpBindingFailure) {
  TestBindingFailure(&RegexpFullPatterned<STAR>, DATETIME);
  TestBindingFailure(&RegexpPartialPatterned<WORDS>, BOOL);
  TestBindingFailure(&RegexpExtractPatterned<X>, INT64);
  TestBindingFailure(&RegexpReplacePatterned<EXTRACT>, BINARY, STRING);
  TestBindingFailure(&RegexpReplacePatterned<XPLUS>, STRING, DATE);
}

TEST(StringExpressionTest, RegexpPatternFailure) {
  TestBindingFailure(&RegexpFullPatterned<WRONG>, STRING);
  TestBindingFailure(&RegexpPartialPatterned<WRONG>, STRING);
  TestBindingFailure(&RegexpExtractPatterned<WRONG>, STRING);
  TestBindingFailure(&RegexpReplacePatterned<WRONG>, STRING, STRING);
}

TEST(StringExpressionTest, RegexpFullMatch) {
  // The pattern is "fo+b*a."
  TestEvaluation(BlockBuilder<STRING, BOOL>()
      .AddRow("foobar",      true)
      .AddRow("fooooooobar", true)
      .AddRow("fobar",       true)
      .AddRow("foobbar",     true)
      .AddRow("foobbaar",    false)
      .AddRow("fooba",       false)
      .AddRow("foobarr",     false)
      .AddRow("",            false)
      .AddRow(__,            __)
      .AddRow("fbar",        false)
      .AddRow("fooar",       true)
      .Build(), &RegexpFullPatterned<FOOBAR>);

  // The pattern is "[a-z]*", this is a non-null test.
  TestEvaluation(BlockBuilder<STRING, BOOL>()
    .AddRow("foobar",     true)
    .Build(), &RegexpFullPatterned<STAR>);
}

TEST(StringExpressionTest, RegexpPartialMatch) {
  // The pattern is "fo+b*a.".
  TestEvaluation(BlockBuilder<STRING, BOOL>()
      .AddRow("foobar",             true)
      .AddRow("fooooooobar",        true)
      .AddRow("fobar",              true)
      .AddRow("foobbar",            true)
      .AddRow("foobbaar",           true)
      .AddRow("fooba",              false)
      .AddRow("foobarr",            true)
      .AddRow("",                   false)
      .AddRow(__,                   __)
      .AddRow("fbar",               false)
      .AddRow("fooar",              true)
      .AddRow("I have a foobar",    true)
      .AddRow("I have two foobars", true)
      .AddRow("I hath foobed, arr", false)
      .Build(), &RegexpPartialPatterned<FOOBAR>);
}

TEST(StringExpressionTest, RegexpExtract) {
  // The pattern is "f(\\w+)r".
  TestEvaluation(BlockBuilder<STRING, STRING>()
      .AddRow("foobar",             "ooba")
      .AddRow("fooooooobar",        "oooooooba")
      .AddRow("fobar",              "oba")
      .AddRow("foobbar",            "oobba")
      .AddRow("foobbaar",           "oobbaa")
      .AddRow("fooba",              __)
      .AddRow("foobarr",            "oobar")
      .AddRow("",                   __)
      .AddRow(__,                   __)
      .AddRow("fbar",               "ba")
      .AddRow("fooar",              "ooa")
      .AddRow("I have a foobar",    "ooba")
      .AddRow("I have two foobars", "ooba")
      .AddRow("I hath foobed, arr", __)
      .AddRow("foa foobar",         "ooba")
      .Build(), &RegexpExtractPatterned<EXTRACT>);
}

// This test assures that RegexpExtract does not allocate new memory for the
// content string, but simply points the resultant StringPiece to the string
// being extracted. Note - this is necessary to assure that all our strings lie
// in the arena. If this test should fail, we need to rewrite the strings
// returned by RE2::PartialMatch into the arena.
TEST(StringExpressionTest, RegexpExtractDoesNotRewrite) {
  scoped_ptr<Block> block(BlockBuilder<STRING>().AddRow("SuperSonic").Build());
  const Expression* child = AttributeAt(0);
  scoped_ptr<BoundExpressionTree> extract(DefaultBind(
      block->view().schema(), 100, RegexpExtract(child, "u(\\w+)i")));
  const View& result_view = DefaultEvaluate(extract.get(), block->view());
  scoped_ptr<Block> expected(BlockBuilder<STRING>().AddRow("perSon").Build());
  EXPECT_VIEWS_EQUAL(expected->view(), result_view);
  const char* input = block->view().column(0).typed_data<STRING>()->data();
  const char* result = result_view.column(0).typed_data<STRING>()->data();
  EXPECT_EQ(input + 2, result);
}

TEST(StringExpressionTest, RegexpReplace) {
  // The pattern is "X+".
  TestEvaluation(BlockBuilder<STRING, STRING, STRING>()
      .AddRow("XxX",          "Y",      "YxY")
      .AddRow("XXX",          "Y",      "Y")
      .AddRow("SuperSonic",   "Run",    "SuperSonic")
      .AddRow("BOX",          "Y",      "BOY")
      .AddRow(__,             "NUL",    __)
      .AddRow("XXuXX",        "YXY",    "YXYuYXY")
      .Build(), &RegexpReplacePatterned<XPLUS>);

  // The pattern is "X".
  TestEvaluation(BlockBuilder<STRING, STRING, STRING>()
      .AddRow("XxX",          "Y",      "YxY")
      .AddRow("XXX",          "Y",      "YYY")
      .Build(), &RegexpReplacePatterned<X>);
}

TEST(StringExpressionTest, StringReplace) {
  TestEvaluation(BlockBuilder<STRING, STRING, STRING, STRING>()
      .AddRow("Cow",   "ow",   "ar",   "Car")
      .AddRow("sooon", "oo",   "o",    "soon")
      .AddRow("s101",  "|",    "||",   "s101")
      .AddRow("ssss",  "s",    "a",    "aaaa")
      .Build(), &StringReplace);
}

// Unfortunately the Concat expression does not fit into the general testing
// scheme. We create wrappers around concat, which take a fixed number of
// parameters.
const Expression* TernaryConcat(const Expression* first,
                                const Expression* second,
                                const Expression* third) {
  return Concat((new ExpressionList())->add(first)->add(second)->add(third));
}

TEST(StringExpressionTest, Concat) {
  TestEvaluation(BlockBuilder<STRING, STRING, STRING, STRING>()
      .AddRow("Super", "",    "Sonic", "SuperSonic")
      .AddRow("",      "",    "Carpathian", "Carpathian")
      .AddRow("Ev",  "erest", "",      "Everest")
      .AddRow("",      "",    "",      "")
      .AddRow("K",     "R",   "K",     "KRK")
      .Build(), &TernaryConcat);
}

TEST(StringExpressionTest, ConcatSchema) {
  scoped_ptr<Block> block(BlockBuilder<STRING, STRING>()
                          .AddRow("SuperSonic", __)
                          .Build());
  ExpressionList* expr_list = new ExpressionList();
  expr_list->add(AttributeAt(0))->add(AttributeAt(1));
  scoped_ptr<BoundExpressionTree>
      concat(DefaultBind(block->view().schema(), 100, Concat(expr_list)));
  EXPECT_TUPLE_SCHEMAS_EQUAL(
      concat->result_schema(),
      TupleSchema::Singleton("CONCAT(col0, col1)", STRING, NULLABLE));
}

TEST(StringExpressionTest, ConcatSchemaWithNull) {
  scoped_ptr<Block> block(BlockBuilder<STRING>()
                          .AddRow("SuperSonic")
                          .Build());
  ExpressionList* expr_list = new ExpressionList();
  expr_list->add(AttributeAt(0));
  scoped_ptr<BoundExpressionTree>
      concat(DefaultBind(block->view().schema(), 100, Concat(expr_list)));
  EXPECT_TUPLE_SCHEMAS_EQUAL(
      concat->result_schema(),
      TupleSchema::Singleton("CONCAT(col0)", STRING, NOT_NULLABLE));
}

TEST(StringExpressionTest, ConcatWithNullFields) {
  scoped_ptr<Block> block(BlockBuilder<STRING, STRING, STRING>()
                          .AddRow("Super", __, "Sonic")
                          .AddRow("Everest", __, "Carpathian")
                          .AddRow(__, "Everest", "Carpathian")
                          .AddRow("K", "R", "K")
                          .Build());
  ExpressionList* expr_list = new ExpressionList();
  expr_list->add(AttributeAt(0))->add(AttributeAt(1))->add(AttributeAt(2));
  scoped_ptr<BoundExpressionTree> concat(
      DefaultBind(block->view().schema(), 100, Concat(expr_list)));
  const View& result = DefaultEvaluate(concat.get(), block->view());
  scoped_ptr<Block> expected(BlockBuilder<STRING>()
                             .AddRow(__)
                             .AddRow(__)
                             .AddRow(__)
                             .AddRow("KRK")
                             .Build());
  EXPECT_VIEWS_EQUAL(expected->view(), result);
}


TEST(StringExpressionTest, ConcatTenInputs) {
  scoped_ptr<Block> block(BlockBuilder<STRING, STRING, STRING, STRING, STRING,
                          STRING, STRING, STRING, STRING, STRING>()
                          .AddRow("S", "u", "p", "e", "r",
                                  "S", "o", "n", "i", "c")
                          .Build());
  ExpressionList* expr_list = new ExpressionList();
  for (int i = 0; i < 10; ++i) expr_list->add(AttributeAt(i));
  scoped_ptr<BoundExpressionTree> concat(
      DefaultBind(block->view().schema(), 100, Concat(expr_list)));
  const View& result = DefaultEvaluate(concat.get(), block->view());
  scoped_ptr<Block> expected(BlockBuilder<STRING>()
                             .AddRow("SuperSonic")
                             .Build());
  EXPECT_VIEWS_EQUAL(expected->view(), result);
}

TEST(StringExpressionTest, ConcatNumbers) {
  scoped_ptr<Block> block(BlockBuilder<STRING, INT32>()
                          .AddRow("Super", 123)
                          .AddRow("Sonic", 124)
                          .AddRow("", -12)
                          .Build());
  ExpressionList* expr_list = new ExpressionList();
  expr_list->add(AttributeAt(0))->add(AttributeAt(1));
  scoped_ptr<BoundExpressionTree> concat(
      DefaultBind(block->view().schema(), 100, Concat(expr_list)));
  const View& result = DefaultEvaluate(concat.get(), block->view());
  scoped_ptr<Block> expected(BlockBuilder<STRING>()
                             .AddRow("Super123")
                             .AddRow("Sonic124")
                             .AddRow("-12")
                             .Build());
  EXPECT_VIEWS_EQUAL(expected->view(), result);
}

}  // namespace

}  // namespace supersonic
