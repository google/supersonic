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

#include "supersonic/expression/core/regexp_expressions.h"

#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/cursor/infrastructure/value_ref.h"
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

}  // namespace

}  // namespace supersonic
