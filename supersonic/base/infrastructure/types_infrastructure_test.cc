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

#include "supersonic/base/infrastructure/types_infrastructure.h"

#include <string>
namespace supersonic {using std::string; }

#include "supersonic/utils/integral_types.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/operators.h"
#include "gtest/gtest.h"

namespace supersonic {

class PrinterTest : public testing::Test {
 public:
  template<DataType type>
  void TestPrinter(const typename TypeTraits<type>::cpp_type& value,
                   const char* expected_value) {
    string target = "dummy ";
    GetDefaultPrinterFn(type)(&value, &target);
    EXPECT_EQ(string("dummy ") + expected_value, target);
  }

  template<DataType type>
  void TestPrinterForNull() {
    string target = "dummy ";
    GetDefaultPrinterFn(type)(NULL, &target);
    EXPECT_EQ("dummy NULL", target);
  }

  template<DataType type>
  void TestAsString(const typename TypeTraits<type>::cpp_type& value,
                    const char* expected_value) {
    AsString<type> as_string;
    StringPiece result = as_string(value);
    EXPECT_EQ(result, expected_value);
  }
};

TEST_F(PrinterTest, ShouldPrintCorrectValues) {
  string target = "dummy";
  TestPrinter<INT32>(316, "316");
  TestPrinter<INT32>(-5, "-5");

  TestPrinter<UINT32>(2316, "2316");
  TestPrinter<UINT32>(-5, "4294967291");

  TestPrinter<INT64>(334153124625418816LL, "334153124625418816");
  TestPrinter<INT64>(-334153124625418816LL, "-334153124625418816");

  TestPrinter<UINT64>(334153124625418816LL, "334153124625418816");
  TestPrinter<UINT64>(-334153124625418816LL, "18112590949084132800");

  TestPrinter<FLOAT>(1.18, "1.18");
  TestPrinter<FLOAT>(-1.18, "-1.18");
  TestPrinter<FLOAT>(0.0, "0");
  TestPrinter<FLOAT>(-1.244e24, "-1.244e+24");
  TestPrinter<FLOAT>(-1.43e5, "-143000");

  TestPrinter<DOUBLE>(1.18, "1.18");
  TestPrinter<DOUBLE>(-1.18, "-1.18");
  TestPrinter<DOUBLE>(0.0, "0");
  TestPrinter<DOUBLE>(-1.244e24, "-1.244e+24");
  TestPrinter<DOUBLE>(-1.43e5, "-143000");

  TestPrinter<BOOL>(true, "TRUE");
  TestPrinter<BOOL>(false, "FALSE");

  TestPrinter<DATETIME>(1260189023 * 1000000LL, "2009/12/07-12:30:23");
  TestPrinter<DATETIME>(-24 * 60 * 60 * 1000000LL, "1969/12/31-00:00:00");
  TestPrinter<DATETIME>(24 * 60 * 60 * 1000000LL, "1970/01/02-00:00:00");

  TestPrinter<DATE>(14585, "2009/12/07");
  TestPrinter<DATE>(-1, "1969/12/31");
  TestPrinter<DATE>(1, "1970/01/02");

  TestPrinter<BINARY>("", "<0x>");
  TestPrinter<BINARY>("123", "<0x 313233>");
  TestPrinter<BINARY>("deadBABE12343", "<0x 64656164 42414245 31323334 33>");
  TestPrinter<BINARY>("\xfa\xce", "<0x face>");
  TestPrinter<BINARY>(StringPiece("\0", 1), "<0x 00>");

  TestPrinter<DATA_TYPE>(INT32, "INT32");
  TestPrinter<DATA_TYPE>(STRING, "STRING");
}

TEST_F(PrinterTest, ShouldHandleNulls) {
  TestPrinterForNull<INT32>();
  TestPrinterForNull<UINT32>();
  TestPrinterForNull<INT64>();
  TestPrinterForNull<UINT64>();
  TestPrinterForNull<FLOAT>();
  TestPrinterForNull<DOUBLE>();
  TestPrinterForNull<BOOL>();
  TestPrinterForNull<STRING>();
  TestPrinterForNull<DATETIME>();
  TestPrinterForNull<DATE>();
  TestPrinterForNull<BINARY>();
  TestPrinterForNull<DATA_TYPE>();
}

TEST_F(PrinterTest, AsString) {
  TestAsString<INT32>(1, "1");
  TestAsString<UINT32>(2, "2");
  TestAsString<INT64>(3LL, "3");
  TestAsString<UINT64>(4LL, "4");
  TestAsString<FLOAT>(5., "5");
  TestAsString<DOUBLE>(6., "6");
  TestAsString<BOOL>(true, "TRUE");
  TestAsString<DATE>(1, "1970/01/02");
  TestAsString<DATETIME>(24 * 60 * 60 * 1000000LL, "1970/01/02-00:00:00");
  TestAsString<STRING>("String", "String");
  TestAsString<BINARY>("123", "<0x 313233>");
  TestAsString<DATA_TYPE>(DATA_TYPE, "DATA_TYPE");
}

class ParserTest : public testing::Test {
 public:
  template<DataType type>
  void TestParser(const char* value,
                  const typename TypeTraits<type>::cpp_type& expected_value) {
    typedef typename TypeTraits<type>::cpp_type cpp_type;
    cpp_type target = cpp_type();
    EXPECT_TRUE(GetDefaultParserFn(type)(value, &target))
        << "Failed to parse: " << value;
    EXPECT_EQ(expected_value, target) << "for: " << value;
  }

  template<DataType type>
  void TestParserForNull() {
    typename TypeTraits<type>::cpp_type target;
    EXPECT_FALSE(GetDefaultParserFn(type)("NULL", &target));
    EXPECT_FALSE(GetDefaultParserFn(type)("gibberish", &target));
  }
};

TEST_F(ParserTest, ShouldParseCorrectValues) {
  // TODO(user): check if parse errors are propagated.
  TestParser<INT32>("316", 316);
  TestParser<INT32>("  316 \t ", 316);
  TestParser<INT32>("2147483647", 2147483647);
  TestParser<INT32>("-2147483648", -2147483648LL);
  TestParser<INT32>("0", 0);
  TestParser<INT32>("-5", -5);

  TestParser<UINT32>("2316", 2316);
  TestParser<UINT32>("4294967291", 4294967291LL);
  TestParser<UINT32>("0", 0);

  TestParser<INT64>("334153124625418816", 334153124625418816LL);
  TestParser<INT64>("0", 0);
  TestParser<INT64>("-334153124625418816", -334153124625418816LL);

  TestParser<UINT64>("334153124625418816", 334153124625418816ULL);
  TestParser<UINT64>("18112590949084132800", 18112590949084132800ULL);

  TestParser<FLOAT>("1.18", 1.18);
  TestParser<FLOAT>("-1.18", -1.18);
  TestParser<FLOAT>("0", 0.0);
  TestParser<FLOAT>("0.0", 0.0);
  TestParser<FLOAT>("-1.244e24", -1.244e+24);
  TestParser<FLOAT>("-1.244e+24", -1.244e+24);
  TestParser<FLOAT>("-1.43e5", -143000);
  TestParser<FLOAT>("-143000", -143000);

  TestParser<DOUBLE>("1.18", 1.18);
  TestParser<DOUBLE>("-1.18", -1.18);
  TestParser<DOUBLE>("0", 0.0);
  TestParser<DOUBLE>("0.0", 0.0);
  TestParser<DOUBLE>("-1.244e24", -1.244e+24);
  TestParser<DOUBLE>("-1.244e+24", -1.244e+24);
  TestParser<DOUBLE>("-1.43e5", -143000);
  TestParser<DOUBLE>("-143000", -143000);

  TestParser<BOOL>("TRUE", true);
  TestParser<BOOL>(" TRUE \t ", true);
  TestParser<BOOL>("true", true);
  TestParser<BOOL>("tRuE", true);
  TestParser<BOOL>("FALSE", false);
  TestParser<BOOL>(" FALSE \t ", false);
  TestParser<BOOL>("FaLSe", false);
  TestParser<BOOL>("yes ", true);
  TestParser<BOOL>(" Yes \t ", true);
  TestParser<BOOL>(" no", false);
  TestParser<BOOL>("NO", false);

  TestParser<DATETIME>("2009/12/07-12:30:23", 1260189023 * 1000000LL);
  TestParser<DATETIME>("1970/01/02-00:00:00", 24 * 60 * 60 * 1000000LL);
  TestParser<DATETIME>(" 1970/01/02-00:00:00", 24 * 60 * 60 * 1000000LL);
  TestParser<DATETIME>("   1970/01/02-00:00:00 ", 24 * 60 * 60 * 1000000LL);
  TestParser<DATETIME>(" \n\t  1970/01/02-00:00:00 ", 24 * 60 * 60 * 1000000LL);

  TestParser<DATE>("1970/01/02", 1);
  TestParser<DATE>(" 1970/01/02", 1);
  TestParser<DATE>("   1970/01/02 ", 1);
  TestParser<DATE>(" \n\t  1970/01/02 ", 1);
  TestParser<DATE>("2009/12/07", 14585);

  TestParser<DATA_TYPE>("INT32", INT32);
  TestParser<DATA_TYPE>("STRING", STRING);
}

TEST_F(ParserTest, ShouldParseNulls) {
  TestParserForNull<INT32>();
  TestParserForNull<UINT32>();
  TestParserForNull<INT64>();
  TestParserForNull<UINT64>();
  TestParserForNull<FLOAT>();
  TestParserForNull<DOUBLE>();
  TestParserForNull<BOOL>();
  TestParserForNull<DATETIME>();
  TestParserForNull<DATE>();
  TestParserForNull<DATA_TYPE>();
}

class SortComparatorTest : public testing::Test {};

TEST(SortComparatorTest, ThreeWayCompareInt32ToInt32) {
  ComparisonResult result;
  result = ThreeWayCompare<INT32, INT32, false>(1, 2);
  EXPECT_EQ(RESULT_LESS, result);

  result = ThreeWayCompare<INT32, INT32, false>(2, 1);
  EXPECT_EQ(RESULT_GREATER, result);

  result = ThreeWayCompare<INT32, INT32, false>(1, 1);
  EXPECT_EQ(RESULT_EQUAL, result);

  result = ThreeWayCompare<INT32, INT32, true>(-2, -1);
  EXPECT_EQ(RESULT_LESS, result);

  result = ThreeWayCompare<INT32, INT32, true>(-1, -2);
  EXPECT_EQ(RESULT_GREATER_OR_EQUAL, result);

  result = ThreeWayCompare<INT32, INT32, true>(-4, -4);
  EXPECT_EQ(RESULT_GREATER_OR_EQUAL, result);
}

TEST(SortComparatorTest, ThreeWayCompareInt32ToUInt32) {
  ComparisonResult result;
  result = ThreeWayCompare<INT32, UINT32, false>(-1, 4000000000U);
  EXPECT_EQ(RESULT_LESS, result);

  result = ThreeWayCompare<INT32, UINT32, false>(12, 4);
  EXPECT_EQ(RESULT_GREATER, result);

  result = ThreeWayCompare<INT32, UINT32, false>(1, 1);
  EXPECT_EQ(RESULT_EQUAL, result);

  result = ThreeWayCompare<INT32, UINT32, true>(-2, 3000000000U);
  EXPECT_EQ(RESULT_LESS, result);

  result = ThreeWayCompare<INT32, UINT32, true>(20, 4);
  EXPECT_EQ(RESULT_GREATER_OR_EQUAL, result);

  result = ThreeWayCompare<INT32, UINT32, true>(4, 4);
  EXPECT_EQ(RESULT_GREATER_OR_EQUAL, result);
}

TEST(SortComparatorTest, ThreeWayCompareStringToString) {
  ComparisonResult result;
  result = ThreeWayCompare<STRING, STRING, false>("allons", "enfants");
  EXPECT_EQ(RESULT_LESS, result);

  result = ThreeWayCompare<STRING, STRING, false>("star", "spangled");
  EXPECT_EQ(RESULT_GREATER, result);

  result = ThreeWayCompare<STRING, STRING, false>("Deutschland", "Deutschland");
  EXPECT_EQ(RESULT_EQUAL, result);

  result = ThreeWayCompare<STRING, STRING, true>("God", "Save");
  EXPECT_EQ(RESULT_LESS, result);

  result = ThreeWayCompare<STRING, STRING, true>("siam", "pronti");
  EXPECT_EQ(RESULT_GREATER_OR_EQUAL, result);

  result = ThreeWayCompare<STRING, STRING, true>("Marsz", "Marsz");
  EXPECT_EQ(RESULT_GREATER_OR_EQUAL, result);
}

TEST(SortComparatorTest, ShouldSortIntegersAscendingNonTerminal) {
  const int32 data[] = { -5, 0, 4, 4};
  InequalityComparator comp = GetSortComparator(INT32, false, false, false);
  EXPECT_EQ(RESULT_LESS, comp(&data[0], &data[1]));
  EXPECT_EQ(RESULT_LESS, comp(&data[1], &data[2]));
  EXPECT_EQ(RESULT_EQUAL, comp(&data[2], &data[3]));
  EXPECT_EQ(RESULT_LESS, comp(NULL, &data[3]));
  EXPECT_EQ(RESULT_EQUAL, comp(NULL, NULL));
  EXPECT_EQ(RESULT_GREATER, comp(&data[3], NULL));
  EXPECT_EQ(RESULT_GREATER, comp(&data[3], &data[1]));
}

TEST(SortComparatorTest, ShouldSortIntegersAscendingTerminal) {
  const int32 data[] = { -5, 0, 4, 4};
  InequalityComparator comp = GetSortComparator(INT32, false, false, true);
  EXPECT_EQ(RESULT_LESS, comp(&data[0], &data[1]));
  EXPECT_EQ(RESULT_LESS, comp(&data[1], &data[2]));
  EXPECT_EQ(RESULT_GREATER_OR_EQUAL, comp(&data[2], &data[3]));
  EXPECT_EQ(RESULT_LESS, comp(NULL, &data[3]));
  EXPECT_EQ(RESULT_GREATER_OR_EQUAL, comp(NULL, NULL));
  EXPECT_EQ(RESULT_GREATER_OR_EQUAL, comp(&data[3], NULL));
  EXPECT_EQ(RESULT_GREATER_OR_EQUAL, comp(&data[3], &data[1]));
}

TEST(SortComparatorTest, ShouldSortIntegersDescendingNonTerminal) {
  const int32 data[] = { -5, 0, 4, 4};
  InequalityComparator comp = GetSortComparator(INT32, true, false, false);
  EXPECT_EQ(RESULT_LESS, comp(&data[1], &data[0]));
  EXPECT_EQ(RESULT_LESS, comp(&data[2], &data[1]));
  EXPECT_EQ(RESULT_EQUAL, comp(&data[3], &data[2]));
  EXPECT_EQ(RESULT_LESS, comp(&data[3], NULL));
  EXPECT_EQ(RESULT_EQUAL, comp(NULL, NULL));
  EXPECT_EQ(RESULT_GREATER, comp(NULL, &data[3]));
  EXPECT_EQ(RESULT_GREATER, comp(&data[1], &data[3]));
}

TEST(SortComparatorTest, ShouldSortIntegersDescendingTerminal) {
  const int32 data[] = { -5, 0, 4, 4};
  InequalityComparator comp = GetSortComparator(INT32, true, false, true);
  EXPECT_EQ(RESULT_LESS, comp(&data[1], &data[0]));
  EXPECT_EQ(RESULT_LESS, comp(&data[2], &data[1]));
  EXPECT_EQ(RESULT_GREATER_OR_EQUAL, comp(&data[3], &data[2]));
  EXPECT_EQ(RESULT_LESS, comp(&data[3], NULL));
  EXPECT_EQ(RESULT_GREATER_OR_EQUAL, comp(NULL, NULL));
  EXPECT_EQ(RESULT_GREATER_OR_EQUAL, comp(NULL, &data[3]));
  EXPECT_EQ(RESULT_GREATER_OR_EQUAL, comp(&data[1], &data[3]));
}

TEST(SortComparatorTest, ShouldSortStringsAscendingTerminal) {
  const StringPiece data[] = { "bar", "barr", "foo", "foo" };
  InequalityComparator comp = GetSortComparator(STRING, false, false, true);
  EXPECT_EQ(RESULT_LESS, comp(&data[0], &data[1]));
  EXPECT_EQ(RESULT_LESS, comp(&data[1], &data[2]));
  EXPECT_EQ(RESULT_GREATER_OR_EQUAL, comp(&data[2], &data[3]));
  EXPECT_EQ(RESULT_LESS, comp(NULL, &data[3]));
  EXPECT_EQ(RESULT_GREATER_OR_EQUAL, comp(NULL, NULL));
  EXPECT_EQ(RESULT_GREATER_OR_EQUAL, comp(&data[3], NULL));
  EXPECT_EQ(RESULT_GREATER_OR_EQUAL, comp(&data[3], &data[1]));
}

TEST(SortComparatorTest, ShouldSortStringsAscendingNonTerminal) {
  const StringPiece data[] = { "bar", "barr", "foo", "foo" };
  InequalityComparator comp = GetSortComparator(STRING, false, false, false);
  EXPECT_EQ(RESULT_LESS, comp(&data[0], &data[1]));
  EXPECT_EQ(RESULT_LESS, comp(&data[1], &data[2]));
  EXPECT_EQ(RESULT_EQUAL, comp(&data[2], &data[3]));
  EXPECT_EQ(RESULT_LESS, comp(NULL, &data[3]));
  EXPECT_EQ(RESULT_EQUAL, comp(NULL, NULL));
  EXPECT_EQ(RESULT_GREATER, comp(&data[3], NULL));
  EXPECT_EQ(RESULT_GREATER, comp(&data[3], &data[1]));
}

class StaticBindingTest : public testing::Test {};

TEST_F(StaticBindingTest, Equality) {
  EqualityWithNullsComparator<INT32, INT64, false, false> comp;
  const int32 data1[] = { -5, 0, 4 };
  const int64 data2[] = { -5, 0, 4 };
  EXPECT_TRUE(comp(&data1[0], &data2[0]));
  EXPECT_TRUE(comp(&data1[1], &data2[1]));
  EXPECT_TRUE(comp(&data1[2], &data2[2]));
  EXPECT_FALSE(comp(&data1[0], &data2[1]));
  EXPECT_FALSE(comp(&data1[1], &data2[2]));
  EXPECT_FALSE(comp(&data1[2], &data2[0]));
}

TEST_F(StaticBindingTest, Inequality) {
  InequalityWithNullsComparator<INT32, INT64, false, false,
                                false, false, false> comp;
  const int32 data1[] = { -5, 0, 4 };
  const int64 data2[] = { -5, 0, 4 };
  EXPECT_EQ(RESULT_EQUAL, comp(&data1[0], &data2[0]));
  EXPECT_EQ(RESULT_EQUAL, comp(&data1[1], &data2[1]));
  EXPECT_EQ(RESULT_EQUAL, comp(&data1[2], &data2[2]));
  EXPECT_EQ(RESULT_LESS, comp(&data1[0], &data2[1]));
  EXPECT_EQ(RESULT_LESS, comp(&data1[1], &data2[2]));
  EXPECT_EQ(RESULT_LESS, comp(NULL, &data2[3]));
  EXPECT_EQ(RESULT_GREATER, comp(&data1[1], &data2[0]));
  EXPECT_EQ(RESULT_GREATER, comp(&data1[2], &data2[1]));
  EXPECT_EQ(RESULT_GREATER, comp(&data1[2], NULL));
  EXPECT_EQ(RESULT_EQUAL_NULL, comp(NULL, NULL));
}

class EqualsComparatorTest : public testing::Test {};

TEST_F(EqualsComparatorTest, ShouldCompareMixedIntegers) {
  const int32 data1[] = { -5, 0, 4 };
  const int64 data2[] = { -5, 0, 4 };
  EqualityComparator comp = GetEqualsComparator(INT32, INT64, true, true);
  EXPECT_TRUE(comp(&data1[0], &data2[0]));
  EXPECT_TRUE(comp(&data1[1], &data2[1]));
  EXPECT_TRUE(comp(&data1[2], &data2[2]));
  EXPECT_FALSE(comp(&data1[0], &data2[1]));
  EXPECT_FALSE(comp(&data1[1], &data2[2]));
  EXPECT_FALSE(comp(&data1[2], &data2[3]));
}

TEST_F(EqualsComparatorTest, ShouldCompareStrings) {
  const StringPiece data1[] = { "foo", "bar", "baz" };
  const StringPiece data2[] = { "foo", "bar", "baz" };
  EqualityComparator comp = GetEqualsComparator(STRING, STRING, false, false);
  EXPECT_TRUE(comp(&data1[0], &data2[0]));
  EXPECT_TRUE(comp(&data1[1], &data2[1]));
  EXPECT_TRUE(comp(&data1[2], &data2[2]));
  EXPECT_FALSE(comp(&data1[0], &data2[1]));
  EXPECT_FALSE(comp(&data1[1], &data2[2]));
  EXPECT_FALSE(comp(&data1[2], &data2[3]));
}

TEST_F(EqualsComparatorTest, ShouldCompareIntegersWithNull) {
  const int32 data1[] = { -5, NULL, 4 };
  const int32 data2[] = { NULL, 1, 4 };
  EqualityComparator comp = GetEqualsComparator(INT32, INT32, false, false);
  EXPECT_FALSE(comp(&data1[0], NULL));
  EXPECT_FALSE(comp(NULL, &data2[1]));
  EXPECT_TRUE(comp(&data1[2], &data2[2]));
  EXPECT_TRUE(comp(NULL, NULL));
}


class MergeComparatorTest : public testing::Test {};

TEST_F(MergeComparatorTest, ShouldCompareMixedIntegersAscending) {
  const int32 data1[] = { -5, 0, 4 };
  const int64 data2[] = { -5, 0, 4 };
  InequalityComparator comp =
      GetMergeComparator(INT32, INT64, false, false, false);
  EXPECT_EQ(RESULT_EQUAL, comp(&data1[0], &data2[0]));
  EXPECT_EQ(RESULT_EQUAL, comp(&data1[1], &data2[1]));
  EXPECT_EQ(RESULT_EQUAL, comp(&data1[2], &data2[2]));
  EXPECT_EQ(RESULT_LESS, comp(&data1[0], &data2[1]));
  EXPECT_EQ(RESULT_LESS, comp(&data1[1], &data2[2]));
  EXPECT_EQ(RESULT_LESS, comp(NULL, &data2[3]));
  EXPECT_EQ(RESULT_GREATER, comp(&data1[1], &data2[0]));
  EXPECT_EQ(RESULT_GREATER, comp(&data1[2], &data2[1]));
  EXPECT_EQ(RESULT_GREATER, comp(&data1[2], NULL));
  EXPECT_EQ(RESULT_EQUAL_NULL, comp(NULL, NULL));
}

TEST_F(MergeComparatorTest, ShouldCompareStringsDescending) {
  const StringPiece data1[] = { "bar", "barr", "foo" };
  const StringPiece data2[] = { "bar", "barr", "foo" };
  InequalityComparator comp =
      GetMergeComparator(STRING, STRING, false, false, true);
  EXPECT_EQ(RESULT_EQUAL, comp(&data1[0], &data2[0]));
  EXPECT_EQ(RESULT_EQUAL, comp(&data1[1], &data2[1]));
  EXPECT_EQ(RESULT_EQUAL, comp(&data1[2], &data2[2]));
  EXPECT_EQ(RESULT_GREATER, comp(&data1[0], &data2[1]));
  EXPECT_EQ(RESULT_GREATER, comp(&data1[1], &data2[2]));
  EXPECT_EQ(RESULT_GREATER, comp(NULL, &data2[3]));
  EXPECT_EQ(RESULT_LESS, comp(&data1[1], &data2[0]));
  EXPECT_EQ(RESULT_LESS, comp(&data1[2], &data2[1]));
  EXPECT_EQ(RESULT_LESS, comp(&data1[2], NULL));
  EXPECT_EQ(RESULT_EQUAL_NULL, comp(NULL, NULL));
}

class HasherTest : public testing::Test {};

TEST_F(HasherTest, ShouldHashIntegers) {
  const int32 data[] = { -5, 0, 4 };
  Hasher hasher = GetHasher(INT32, false);
  std::hash<int32> reference;
  EXPECT_EQ(0xdeadbabe, hasher(NULL));
  EXPECT_EQ(reference(data[0]), hasher(&data[0]));
  EXPECT_EQ(reference(data[1]), hasher(&data[1]));
  EXPECT_EQ(reference(data[2]), hasher(&data[2]));
}

inline size_t HashString(const StringPiece& s) {
  return operators::MurmurHash64(s.data(), s.length());
}

TEST_F(HasherTest, ShouldHashStrings) {
  const StringPiece data[] = { "bar", "barr", "foo" };
  Hasher hasher = GetHasher(STRING, false);
  EXPECT_EQ(0xdeadbabe, hasher(NULL));
  EXPECT_EQ(HashString(data[0]), hasher(&data[0]));
  EXPECT_EQ(HashString(data[1]), hasher(&data[1]));
  EXPECT_EQ(HashString(data[2]), hasher(&data[2]));
}

class ColumnHasherTest : public testing::Test {};

TEST_F(ColumnHasherTest, ShouldHashColumns) {
  const int32 data[] = { -5, 0, 4, 4 };
  small_bool_array is_null;
  const bool is_null_data[] = { false, false, false, true };
  bit_pointer::FillFrom(is_null.mutable_data(), is_null_data, 4);
  ColumnHasher hasher = GetColumnHasher(INT32, false, false);
  size_t result[4];
  hasher(data, is_null.const_data(), 4, result);
  std::hash<int32> reference;
  EXPECT_EQ(reference(data[0]), result[0]);
  EXPECT_EQ(reference(data[1]), result[1]);
  EXPECT_EQ(reference(data[2]), result[2]);
  EXPECT_EQ(0xdeadbabe, result[3]);
}

TEST_F(ColumnHasherTest, ShouldHashUpdateColumns) {
  const StringPiece data[] = { "bar", "barr", "foo", "foo" };
  small_bool_array is_null;
  const bool is_null_data[] = { false, false, true, false };
  bit_pointer::FillFrom(is_null.mutable_data(), is_null_data, 4);
  ColumnHasher hasher = GetColumnHasher(STRING, true, false);
  size_t result[] = { 1, 2, 3, 4 };
  hasher(data, is_null.const_data(), 4, result);
  EXPECT_EQ(1 * 29 + HashString(data[0]), result[0]);
  EXPECT_EQ(2 * 29 + HashString(data[1]), result[1]);
  EXPECT_EQ(3 * 29 + 0xdeadbabe, result[2]);
  EXPECT_EQ(4 * 29 + HashString(data[2]), result[3]);
}

TEST_F(ColumnHasherTest, ShouldHashNotNullColumns) {
  const int32 data[] = { -5, 0, 4, 4 };
  ColumnHasher hasher = GetColumnHasher(INT32, false, true);
  size_t result[4];
  hasher(data, bool_ptr(NULL), 4, result);
  std::hash<int32> reference;
  EXPECT_EQ(reference(data[0]), result[0]);
  EXPECT_EQ(reference(data[1]), result[1]);
  EXPECT_EQ(reference(data[2]), result[2]);
  EXPECT_EQ(reference(data[3]), result[3]);
}

struct TestFunctor {
  template<DataType type>
  FailureOr<DataType> operator ()() const {
    return Success(type);
  }
};

TEST(IntegerTypeSpecializationTest, TestReturnTypes) {
  TestFunctor functor;
  typedef FailureOr<DataType> ResultType;

  ResultType result =
      IntegerTypeSpecialization<ResultType, TestFunctor>(INT32, functor);
  EXPECT_TRUE(result.is_success());
  EXPECT_EQ(INT32, result.get());

  result = IntegerTypeSpecialization<ResultType, TestFunctor>(INT64, functor);
  EXPECT_TRUE(result.is_success());
  EXPECT_EQ(INT64, result.get());

  result = IntegerTypeSpecialization<ResultType, TestFunctor>(UINT32, functor);
  EXPECT_TRUE(result.is_success());
  EXPECT_EQ(UINT32, result.get());

  result = IntegerTypeSpecialization<ResultType, TestFunctor>(UINT64, functor);
  EXPECT_TRUE(result.is_success());
  EXPECT_EQ(UINT64, result.get());

  result = IntegerTypeSpecialization<ResultType, TestFunctor>(FLOAT, functor);
  EXPECT_FALSE(result.is_success());

  result = IntegerTypeSpecialization<ResultType, TestFunctor>(DOUBLE, functor);
  EXPECT_FALSE(result.is_success());

  result = IntegerTypeSpecialization<ResultType, TestFunctor>(BOOL, functor);
  EXPECT_FALSE(result.is_success());

  result = IntegerTypeSpecialization<ResultType, TestFunctor>(DATE, functor);
  EXPECT_FALSE(result.is_success());

  result = IntegerTypeSpecialization<ResultType, TestFunctor>(DATETIME,
                                                              functor);
  EXPECT_FALSE(result.is_success());

  result = IntegerTypeSpecialization<ResultType, TestFunctor>(STRING, functor);
  EXPECT_FALSE(result.is_success());

  result = IntegerTypeSpecialization<ResultType, TestFunctor>(BINARY, functor);
  EXPECT_FALSE(result.is_success());

  result = IntegerTypeSpecialization<ResultType, TestFunctor>(DATA_TYPE,
                                                              functor);
  EXPECT_FALSE(result.is_success());
}

TEST(NumericTypeSpecializationTest, TestReturnTypes) {
  TestFunctor functor;
  typedef FailureOr<DataType> ResultType;

  ResultType result =
      NumericTypeSpecialization<ResultType, TestFunctor>(INT32, functor);
  EXPECT_TRUE(result.is_success());
  EXPECT_EQ(INT32, result.get());

  result = NumericTypeSpecialization<ResultType, TestFunctor>(INT64, functor);
  EXPECT_TRUE(result.is_success());
  EXPECT_EQ(INT64, result.get());

  result = NumericTypeSpecialization<ResultType, TestFunctor>(UINT32, functor);
  EXPECT_TRUE(result.is_success());
  EXPECT_EQ(UINT32, result.get());

  result = NumericTypeSpecialization<ResultType, TestFunctor>(UINT64, functor);
  EXPECT_TRUE(result.is_success());
  EXPECT_EQ(UINT64, result.get());

  result = NumericTypeSpecialization<ResultType, TestFunctor>(FLOAT, functor);
  EXPECT_TRUE(result.is_success());
  EXPECT_EQ(FLOAT, result.get());

  result = NumericTypeSpecialization<ResultType, TestFunctor>(DOUBLE, functor);
  EXPECT_TRUE(result.is_success());
  EXPECT_EQ(DOUBLE, result.get());

  result = NumericTypeSpecialization<ResultType, TestFunctor>(BOOL, functor);
  EXPECT_FALSE(result.is_success());

  result = NumericTypeSpecialization<ResultType, TestFunctor>(DATE, functor);
  EXPECT_FALSE(result.is_success());

  result = NumericTypeSpecialization<ResultType, TestFunctor>(DATETIME,
                                                              functor);
  EXPECT_FALSE(result.is_success());

  result = NumericTypeSpecialization<ResultType, TestFunctor>(STRING, functor);
  EXPECT_FALSE(result.is_success());

  result = NumericTypeSpecialization<ResultType, TestFunctor>(BINARY, functor);
  EXPECT_FALSE(result.is_success());

  result = NumericTypeSpecialization<ResultType, TestFunctor>(DATA_TYPE,
                                                              functor);
  EXPECT_FALSE(result.is_success());
}

}  // namespace supersonic
