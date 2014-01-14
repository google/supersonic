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

#include "supersonic/base/infrastructure/types.h"

#include <string>
namespace supersonic {using std::string; }

#include "gtest/gtest.h"

namespace supersonic {

class TypeInfoTest : public testing::Test {
 public:
  template<DataType type>
  void TestProperties(bool variable_length,
                      bool numeric,
                      bool integer,
                      bool floating_point) {
    const TypeInfo& type_info = GetTypeInfo(type);
    const string& name = type_info.name();
    EXPECT_EQ(variable_length, type_info.is_variable_length()) << name;
    EXPECT_EQ(numeric, type_info.is_numeric()) << name;
    EXPECT_EQ(integer, type_info.is_integer()) << name;
    EXPECT_EQ(floating_point, type_info.is_floating_point()) << name;
  }

  void TestCreation(DataType type, size_t size) {
    const TypeInfo& t = GetTypeInfo(type);
    EXPECT_EQ(size, t.size());
    EXPECT_EQ(type, t.type());
    // Make sure it's a singleton.
    EXPECT_EQ(&GetTypeInfo(type), &GetTypeInfo(type));
  }
};

TEST_F(TypeInfoTest, ShouldCreateProperly) {
  TestCreation(INT32, 4);
  TestCreation(UINT32, 4);
  TestCreation(INT64, 8);
  TestCreation(UINT64, 8);
  TestCreation(FLOAT, 4);
  TestCreation(DOUBLE, 8);
  TestCreation(BOOL, 1);
  TestCreation(DATE, 4);
  TestCreation(DATETIME, 8);
  TestCreation(STRING, sizeof(StringPiece));
  TestCreation(BINARY, sizeof(StringPiece));
  TestCreation(DATA_TYPE, sizeof(DataType));
}

TEST_F(TypeInfoTest, TypesShouldHaveNames) {
  EXPECT_EQ("INT32", GetTypeInfo(INT32).name());
  EXPECT_EQ("UINT32", GetTypeInfo(UINT32).name());
  EXPECT_EQ("INT64", GetTypeInfo(INT64).name());
  EXPECT_EQ("UINT64", GetTypeInfo(UINT64).name());
  EXPECT_EQ("BOOL", GetTypeInfo(BOOL).name());
  EXPECT_EQ("FLOAT", GetTypeInfo(FLOAT).name());
  EXPECT_EQ("DOUBLE", GetTypeInfo(DOUBLE).name());
  EXPECT_EQ("STRING", GetTypeInfo(STRING).name());
  EXPECT_EQ("DATETIME", GetTypeInfo(DATETIME).name());
  EXPECT_EQ("DATE", GetTypeInfo(DATE).name());
  EXPECT_EQ("BINARY", GetTypeInfo(BINARY).name());
  EXPECT_EQ("DATA_TYPE", GetTypeInfo(DATA_TYPE).name());
}

TEST_F(TypeInfoTest, PropertiesShouldBeCorrect) {
  TestProperties<INT32>(false, true, true, false);
  TestProperties<UINT32>(false, true, true, false);
  TestProperties<UINT64>(false, true, true, false);
  TestProperties<UINT64>(false, true, true, false);
  TestProperties<FLOAT>(false, true, false, true);
  TestProperties<DOUBLE>(false, true, false, true);
  TestProperties<BOOL>(false, false, false, false);
  TestProperties<STRING>(true, false, false, false);
  TestProperties<DATETIME>(false, false, false, false);
  TestProperties<DATE>(false, false, false, false);
  TestProperties<BINARY>(true, false, false, false);
  TestProperties<DATA_TYPE>(false, false, false, false);
}

}  // namespace supersonic
