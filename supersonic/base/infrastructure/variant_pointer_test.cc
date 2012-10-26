// Copyright 2011 Google Inc.  All Rights Reserved
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

#include "supersonic/base/infrastructure/variant_pointer.h"

#include "gtest/gtest.h"

namespace supersonic {

template<typename PointerType, DataType type>
void TestOffset() {
  typename TypeTraits<type>::cpp_type test[100];
  PointerType p(&test[0]);
  EXPECT_EQ(&test[5], p.offset(5, GetTypeInfo(type)).template as<type>());
  EXPECT_EQ(&test[12], p.offset(20, GetTypeInfo(type)).
                         offset(-8, GetTypeInfo(type)).template as<type>());
}

template<typename PointerType>
void TestOffsetForAllTypes() {
  TestOffset<PointerType, INT32>();
  TestOffset<PointerType, INT64>();
  TestOffset<PointerType, UINT32>();
  TestOffset<PointerType, UINT64>();
  TestOffset<PointerType, FLOAT>();
  TestOffset<PointerType, DOUBLE>();
  TestOffset<PointerType, BOOL>();
  TestOffset<PointerType, STRING>();
  TestOffset<PointerType, BINARY>();
  TestOffset<PointerType, DATE>();
  TestOffset<PointerType, DATETIME>();
  TestOffset<PointerType, DATA_TYPE>();
}

TEST(VariantPointer, Offset) {
  TestOffsetForAllTypes<VariantPointer>();
  TestOffsetForAllTypes<VariantConstPointer>();
}

template<typename PointerType, DataType type>
void TestStaticOffset() {
  typename TypeTraits<type>::cpp_type test[100];
  PointerType p(&test[0]);
  EXPECT_EQ(&test[5], p.template static_offset<5>(GetTypeInfo(type)).
                        template as<type>());
  EXPECT_EQ(&test[12], p.template static_offset<20>(GetTypeInfo(type)).
                         template static_offset<-8>(GetTypeInfo(type)).
                         template as<type>());
}

template<typename PointerType>
void TestStaticOffsetForAllTypes() {
  TestStaticOffset<PointerType, INT32>();
  TestStaticOffset<PointerType, INT64>();
  TestStaticOffset<PointerType, UINT32>();
  TestStaticOffset<PointerType, UINT64>();
  TestStaticOffset<PointerType, FLOAT>();
  TestStaticOffset<PointerType, DOUBLE>();
  TestStaticOffset<PointerType, BOOL>();
  TestStaticOffset<PointerType, STRING>();
  TestStaticOffset<PointerType, BINARY>();
  TestStaticOffset<PointerType, DATE>();
  TestStaticOffset<PointerType, DATETIME>();
  TestStaticOffset<PointerType, DATA_TYPE>();
}

TEST(VariantPointer, StaticOffset) {
  TestStaticOffsetForAllTypes<VariantPointer>();
  TestStaticOffsetForAllTypes<VariantConstPointer>();
}

}  // namespace supersonic
