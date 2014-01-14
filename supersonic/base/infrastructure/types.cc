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

#include <unordered_map>

#include <cstddef>
#include <utility>
#include "supersonic/utils/std_namespace.h"

#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/bits.h"
#include "supersonic/utils/linked_ptr.h"

namespace supersonic {

// Constructor for the TypeInfo, bringing all the materializations to life.
template<typename Type>
TypeInfo::TypeInfo(Type ignored)
    : type_(Type::type),
      name_(Type::name()),
      size_(Type::size),
      log2_size_(Bits::Log2Floor(Type::size)),
      is_numeric_(Type::is_numeric),
      is_integer_(Type::is_integer),
      is_floating_point_(Type::is_floating_point),
      is_variable_length_(Type::is_variable_length) {
  CHECK_EQ(Bits::Log2Ceiling(Type::size), log2_size_)
      << "Type " << Type::name() << " has size " << Type::size << ", "
      << "which is not 2^k";
}

// Helper class to instantiate TypeInfo singletons.
class TypeInfoResolver {
 public:
  const TypeInfo& GetTypeInfo(DataType type) {
    const TypeInfo* type_info = mapping_[type].get();
    CHECK(type_info != NULL);
    return *type_info;
  }

  static TypeInfoResolver* GetSingleton() {
    static TypeInfoResolver resolver;
    return &resolver;
  }

 private:
  TypeInfoResolver() {
    AddMapping<INT32>();
    AddMapping<INT64>();
    AddMapping<UINT32>();
    AddMapping<UINT64>();
    AddMapping<FLOAT>();
    AddMapping<DOUBLE>();
    AddMapping<BOOL>();
    AddMapping<ENUM>();
    AddMapping<STRING>();
    AddMapping<DATETIME>();
    AddMapping<DATE>();
    AddMapping<BINARY>();
    AddMapping<DATA_TYPE>();
  }
  template<DataType type> void AddMapping() {
    TypeTraits<type> traits;
    mapping_.insert(make_pair(type, make_linked_ptr(new TypeInfo(traits))));
  }

  std::unordered_map<DataType, linked_ptr<const TypeInfo>, std::hash<size_t> >
      mapping_;
  DISALLOW_COPY_AND_ASSIGN(TypeInfoResolver);
};

const TypeInfo& GetTypeInfo(DataType type) {
  return TypeInfoResolver::GetSingleton()->GetTypeInfo(type);
}

}  // namespace supersonic
