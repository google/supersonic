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

#ifndef SUPERSONIC_CURSOR_INFRASTRUCTURE_VALUE_REF_H_
#define SUPERSONIC_CURSOR_INFRASTRUCTURE_VALUE_REF_H_

#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/macros.h"

namespace supersonic {

enum { UNDEF = -1 };

// Global constant that represents a NULL value for each type.
const struct __Null {} __ = {};

template<int type>
class ValueRef {
 public:
  typedef typename TypeTraits<static_cast<DataType>(type)>::cpp_type cpp_type;

  ValueRef(const cpp_type& value) : value_(&value) {}                  // NOLINT
  ValueRef(const __Null null) : value_(NULL) {}                        // NOLINT
  const bool is_null() const { return value_ == NULL; }
  const cpp_type& value() const {
    DCHECK(!is_null());
    return *value_;
  }
 private:
  const cpp_type* value_;
};

template<>
class ValueRef<STRING> {
 public:
  ValueRef(const StringPiece& value)                                   // NOLINT
      : value_(value),
        is_null_(false) {}
  ValueRef(const char* value)                                          // NOLINT
      : value_(value),
        is_null_(value == NULL) {}
  ValueRef(const string& value)                                        // NOLINT
      : value_(value),
        is_null_(false) {}
  ValueRef(const __Null null)                                          // NOLINT
      : value_(), is_null_(true) {}

  const bool is_null() const { return is_null_; }
  const StringPiece& value() const {
    DCHECK(!is_null());
    return value_;
  }
 private:
  StringPiece value_;
  bool is_null_;
};

template<>
class ValueRef<BINARY> : public ValueRef<STRING> {
 public:
  ValueRef(const StringPiece& value) : ValueRef<STRING>(value) {}      // NOLINT
  ValueRef(const char* value) : ValueRef<STRING>(value) {}             // NOLINT
  ValueRef(const string& value) : ValueRef<STRING>(value)  {}          // NOLINT
  ValueRef(const __Null null) : ValueRef<STRING>(null) {}              // NOLINT
};

template<> struct ValueRef<UNDEF> {};

}  // namespace supersonic

#endif  // SUPERSONIC_CURSOR_INFRASTRUCTURE_VALUE_REF_H_
